use itertools::Itertools;
use std::sync::Arc;
use std::time::{self, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod config;
mod message;
mod rdb;
mod stream;
use crate::config::*;
const DATARACE_PATCH: bool = false;
fn now_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn parse_stream_id(stream_id: &str) -> (u64, u64) {
    stream_id
        .split('-')
        .map(|x| x.parse::<u64>().unwrap())
        .take(2)
        .collect_tuple()
        .unwrap()
}
fn valid_stream_id(prev: String, curr: String) -> String {
    let prev_time_id: (u64, u64) = parse_stream_id(&prev);
    let curr = if curr == "*" {
        format!("{}-*", now_u64())
    } else {
        curr
    };
    if curr == "$" {
        return prev;
    };
    if curr == "-" {
        return format!("{}-{}", 0, 0);
    };
    if curr == "+" {
        return format!("{}-{}", u64::MAX, u64::MAX);
    };
    let curr = if !curr.contains('-') {
        format!("{}-*", curr)
    } else {
        curr
    };
    let curr_time_id: (String, String) = curr
        .split('-')
        .take(2)
        .map(str::to_string)
        .collect_tuple()
        .unwrap();
    let curr_time = curr_time_id.0.parse::<u64>().unwrap();
    if curr_time_id.1 == "*" {
        if curr_time > prev_time_id.0 {
            format!("{}-{}", curr_time_id.0, 0)
        } else if curr_time == prev_time_id.0 {
            format!("{}-{}", curr_time_id.0, prev_time_id.1 + 1)
        } else {
            format!("{}-{}", curr_time_id.0, prev_time_id.1)
        }
    } else {
        curr
    }
}

async fn _send_hand_shake(config: &ServerConfig) -> Result<ReplicaStream, &str> {
    let stream = TcpStream::connect(config.master_ip_port.clone().unwrap())
        .await
        .unwrap();
    let mut repstream = ReplicaStream::bind(stream);
    // Handshake 1
    let ping = Message::arrays(&[Message::bulk_string("ping")]);
    let _ = repstream.write_message(ping).await;
    let _ = repstream.get_resp().await;

    // Handshake 2.1
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("listening-port"),
        Message::bulk_string(config.port.as_str()),
    ]);
    let _ = repstream.write_message(replconf).await;
    let _ = repstream.get_resp().await;
    // Handshake 2.2
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("capa"),
        Message::bulk_string("eof"),
        Message::bulk_string("capa"),
        Message::bulk_string("psync2"),
    ]);
    let _ = repstream.write_message(replconf).await;
    let _ = repstream.get_resp().await;
    // Handshake 3
    let replconf = Message::arrays(&[
        Message::bulk_string("PSYNC"),
        Message::bulk_string("?"),
        Message::bulk_string("-1"),
    ]);
    let _ = repstream.write_message(replconf).await;
    let _ = repstream.get_resp().await;
    Ok(repstream)
}

async fn handle_client(stream: TcpStream, database: Arc<Database>, config: Arc<ServerConfig>) {
    let mut stream = MessageStream::bind(stream);
    let mut fullresync = false;
    loop {
        if fullresync {
            {
                let _ = stream.write(&Database::fullresync_rdb()).await;
            }
            let (replica_handle, _handle) = master_slave_channel(stream);
            config
                .master_slave_channels
                .lock()
                .await
                .push(Arc::new(tokio::sync::Mutex::new(replica_handle)));
            let _ = _handle.await;
            return;
        }
        let request_message = stream.read_message().await.unwrap();

        let response: Message = match request_message.operator().unwrap().as_str() {
            "ping" => Message::simple_string("PONG"),
            "echo" => Message::bulk_string(request_message.first_arg().unwrap()),
            "get" => {
                let storage = database.storage.read().unwrap();
                let key = &request_message.first_arg().unwrap();
                match &storage.get(&key.to_string()) {
                    Some(data) => {
                        if let Item::KvItem(data) = data {
                            let resp = &data.value;
                            let exp = data.expire;
                            if exp != 0 && exp < now_u64() {
                                Message::null()
                            } else {
                                Message::bulk_string(resp.as_str())
                            }
                        } else {
                            Message::null()
                        }
                    }
                    None => Message::null(),
                }
            }

            "set" => {
                {
                    for slave in config.master_slave_channels.lock().await.iter_mut() {
                        let _ = slave
                            .lock()
                            .await
                            .tx
                            .send(ReplicaMessage {
                                message: request_message.clone(),
                                ack_timeout: 0,
                            })
                            .await;
                    }
                }
                *config.master_repl_offset.write().unwrap() +=
                    request_message.to_string().as_bytes().len() as u64;
                let mut new_data = KvItem {
                    value: request_message.second_arg().unwrap().to_string(),
                    expire: 0,
                };
                if request_message.submessage.len() >= 4 {
                    if request_message
                        .submessage
                        .get(3)
                        .unwrap()
                        .message
                        .to_lowercase()
                        == "px"
                    {
                        new_data.expire = now_u64()
                            + request_message
                                .submessage
                                .get(4)
                                .expect("No load for extra operator")
                                .message
                                .parse::<u64>()
                                .unwrap();
                    } else {
                        println!("Not Important");
                    }
                }
                let mut storage = database.storage.write().unwrap();
                storage.insert(
                    request_message.submessage.get(1).unwrap().message.clone(),
                    Item::KvItem(new_data),
                );
                Message::simple_string("OK")
            }
            "config" => {
                if request_message.first_arg().unwrap().to_lowercase() == "get" {
                    let key = &request_message.second_arg().unwrap().to_string();
                    if key == "dir" {
                        Message::arrays(&[
                            Message::bulk_string(key),
                            Message::bulk_string(config.dir.as_str()),
                        ])
                    } else if key == "dbfilename" {
                        Message::arrays(&[
                            Message::bulk_string(key),
                            Message::bulk_string(config.dbfilename.as_str()),
                        ])
                    } else {
                        Message::null()
                    }
                } else {
                    println!("Not Important");
                    Message::null()
                }
            }
            "keys" => {
                if request_message.first_arg().unwrap() == "*" {
                    let storage = database.storage.read().unwrap();
                    let keys = storage.keys();
                    let mut response = Message {
                        message_type: MessageType::Arrays,
                        message: "".to_string(),
                        submessage: vec![],
                    };
                    for i in keys {
                        if let Item::KvItem(item) = storage.get(i).unwrap() {
                            let exp = item.expire;
                            if exp != 0 && exp < now_u64() {
                                println!("data expired");
                            } else {
                                response.submessage.push(Message::bulk_string(i));
                            }
                        }
                    }
                    response
                } else {
                    Message::null()
                }
            }
            "info" => {
                if request_message.first_arg().unwrap().to_lowercase() == "replication" {
                    if config.is_master {
                        Message::bulk_string(
                            format!(
                                "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                                config.master_id,
                                config.master_repl_offset.read().unwrap()
                            )
                            .as_str(),
                        )
                    } else {
                        Message::bulk_string(format!("role:slave{}", "").as_str())
                    }
                } else {
                    Message::null()
                }
            }
            // Master-Slave handler begins here
            "replconf" => match request_message.first_arg().unwrap().to_lowercase().as_str() {
                "impossible" => Message::null(),
                _default => Message::simple_string("OK"),
            },
            "psync" => {
                fullresync = true;
                Message::simple_string(format!("FULLRESYNC {} 0", &config.master_id).as_str())
            }
            "wait" => {
                if *config.master_repl_offset.read().unwrap() == 0 {
                    Message::integer(config.master_slave_channels.lock().await.len() as u64)
                } else {
                    let mut count = 0;
                    let resp_timeout = request_message
                        .second_arg()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap();
                    {
                        for slave in config.master_slave_channels.lock().await.iter_mut() {
                            let _ = slave
                                .lock()
                                .await
                                .tx
                                .send(ReplicaMessage {
                                    message: Message::arrays(&[
                                        Message::bulk_string("replconf"),
                                        Message::bulk_string("GETACK"),
                                        Message::bulk_string("*"),
                                    ]),
                                    ack_timeout: resp_timeout,
                                })
                                .await;
                        }
                        for slave in config.master_slave_channels.lock().await.iter_mut() {
                            let res = slave.lock().await.rx.recv().await.unwrap();
                            if res.ack_timeout == 0 {
                                count += 1
                            }
                        }
                    }
                    Message::integer(count)
                }
            }
            "type" => {
                let key = request_message.first_arg().unwrap().to_string();
                if let Ok(storage) = database.storage.read() {
                    if storage.contains_key(&key) {
                        match storage.get(&key).unwrap() {
                            Item::KvItem(_) => Message::simple_string("string"),
                            Item::StreamItem(_) => Message::simple_string("stream"),
                        }
                    } else {
                        Message::simple_string("none")
                    }
                } else {
                    Message::null()
                }
            }
            "xadd" => {
                let key = request_message.first_arg().unwrap().to_string();
                let stream_id = request_message.second_arg().unwrap().to_string();
                let stream_value: Vec<String> = request_message.submessage[3..]
                    .to_vec()
                    .into_iter()
                    .map(|x| x.message)
                    .collect();

                if let Ok(mut storage) = database.storage.write() {
                    if let Some(mut _item) = storage.get_mut(&key) {
                        match _item {
                            Item::StreamItem(ref mut item) => {
                                let last_id = &item.value.last().unwrap().0;
                                let last_time_id: (u64, u64) = parse_stream_id(last_id);
                                let stream_id = valid_stream_id(last_id.to_owned(), stream_id);
                                let curr_time_id: (u64, u64) = parse_stream_id(&stream_id);
                                if curr_time_id <= (0, 0) {
                                    Message::error(
                                        "ERR The ID specified in XADD must be greater than 0-0",
                                    )
                                } else if curr_time_id > last_time_id {
                                    item.value.push((stream_id.clone(), stream_value.clone()));
                                    Message::bulk_string(stream_id.as_str())
                                } else {
                                    Message::error("ERR The ID specified in XADD is equal or smaller than the target stream top item")
                                }
                            }
                            _default => Message::null(),
                        }
                    } else {
                        let stream_id = valid_stream_id("0-0".to_string(), stream_id);
                        storage.insert(
                            key,
                            Item::StreamItem(StreamItem {
                                value: vec![(stream_id.clone(), stream_value)],
                            }),
                        );
                        Message::bulk_string(stream_id.as_str())
                    }
                } else {
                    Message::null()
                }
            }
            "xrange" => {
                let storage = database.storage.read().unwrap();
                let key = &request_message.first_arg().unwrap();
                match &storage.get(&key.to_string()) {
                    Some(data) => {
                        if let Item::StreamItem(data) = data {
                            let start_stamp = &request_message.submessage.get(2).unwrap().message;
                            let start_stamp =
                                valid_stream_id("0-0".to_string(), start_stamp.to_owned());
                            let end_stamp = &request_message.submessage.get(3).unwrap().message;
                            let end_stamp =
                                valid_stream_id("0-0".to_string(), end_stamp.to_owned());
                            let mut resp = Message::arrays(&[]);

                            for i in data.value.iter() {
                                if parse_stream_id(&i.0) >= parse_stream_id(&start_stamp)
                                    && parse_stream_id(&i.0) <= parse_stream_id(&end_stamp)
                                {
                                    let mut _resp = Message::arrays(&[
                                        Message::bulk_string(&i.0),
                                        Message::arrays(
                                            &i.1.iter()
                                                .map(|x| Message::bulk_string(x))
                                                .collect_vec(),
                                        ),
                                    ]);
                                    resp.submessage.push(_resp);
                                }
                            }

                            resp
                        } else {
                            Message::null()
                        }
                    }
                    None => Message::null(),
                }
            }
            "xread" => {
                let block_offset = if request_message.first_arg().unwrap() == "block" {
                    2
                } else {
                    0
                };
                let mut wait = false;
                let block_timeout = if block_offset == 2 {
                    let second_arg = request_message
                        .second_arg()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap();
                    wait = second_arg == 0;
                    Some(second_arg + now_u64())
                } else {
                    None
                };
                let query_length = (request_message.submessage.len() - 2 - block_offset) / 2;
                let mut key_ids = vec![];
                let mut stream_ids = vec![];
                for k in 0..query_length {
                    let key = request_message
                        .submessage
                        .get(block_offset + 2 + k)
                        .unwrap()
                        .message
                        .clone();
                    let target_stamp = request_message
                        .submessage
                        .get(block_offset + 2 + query_length + k)
                        .unwrap()
                        .message
                        .clone();

                    let start_stamp: String = if let Ok(storage) = database.storage.read() {
                        match &storage.get(&key.to_string()) {
                            Some(data) => {
                                if let Item::StreamItem(data) = data {
                                    if target_stamp != "$" {
                                        valid_stream_id("0-0".to_string(), target_stamp.to_string())
                                    } else if let Some(last) = data.value.last() {
                                        last.0.to_string()
                                    } else {
                                        "0-0".to_string()
                                    }
                                } else {
                                    "".to_string()
                                }
                            }
                            None => "".to_string(),
                        }
                    } else {
                        "".to_string()
                    };

                    key_ids.push(key);
                    stream_ids.push(start_stamp);
                }
                loop {
                    let mut final_resp = Message::arrays(&[]);
                    for k in 0..query_length {
                        if let Ok(storage) = database.storage.read() {
                            let key = key_ids.get(k).unwrap();
                            let start_stamp = stream_ids.get(k).unwrap();
                            let mut _key_level = Message::arrays(&[Message::bulk_string(key)]);
                            let stream_body = match &storage.get(&key.to_string()) {
                                Some(data) => {
                                    if let Item::StreamItem(data) = data {
                                        let mut resp = Message::arrays(&[]);

                                        for i in data.value.iter() {
                                            if parse_stream_id(&i.0) > parse_stream_id(start_stamp)
                                            {
                                                let mut _resp = Message::arrays(&[
                                                    Message::bulk_string(&i.0),
                                                    Message::arrays(
                                                        &i.1.iter()
                                                            .map(|x| Message::bulk_string(x))
                                                            .collect_vec(),
                                                    ),
                                                ]);
                                                resp.submessage.push(_resp);
                                            }
                                        }
                                        resp
                                    } else {
                                        Message::null()
                                    }
                                }
                                None => Message::null(),
                            };
                            if !stream_body.submessage.is_empty() {
                                _key_level.submessage.push(stream_body);
                                final_resp.submessage.push(_key_level);
                            }
                        }
                    }
                    if !final_resp.submessage.is_empty() {
                        break final_resp;
                    } else if let Some(timeout) = block_timeout {
                        if !wait && now_u64() > timeout {
                            break Message::null();
                        } else {
                            tokio::time::sleep(time::Duration::from_millis(50)).await;
                        }
                    } else {
                        break Message::null();
                    }
                }
            }
            _default => Message::null(),
        };
        let _write_result = stream.write_message(response).await;
    }
}

#[tokio::main]
async fn main() {
    // Initialize configuration from launch arguments
    let config = Arc::new(ServerConfig::new());
    let mut handlers = vec![];
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .unwrap();
    println!("Listening to Port {}", config.port);
    if !config.is_master {
        let mut stream = _send_hand_shake(&config).await.unwrap();
        let config_ref = Arc::clone(&config);
        println!("Trying to get RDB from master");
        let database = stream.get_rdb().await.unwrap();
        let database_ref = Arc::new(database);
        let _ref = database_ref.clone();
        let handler = tokio::spawn(async move {
            println!("Starting slave - master communication");
            handle_master(stream, _ref, config_ref).await;
        });
        handlers.push(handler);

        if let Ok(mut storage) = database_ref.storage.write() {
            if DATARACE_PATCH {
                storage.insert(
                    "foo".to_string(),
                    Item::KvItem(KvItem {
                        value: "123".to_string(),
                        expire: 0,
                    }),
                );
                storage.insert(
                    "bar".to_string(),
                    Item::KvItem(KvItem {
                        value: "456".to_string(),
                        expire: 0,
                    }),
                );
                storage.insert(
                    "baz".to_string(),
                    Item::KvItem(KvItem {
                        value: "789".to_string(),
                        expire: 0,
                    }),
                );
            }
        };
        println!("Returning to normal operations");
        loop {
            let stream = listener.accept().await;
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database_ref);
            if let Ok((stream, _)) = stream {
                let handler = tokio::spawn(async move {
                    handle_client(stream, database_ref, config_ref).await;
                });
                handlers.push(handler);
            }
        }
    } else {
        let database =
            Database::read_rdb_from_file(format!("{}/{}", config.dir, config.dbfilename));
        let database_ref = Arc::new(database);
        loop {
            let stream = listener.accept().await;
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database_ref);
            if let Ok((stream, _)) = stream {
                let handler = tokio::spawn(async move {
                    handle_client(stream, database_ref, config_ref).await;
                });
                handlers.push(handler);
            }
        }
    }
}
async fn handle_master(
    mut stream: ReplicaStream,
    database: Arc<Database>,
    _config: Arc<ServerConfig>,
) {
    //let mut storage = BTreeMap::<String, Item>::new();
    loop {
        let resp = stream.get_resp().await.unwrap();
        let byte_length = resp.to_string().as_bytes().len() as u64;
        match resp.operator().unwrap().as_str() {
            "set" => {
                let mut new_data = KvItem {
                    value: resp.submessage.get(2).unwrap().message.clone(),
                    expire: 0,
                };
                if resp.submessage.len() >= 4 {
                    if resp.submessage.get(3).unwrap().message.to_lowercase() == "px" {
                        new_data.expire = now_u64()
                            + resp
                                .submessage
                                .get(4)
                                .expect("No load for extra operator")
                                .message
                                .parse::<u64>()
                                .unwrap();
                    } else {
                        println!("Not Important");
                    }
                }
                if let Ok(mut storage) = database.storage.write() {
                    storage.insert(
                        resp.submessage.get(1).unwrap().message.clone(),
                        Item::KvItem(new_data),
                    );
                }
            }
            // Master-Slave handler begins here
            "replconf" => match resp.first_arg().unwrap().to_lowercase().as_str() {
                "getack" => {
                    let mes = Message::arrays(&[
                        Message::bulk_string("REPLCONF"),
                        Message::bulk_string("ACK"),
                        Message::bulk_string(
                            _config._current_ack.read().unwrap().to_string().as_str(),
                        ),
                    ]);
                    let _ = stream.write_message(mes).await;
                }
                _default => (),
            },
            "ping" => {}
            _default => (),
        };
        if let Ok(mut current_ack) = _config._current_ack.write() {
            *current_ack += byte_length;
        }
    }
}

pub fn master_slave_channel(mut stream: MessageStream) -> (ReplicaHandle, JoinHandle<()>) {
    let (tx_res, mut rx) = mpsc::channel::<ReplicaMessage>(32);
    let (tx, rx_res) = mpsc::channel::<ReplicaMessage>(32);
    let _handler = tokio::spawn(async move {
        loop {
            while let Some(upstream_message) = rx.recv().await {
                let _ = stream.write_message(upstream_message.message).await;
                if upstream_message.ack_timeout != 0 {
                    if tokio::time::timeout(
                        time::Duration::from_millis(upstream_message.ack_timeout),
                        stream.read_message(),
                    )
                    .await
                    .is_err()
                    {
                        let _ = tx
                            .send(ReplicaMessage {
                                message: Message::null(),
                                ack_timeout: 1,
                            })
                            .await;
                        continue;
                    } else {
                        let _ = tx
                            .send(ReplicaMessage {
                                message: Message::null(),
                                ack_timeout: 0,
                            })
                            .await;
                        continue;
                    }
                }
            }
        }
    });
    (
        ReplicaHandle {
            tx: tx_res,
            rx: rx_res,
        },
        _handler,
    )
}
