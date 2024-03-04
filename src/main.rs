use core::time;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;

mod message;
mod rdb;
pub use crate::message::*;
pub use crate::rdb::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "example")]
struct Opt {
    #[structopt(long, parse(from_os_str), default_value = "/tmp/redis-files")]
    _dir: PathBuf,

    #[structopt(long, parse(from_os_str), default_value = "dump.rdb")]
    _dbfilename: PathBuf,

    #[structopt(long, default_value = "6379")]
    _port: u32,

    #[structopt(long, parse(from_str))]
    _replicaof: Option<Vec<String>>,
}

async fn _send_hand_shake(config: &ServerConfig) -> Result<ReplicaStream, &str> {
    let stream = TcpStream::connect(config.master_ip_port.clone().unwrap())
        .await
        .unwrap();
    let mut repstream = ReplicaStream::bind(stream);
    // Handshake 1
    let ping = Message::arrays(&[Message::bulk_string("ping")]);
    let _ = repstream
        .stream
        .lock()
        .await
        .write_all(ping.to_string().as_bytes())
        .await;
    while repstream.get_resp().await.is_none() {}

    // Handshake 2.1
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("listening-port"),
        Message::bulk_string(config.port.as_str()),
    ]);
    let _ = repstream
        .stream
        .lock()
        .await
        .write_all(replconf.to_string().as_bytes())
        .await;
    while repstream.get_resp().await.is_none() {}
    // Handshake 2.2
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("capa"),
        Message::bulk_string("eof"),
        Message::bulk_string("capa"),
        Message::bulk_string("psync2"),
    ]);
    let _ = repstream
        .stream
        .lock()
        .await
        .write_all(replconf.to_string().as_bytes())
        .await;
    while repstream.get_resp().await.is_none() {}
    // Handshake 3
    let replconf = Message::arrays(&[
        Message::bulk_string("PSYNC"),
        Message::bulk_string("?"),
        Message::bulk_string("-1"),
    ]);
    let _ = repstream
        .stream
        .lock()
        .await
        .write_all(replconf.to_string().as_bytes())
        .await;
    while repstream.get_resp().await.is_none() {}
    Ok(repstream)
}

async fn handle_client(mut stream: TcpStream, database: Arc<RDB>, config: Arc<ServerConfig>) {
    let mut read_buf = [0; 256];
    //let mut storage = BTreeMap::<String, Item>::new();
    let mut fullresync = false;
    loop {
        if fullresync {
            {
                let _ = stream.write_all(&RDB::fullresync_rdb()).await;
                let _ = stream.flush().await;
            }
            let (replica_handle, _handle) = master_slave_channel(stream);
            config
                .master_slave_channels
                .lock()
                .await
                .push(Arc::new(tokio::sync::Mutex::new(replica_handle)));
            let _ = _handle.await;
            return;

            // fullresync = false;
            // continue;
        }
        let read_result = stream.read(&mut read_buf).await;
        println!("Read new request");
        if let Ok(length) = read_result {
            let request_message =
                Message::from_str(&String::from_utf8(read_buf[..length].to_vec()).unwrap())
                    .expect("Should be OK");
            let response: Message = match request_message
                .submessage
                .first()
                .expect("Invalid Operator")
                .message
                .to_lowercase()
                .as_str()
            {
                "ping" => Message::simple_string("PONG"),
                "echo" => Message::bulk_string(
                    request_message
                        .submessage
                        .get(1)
                        .expect("No Load on echo")
                        .message
                        .as_str(),
                ),
                "get" => {
                    let storage = database._storage.read().unwrap();
                    let key = &request_message
                        .submessage
                        .get(1)
                        .expect("No get load")
                        .message;
                    match &storage.get(key) {
                        Some(data) => {
                            let resp = &data.value;
                            let exp = data.expire;
                            if exp != 0
                                && exp
                                    < SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64
                            {
                                Message::null()
                            } else {
                                Message::bulk_string(resp.as_str())
                            }
                        }
                        None => Message::null(),
                    }
                }

                "set" => {
                    println!("get new set");
                    {
                        for slave in config.master_slave_channels.lock().await.iter_mut() {
                            let _ = slave
                                .lock()
                                .await
                                .tx
                                .send(ReplicaMessage {
                                    message: request_message.clone(),
                                    require_ack: false,
                                })
                                .await;
                        }
                        println!("recasting to downstream {:?}", request_message)
                    }
                    *config.master_repl_offset.write().unwrap() += length as u64;
                    let mut new_data = Item {
                        value: request_message
                            .submessage
                            .get(2)
                            .expect("No set load")
                            .message
                            .clone(),
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
                            new_data.expire = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64
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
                    let mut storage = database._storage.write().unwrap();
                    storage.insert(
                        request_message.submessage.get(1).unwrap().message.clone(),
                        new_data,
                    );
                    println!("return to client");
                    Message::simple_string("OK")
                }
                "config" => {
                    if request_message
                        .submessage
                        .get(1)
                        .unwrap()
                        .message
                        .to_lowercase()
                        == "get"
                    {
                        let key = &request_message.submessage.get(2).unwrap().message;
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
                    if request_message.submessage.get(1).unwrap().message == "*" {
                        let storage = database._storage.read().unwrap();
                        let keys = storage.keys();
                        let mut response = Message {
                            message_type: MessageType::Arrays,
                            message: "".to_string(),
                            submessage: vec![],
                        };
                        for i in keys {
                            let exp = storage.get(i).unwrap().expire;
                            if exp != 0
                                && exp
                                    < SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64
                            {
                                println!("data expired");
                            } else {
                                response.submessage.push(Message::bulk_string(i));
                            }
                        }
                        response
                    } else {
                        Message::null()
                    }
                }
                "info" => {
                    if request_message
                        .submessage
                        .get(1)
                        .unwrap()
                        .message
                        .to_lowercase()
                        == "replication"
                    {
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
                "replconf" => {
                    match request_message
                        .submessage
                        .get(1)
                        .unwrap()
                        .message
                        .to_lowercase()
                        .as_str()
                    {
                        "impossible" => Message::null(),
                        _default => Message::simple_string("OK"),
                    }
                }
                "psync" => {
                    if request_message.submessage.get(1).unwrap().message == "?" {
                        fullresync = true;
                        Message::simple_string(
                            format!("FULLRESYNC {} 0", &config.master_id).as_str(),
                        )
                    } else {
                        Message::simple_string("OK")
                    }
                }
                "wait" => {
                    if *config.master_repl_offset.read().unwrap() == 0 {
                        Message::integer(config.master_slave_channels.lock().await.len() as u64)
                    } else {
                        let needed_num: usize = request_message
                            .submessage
                            .get(1)
                            .unwrap()
                            .message
                            .parse()
                            .unwrap();
                        println!("Needed Ack :{}", needed_num);
                        let mut count = 0;
                        let resp_timeout = request_message
                            .submessage
                            .get(2)
                            .unwrap()
                            .message
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
                                        require_ack: true,
                                    })
                                    .await;
                            }
                            let (tx, mut rx) = mpsc::channel::<bool>(15);
                            for _slave in config.master_slave_channels.lock().await.iter_mut() {
                                let slave = _slave.clone();

                                let tx = tx.clone();
                                let _ = tokio::spawn(async move {
                                    if let Err(_) = timeout(
                                        time::Duration::from_millis(resp_timeout),
                                        slave.lock().await.rx.recv(),
                                    )
                                    .await
                                    {
                                        let _ = tx.send(false).await;
                                    } else {
                                        let _ = tx.send(true).await;
                                    }
                                });
                            }
                            for _ in 0..config.master_slave_channels.lock().await.len() {
                                if rx.recv().await.unwrap() {
                                    count += 1
                                }
                            }
                            Message::integer(count)
                        }
                    }
                }
                "type" => {
                    let key = request_message.submessage.get(1).unwrap().message.clone();
                    if let Ok(storage) = database._storage.read() {
                        if storage.contains_key(&key) {
                            Message::simple_string("string")
                        } else {
                            Message::simple_string("none")
                        }
                    } else {
                        Message::null()
                    }
                }
                _default => Message::null(),
            };
            let _write_result = stream
                .write_all(response.to_string().as_bytes())
                .await
                .unwrap();
        };
    }
}

pub struct ServerConfig {
    port: String,
    dir: String,
    dbfilename: String,
    is_master: bool,
    master_ip_port: Option<String>,
    master_id: String,
    master_repl_offset: RwLock<u64>,
    master_slave_channels: tokio::sync::Mutex<Vec<Arc<tokio::sync::Mutex<ReplicaHandle>>>>,
    _current_ack: RwLock<u64>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self::new()
    }
}
impl ServerConfig {
    pub fn new() -> ServerConfig {
        // Parse and Set configuration from launch arguments
        let opt = Opt::from_args();

        let config = ServerConfig {
            port: opt._port.to_string(),
            dir: opt._dir.to_string_lossy().to_string(),
            dbfilename: opt._dbfilename.to_string_lossy().to_string(),
            is_master: opt._replicaof.is_none(),
            master_ip_port: opt
                ._replicaof
                .as_ref()
                .map(|master| format!("{}:{}", master.first().unwrap(), master.get(1).unwrap())),
            master_id: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            master_repl_offset: RwLock::new(0),
            master_slave_channels: tokio::sync::Mutex::new(vec![]),
            _current_ack: RwLock::new(0),
        };

        config
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

        if let Ok(mut storage) = database_ref._storage.write() {
            storage.insert(
                "foo".to_string(),
                Item {
                    value: "123".to_string(),
                    expire: 0,
                },
            );
            storage.insert(
                "bar".to_string(),
                Item {
                    value: "456".to_string(),
                    expire: 0,
                },
            );
            storage.insert(
                "baz".to_string(),
                Item {
                    value: "789".to_string(),
                    expire: 0,
                },
            );
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
        let database = RDB::read_rdb_from_file(format!("{}/{}", config.dir, config.dbfilename));
        let database_ref = Arc::new(database);
        loop {
            let stream = listener.accept().await;
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database_ref);
            if let Ok((stream, _)) = stream {
                println!("getting new incoming client");
                let handler = tokio::spawn(async move {
                    handle_client(stream, database_ref, config_ref).await;
                });
                handlers.push(handler);
            }
        }
    }

    //println!("database length: {}", _database._storage.len());
}
async fn handle_master(mut stream: ReplicaStream, database: Arc<RDB>, _config: Arc<ServerConfig>) {
    //let mut storage = BTreeMap::<String, Item>::new();
    loop {
        let resp = stream.get_resp().await.unwrap();
        println!("{:?}", resp.submessage);
        let byte_length = resp.to_string().as_bytes().len() as u64;
        match resp
            .submessage
            .first()
            .unwrap()
            .message
            .to_lowercase()
            .as_str()
        {
            "set" => {
                let mut new_data = Item {
                    value: resp.submessage.get(2).unwrap().message.clone(),
                    expire: 0,
                };
                if resp.submessage.len() >= 4 {
                    if resp.submessage.get(3).unwrap().message.to_lowercase() == "px" {
                        new_data.expire = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
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
                if let Ok(mut storage) = database._storage.write() {
                    storage.insert(resp.submessage.get(1).unwrap().message.clone(), new_data);
                }
            }
            // Master-Slave handler begins here
            "replconf" => {
                match resp
                    .submessage
                    .get(1)
                    .unwrap()
                    .message
                    .to_lowercase()
                    .as_str()
                {
                    "getack" => {
                        let mes = Message::arrays(&[
                            Message::bulk_string("REPLCONF"),
                            Message::bulk_string("ACK"),
                            Message::bulk_string(
                                _config._current_ack.read().unwrap().to_string().as_str(),
                            ),
                        ]);
                        let _ = stream
                            .stream
                            .lock()
                            .await
                            .write_all(mes.to_string().as_bytes())
                            .await;
                    }
                    _default => (),
                }
            }
            "ping" => {}
            _default => (),
        };
        if let Ok(mut current_ack) = _config._current_ack.write() {
            *current_ack += byte_length;
        }
    }
}

pub fn master_slave_channel(mut stream: TcpStream) -> (ReplicaHandle, JoinHandle<()>) {
    let (tx_res, mut rx) = mpsc::channel::<ReplicaMessage>(32);
    let (tx, rx_res) = mpsc::channel::<ReplicaMessage>(32);
    let _handler = tokio::spawn(async move {
        loop {
            while let Some(upstream_message) = rx.recv().await {
                let _ = stream
                    .write_all(upstream_message.message.to_string().as_bytes())
                    .await;
                let _ = stream.flush().await;
                println!("recasting upstream message");
                if upstream_message.require_ack {
                    let mut read_buf = [0; 256];
                    let length = stream.read(&mut read_buf).await.unwrap();
                    if length == 0 {
                        let _ = tx
                            .send(ReplicaMessage {
                                message: Message::null(),
                                require_ack: false,
                            })
                            .await;
                    } else {
                        let _ = tx
                            .send(ReplicaMessage {
                                message: Message::null(),
                                require_ack: true,
                            })
                            .await;
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

pub struct ReplicaMessage {
    message: Message,
    require_ack: bool,
}

pub struct ReplicaHandle {
    tx: mpsc::Sender<ReplicaMessage>,
    rx: mpsc::Receiver<ReplicaMessage>,
}
pub struct ReplicaStream {
    // A wrapper of TcpStream for handling RDB and resp format difference
    pub stream: Arc<Mutex<TcpStream>>,
    pub cache: VecDeque<StreamToken>,
}
impl ReplicaStream {
    pub fn bind(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
            cache: vec![].into(),
        }
    }

    pub async fn get_rdb(&mut self) -> Option<RDB> {
        if self.cache.is_empty() && !self.fetch_stream().await {
            None
        } else {
            match self.cache.pop_front().unwrap() {
                StreamToken::Rdb(rdb) => Some(rdb),
                _default => None,
            }
        }
    }
    pub async fn get_resp(&mut self) -> Option<Message> {
        if self.cache.is_empty() && !self.fetch_stream().await {
            None
        } else {
            match self.cache.pop_front().unwrap() {
                StreamToken::Resp(resp) => Some(resp),
                _default => None,
            }
        }
    }
    async fn fetch_stream(&mut self) -> bool {
        let mut read_buf = [0; 256];
        match self.stream.lock().await.read(&mut read_buf).await {
            Ok(length) => {
                let mut probe = 0;
                let data = &read_buf[..length];
                while probe < data.len() {
                    let token = match data[probe] {
                        b'$' => StreamToken::Rdb(Self::read_rdb(data, &mut probe)),
                        b'+' => StreamToken::Resp(Self::read_simple(data, &mut probe)),
                        _default => StreamToken::Resp(Self::read_array(data, &mut probe)),
                    };
                    self.cache.push_back(token);
                }
                true
            }
            Err(_) => false,
        }
    }
    pub fn read_simple(data: &[u8], index: &mut usize) -> Message {
        let mut probe = *index;
        while probe + 1 < data.len() && !(data[probe] == b'\r' && data[probe + 1] == b'\n') {
            probe += 1;
        }
        let parsed = Message::simple_string(
            String::from_utf8(data[*index + 1..probe].to_vec())
                .unwrap()
                .as_str(),
        );
        *index = probe + 2;
        parsed
    }
    pub fn read_bulk(data: &[u8], index: &mut usize) -> Message {
        let mut probe = *index;
        while probe + 1 < data.len() && !(data[probe] == b'\r' && data[probe + 1] == b'\n') {
            probe += 1;
        }
        let length: usize = String::from_utf8(data[*index + 1..probe].to_vec())
            .unwrap()
            .parse()
            .unwrap();
        let message = Message::bulk_string(
            String::from_utf8(data[probe + 2..probe + 2 + length].to_vec())
                .unwrap()
                .as_str(),
        );
        *index = probe + 4 + length;
        message
    }

    pub fn read_rdb(data: &[u8], index: &mut usize) -> RDB {
        let mut probe = *index;
        while probe + 1 < data.len() && !(data[probe] == b'\r' && data[probe + 1] == b'\n') {
            probe += 1;
        }
        let length: usize = String::from_utf8(data[*index + 1..probe].to_vec())
            .unwrap()
            .parse()
            .unwrap();
        let rdb = RDB::new();
        rdb.read_rdb(&data[probe + 2..probe + 2 + length]);
        *index = probe + 2 + length;
        rdb
    }
    pub fn read_array(data: &[u8], index: &mut usize) -> Message {
        let mut probe = *index;
        while probe + 1 < data.len() && !(data[probe] == b'\r' && data[probe + 1] == b'\n') {
            probe += 1;
        }
        let length: usize = String::from_utf8(data[*index + 1..probe].to_vec())
            .unwrap()
            .parse()
            .unwrap();
        *index = probe + 2;
        let mut message = Message::arrays(&[]);
        for _ in 0..length {
            let mess = match data[*index] {
                b'$' => Self::read_bulk(data, index),
                b'+' => Self::read_simple(data, index),
                _default => Self::read_array(data, index),
            };
            message.submessage.push(mess);
        }
        message
    }
}

pub enum StreamToken {
    Rdb(RDB),
    Resp(Message),
}

#[cfg(test)]
mod test {
    use crate::{Message, ReplicaStream};

    #[test]
    fn test_read_simple() {
        let mes = Message::simple_string("Hello");
        let mut index = 0;
        assert_eq!(
            mes,
            ReplicaStream::read_simple(&mes.to_string().as_bytes(), &mut index)
        )
    }
}
