use rand::{distributions::Alphanumeric, Rng};
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;

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

fn _send_hand_shake(config: &ServerConfig) -> Result<TcpStream, &str> {
    let mut stream = TcpStream::connect(config._master_ip_port.clone().unwrap()).unwrap();
    let mut read_buf: [u8; 256] = [0; 256];
    // Handshake 1
    let ping = Message::arrays(&[Message::bulk_string("ping")]);
    stream.write_all(ping.to_string().as_bytes()).unwrap();
    let _read_result = stream.read(&mut read_buf);
    // Handshake 2.1
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("listening-port"),
        Message::bulk_string(config._port.as_str()),
    ]);
    stream.write_all(replconf.to_string().as_bytes()).unwrap();
    stream.flush().unwrap();
    let _read_result = stream.read(&mut read_buf);
    stream.flush().unwrap();
    // Handshake 2.2
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("capa"),
        Message::bulk_string("eof"),
        Message::bulk_string("capa"),
        Message::bulk_string("psync2"),
    ]);
    stream.write_all(replconf.to_string().as_bytes()).unwrap();
    stream.flush().unwrap();
    let _read_result = stream.read(&mut read_buf);
    stream.flush().unwrap();
    // Handshake 3
    let replconf = Message::arrays(&[
        Message::bulk_string("PSYNC"),
        Message::bulk_string("?"),
        Message::bulk_string("-1"),
    ]);
    stream.write_all(replconf.to_string().as_bytes()).unwrap();
    stream.flush().unwrap();
    let _read_result = stream.read(&mut read_buf).unwrap();
    stream.flush().unwrap();

    Ok(stream)
}

fn handle_client(mut stream: TcpStream, database: Arc<RDB>, config: Arc<ServerConfig>) {
    let mut read_buf: [u8; 256];
    //let mut storage = BTreeMap::<String, Item>::new();
    let mut fullresync = false;
    loop {
        read_buf = [0; 256];
        if fullresync {
            stream.write_all(&RDB::fullresync_rdb()).unwrap();
            stream.flush().unwrap();
            config._slave_list.write().unwrap().push(stream);
            return;

            // fullresync = false;
            // continue;
        }
        let read_result = stream.read(&mut read_buf);
        if let Ok(length) = read_result {
            if length == 0 {
                continue;
            }
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
                                Message::null_blk_string()
                            } else {
                                Message::bulk_string(resp.as_str())
                            }
                        }
                        None => Message::null_blk_string(),
                    }
                }

                "set" => {
                    {
                        for mut slave in config._slave_list.write().unwrap().iter() {
                            slave.write_all(&read_buf[..length]).unwrap();
                            slave.flush().unwrap();
                        }
                    }
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
                                Message::bulk_string(config._dir.as_str()),
                            ])
                        } else if key == "dbfilename" {
                            Message::arrays(&[
                                Message::bulk_string(key),
                                Message::bulk_string(config._dbfilename.as_str()),
                            ])
                        } else {
                            Message::null_blk_string()
                        }
                    } else {
                        println!("Not Important");
                        Message::null_blk_string()
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
                        Message::null_blk_string()
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
                        if config._master {
                            Message::bulk_string(
                                format!(
                                    "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                                    config._master_id, config._master_repl_offset
                                )
                                .as_str(),
                            )
                        } else {
                            Message::bulk_string(format!("role:slave{}", "").as_str())
                        }
                    } else {
                        Message::null_blk_string()
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
                        "impossible" => Message::null_blk_string(),
                        _default => Message::simple_string("OK"),
                    }
                }
                "psync" => {
                    if request_message.submessage.get(1).unwrap().message == "?" {
                        fullresync = true;
                        Message::simple_string(
                            format!("FULLRESYNC {} 0", &config._master_id).as_str(),
                        )
                    } else {
                        Message::simple_string("OK")
                    }
                }
                _default => Message::null_blk_string(),
            };
            let _write_result = stream.write_all(response.to_string().as_bytes());
        };
        stream.flush().unwrap();
    }
}

pub struct ServerConfig {
    _port: String,
    _dir: String,
    _dbfilename: String,
    _master: bool,
    _master_ip_port: Option<String>,
    _master_id: String,
    _master_repl_offset: u64,
    _slave_list: RwLock<Vec<TcpStream>>,
    _write_op_queue: RwLock<Vec<Message>>,
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
            _port: opt._port.to_string(),
            _dir: opt._dir.to_string_lossy().to_string(),
            _dbfilename: opt._dbfilename.to_string_lossy().to_string(),
            _master: opt._replicaof.is_none(),
            _master_ip_port: opt
                ._replicaof
                .as_ref()
                .map(|master| format!("{}:{}", master.first().unwrap(), master.get(1).unwrap())),
            _master_id: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            _master_repl_offset: 0,
            _slave_list: RwLock::new(vec![]),
            _write_op_queue: RwLock::new(vec![]),
        };

        config
    }
}

fn main() {
    // Initialize configuration from launch arguments
    let config = Arc::new(ServerConfig::new());
    let mut thread_handles = vec![];
    let _database = RDB::new();
    let database = Arc::new(RDB::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config._port)).unwrap();
    println!("Listening to Port {}", config._port);
    if !config._master {
        if let Ok(mut _stream) = _send_hand_shake(&config) {
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database);

            let _handler = thread::spawn(move || {
                println!("Trying to get RDB from master");
                database_ref.load_rdb_from_stream(&mut _stream);
                println!("Starting slave - master communication");
                handle_master(_stream, database_ref, config_ref);
            });
            thread_handles.push(_handler);
            println!("Returning to normal operations");
        } else {
            database.load_rdb_from_file(format!("{}/{}", config._dir, config._dbfilename));
        }
    } else {
        database.load_rdb_from_file(format!("{}/{}", config._dir, config._dbfilename));
    }

    //println!("database length: {}", _database._storage.len());

    for stream in listener.incoming() {
        let config_ref = Arc::clone(&config);
        let database_ref = Arc::clone(&database);
        if let Ok(stream) = stream {
            let handler = thread::spawn(move || {
                handle_client(stream, database_ref, config_ref);
            });
            thread_handles.push(handler);
        }
    }

    for handler in thread_handles {
        let _ = handler.join();
    }
}
fn handle_master(mut stream: TcpStream, database: Arc<RDB>, _config: Arc<ServerConfig>) {
    let mut read_buf: [u8; 256];
    //let mut storage = BTreeMap::<String, Item>::new();
    if let Ok(mut storage) = database._storage.write() {
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
    }
    loop {
        read_buf = [0; 256];
        let read_result = stream.read(&mut read_buf);
        if let Ok(length) = read_result {
            if length == 0 || length > 50 {
                continue;
            }
            let request_message =
                Message::from_str(&String::from_utf8(read_buf[..length].to_vec()).unwrap())
                    .expect("Should be OK");
            let _response: Message = match request_message
                .submessage
                .first()
                .expect("Invalid Operator")
                .message
                .to_lowercase()
                .as_str()
            {
                "set" => {
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
                    if let Ok(mut storage) = database._storage.write() {
                        storage.insert(
                            request_message.submessage.get(1).unwrap().message.clone(),
                            new_data,
                        );
                    }
                    Message::null_blk_string()
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
                        "impossible" => Message::null_blk_string(),
                        _default => Message::simple_string("OK"),
                    }
                }
                _default => Message::null_blk_string(),
            };
            //let _write_result = stream.write_all(response.to_string().as_bytes());
        };
        stream.flush().unwrap();
    }
}
