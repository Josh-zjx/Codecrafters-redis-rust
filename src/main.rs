use rand::{distributions::Alphanumeric, Rng};
use std::collections::VecDeque;
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

fn _send_hand_shake(config: &ServerConfig) -> Result<ReplicaStream, &str> {
    let stream = TcpStream::connect(config._master_ip_port.clone().unwrap()).unwrap();
    let mut repstream = ReplicaStream::bind(stream);
    // Handshake 1
    let ping = Message::arrays(&[Message::bulk_string("ping")]);
    repstream
        ._stream
        .write_all(ping.to_string().as_bytes())
        .unwrap();
    while repstream.get_resp().is_none() {}

    // Handshake 2.1
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("listening-port"),
        Message::bulk_string(config._port.as_str()),
    ]);
    repstream
        ._stream
        .write_all(replconf.to_string().as_bytes())
        .unwrap();
    while repstream.get_resp().is_none() {}
    // Handshake 2.2
    let replconf = Message::arrays(&[
        Message::bulk_string("REPLCONF"),
        Message::bulk_string("capa"),
        Message::bulk_string("eof"),
        Message::bulk_string("capa"),
        Message::bulk_string("psync2"),
    ]);
    repstream
        ._stream
        .write_all(replconf.to_string().as_bytes())
        .unwrap();
    while repstream.get_resp().is_none() {}
    // Handshake 3
    let replconf = Message::arrays(&[
        Message::bulk_string("PSYNC"),
        Message::bulk_string("?"),
        Message::bulk_string("-1"),
    ]);
    repstream
        ._stream
        .write_all(replconf.to_string().as_bytes())
        .unwrap();
    while repstream.get_resp().is_none() {}
    Ok(repstream)
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
            config
                ._slave_list
                .write()
                .unwrap()
                .push(ReplicaStream::bind(stream));
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
                        for slave in config._slave_list.write().unwrap().iter_mut() {
                            slave._stream.write_all(&read_buf[..length]).unwrap();
                        }
                    }
                    *config._master_repl_offset.write().unwrap() += length as u64;
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
                                    config._master_id,
                                    config._master_repl_offset.read().unwrap()
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
                "wait" => {
                    let mut ans = Message::integer(config._slave_list.read().unwrap().len() as u64);
                    if *config._master_repl_offset.read().unwrap() == 0 {
                        ans
                    } else {
                        let needed_num: usize = request_message
                            .submessage
                            .get(1)
                            .unwrap()
                            .message
                            .parse()
                            .unwrap();
                        let mut count = 0;
                        let due = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                            + request_message
                                .submessage
                                .get(2)
                                .expect("No load for extra operator")
                                .message
                                .parse::<u64>()
                                .unwrap();

                        for slave in config._slave_list.write().unwrap().iter_mut() {
                            slave
                                ._stream
                                .write_all(
                                    Message::arrays(&[
                                        Message::bulk_string("replconf"),
                                        Message::bulk_string("GETACK"),
                                        Message::bulk_string("*"),
                                    ])
                                    .to_string()
                                    .as_bytes(),
                                )
                                .unwrap();
                            let mes = slave.get_resp().unwrap();
                            if mes
                                .submessage
                                .get(2)
                                .unwrap()
                                .message
                                .parse::<u64>()
                                .unwrap()
                                >= *config._master_repl_offset.read().unwrap()
                            {
                                count += 1;
                            }
                            if count >= needed_num
                                || SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64
                                    > due
                            {
                                ans = Message::integer(count as u64);
                                break;
                            }
                        }
                        ans
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
    _master_repl_offset: RwLock<u64>,
    _slave_list: RwLock<Vec<ReplicaStream>>,
    _write_op_queue: RwLock<Vec<Message>>,
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
            _master_repl_offset: RwLock::new(0),
            _slave_list: RwLock::new(vec![]),
            _write_op_queue: RwLock::new(vec![]),
            _current_ack: RwLock::new(0),
        };

        config
    }
}

fn main() {
    // Initialize configuration from launch arguments
    let config = Arc::new(ServerConfig::new());
    let mut thread_handles = vec![];
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config._port)).unwrap();
    println!("Listening to Port {}", config._port);
    if !config._master {
        let mut stream = _send_hand_shake(&config).unwrap();
        let config_ref = Arc::clone(&config);
        println!("Trying to get RDB from master");
        let database = stream.get_rdb().unwrap();
        let database_ref = Arc::new(database);
        let _ref = database_ref.clone();
        let _handler = thread::spawn(move || {
            println!("Starting slave - master communication");
            handle_master(stream, _ref, config_ref);
        });

        thread_handles.push(_handler);
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
        for stream in listener.incoming() {
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database_ref);
            if let Ok(stream) = stream {
                let handler = thread::spawn(move || {
                    handle_client(stream, database_ref, config_ref);
                });
                thread_handles.push(handler);
            }
        }
    } else {
        let database = RDB::read_rdb_from_file(format!("{}/{}", config._dir, config._dbfilename));
        let database_ref = Arc::new(database);
        for stream in listener.incoming() {
            let config_ref = Arc::clone(&config);
            let database_ref = Arc::clone(&database_ref);
            if let Ok(stream) = stream {
                let handler = thread::spawn(move || {
                    handle_client(stream, database_ref, config_ref);
                });
                thread_handles.push(handler);
            }
        }
    }

    //println!("database length: {}", _database._storage.len());

    for handler in thread_handles {
        let _ = handler.join();
    }
}
fn handle_master(mut stream: ReplicaStream, database: Arc<RDB>, _config: Arc<ServerConfig>) {
    //let mut storage = BTreeMap::<String, Item>::new();
    loop {
        let resp = stream.get_resp().unwrap();
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
                        stream
                            ._stream
                            .write_all(mes.to_string().as_bytes())
                            .unwrap();
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

pub struct ReplicaStream {
    // A wrapper of TcpStream for handling RDB and resp format difference
    pub _stream: TcpStream,
    pub _cache: VecDeque<StreamToken>,
}
impl ReplicaStream {
    pub fn bind(stream: TcpStream) -> Self {
        Self {
            _stream: stream,
            _cache: vec![].into(),
        }
    }

    pub fn get_rdb(&mut self) -> Option<RDB> {
        if self._cache.is_empty() && !self.fetch_stream() {
            None
        } else {
            match self._cache.pop_front().unwrap() {
                StreamToken::Rdb(rdb) => Some(rdb),
                _default => None,
            }
        }
    }
    pub fn get_resp(&mut self) -> Option<Message> {
        if self._cache.is_empty() && !self.fetch_stream() {
            None
        } else {
            match self._cache.pop_front().unwrap() {
                StreamToken::Resp(resp) => Some(resp),
                _default => None,
            }
        }
    }
    fn fetch_stream(&mut self) -> bool {
        let mut read_buf = [0; 256];
        match self._stream.read(&mut read_buf) {
            Ok(length) => {
                let mut probe = 0;
                let data = &read_buf[..length];
                while probe < data.len() {
                    let token = match data[probe] {
                        b'$' => StreamToken::Rdb(Self::read_rdb(data, &mut probe)),
                        b'+' => StreamToken::Resp(Self::read_simple(data, &mut probe)),
                        _default => StreamToken::Resp(Self::read_array(data, &mut probe)),
                    };
                    self._cache.push_back(token);
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
