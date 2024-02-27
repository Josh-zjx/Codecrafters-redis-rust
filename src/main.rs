use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;

mod message;
pub use crate::message::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "example")]
struct Opt {
    #[structopt(long, parse(from_os_str), default_value = "42")]
    _dir: PathBuf,

    #[structopt(long, parse(from_os_str), default_value = "42")]
    _dbfilename: PathBuf,
}

fn handle_client(mut stream: TcpStream, database: Arc<RDB>, config: Arc<BTreeMap<String, String>>) {
    let mut read_buf: [u8; 256];
    //let mut storage = BTreeMap::<String, Item>::new();
    let mut storage = database._storage.clone();

    loop {
        read_buf = [0; 256];
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
                                        .as_millis() as usize
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
                                .as_millis() as usize
                                + request_message
                                    .submessage
                                    .get(4)
                                    .expect("No load for extra operator")
                                    .message
                                    .parse::<usize>()
                                    .unwrap();
                        } else {
                            println!("Not Important");
                        }
                    }
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
                        let resp = config.get(key).unwrap();
                        Message {
                            message_type: MessageType::Arrays,
                            message: "".to_string(),
                            submessage: vec![Message::bulk_string(key), Message::bulk_string(resp)],
                        }
                    } else {
                        println!("Not Important");
                        Message::null_blk_string()
                    }
                }
                "keys" => {
                    if request_message.submessage.get(1).unwrap().message == "*" {
                        let keys = storage.keys();
                        let mut response = Message {
                            message_type: MessageType::Arrays,
                            message: "".to_string(),
                            submessage: vec![],
                        };
                        for i in keys {
                            response.submessage.push(Message::bulk_string(&i));
                        }
                        response
                    } else {
                        Message::null_blk_string()
                    }
                }
                _default => Message::null_blk_string(),
            };
            let _write_result = stream.write_all(response.to_string().as_bytes());
        };
        stream.flush().unwrap();
    }
}

fn initialize() -> BTreeMap<String, String> {
    // Parse and Set configuration from launch arguments
    let opt = Opt::from_args();

    let mut config: BTreeMap<String, String> = BTreeMap::new();
    config.insert("dir".to_string(), opt._dir.to_string_lossy().to_string());
    config.insert(
        "dbfilename".to_string(),
        opt._dbfilename.to_string_lossy().to_string(),
    );
    config
}

pub struct RDB {
    _comments: String,
    _storage: BTreeMap<String, Item>,
    _db_selector: usize,
}

impl RDB {
    pub fn read_header(&mut self, s: &[u8]) -> Option<usize> {
        let mut probe = 0;
        while s[probe] != 0xFE && probe < s.len() {
            probe += 1
        }
        println!("skipping {} bytes", probe);
        // TODO: Implementing Boundary/Validity check
        self._db_selector = s[probe + 1] as usize;
        Some(probe + 5)
    }
    pub fn read_data(&mut self, s: &[u8], index: usize) -> Option<usize> {
        if index >= s.len() {
            return None;
        }
        // TODO: Implement parser for other type of data
        if s[index] == 0xFF {
            return None;
        }
        let mut index = index + 1;
        let key;
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            key = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new key {}", key);
            index = nindex + length;
        } else {
            return None;
        }
        if let Some((nindex, length)) = self.parse_length_encoding(s, index) {
            println!("Reading from {} to {}", nindex, nindex + length);
            let value = String::from_utf8(s[nindex..nindex + length].to_vec()).unwrap();
            println!("new value {}", value);
            self._storage.insert(key, Item { value, expire: 0 });
            Some(nindex + length)
        } else {
            None
        }
    }
    pub fn parse_length_encoding(&mut self, s: &[u8], index: usize) -> Option<(usize, usize)> {
        if index >= s.len() {
            return None;
        }
        if s[index] == 0xFF {
            return None;
        }
        if s[index] < 64 {
            return Some((index + 1, s[index] as usize));
        }
        // TODO: Rightnow only implementing length-coding case one
        //if s[0] < 128 {
        //    return Some((&s[2..], (s[0] % 64 * 256 + s[1]) as usize));
        //}
        None
    }
}

fn read_rdb(dbfilename: String) -> RDB {
    let path = Path::new(&dbfilename);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_err) => {
            return RDB {
                _comments: "".to_string(),
                _db_selector: 0,
                _storage: BTreeMap::new(),
            }
        }
    };
    let mut data = vec![];
    if file.read_to_end(&mut data).is_ok() {
        println!("Reading {} bytes from rdb file", &data.len());
    } else {
        panic!("Cannot read file");
    }
    let data: &[u8] = &data;
    let mut rdb = RDB {
        _comments: dbfilename.to_string(),
        _storage: BTreeMap::new(),
        _db_selector: 0,
    };
    let mut res = rdb.read_header(data);
    while let Some(index) = res {
        res = rdb.read_data(data, index);
    }
    rdb
}

fn main() {
    println!("Logs from your program will appear here!");

    // Initialize configuration from launch arguments
    let config = Arc::new(initialize());
    let _database = read_rdb(format!(
        "{}/{}",
        config.get("dir").unwrap(),
        config.get("dbfilename").unwrap()
    ));
    println!("database length: {}", _database._storage.len());
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Listen to 6379 ports");
    let mut thread_handles = vec![];
    let database = Arc::new(_database);

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
