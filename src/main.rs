use std::collections::BTreeMap;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ParseError;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "example")]
struct Opt {
    #[structopt(long, parse(from_os_str), default_value = "42")]
    _dir: PathBuf,

    #[structopt(long, parse(from_os_str), default_value = "42")]
    _dbfilename: PathBuf,
}

pub struct Item {
    pub value: String,
    pub expire: usize,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum MessageType {
    SimpleString,
    BulkString,
    Arrays,
    Error,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Message {
    pub message_type: MessageType,
    pub message: String,
    pub submessage: Vec<Message>,
}

impl Message {
    // Generate null bulk string message
    pub fn null_blk_string() -> Self {
        Message {
            message_type: MessageType::Error,
            message: "".to_string(),
            submessage: vec![],
        }
    }

    // Generate simple string message
    pub fn simple_string(message: &str) -> Self {
        Message {
            message_type: MessageType::SimpleString,
            message: message.to_string(),
            submessage: vec![],
        }
    }

    // Generate bulk string message
    pub fn bulk_string(message: &str) -> Self {
        Message {
            message_type: MessageType::BulkString,
            message: message.to_string(),
            submessage: vec![],
        }
    }

    // Generate Message from str
}
impl FromStr for Message {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let params: Vec<&str> = s.split("\r\n").collect();
        let array_length = params.first().expect("Cannot find the operator num")[1..]
            .parse()
            .expect("Not a valid array length declaration");
        let mut message = Message {
            message_type: MessageType::Arrays,
            message: "".to_string(),
            submessage: vec![],
        };
        for i in 0..array_length {
            message.submessage.push(Message::bulk_string(
                params
                    .get(2 * i + 2)
                    .expect("Not enough params for requeset"),
            ));
        }
        Ok(message)
    }
}
impl ToString for Message {
    // Generate string from message
    fn to_string(&self) -> String {
        match &self.message_type {
            MessageType::Error => "$-1\r\n".to_string(),
            MessageType::BulkString => {
                format!("${}\r\n{}\r\n", &self.message.len(), self.message)
            }
            MessageType::SimpleString => {
                format!("+{}\r\n", self.message)
            }
            MessageType::Arrays => {
                let items_length = self.submessage.len();
                let mut response_string: String = format!("*{}\r\n", items_length);
                for i in 0..items_length {
                    response_string.push_str(&self.submessage.get(i).unwrap().to_string());
                }
                response_string
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, config: Arc<BTreeMap<String, String>>) {
    let mut read_buf: [u8; 256];
    let mut storage = BTreeMap::<String, Item>::new();

    loop {
        read_buf = [0; 256];
        let read_result = stream.read(&mut read_buf);
        match read_result {
            Ok(length) => {
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
                                            .as_millis()
                                            as usize
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
                            expire: 0 as usize,
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
                                    .as_millis()
                                    as usize
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
                                submessage: vec![
                                    Message::bulk_string(key),
                                    Message::bulk_string(&resp),
                                ],
                            }
                        } else {
                            println!("Not Important");
                            Message::null_blk_string()
                        }
                    }
                    _default => Message::null_blk_string(),
                };
                let _write_result = stream.write_all(response.to_string().as_bytes());
            }
            Err(_) => {}
        };
        stream.flush().unwrap();
    }
}

fn main() {
    println!("Logs from your program will appear here!");
    let opt = Opt::from_args();
    println!("{:?}", opt);

    let mut config: BTreeMap<String, String> = BTreeMap::new();
    config.insert("dir".to_string(), opt._dir.to_string_lossy().to_string());
    config.insert(
        "dbfilename".to_string(),
        opt._dbfilename.to_string_lossy().to_string(),
    );

    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Listen to 6379 ports");
    let mut thread_handles = vec![];

    for stream in listener.incoming() {
        let config_ref = Arc::clone(&config);
        match stream {
            Ok(stream) => {
                let handler = thread::spawn(move || {
                    handle_client(stream, config_ref);
                });
                thread_handles.push(handler);
            }
            Err(_e) => {
                println!("Error");
            }
        }
    }
    for handler in thread_handles {
        let _ = handler.join();
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    #[test]
    fn test_null_bulk_string() {
        assert_eq!(
            Message {
                message_type: MessageType::Error,
                message: "".to_string(),
                submessage: vec![]
            },
            Message::null_blk_string()
        );
    }

    #[test]
    fn test_bulk_string() {
        assert_eq!(
            Message {
                message_type: MessageType::BulkString,
                message: "test_string".to_string(),
                submessage: vec![]
            },
            Message::bulk_string("test_string")
        )
    }
    #[test]
    fn test_simple_string() {
        assert_eq!(
            Message {
                message_type: MessageType::SimpleString,
                message: "test_string".to_string(),
                submessage: vec![]
            },
            Message::simple_string("test_string")
        )
    }
    #[test]
    fn test_bulk_string2() {
        assert_ne!(
            Message {
                message_type: MessageType::BulkString,
                message: "Test_string".to_string(),
                submessage: vec![]
            },
            Message::bulk_string("test_string")
        )
    }

    #[test]
    fn test_bulk_string_as_bytes() {
        assert_eq!(
            Message::bulk_string("test").to_string().as_bytes(),
            b"$4\r\ntest\r\n"
        )
    }

    #[test]
    fn test_simple_string_as_bytes() {
        assert_eq!(
            Message::simple_string("test").to_string().as_bytes(),
            b"+test\r\n"
        )
    }

    #[test]
    fn test_null_bulk_string_as_bytes() {
        assert_eq!(
            Message::null_blk_string().to_string().as_bytes(),
            b"$-1\r\n"
        )
    }

    #[test]
    fn test_from_str() {
        let test_message = Message {
            message_type: MessageType::Arrays,
            message: "".to_string(),
            submessage: vec![
                Message {
                    message_type: MessageType::BulkString,
                    message: "line1".to_string(),
                    submessage: vec![],
                },
                Message {
                    message_type: MessageType::BulkString,
                    message: "line2".to_string(),
                    submessage: vec![],
                },
            ],
        };
        assert_eq!(
            test_message,
            Message::from_str(test_message.to_string().as_str()).unwrap()
        )
    }
}
