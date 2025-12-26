use std::str::FromStr;
use std::string::ParseError;

#[derive(Clone, Debug)]
pub struct ReplicaMessage {
    pub message: Message,
    pub ack_timeout: u64,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KvItem {
    pub value: String,
    pub expire: u64,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct StreamItem {
    pub value: Vec<(String, Vec<String>)>,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Item {
    KvItem(KvItem),
    StreamItem(StreamItem),
}
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum MessageType {
    SimpleString,
    BulkString,
    Arrays,
    Null,
    Integer,
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
    pub fn null() -> Self {
        Message {
            message_type: MessageType::Null,
            message: "".to_string(),
            submessage: vec![],
        }
    }

    // Generate simple error message
    pub fn error(message: &str) -> Self {
        Message {
            message_type: MessageType::Error,
            message: message.to_string(),
            submessage: vec![],
        }
    }
    pub fn simple_string(message: &str) -> Self {
        Message {
            message_type: MessageType::SimpleString,
            message: message.to_string(),
            submessage: vec![],
        }
    }
    pub fn integer(message: u64) -> Self {
        Message {
            message_type: MessageType::Integer,
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

    pub fn arrays(messages: &[Message]) -> Self {
        Message {
            message_type: MessageType::Arrays,
            message: "".to_string(),
            submessage: messages.to_vec(),
        }
    }
    pub fn operator(&self) -> Option<String> {
        Some(self.submessage.first()?.message.to_lowercase())
    }
    pub fn first_arg(&self) -> Option<&str> {
        Some(self.submessage.get(1)?.message.as_str())
    }
    pub fn second_arg(&self) -> Option<&str> {
        Some(self.submessage.get(2)?.message.as_str())
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
            MessageType::Null => "$-1\r\n".to_string(),
            MessageType::BulkString => {
                format!("${}\r\n{}\r\n", &self.message.len(), self.message)
            }
            MessageType::SimpleString => {
                format!("+{}\r\n", self.message)
            }
            MessageType::Integer => {
                format!(":{}\r\n", self.message)
            }
            MessageType::Arrays => {
                let items_length = self.submessage.len();
                let mut response_string: String = format!("*{}\r\n", items_length);
                for i in 0..items_length {
                    response_string.push_str(&self.submessage.get(i).unwrap().to_string());
                }
                response_string
            }
            MessageType::Error => {
                format!("-{}\r\n", self.message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::*;
    use std::str::FromStr;
    #[test]
    fn test_null_bulk_string() {
        assert_eq!(
            Message {
                message_type: MessageType::Null,
                message: "".to_string(),
                submessage: vec![]
            },
            Message::null()
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
    fn test_arrays() {
        assert_eq!(
            Message::arrays(&[Message::simple_string("OK"), Message::bulk_string("ECHO")]),
            Message {
                message_type: MessageType::Arrays,
                message: "".to_string(),
                submessage: vec![Message::simple_string("OK"), Message::bulk_string("ECHO")]
            }
        )
    }

    #[test]
    fn test_null_bulk_string_as_bytes() {
        assert_eq!(Message::null().to_string().as_bytes(), b"$-1\r\n")
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
    #[test]
    fn test_from_str2() {
        let test_message =
            Message::arrays(&[Message::bulk_string("line1"), Message::bulk_string("line2")]);
        assert_eq!(
            test_message,
            Message::from_str(test_message.to_string().as_str()).unwrap()
        )
    }

    #[test]
    fn test_error_message() {
        assert_eq!(
            Message {
                message_type: MessageType::Error,
                message: "ERR unknown command".to_string(),
                submessage: vec![]
            },
            Message::error("ERR unknown command")
        );
    }

    #[test]
    fn test_error_message_as_bytes() {
        assert_eq!(
            Message::error("ERR unknown command").to_string().as_bytes(),
            b"-ERR unknown command\r\n"
        );
    }

    #[test]
    fn test_integer_message() {
        assert_eq!(
            Message {
                message_type: MessageType::Integer,
                message: "42".to_string(),
                submessage: vec![]
            },
            Message::integer(42)
        );
    }

    #[test]
    fn test_integer_message_as_bytes() {
        assert_eq!(
            Message::integer(100).to_string().as_bytes(),
            b":100\r\n"
        );
    }

    #[test]
    fn test_integer_zero() {
        assert_eq!(
            Message::integer(0).to_string().as_bytes(),
            b":0\r\n"
        );
    }

    #[test]
    fn test_read_bulk_basic() {
        let data = b"$4\r\ntest\r\n";
        let mut index = 0;
        let result = Message::read_bulk(data, &mut index);
        assert_eq!(result, Message::bulk_string("test"));
        assert_eq!(index, 10); // Should advance past the entire message
    }

    #[test]
    fn test_read_bulk_empty_string() {
        let data = b"$0\r\n\r\n";
        let mut index = 0;
        let result = Message::read_bulk(data, &mut index);
        assert_eq!(result, Message::bulk_string(""));
    }

    #[test]
    fn test_read_simple_basic() {
        let data = b"+PONG\r\n";
        let mut index = 0;
        let result = Message::read_simple(data, &mut index);
        assert_eq!(result, Message::simple_string("PONG"));
        assert_eq!(index, 7); // Should advance past the entire message
    }

    #[test]
    fn test_read_array_basic() {
        let data = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let mut index = 0;
        let result = Message::read_array(data, &mut index);
        assert_eq!(
            result,
            Message::arrays(&[
                Message::bulk_string("ECHO"),
                Message::bulk_string("hello")
            ])
        );
    }

    #[test]
    fn test_read_array_empty() {
        let data = b"*0\r\n";
        let mut index = 0;
        let result = Message::read_array(data, &mut index);
        assert_eq!(result, Message::arrays(&[]));
    }

    #[test]
    fn test_read_array_nested() {
        let data = b"*1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut index = 0;
        let result = Message::read_array(data, &mut index);
        assert_eq!(
            result,
            Message::arrays(&[Message::arrays(&[
                Message::bulk_string("foo"),
                Message::bulk_string("bar")
            ])])
        );
    }

    #[test]
    fn test_operator() {
        let msg = Message::arrays(&[
            Message::bulk_string("GET"),
            Message::bulk_string("key1"),
        ]);
        assert_eq!(msg.operator(), Some("get".to_string()));
    }

    #[test]
    fn test_operator_empty_array() {
        let msg = Message::arrays(&[]);
        assert_eq!(msg.operator(), None);
    }

    #[test]
    fn test_first_arg() {
        let msg = Message::arrays(&[
            Message::bulk_string("GET"),
            Message::bulk_string("mykey"),
        ]);
        assert_eq!(msg.first_arg(), Some("mykey"));
    }

    #[test]
    fn test_first_arg_missing() {
        let msg = Message::arrays(&[Message::bulk_string("PING")]);
        assert_eq!(msg.first_arg(), None);
    }

    #[test]
    fn test_second_arg() {
        let msg = Message::arrays(&[
            Message::bulk_string("SET"),
            Message::bulk_string("key"),
            Message::bulk_string("value"),
        ]);
        assert_eq!(msg.second_arg(), Some("value"));
    }

    #[test]
    fn test_second_arg_missing() {
        let msg = Message::arrays(&[
            Message::bulk_string("GET"),
            Message::bulk_string("key"),
        ]);
        assert_eq!(msg.second_arg(), None);
    }

    #[test]
    fn test_empty_bulk_string() {
        let msg = Message::bulk_string("");
        assert_eq!(msg.to_string().as_bytes(), b"$0\r\n\r\n");
    }

    #[test]
    fn test_arrays_as_bytes() {
        let msg = Message::arrays(&[
            Message::bulk_string("SET"),
            Message::bulk_string("key"),
        ]);
        assert_eq!(
            msg.to_string().as_bytes(),
            b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n"
        );
    }

    #[test]
    fn test_empty_arrays() {
        let msg = Message::arrays(&[]);
        assert_eq!(msg.to_string().as_bytes(), b"*0\r\n");
    }

    #[test]
    fn test_operator_case_insensitive() {
        let msg = Message::arrays(&[
            Message::bulk_string("PING"),
        ]);
        assert_eq!(msg.operator(), Some("ping".to_string()));
    }

    #[test]
    fn test_read_array_with_simple_strings() {
        let data = b"*2\r\n+OK\r\n$4\r\ntest\r\n";
        let mut index = 0;
        let result = Message::read_array(data, &mut index);
        assert_eq!(
            result,
            Message::arrays(&[
                Message::simple_string("OK"),
                Message::bulk_string("test")
            ])
        );
    }

    // Tests for data structures
    #[test]
    fn test_kv_item_equality() {
        let item1 = KvItem {
            value: "test".to_string(),
            expire: 0,
        };
        let item2 = KvItem {
            value: "test".to_string(),
            expire: 0,
        };
        assert_eq!(item1, item2);
    }

    #[test]
    fn test_kv_item_inequality_value() {
        let item1 = KvItem {
            value: "test1".to_string(),
            expire: 0,
        };
        let item2 = KvItem {
            value: "test2".to_string(),
            expire: 0,
        };
        assert_ne!(item1, item2);
    }

    #[test]
    fn test_kv_item_inequality_expire() {
        let item1 = KvItem {
            value: "test".to_string(),
            expire: 0,
        };
        let item2 = KvItem {
            value: "test".to_string(),
            expire: 1000,
        };
        assert_ne!(item1, item2);
    }

    #[test]
    fn test_kv_item_clone() {
        let item1 = KvItem {
            value: "test".to_string(),
            expire: 1000,
        };
        let item2 = item1.clone();
        assert_eq!(item1, item2);
    }

    #[test]
    fn test_stream_item_empty() {
        let item = StreamItem { value: vec![] };
        assert!(item.value.is_empty());
    }

    #[test]
    fn test_stream_item_with_data() {
        let item = StreamItem {
            value: vec![("1000-1".to_string(), vec!["key".to_string(), "value".to_string()])],
        };
        assert_eq!(item.value.len(), 1);
        assert_eq!(item.value[0].0, "1000-1");
    }

    #[test]
    fn test_stream_item_equality() {
        let item1 = StreamItem {
            value: vec![("1000-1".to_string(), vec!["a".to_string()])],
        };
        let item2 = StreamItem {
            value: vec![("1000-1".to_string(), vec!["a".to_string()])],
        };
        assert_eq!(item1, item2);
    }

    #[test]
    fn test_item_enum_kv() {
        let kv = KvItem {
            value: "test".to_string(),
            expire: 0,
        };
        let item = Item::KvItem(kv.clone());
        match item {
            Item::KvItem(inner) => assert_eq!(inner, kv),
            Item::StreamItem(_) => panic!("Expected KvItem"),
        }
    }

    #[test]
    fn test_item_enum_stream() {
        let stream = StreamItem { value: vec![] };
        let item = Item::StreamItem(stream.clone());
        match item {
            Item::StreamItem(inner) => assert_eq!(inner, stream),
            Item::KvItem(_) => panic!("Expected StreamItem"),
        }
    }

    #[test]
    fn test_message_type_equality() {
        assert_eq!(MessageType::SimpleString, MessageType::SimpleString);
        assert_eq!(MessageType::BulkString, MessageType::BulkString);
        assert_eq!(MessageType::Arrays, MessageType::Arrays);
        assert_eq!(MessageType::Null, MessageType::Null);
        assert_eq!(MessageType::Integer, MessageType::Integer);
        assert_eq!(MessageType::Error, MessageType::Error);
    }

    #[test]
    fn test_message_type_inequality() {
        assert_ne!(MessageType::SimpleString, MessageType::BulkString);
        assert_ne!(MessageType::Arrays, MessageType::Null);
    }

    #[test]
    fn test_replica_message_clone() {
        let msg = ReplicaMessage {
            message: Message::simple_string("test"),
            ack_timeout: 100,
        };
        let cloned = msg.clone();
        assert_eq!(cloned.message, msg.message);
        assert_eq!(cloned.ack_timeout, msg.ack_timeout);
    }

    #[test]
    fn test_message_clone() {
        let msg = Message::arrays(&[
            Message::bulk_string("SET"),
            Message::bulk_string("key"),
            Message::bulk_string("value"),
        ]);
        let cloned = msg.clone();
        assert_eq!(msg, cloned);
    }
}
