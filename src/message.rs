use std::str::FromStr;
use std::string::ParseError;

#[derive(Clone, Debug)]
pub struct Item {
    pub value: String,
    pub expire: u64,
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

    pub fn arrays(messages: &[Message]) -> Self {
        Message {
            message_type: MessageType::Arrays,
            message: "".to_string(),
            submessage: messages.to_vec(),
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

#[cfg(test)]
mod tests {
    use crate::message::*;
    use std::str::FromStr;
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
    #[test]
    fn test_from_str2() {
        let test_message =
            Message::arrays(&[Message::bulk_string("line1"), Message::bulk_string("line2")]);
        assert_eq!(
            test_message,
            Message::from_str(test_message.to_string().as_str()).unwrap()
        )
    }
}
