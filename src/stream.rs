use crate::message::*;
use crate::rdb::*;
use std::collections::VecDeque;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub struct MessageStream {
    // A wrapper of TcpStream for handling RDB and resp format difference
    pub stream: TcpStream,
    pub cache: VecDeque<Message>,
}
impl MessageStream {
    pub async fn write(&mut self, data: &[u8]) {
        self.stream.write_all(data).await.unwrap();
        self.stream.flush().await.unwrap();
    }
    pub async fn write_message(&mut self, message: Message) {
        self.stream
            .write_all(message.to_string().as_bytes())
            .await
            .unwrap();
        self.stream.flush().await.unwrap();
    }
    pub fn bind(stream: TcpStream) -> Self {
        Self {
            stream,
            cache: vec![].into(),
        }
    }
    pub async fn read_message(&mut self) -> Option<Message> {
        if self.cache.is_empty() {
            self.read_stream().await;
        }
        self.cache.pop_front()
    }
    async fn read_stream(&mut self) {
        let mut read_buf = [0; 512];
        {
            if let Ok(length) = self.stream.read(&mut read_buf).await {
                let mut probe = 0;
                let data = &read_buf[..length];
                while probe < data.len() {
                    let token = match data[probe] {
                        b'+' => Message::read_simple(data, &mut probe),
                        _default => Message::read_array(data, &mut probe),
                    };
                    self.cache.push_back(token);
                }
            }
        }
    }
}
pub struct ReplicaHandle {
    pub tx: mpsc::Sender<ReplicaMessage>,
    pub rx: mpsc::Receiver<ReplicaMessage>,
}
pub struct ReplicaStream {
    // A wrapper of TcpStream for handling RDB and resp format difference
    pub stream: TcpStream,
    pub cache: VecDeque<StreamToken>,
}
impl ReplicaStream {
    pub async fn _write(&mut self, data: &[u8]) {
        self.stream.write_all(data).await.unwrap();
        self.stream.flush().await.unwrap();
    }
    pub async fn write_message(&mut self, message: Message) {
        self.stream
            .write_all(message.to_string().as_bytes())
            .await
            .unwrap();
        self.stream.flush().await.unwrap();
    }
    pub fn bind(stream: TcpStream) -> Self {
        Self {
            stream,
            cache: vec![].into(),
        }
    }

    pub async fn get_rdb(&mut self) -> Option<RDB> {
        if self.cache.is_empty() && !self.fetch_stream().await {
            Some(RDB::new())
        } else {
            match self.cache.pop_front().unwrap() {
                StreamToken::Rdb(rdb) => Some(rdb),
                _default => Some(RDB::new()),
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
        let mut read_buf = [0; 512];
        match self.stream.read(&mut read_buf).await {
            Ok(length) => {
                let mut probe = 0;
                let data = &read_buf[..length];
                while probe < data.len() {
                    let token = match data[probe] {
                        b'$' => StreamToken::Rdb(RDB::read_rdb(data, &mut probe)),
                        b'+' => StreamToken::Resp(Message::read_simple(data, &mut probe)),
                        _default => StreamToken::Resp(Message::read_array(data, &mut probe)),
                    };
                    self.cache.push_back(token);
                }
                true
            }
            Err(_) => false,
        }
    }
}
pub enum StreamToken {
    Rdb(RDB),
    Resp(Message),
}

#[cfg(test)]
mod test {
    use crate::Message;

    #[test]
    fn test_read_simple() {
        let mes = Message::simple_string("Hello");
        let mut index = 0;
        assert_eq!(
            mes,
            Message::read_simple(&mes.to_string().as_bytes(), &mut index)
        )
    }
}
