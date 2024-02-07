use std::collections::BTreeMap;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").expect("Listen to 6379 ports");
    let mut thread_handles = vec![];

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let handler = thread::spawn(move || {
                    handle_client(stream);
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

fn handle_client(mut stream: TcpStream) {
    let mut read_buf: [u8; 256];
    let mut storage = BTreeMap::<String, String>::new();

    loop {
        read_buf = [0; 256];
        let read_result = stream.read(&mut read_buf);
        let _ = match read_result {
            Ok(length) => {
                if length == 0 {
                    continue;
                }
                let req = String::from_utf8(read_buf[..length].to_vec()).unwrap();
                let params: Vec<&str> = req.lines().collect();
                let operator_num: usize = params[0][1..].parse().unwrap();
                if operator_num == 2 {
                    if params[2].as_bytes() == b"ECHO" || params[2].as_bytes() == b"echo" {
                        let _write_result = stream.write_all(&read_buf[14..length]);
                        println!("ECHO")
                    } else if params[2].as_bytes() == b"PING" || params[2].as_bytes() == b"ping" {
                        let _write_result = stream.write_all(b"+PONG\r\n");
                    } else if params[2].as_bytes() == b"GET" || params[2].as_bytes() == b"get" {
                        if storage.contains_key(params[4]) {
                            let resp = storage.get(params[4]).unwrap();
                            let length = resp.len();
                            let _write_result = stream.write_all(
                                [
                                    "$",
                                    &length.to_string() as &str,
                                    "\r\n",
                                    &resp as &str,
                                    "\r\n",
                                ]
                                .concat()
                                .as_bytes(),
                            );
                        } else {
                            let _write_result = stream.write_all(b"+(nil)\r\n");
                        }
                    }
                } else if operator_num == 3 {
                    if params[2].as_bytes() == b"SET" || params[2].as_bytes() == b"set" {
                        storage.insert(params[4].to_string(), params[6].to_string());
                        let _write_result = stream.write_all(b"+OK\r\n");
                    }
                } else {
                    let _write_result = stream.write_all(b"+PONG\r\n");
                }
            }
            Err(_) => {}
        };
        stream.flush().unwrap();
    }
}
