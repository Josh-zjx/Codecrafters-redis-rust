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
    let mut read_buf: [u8; 256] = [0; 256];
    loop {
        read_buf = [0; 256];
        let read_result = stream.read(&mut read_buf);
        let size = match read_result {
            Ok(length) => {
                if length == 0 {
                    continue;
                }
                if &read_buf[8..12] == b"ECHO" || &read_buf[8..12] == b"echo" {
                    let _write_result = stream.write_all(&read_buf[14..length]);
                    println!("ECHO")
                } else {
                    let _write_result = stream.write_all(b"+PONG\r\n");
                }
            }
            Err(_) => {}
        };
        stream.flush().unwrap();
    }
}
