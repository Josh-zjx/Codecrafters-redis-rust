use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // In this stage, you'll implement a TCP server that listens on port 6379.
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Listen to 6379 ports");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_client(stream),
            Err(_e) => {
                println!("Error");
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut read_buf: [u8; 256] = [0; 256];
    loop {
        let _read_result = stream.read(&mut read_buf).expect("Cannot read from stream");
        let _write_result = stream
            .write_all(b"+PONG\r\n")
            .expect("Cannot write to the stream");
    }
}
