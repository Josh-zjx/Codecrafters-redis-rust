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
    let echo_tag = b"ECHO";
    loop {
        let _read_result = stream.read(&mut read_buf).expect("Cannot read from stream");
        if &read_buf[8..12] == echo_tag {
            let _write_result = stream.write_all(&read_buf[18..]);
        } else {
            let _write_result = stream.write_all(b"+PONG\r\n");
        }
    }
}
