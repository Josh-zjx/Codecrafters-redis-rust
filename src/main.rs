use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // In this stage, you'll implement a TCP server that listens on port 6379.
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Listen to 6379 ports");
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("Accepting connection");
            }
            Err(_e) => {
                println!("Error");
            }
        }
    }
}
