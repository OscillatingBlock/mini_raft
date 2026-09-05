use std::io::Bytes;

use tokio::{
    io::{BufReader, BufWriter},
    net::TcpStream,
};

pub struct NetworkManager {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

pub trait Network {
    fn read_bytes(&self) -> Bytes<String>;
    fn write_bytes(&self, message: String);
}

impl Network for NetworkManager {
    fn read_bytes(&self) -> Bytes<String> {
        !unimplemented!()
    }
    fn write_bytes(&self, message: String) {
        !unimplemented!()
    }
}
