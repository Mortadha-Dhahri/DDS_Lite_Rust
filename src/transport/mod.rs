use std::io;
use std::net::SocketAddr;

pub mod udp;

pub trait Transport {
    fn send(&self, data: &[u8], destination: SocketAddr) -> io::Result<usize>;

    fn receive(&self) -> io::Result<(Vec<u8>, SocketAddr)>;
}

pub use udp::UdpTransport;