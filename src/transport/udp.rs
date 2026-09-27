use std::io;
use std::net::{SocketAddr, UdpSocket};

use super::Transport;

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(address)?;

        Ok(Self { socket })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Transport for UdpTransport {
    fn send(&self, data: &[u8], destination: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(data, destination)
    }

    fn receive(&self) -> io::Result<(Vec<u8>, SocketAddr)> {
        let mut buffer = vec![0u8; 65_535];

        let (size, sender) = self.socket.recv_from(&mut buffer)?;

        buffer.truncate(size);

        Ok((buffer, sender))
    }
}