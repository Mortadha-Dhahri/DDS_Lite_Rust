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

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    pub fn try_receive(&self) -> io::Result<Option<(Vec<u8>, SocketAddr)>> {
        let mut buffer = vec![0u8; 65_535];

        match self.socket.recv_from(&mut buffer) {
            Ok((size, sender)) => {
                buffer.truncate(size);

                Ok(Some((buffer, sender)))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error),
        }
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

/*
    receive()
        blocking
        existing behavior
        nothing breaks

    try_receive()
        non-blocking
        returns Some(message)
        returns None when nothing is available

*/
