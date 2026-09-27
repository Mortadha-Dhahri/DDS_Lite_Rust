use std::net::SocketAddr;

#[derive(Debug, Clone)]

pub struct Participant {
    id: u64,
    address: SocketAddr,
}

impl Participant {
    pub fn new(id: u64, address: SocketAddr) -> Self {
        Self { id, address }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}