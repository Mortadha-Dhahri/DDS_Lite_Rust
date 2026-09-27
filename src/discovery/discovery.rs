use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    Publisher,
    Subscriber,
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub participant_id: u64,
    pub topic: String,
    pub address: SocketAddr,
    pub kind: EndpointKind,
}

pub struct DiscoveryServer {
    endpoints: Vec<Endpoint>,
}

impl DiscoveryServer {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }

    pub fn register(&mut self, endpoint: Endpoint) {
        self.endpoints.push(endpoint);
    }

    pub fn lookup(&self, topic: &str) -> Vec<&Endpoint> {
        self.endpoints
            .iter()
            .filter(|endpoint| endpoint.topic == topic)
            .collect()
    }
}