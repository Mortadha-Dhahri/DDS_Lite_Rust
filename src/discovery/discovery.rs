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

pub trait Discovery {
    fn register(&mut self, endpoint: Endpoint);

    fn lookup(&self, topic: &str, kind: EndpointKind) -> Vec<Endpoint>;
}

pub struct LocalDiscovery {
    endpoints: Vec<Endpoint>,
}

impl LocalDiscovery {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }
}

impl Discovery for LocalDiscovery {
    fn register(&mut self, endpoint: Endpoint) {
        self.endpoints.push(endpoint);
    }

    fn lookup(&self, topic: &str, kind: EndpointKind) -> Vec<Endpoint> {
        self.endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.topic == topic && endpoint.kind == kind
            })
            .cloned()
            .collect()
    }

    /*
        lookup("vehicle/state", EndpointKind::Subscriber) means Find subscribers interested in this topic.
        lookup("vehicle/state", EndpointKind::Publisher) means Find publishers for this topic.
     */
}