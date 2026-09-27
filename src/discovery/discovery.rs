use serde::{Deserialize,Serialize};
use crate::core::Participant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointKind {
    Publisher,
    Subscriber,
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub participant_id: u64,
    pub topic: String,
    pub kind: EndpointKind,
}

pub struct DiscoveryServer {
    participants: Vec<Participant>,
    endpoints: Vec<Endpoint>,
}

pub trait Discovery {
    fn register_participant(&mut self, participant: Participant);

    fn register_endpoint(&mut self, endpoint: Endpoint);

    fn lookup(&self, topic: &str, kind: EndpointKind) -> Vec<Endpoint>;
}

pub struct LocalDiscovery {
    participants:Vec<Participant>,
    endpoints: Vec<Endpoint>,
}

impl LocalDiscovery {
    pub fn new() -> Self {
        Self {
            participants: Vec::new(),
            endpoints: Vec::new(),
        }
    }
}

impl Discovery for LocalDiscovery {

    fn register_endpoint(&mut self, endpoint: Endpoint) {
        self.endpoints.push(endpoint);
    }

    fn register_participant(&mut self, participant: Participant) {
        self.participants.push(participant);
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

