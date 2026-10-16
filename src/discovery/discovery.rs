use crate::core::Participant;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

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

pub trait Discovery {
    fn register_participant(&mut self, participant: Participant);

    fn register_endpoint(&mut self, endpoint: Endpoint);

    fn lookup(&self, topic: &str, kind: EndpointKind) -> Vec<Endpoint>;
}

pub struct LocalDiscovery {
    participants: Vec<Participant>,
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
        let already_registered = self.endpoints.iter().any(|registered| {
            registered.participant_id == endpoint.participant_id
                && registered.topic == endpoint.topic
                && registered.kind == endpoint.kind
        });

        if !already_registered {
            self.endpoints.push(endpoint);
        }
    }

    fn register_participant(&mut self, participant: Participant) {
        if let Some(registered) = self
            .participants
            .iter_mut()
            .find(|registered| registered.id() == participant.id())
        {
            *registered = participant;
        } else {
            self.participants.push(participant);
        }
    }

    fn lookup(&self, topic: &str, kind: EndpointKind) -> Vec<Endpoint> {
        self.endpoints
            .iter()
            .filter(|endpoint| endpoint.topic == topic && endpoint.kind == kind)
            .cloned()
            .collect()
    }

    /*
       lookup("vehicle/state", EndpointKind::Subscriber) means Find subscribers interested in this topic.
       lookup("vehicle/state", EndpointKind::Publisher) means Find publishers for this topic.
    */
}

impl LocalDiscovery {
    pub fn participant_address(&self, participant_id: u64) -> Option<SocketAddr> {
        self.participants
            .iter()
            .find(|participant| participant.id() == participant_id)
            .map(|participant| participant.address())
    }
    pub fn remove_participant(&mut self, participant_id: u64) {
        self.participants
            .retain(|participant| participant.id() != participant_id);

        self.endpoints
            .retain(|endpoint| endpoint.participant_id != participant_id);
    }
}

impl Default for LocalDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "../../unit_tests/discovery/endpoint_registry.rs"]
mod endpoint_registry_tests;
