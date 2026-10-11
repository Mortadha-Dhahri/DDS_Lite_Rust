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
        self.endpoints.push(endpoint);
    }

    fn register_participant(&mut self, participant: Participant) {
        self.participants.push(participant);
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
mod tests {
    use super::*;

    #[test]
    fn removing_participant_cleans_up_its_endpoints() {
        let mut discovery = LocalDiscovery::new();

        let address_1 = "127.0.0.1:7001".parse().unwrap();
        let address_2 = "127.0.0.1:7002".parse().unwrap();

        discovery.register_participant(Participant::new(1, address_1));
        discovery.register_participant(Participant::new(2, address_2));

        discovery.register_endpoint(Endpoint {
            participant_id: 1,
            topic: "vehicle/state".to_string(),
            kind: EndpointKind::Publisher,
        });

        discovery.register_endpoint(Endpoint {
            participant_id: 1,
            topic: "vehicle/speed".to_string(),
            kind: EndpointKind::Publisher,
        });

        discovery.register_endpoint(Endpoint {
            participant_id: 2,
            topic: "vehicle/state".to_string(),
            kind: EndpointKind::Subscriber,
        });

        // Remove participant 1 and all its endpoints.
        discovery.remove_participant(1);

        // Its participant registration must be gone.
        assert_eq!(discovery.participant_address(1), None);

        // Its publisher endpoints must no longer be discoverable.
        assert!(
            discovery
                .lookup("vehicle/state", EndpointKind::Publisher)
                .is_empty()
        );

        assert!(
            discovery
                .lookup("vehicle/speed", EndpointKind::Publisher)
                .is_empty()
        );

        // Participant 2 must remain registered.
        assert_eq!(discovery.participant_address(2), Some(address_2));

        // Participant 2's subscriber endpoint must remain discoverable.
        let subscribers = discovery.lookup("vehicle/state", EndpointKind::Subscriber);

        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].participant_id, 2);
    }
}
