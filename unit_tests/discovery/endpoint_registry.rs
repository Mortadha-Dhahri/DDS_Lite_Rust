//! Unit tests for endpoint registry.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

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
#[test]
fn duplicate_participant_and_endpoint_registration_is_idempotent() {
    let mut discovery = LocalDiscovery::new();
    let address = "127.0.0.1:7001".parse().unwrap();

    discovery.register_participant(Participant::new(1, address));
    discovery.register_participant(Participant::new(1, address));
    for _ in 0..2 {
        discovery.register_endpoint(Endpoint {
            participant_id: 1,
            topic: "vehicle/state".to_string(),
            kind: EndpointKind::Subscriber,
        });
    }

    let endpoints = discovery.lookup("vehicle/state", EndpointKind::Subscriber);
    assert_eq!(endpoints.len(), 1);
    assert_eq!(discovery.participants.len(), 1);
}
