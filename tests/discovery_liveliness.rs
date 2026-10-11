use dds_lite_rust::{DiscoveryServer, EndpointKind, NetworkDiscovery, Participant};
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

const TOPIC: &str = "vehicle/state";
const TIMEOUT: Duration = Duration::from_millis(500);
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

#[test]
fn expired_participant_is_removed_while_active_participant_remains() {
    // Start the discovery server on an OS-assigned UDP port.
    let mut server = DiscoveryServer::bind_with_timeout("127.0.0.1:0".parse().unwrap(), TIMEOUT)
        .expect("failed to bind discovery server");

    let server_address = server
        .local_addr()
        .expect("failed to obtain server address");

    // Start the server in the background.
    thread::spawn(move || {
        server.run().expect("discovery server failed");
    });

    // Create two independent discovery clients.
    let client_1 = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address)
        .expect("failed to create client 1");

    let client_2 = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address)
        .expect("failed to create client 2");

    // Give the participants distinct addresses.
    let participant_1 = Participant::new(1, "127.0.0.1:7101".parse::<SocketAddr>().unwrap());

    let participant_2 = Participant::new(2, "127.0.0.1:7102".parse::<SocketAddr>().unwrap());

    // Register both participants.
    client_1
        .register_participant(&participant_1)
        .expect("failed to register participant 1");

    client_2
        .register_participant(&participant_2)
        .expect("failed to register participant 2");

    // Register a publisher endpoint for each participant.
    client_1
        .register_endpoint(1, TOPIC, EndpointKind::Publisher)
        .expect("failed to register participant 1 endpoint");

    client_2
        .register_endpoint(2, TOPIC, EndpointKind::Publisher)
        .expect("failed to register participant 2 endpoint");

    // Verify that both endpoints are initially discoverable.
    let initial_endpoints = client_1
        .lookup(TOPIC, EndpointKind::Publisher)
        .expect("initial lookup failed");

    assert_eq!(
        initial_endpoints.len(),
        2,
        "both participants should initially be discoverable"
    );

    // Keep participant 2 alive while participant 1 remains silent.
    // The server's timeout loop should independently expire participant 1.
    for _ in 0..8 {
        client_2
            .heartbeat(participant_2.id())
            .expect("heartbeat for participant 2 failed");

        thread::sleep(HEARTBEAT_INTERVAL);
    }

    // Allow the server's expiration loop to process the timeout.
    thread::sleep(Duration::from_millis(150));

    // Perform a real network lookup.
    let remaining_endpoints = client_1
        .lookup(TOPIC, EndpointKind::Publisher)
        .expect("lookup after expiration failed");

    // Only participant 2 should remain discoverable.
    assert_eq!(
        remaining_endpoints.len(),
        1,
        "the expired participant's endpoint should have been removed"
    );

    assert_eq!(
        remaining_endpoints[0].participant_id, 2,
        "only the active participant should remain discoverable"
    );
}
