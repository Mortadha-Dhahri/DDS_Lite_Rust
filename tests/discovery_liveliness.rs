use std::thread;
use std::time::Duration;

use dds_lite_rust::{
    DiscoveryServer, EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy,
    Reliability, Topic, UdpTransport,
};
use serde::Serialize;
use std::net::{SocketAddr, UdpSocket};

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

#[derive(Clone, Serialize)]
struct VehicleState {
    speed: u32,
}

#[test]
fn publisher_stops_sending_to_expired_subscriber_after_refresh() {
    const TOPIC: &str = "vehicle/state";
    const TIMEOUT: Duration = Duration::from_millis(500);
    const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

    // Start the discovery server.
    let mut server = DiscoveryServer::bind_with_timeout("127.0.0.1:0".parse().unwrap(), TIMEOUT)
        .expect("failed to bind discovery server");

    let server_address = server
        .local_addr()
        .expect("failed to obtain discovery server address");

    thread::spawn(move || {
        server.run().expect("discovery server failed");
    });

    // Create two UDP sockets representing two subscribers.
    let inactive_socket =
        UdpSocket::bind("127.0.0.1:0").expect("failed to bind inactive subscriber");

    let active_socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind active subscriber");

    let inactive_address = inactive_socket.local_addr().unwrap();
    let active_address = active_socket.local_addr().unwrap();

    // Allow enough time to observe whether an unwanted packet arrives.
    inactive_socket
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();

    active_socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    // Create discovery clients.
    let inactive_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address).unwrap();

    let active_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address).unwrap();

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address).unwrap();

    // Register both subscribers.
    let inactive_participant = Participant::new(101, inactive_address);
    let active_participant = Participant::new(102, active_address);

    inactive_discovery
        .register_participant(&inactive_participant)
        .expect("failed to register inactive participant");

    active_discovery
        .register_participant(&active_participant)
        .expect("failed to register active participant");

    inactive_discovery
        .register_endpoint(101, TOPIC, EndpointKind::Subscriber)
        .expect("failed to register inactive subscriber endpoint");

    active_discovery
        .register_endpoint(102, TOPIC, EndpointKind::Subscriber)
        .expect("failed to register active subscriber endpoint");

    // Create the publisher.
    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

    let topic = Topic::new(TOPIC, "VehicleState");
    let qos = QosPolicy::new(10, Reliability::BestEffort);

    let mut publisher = Publisher::<VehicleState>::new(topic, publisher_discovery, transport, qos)
        .expect("failed to create publisher");

    // Make cache refresh quick enough for this test.
    publisher.set_subscriber_refresh_interval(Duration::from_millis(50));

    // First publication should reach both subscribers.
    publisher
        .publish(&VehicleState { speed: 10 })
        .expect("initial publication failed");

    let mut buffer = [0u8; 65_535];

    inactive_socket
        .recv_from(&mut buffer)
        .expect("inactive subscriber should receive the initial publication");

    active_socket
        .recv_from(&mut buffer)
        .expect("active subscriber should receive the initial publication");

    // Keep participant 102 alive; participant 101 sends no more heartbeats.
    for _ in 0..8 {
        active_discovery
            .heartbeat(active_participant.id())
            .expect("active subscriber heartbeat failed");

        thread::sleep(HEARTBEAT_INTERVAL);
    }

    // Give the server time to expire participant 101.
    thread::sleep(Duration::from_millis(150));

    // This publication should refresh discovery before sending.
    publisher
        .publish(&VehicleState { speed: 20 })
        .expect("publication after expiration failed");

    // The active subscriber must still receive data.
    active_socket
        .recv_from(&mut buffer)
        .expect("active subscriber should continue receiving publications");

    // The expired subscriber's socket is still open, so a packet here would
    // prove the publisher sent to a stale cached endpoint.
    let result = inactive_socket.recv_from(&mut buffer);

    assert!(
        matches!(
            result,
            Err(ref error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut
        ),
        "expired subscriber unexpectedly received data: {result:?}"
    );
}
