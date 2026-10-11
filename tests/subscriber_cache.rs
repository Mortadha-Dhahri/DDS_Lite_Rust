use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use dds_lite_rust::{
    DiscoveryServer, EndpointKind, NetworkDiscovery, NetworkMessage, Participant, Publisher,
    QosPolicy, Reliability, Topic, UdpTransport, decode_network_message,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    value: u32,
}

fn receive_data(receiver: &UdpSocket, expected_sequence: u64, expected_topic: &str) {
    let mut buffer = vec![0u8; 65_535];

    let (size, _) = receiver
        .recv_from(&mut buffer)
        .expect("Expected to receive a DATA packet");

    let message = decode_network_message(&buffer[..size]).unwrap();

    match message {
        NetworkMessage::Data(data) => {
            assert_eq!(data.sequence_number, expected_sequence);
            assert_eq!(data.topic, expected_topic);

            let decoded: TestMessage = dds_lite_rust::deserialize_payload(&data.payload).unwrap();

            assert_eq!(decoded.value, expected_sequence as u32);
        }
        NetworkMessage::Control(_) => {
            panic!("Expected a DATA packet");
        }
    }
}

#[test]
fn publisher_refresh_discovers_late_subscriber() {
    let discovery_server_address = "127.0.0.1:6102".parse().unwrap();

    thread::spawn(move || {
        let mut server = DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    // Both subscribers use real UDP sockets with dynamically assigned ports.
    let subscriber_a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let subscriber_b = UdpSocket::bind("127.0.0.1:0").unwrap();

    subscriber_a
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    subscriber_b
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();

    let subscriber_a_address = subscriber_a.local_addr().unwrap();
    let subscriber_b_address = subscriber_b.local_addr().unwrap();

    // Use a dedicated port for this test's publisher.
    let publisher_address = "127.0.0.1:7114".parse().unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_a_participant = Participant::new(2, subscriber_a_address);
    let subscriber_b_participant = Participant::new(3, subscriber_b_address);

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let subscriber_a_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_a_discovery
        .register_participant(&subscriber_a_participant)
        .unwrap();

    let topic = Topic::new("test/subscriber-cache", "TestMessage");

    publisher_discovery
        .register_endpoint(
            publisher_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    subscriber_a_discovery
        .register_endpoint(
            subscriber_a_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let publisher_transport = UdpTransport::bind(publisher_address).unwrap();

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();

    publisher.set_subscriber_refresh_interval(Duration::from_millis(500));

    // 1. First publication discovers subscriber A and populates the cache.
    publisher.publish(&TestMessage { value: 1 }).unwrap();

    receive_data(&subscriber_a, 1, topic.name());

    // 2. Subscriber B registers after the publisher has populated its cache.
    let subscriber_b_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    subscriber_b_discovery
        .register_participant(&subscriber_b_participant)
        .unwrap();

    subscriber_b_discovery
        .register_endpoint(
            subscriber_b_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    // 3. Without refreshing, the publisher still uses its cached subscriber list.
    publisher.publish(&TestMessage { value: 2 }).unwrap();

    receive_data(&subscriber_a, 2, topic.name());

    let result = subscriber_b.recv_from(&mut [0u8; 65_535]);

    assert!(
        matches!(
            result,
            Err(ref error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut
        ),
        "Subscriber B should not receive data before the cache is refreshed"
    );

    // 4. Refresh discovery so the publisher learns about subscriber B.
    // 4. Wait for the automatic refresh interval to expire.
    thread::sleep(Duration::from_millis(600));

    // 5. Publish again. The publisher should automatically refresh discovery.
    // No manual call to refresh_subscribers() is needed.
    publisher.publish(&TestMessage { value: 3 }).unwrap();

    // Both subscribers should receive the third publication.
    receive_data(&subscriber_a, 3, topic.name());
    receive_data(&subscriber_b, 3, topic.name());
}
