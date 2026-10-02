use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use dds_lite_rust::{
    EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy, Reliability, Subscriber,
    Topic, UdpTransport,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    value: u32,
}

#[test]
fn reliable_delivery_clears_pending_ack() {
    let discovery_server_address = "127.0.0.1:6100".parse().unwrap();

    thread::spawn(move || {
        let mut server = dds_lite_rust::DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let publisher_address = "127.0.0.1:7101".parse().unwrap();
    let subscriber_address = "127.0.0.1:7102".parse().unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/reliable", "TestMessage");

    publisher_discovery
        .register_endpoint(
            publisher_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    subscriber_discovery
        .register_endpoint(
            subscriber_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let publisher_transport = UdpTransport::bind(publisher_address).unwrap();
    let subscriber_transport = UdpTransport::bind(subscriber_address).unwrap();

    let publisher_qos = QosPolicy::new(10, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        publisher_qos,
    )
    .unwrap();

    let mut subscriber =
        Subscriber::<TestMessage>::new(topic, subscriber_discovery, subscriber_transport).unwrap();

    let message = TestMessage { value: 42 };

    publisher.publish(&message).unwrap();

    assert_eq!(publisher.pending_ack_count(), 1);

    let received = subscriber.receive().unwrap();

    assert_eq!(received, message);

    publisher.receive_control().unwrap();

    assert_eq!(publisher.pending_ack_count(), 0);
}
