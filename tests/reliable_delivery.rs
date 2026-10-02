use std::net::UdpSocket;
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

#[test]
fn reliable_delivery_recovers_from_lost_ack() {
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

    receiver
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let receiver_address = receiver.local_addr().unwrap();

    let publisher_address = "127.0.0.1:7103".parse().unwrap();

    let discovery_server_address = "127.0.0.1:6101".parse().unwrap();

    thread::spawn(move || {
        let mut server = dds_lite_rust::DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let publisher_participant = Participant::new(3, publisher_address);

    let discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    discovery
        .register_participant(&publisher_participant)
        .unwrap();

    let topic = Topic::new("test/reliable-loss", "TestMessage");

    discovery
        .register_endpoint(
            publisher_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    // Register the test UDP socket as the subscriber.
    let subscriber_participant = Participant::new(4, receiver_address);

    discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    discovery
        .register_endpoint(
            subscriber_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let transport = UdpTransport::bind(publisher_address).unwrap();

    let qos = QosPolicy::new(10, Reliability::Reliable);

    let mut publisher =
        Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

    let message = TestMessage { value: 99 };

    publisher.publish(&message).unwrap();

    assert_eq!(publisher.pending_ack_count(), 1);

    // First DATA packet arrives.
    let mut buffer = vec![0u8; 65_535];

    let (size, _) = receiver.recv_from(&mut buffer).unwrap();

    let first_message = dds_lite_rust::decode_network_message(&buffer[..size]).unwrap();

    match first_message {
        dds_lite_rust::NetworkMessage::Data(data) => {
            assert_eq!(data.sequence_number, 1);
        }
        _ => panic!("Expected DATA message"),
    }

    // Deliberately DO NOT send an ACK.
    //
    // This simulates ACK loss.

    thread::sleep(Duration::from_millis(20));

    publisher
        .retransmit_expired_acknowledgements(Duration::from_millis(10))
        .unwrap();

    // The retransmitted DATA packet should arrive.
    let (size, _) = receiver.recv_from(&mut buffer).unwrap();

    let retransmitted = dds_lite_rust::decode_network_message(&buffer[..size]).unwrap();

    match retransmitted {
        dds_lite_rust::NetworkMessage::Data(data) => {
            assert_eq!(data.sequence_number, 1);
        }
        _ => panic!("Expected retransmitted DATA message"),
    }

    // Now send the ACK manually.
    let ack = dds_lite_rust::NetworkMessage::Control(dds_lite_rust::ControlMessage::Ack {
        topic: topic.name().to_string(),
        sequence_number: 1,
    });

    let ack_bytes = dds_lite_rust::encode_network_message(&ack).unwrap();

    receiver.send_to(&ack_bytes, publisher_address).unwrap();

    publisher.receive_control().unwrap();

    assert_eq!(publisher.pending_ack_count(), 0);
}
#[test]
fn reliable_tracks_acknowledgements() {
    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    let receiver_address = receiver.local_addr().unwrap();

    let publisher_address = "127.0.0.1:7202".parse().unwrap();

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    )
    .unwrap();

    let publisher_participant = Participant::new(1, publisher_address);

    let subscriber_participant = Participant::new(2, receiver_address);

    discovery
        .register_participant(&publisher_participant)
        .unwrap();

    discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/qos", "TestMessage");

    discovery
        .register_endpoint(
            publisher_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    discovery
        .register_endpoint(
            subscriber_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let transport = UdpTransport::bind(publisher_address).unwrap();

    let qos = QosPolicy::new(10, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

    publisher.publish(&TestMessage { value: 42 }).unwrap();

    assert_eq!(publisher.pending_ack_count(), 1);
}
