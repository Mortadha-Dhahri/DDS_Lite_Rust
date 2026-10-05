use std::thread;
use std::time::Duration;

use dds_lite_rust::{
    ControlMessage, DiscoveryServer, EndpointKind, NetworkDiscovery, NetworkMessage, Participant,
    QosPolicy, Reliability, Subscriber, Topic, Transport, UdpTransport, encode_network_message,
};

#[test]
fn subscriber_detects_publisher_liveliness_expiration() {
    let discovery_server_address = "127.0.0.1:6300".parse().unwrap();

    thread::spawn(move || {
        let mut server = DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let publisher_address = "127.0.0.1:7201".parse().unwrap();
    let subscriber_address = "127.0.0.1:7202".parse().unwrap();

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

    let topic = Topic::new("test/liveliness", "TestMessage");

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

    let qos = QosPolicy::new(10, Reliability::BestEffort)
        .with_liveliness_timeout(Duration::from_millis(100));

    let mut subscriber =
        Subscriber::<Vec<u8>>::new(topic, subscriber_discovery, subscriber_transport, qos).unwrap();

    let heartbeat = NetworkMessage::Control(ControlMessage::Heartbeat {
        participant_id: publisher_participant.id(),
    });

    let heartbeat_bytes = encode_network_message(&heartbeat).unwrap();

    publisher_transport
        .send(&heartbeat_bytes, subscriber_participant.address())
        .unwrap();

    subscriber.receive_control().unwrap();

    assert_eq!(
        subscriber.liveliness_state(publisher_participant.id()),
        dds_lite_rust::LivelinessState::Alive
    );

    thread::sleep(Duration::from_millis(150));

    assert_eq!(
        subscriber.liveliness_state(publisher_participant.id()),
        dds_lite_rust::LivelinessState::Expired
    );
}
