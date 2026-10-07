use std::net::UdpSocket;
use std::time::Duration;

use dds_lite_rust::{
    ControlMessage, DiscoveryServer, EndpointKind, NetworkDiscovery, NetworkMessage, Participant,
    QosPolicy, Reliability, Subscriber, Topic, UdpTransport, WireMessage, decode_network_message,
    encode_network_message, serialize_payload,
};

fn send_data(
    socket: &UdpSocket,
    subscriber_address: std::net::SocketAddr,
    sequence_number: u64,
    topic: &Topic,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let message = NetworkMessage::Data(WireMessage {
        sequence_number,
        topic: topic.name().to_string(),
        type_name: topic.type_name().to_string(),
        payload: serialize_payload(&value)?,
    });

    let bytes = encode_network_message(&message)?;

    socket.send_to(&bytes, subscriber_address)?;

    Ok(())
}

#[test]
fn duplicate_and_late_messages_are_handled_correctly() -> Result<(), Box<dyn std::error::Error>> {
    let discovery_server_address = "127.0.0.1:6700".parse().unwrap();

    std::thread::spawn(move || {
        let mut server = DiscoveryServer::bind(discovery_server_address).unwrap();
        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let topic = Topic::new("test/sequence-robustness", "TestMessage");

    let subscriber_address = "127.0.0.1:7501".parse().unwrap();

    let subscriber_participant = Participant::new(1, subscriber_address);

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    subscriber_discovery.register_participant(&subscriber_participant)?;

    subscriber_discovery.register_endpoint(
        subscriber_participant.id(),
        topic.name(),
        EndpointKind::Subscriber,
    )?;

    let subscriber_transport = UdpTransport::bind(subscriber_address)?;

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        topic.clone(),
        subscriber_discovery,
        subscriber_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    let sender = UdpSocket::bind("127.0.0.1:0")?;

    sender.set_read_timeout(Some(Duration::from_millis(200)))?;

    // Sequence 1: first message.
    send_data(&sender, subscriber_address, 1, &topic, vec![1])?;

    assert_eq!(subscriber.receive()?, vec![1]);

    // Consume the ACK for sequence 1.
    let mut buffer = [0u8; 65_535];
    sender.recv_from(&mut buffer)?;

    // Sequence 2: contiguous message.
    send_data(&sender, subscriber_address, 2, &topic, vec![2])?;

    assert_eq!(subscriber.receive()?, vec![2]);

    // Consume the ACK for sequence 2.
    sender.recv_from(&mut buffer)?;

    // Sequence 2 again: duplicate.
    send_data(&sender, subscriber_address, 2, &topic, vec![2])?;

    assert_eq!(subscriber.try_receive()?, None);

    // A duplicate must not generate another ACK.
    assert!(
        sender.recv_from(&mut buffer).is_err(),
        "duplicate message should not generate an ACK"
    );

    // Sequence 4: sequence 3 is missing.
    send_data(&sender, subscriber_address, 4, &topic, vec![4])?;

    // The message is still delivered, but the subscriber should
    // request the missing sequence 3.
    assert_eq!(subscriber.receive()?, vec![4]);

    let (size, _) = sender.recv_from(&mut buffer)?;

    let control = decode_network_message(&buffer[..size])?;

    match control {
        NetworkMessage::Control(ControlMessage::Nack {
            topic,
            missing_sequences,
        }) => {
            assert_eq!(topic, "test/sequence-robustness");
            assert_eq!(missing_sequences, vec![3]);
        }
        other => panic!("Expected NACK, got {:?}", other),
    }

    // Sequence 3 arrives late.
    send_data(&sender, subscriber_address, 3, &topic, vec![3])?;

    assert_eq!(subscriber.receive()?, vec![3]);

    // Consume the ACK for the late message.
    sender.recv_from(&mut buffer)?;

    Ok(())
}
