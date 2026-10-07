use std::net::UdpSocket;

use dds_lite_rust::{
    EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy, Reliability, Subscriber,
    Topic, UdpTransport,
};

#[test]
fn malformed_packet_does_not_stop_receiver() -> Result<(), Box<dyn std::error::Error>> {
    let discovery_server_address = "127.0.0.1:6500".parse().unwrap();

    std::thread::spawn(move || {
        let mut server =
            dds_lite_rust::DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    let topic = Topic::new("test/robustness", "TestMessage");

    let publisher_address = "127.0.0.1:7401".parse().unwrap();
    let subscriber_address = "127.0.0.1:7402".parse().unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    publisher_discovery.register_participant(&publisher_participant)?;
    subscriber_discovery.register_participant(&subscriber_participant)?;

    publisher_discovery.register_endpoint(
        publisher_participant.id(),
        topic.name(),
        EndpointKind::Publisher,
    )?;

    subscriber_discovery.register_endpoint(
        subscriber_participant.id(),
        topic.name(),
        EndpointKind::Subscriber,
    )?;

    let publisher_transport = UdpTransport::bind(publisher_address)?;
    let subscriber_transport = UdpTransport::bind(subscriber_address)?;

    let mut publisher = Publisher::<Vec<u8>>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        topic,
        subscriber_discovery,
        subscriber_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    let raw_sender = UdpSocket::bind("127.0.0.1:0")?;

    raw_sender.send_to(&[0, 1, 2, 3, 4, 5], subscriber_address)?;

    publisher.publish(&vec![10, 20, 30])?;

    let received = subscriber.receive()?;

    assert_eq!(received, vec![10, 20, 30]);

    Ok(())
}