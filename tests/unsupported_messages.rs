use dds_lite_rust::{
    DiscoveryServer, EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy,
    Reliability, Subscriber, Topic, UdpTransport,
};

#[test]
fn unsupported_type_is_ignored_and_receiver_continues() -> Result<(), Box<dyn std::error::Error>> {
    let discovery_server_address = "127.0.0.1:6600".parse().unwrap();

    std::thread::spawn(move || {
        let mut server = DiscoveryServer::bind(discovery_server_address).unwrap();
        server.run().unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    let wrong_topic = Topic::new("test/unsupported", "WrongMessage");
    let expected_topic = Topic::new("test/unsupported", "ExpectedMessage");

    let wrong_publisher_address = "127.0.0.1:7411".parse().unwrap();
    let expected_publisher_address = "127.0.0.1:7412".parse().unwrap();
    let subscriber_address = "127.0.0.1:7413".parse().unwrap();

    let wrong_publisher_participant = Participant::new(1, wrong_publisher_address);

    let expected_publisher_participant = Participant::new(2, expected_publisher_address);

    let subscriber_participant = Participant::new(3, subscriber_address);

    let wrong_publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    let expected_publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)?;

    wrong_publisher_discovery.register_participant(&wrong_publisher_participant)?;

    expected_publisher_discovery.register_participant(&expected_publisher_participant)?;

    subscriber_discovery.register_participant(&subscriber_participant)?;

    wrong_publisher_discovery.register_endpoint(
        wrong_publisher_participant.id(),
        wrong_topic.name(),
        EndpointKind::Publisher,
    )?;

    expected_publisher_discovery.register_endpoint(
        expected_publisher_participant.id(),
        expected_topic.name(),
        EndpointKind::Publisher,
    )?;

    subscriber_discovery.register_endpoint(
        subscriber_participant.id(),
        expected_topic.name(),
        EndpointKind::Subscriber,
    )?;

    // Register the subscriber for the same topic name under the
    // wrong type as well, so the wrong-type publisher can send to it.
    subscriber_discovery.register_endpoint(
        subscriber_participant.id(),
        wrong_topic.name(),
        EndpointKind::Subscriber,
    )?;

    let wrong_publisher_transport = UdpTransport::bind(wrong_publisher_address)?;

    let expected_publisher_transport = UdpTransport::bind(expected_publisher_address)?;

    let subscriber_transport = UdpTransport::bind(subscriber_address)?;

    let mut wrong_publisher = Publisher::<Vec<u8>>::new(
        wrong_topic,
        wrong_publisher_discovery,
        wrong_publisher_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    let mut expected_publisher = Publisher::<Vec<u8>>::new(
        expected_topic.clone(),
        expected_publisher_discovery,
        expected_publisher_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        expected_topic,
        subscriber_discovery,
        subscriber_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )?;

    // Publish a valid DATA message with the wrong type.
    wrong_publisher.publish(&vec![1, 2, 3])?;

    // The subscriber must ignore the unsupported type.
    assert_eq!(subscriber.try_receive()?, None);

    // Publish a valid DATA message with the expected type.
    expected_publisher.publish(&vec![10, 20, 30])?;

    // The subscriber must continue receiving normally.
    let received = subscriber.receive()?;

    assert_eq!(received, vec![10, 20, 30]);

    Ok(())
}
