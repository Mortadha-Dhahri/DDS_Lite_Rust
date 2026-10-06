use std::thread;
use std::time::{Duration, Instant};

use dds_lite_rust::{
    EndpointKind, NetworkDiscovery, Participant, ParticipantRuntime, Publisher, QosPolicy,
    Reliability, Subscriber, Topic, UdpTransport,
};

#[test]
fn runtime_processes_data_and_control_without_blocking() {
    let discovery_server_address = "127.0.0.1:6400".parse().unwrap();

    thread::spawn(move || {
        let mut server =
            dds_lite_rust::DiscoveryServer::bind(discovery_server_address).unwrap();

        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let topic = Topic::new("test/runtime", "TestMessage");

    let publisher_address = "127.0.0.1:7301".parse().unwrap();
    let subscriber_address = "127.0.0.1:7302".parse().unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)
            .unwrap();

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address)
            .unwrap();

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

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

    publisher_transport.set_nonblocking(true).unwrap();
    subscriber_transport.set_nonblocking(true).unwrap();

    let publisher = Publisher::<Vec<u8>>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        QosPolicy::new(10, Reliability::Reliable),
    )
    .unwrap();

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        topic,
        subscriber_discovery,
        subscriber_transport,
        QosPolicy::new(10, Reliability::Reliable),
    )
    .unwrap();

    let mut publisher = publisher;

    publisher.publish(vec![1, 2, 3]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut received = false;

    while Instant::now() < deadline {
        if subscriber.try_receive()?.is_some() {
            received = true;
            break;
        }

        thread::sleep(Duration::from_millis(5));
    }

    assert!(received, "subscriber did not receive data");

    let mut runtime = ParticipantRuntime::new(
        1,
        7301,
        EndpointKind::Publisher,
        "test/runtime",
        discovery_server_address,
    )
    .unwrap()
    .with_heartbeat_interval(Duration::from_millis(20));

    runtime.poll().unwrap();

    let control_deadline = Instant::now() + Duration::from_secs(1);

    while Instant::now() < control_deadline {
        if subscriber.try_receive()?.is_none() {
            thread::sleep(Duration::from_millis(5));
            continue;
        }

        break;
    }

    let _ = &mut publisher;
}