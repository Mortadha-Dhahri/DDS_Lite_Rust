use std::thread;
use std::time::{Duration, Instant};

use dds_lite_rust::{
    DiscoveryServer, EndpointKind, NetworkDiscovery, Participant, ParticipantRuntime, Publisher,
    QosPolicy, Reliability, Subscriber, Topic, UdpTransport,
};

#[test]
fn runtime_processes_data_and_heartbeat_without_blocking() -> Result<(), Box<dyn std::error::Error>>
{
    let discovery_server_address = "127.0.0.1:6400".parse().unwrap();

    thread::spawn(move || {
        let mut server = DiscoveryServer::bind(discovery_server_address).unwrap();
        server.run().unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let topic = Topic::new("test/runtime", "TestMessage");

    let publisher_address = "127.0.0.1:7301".parse().unwrap();
    let subscriber_address = "127.0.0.1:7302".parse().unwrap();

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

    let mut publisher = Publisher::<Vec<u8>>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();

    let subscriber_qos = QosPolicy::new(10, Reliability::BestEffort)
        .with_liveliness_timeout(Duration::from_millis(100));

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        topic,
        subscriber_discovery,
        subscriber_transport,
        subscriber_qos,
    )
    .unwrap();

    let heartbeat_runtime = ParticipantRuntime::new(
        3,
        7303,
        EndpointKind::Publisher,
        "test/runtime",
        discovery_server_address,
    )
    .unwrap()
    .with_heartbeat_interval(Duration::from_millis(20));

    let mut heartbeat_runtime = heartbeat_runtime;

    publisher.publish(&vec![1, 2, 3]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut received_data = false;
    let mut received_heartbeat = false;

    while Instant::now() < deadline {
        heartbeat_runtime.tick().unwrap();

        if subscriber.try_receive()?.is_some() {
            received_data = true;
        }

        if subscriber.liveliness_state(3) == dds_lite_rust::LivelinessState::Alive {
            received_heartbeat = true;
        }

        if received_data && received_heartbeat {
            break;
        }

        thread::sleep(Duration::from_millis(5));
    }

    assert!(received_data, "subscriber did not receive data");
    assert!(received_heartbeat, "subscriber did not receive heartbeat");

    Ok(())
}
