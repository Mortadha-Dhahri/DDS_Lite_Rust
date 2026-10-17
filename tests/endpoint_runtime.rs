use std::time::{Duration, Instant};

use dds_lite_rust::{
    EndpointRuntime, ManagedEndpoint, NetworkDiscovery, Publisher, QosPolicy, Reliability,
    Subscriber, Topic, UdpTransport,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Sample {
    value: u32,
}

fn discovery() -> NetworkDiscovery {
    NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    )
    .unwrap()
}

#[test]
fn managed_subscriber_poll_returns_when_idle() {
    let topic = Topic::new("runtime/idle", "Sample");
    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let subscriber = Subscriber::<Sample>::new(
        topic,
        discovery(),
        transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();
    let mut runtime =
        EndpointRuntime::new(ManagedEndpoint::Subscriber(subscriber)).unwrap();

    let started = Instant::now();
    assert!(runtime.poll_once().unwrap().is_none());
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "managed poll should not block when no datagram is available"
    );
}

#[test]
fn managed_publisher_poll_returns_when_idle() {
    let topic = Topic::new("runtime/idle-publisher", "Sample");
    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let publisher = Publisher::<Sample>::new(
        topic,
        discovery(),
        transport,
        QosPolicy::new(10, Reliability::Reliable),
    )
    .unwrap();
    let mut runtime = EndpointRuntime::new(ManagedEndpoint::Publisher(publisher)).unwrap();

    let started = Instant::now();
    assert!(runtime.poll_once().unwrap().is_none());
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "managed poll should not block when no datagram is available"
    );
}
