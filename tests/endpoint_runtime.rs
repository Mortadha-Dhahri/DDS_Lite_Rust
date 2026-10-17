use std::time::{Duration, Instant};

use dds_lite_rust::{
    EndpointRuntime, ManagedEndpoint, NetworkDiscovery, Publisher, QosPolicy, Reliability,
    Subscriber, Topic, UdpTransport, ControlMessage, NetworkMessage, decode_network_message,
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


#[test]
fn managed_runtime_runs_heartbeat_action_when_due() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    let topic = Topic::new("runtime/heartbeat", "Sample");
    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let subscriber = Subscriber::<Sample>::new(
        topic,
        discovery(),
        transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();

    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let observed_count = Arc::clone(&heartbeat_count);
    let mut runtime = EndpointRuntime::new(ManagedEndpoint::Subscriber(subscriber))
        .unwrap()
        .with_heartbeat(Duration::from_millis(5), move || {
            observed_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

    let deadline = Instant::now() + Duration::from_millis(250);
    while Instant::now() < deadline && heartbeat_count.load(Ordering::SeqCst) == 0 {
        assert!(runtime.poll_once().unwrap().is_none());
        std::thread::yield_now();
    }

    assert!(
        heartbeat_count.load(Ordering::SeqCst) >= 1,
        "heartbeat action should run once its deadline is due"
    );
}


#[test]
fn managed_runtime_sends_heartbeat_to_known_peer() {
    use std::net::UdpSocket;

    let topic = Topic::new("runtime/heartbeat-wire", "Sample");
    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let subscriber = Subscriber::<Sample>::new(
        topic,
        discovery(),
        transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();

    let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.set_nonblocking(true).unwrap();
    let peer_addr = peer.local_addr().unwrap();
    let participant_id = 42;

    let mut runtime = EndpointRuntime::new(ManagedEndpoint::Subscriber(subscriber))
        .unwrap()
        .with_heartbeat_peer(Duration::from_millis(5), participant_id, peer_addr);

    let deadline = Instant::now() + Duration::from_millis(250);
    let mut received = None;
    while Instant::now() < deadline {
        assert!(runtime.poll_once().unwrap().is_none());
        let mut buffer = [0u8; 2048];
        match peer.recv_from(&mut buffer) {
            Ok((size, _sender)) => {
                received = Some(decode_network_message(&buffer[..size]).unwrap());
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::yield_now();
            }
            Err(error) => panic!("failed to receive heartbeat: {error}"),
        }
    }

    assert!(
        matches!(
            received,
            Some(NetworkMessage::Control(ControlMessage::Heartbeat { participant_id: id }))
                if id == participant_id
        ),
        "expected a heartbeat control message from participant {participant_id}"
    );
}
