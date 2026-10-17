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


#[test]
fn heartbeat_timer_advances_while_subscriber_receives_data() {
    use std::net::UdpSocket;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    let topic = Topic::new("runtime/data-under-load", "Sample");
    let subscriber_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let subscriber_addr = subscriber_transport.local_addr().unwrap();
    let subscriber = Subscriber::<Sample>::new(
        topic.clone(),
        discovery(),
        subscriber_transport,
        QosPolicy::new(10, Reliability::BestEffort),
    )
    .unwrap();

    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let observed_count = Arc::clone(&heartbeat_count);
    let mut runtime = EndpointRuntime::new(ManagedEndpoint::Subscriber(subscriber))
        .unwrap()
        .with_heartbeat(Duration::from_millis(2), move || {
            observed_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.set_nonblocking(true).unwrap();
    let publisher_session_id = "runtime-test-session".to_string();
    let payload = dds_lite_rust::serialize_payload(&Sample { value: 7 }).unwrap();
    let wire_message = dds_lite_rust::WireMessage {
        publisher_session_id,
        sequence_number: 1,
        topic: topic.name().to_string(),
        type_name: topic.type_name().to_string(),
        payload,
    };
    let bytes = dds_lite_rust::encode_network_message(&NetworkMessage::Data(wire_message)).unwrap();

    let deadline = Instant::now() + Duration::from_millis(300);
    let mut delivered = 0usize;
    let mut next_send = Instant::now();
    while Instant::now() < deadline {
        if Instant::now() >= next_send {
            sender.send_to(&bytes, subscriber_addr).unwrap();
            next_send = Instant::now() + Duration::from_millis(1);
        }

        if runtime.poll_once().unwrap().is_some() {
            delivered += 1;
        }
        std::thread::yield_now();
    }

    assert!(delivered > 0, "runtime should deliver DATA while traffic is arriving");
    assert!(
        heartbeat_count.load(Ordering::SeqCst) >= 2,
        "heartbeat deadlines should continue advancing while DATA arrives"
    );
}


#[test]
fn managed_reliable_subscriber_delivers_data_and_sends_ack() {
    use std::net::UdpSocket;

    let topic = Topic::new("runtime/reliable-roundtrip", "Sample");
    let subscriber_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let subscriber_address = subscriber_transport.local_addr().unwrap();
    let subscriber = Subscriber::<Sample>::new(
        topic.clone(),
        discovery(),
        subscriber_transport,
        QosPolicy::new(10, Reliability::Reliable),
    )
    .unwrap();
    let mut runtime = EndpointRuntime::new(ManagedEndpoint::Subscriber(subscriber)).unwrap();

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let payload = dds_lite_rust::serialize_payload(&Sample { value: 123 }).unwrap();
    let data = NetworkMessage::Data(dds_lite_rust::WireMessage {
        publisher_session_id: "managed-reliable-session".to_string(),
        sequence_number: 1,
        topic: topic.name().to_string(),
        type_name: topic.type_name().to_string(),
        payload,
    });
    let bytes = dds_lite_rust::encode_network_message(&data).unwrap();
    sender.send_to(&bytes, subscriber_address).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    let received = loop {
        if let Some(sample) = runtime.poll_once().unwrap() {
            break Some(sample);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::yield_now();
    };
    assert_eq!(received, Some(Sample { value: 123 }));

    let mut buffer = [0u8; 2048];
    let (size, _source) = sender
        .recv_from(&mut buffer)
        .expect("reliable subscriber should send an ACK");
    match dds_lite_rust::decode_network_message(&buffer[..size]).unwrap() {
        NetworkMessage::Control(ControlMessage::Ack {
            topic: ack_topic,
            sequence_number,
        }) => {
            assert_eq!(ack_topic, topic.name());
            assert_eq!(sequence_number, 1);
        }
        other => panic!("expected ACK control message, received {other:?}"),
    }
}
