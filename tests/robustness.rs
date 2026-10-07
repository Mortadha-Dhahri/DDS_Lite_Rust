use std::net::UdpSocket;
use std::time::Duration;

use dds_lite_rust::{QosPolicy, Reliability, Subscriber, Topic, UdpTransport};

#[test]
fn malformed_packet_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::new("test/robustness", "TestMessage");

    let discovery = dds_lite_rust::NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    )?;

    let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap())?;

    let mut subscriber = Subscriber::<Vec<u8>>::new(
        topic,
        discovery,
        transport,
        QosPolicy::new(10, Reliability::BestEffort)
            .with_liveliness_timeout(Duration::from_secs(1)),
    )?;

    let destination = subscriber.local_addr()?;

    let sender = UdpSocket::bind("127.0.0.1:0")?;

    sender.send_to(&[0, 1, 2, 3, 4, 5], destination)?;

    let result = subscriber.receive();

    assert!(result.is_err());

    let error = result.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    Ok(())
}