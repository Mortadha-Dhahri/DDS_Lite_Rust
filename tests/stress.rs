use std::net::SocketAddr;
use std::net::UdpSocket;
use std::time::Duration;
use std::time::Instant;

use dds_lite_rust::{
    ControlMessage, DiscoveryServer, EndpointKind, NetworkDiscovery, NetworkMessage, Participant,
    Publisher, QosPolicy, Reliability, Subscriber, Topic, UdpTransport, WireMessage,
    decode_network_message, encode_network_message, serialize_payload,
};

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn run_relay_drops_range(
    relay: UdpSocket,
    publisher_address: SocketAddr,
    subscriber_address: SocketAddr,
    stop: Arc<AtomicBool>,
) {
    let mut buffer = [0u8; 65_535];
    let mut dropped_sequences = false;

    while !stop.load(Ordering::Relaxed) {
        let (size, sender) = match relay.recv_from(&mut buffer) {
            Ok(result) => result,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(error) => {
                panic!("relay receive failed: {error}");
            }
        };

        let message = match decode_network_message(&buffer[..size]) {
            Ok(message) => message,
            Err(_) => continue,
        };

        match (&message, sender) {
            (NetworkMessage::Data(data), sender) if sender == publisher_address => {
                if (21..=23).contains(&data.sequence_number) && !dropped_sequences {
                    println!("Relay: dropping sequence {}", data.sequence_number);

                    if data.sequence_number == 23 {
                        dropped_sequences = true;
                    }

                    continue;
                }

                relay.send_to(&buffer[..size], subscriber_address).unwrap();
            }

            (NetworkMessage::Control(_), sender) if sender == subscriber_address => {
                relay.send_to(&buffer[..size], publisher_address).unwrap();
            }

            _ => {}
        }
    }
}

fn run_relay(
    relay: UdpSocket,
    publisher_address: SocketAddr,
    subscriber_address: SocketAddr,
    stop: Arc<AtomicBool>,
) {
    let mut buffer = [0u8; 65_535];

    let mut dropped_sequence_25 = false;

    while !stop.load(Ordering::Relaxed) {
        let (size, sender) = match relay.recv_from(&mut buffer) {
            Ok(result) => result,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(error) => {
                panic!("relay receive failed: {error}");
            }
        };

        let message = match decode_network_message(&buffer[..size]) {
            Ok(message) => message,
            Err(_) => continue,
        };

        match (&message, sender) {
            (NetworkMessage::Data(data), sender) if sender == publisher_address => {
                if data.sequence_number == 25 && !dropped_sequence_25 {
                    println!("Relay: dropping sequence 25");
                    dropped_sequence_25 = true;
                    continue;
                }

                relay.send_to(&buffer[..size], subscriber_address).unwrap();
            }

            (NetworkMessage::Control(_), sender) if sender == subscriber_address => {
                relay.send_to(&buffer[..size], publisher_address).unwrap();
            }

            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct TestMessage {
    value: u64,
}

#[test]
fn reliable_burst_delivers_all_messages() {
    std::thread::spawn(|| {
        let mut server = DiscoveryServer::bind("127.0.0.1:6800".parse().unwrap()).unwrap();

        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let publisher_address = "127.0.0.1:7701".parse().unwrap();
    let subscriber_address = "127.0.0.1:7702".parse().unwrap();

    let publisher_discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6800".parse().unwrap(),
    )
    .unwrap();

    let subscriber_discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6800".parse().unwrap(),
    )
    .unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    publisher_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/stress", "TestMessage");

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

    let qos = QosPolicy::new(200, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        qos.clone(),
    )
    .unwrap();

    let mut subscriber =
        Subscriber::<TestMessage>::new(topic, subscriber_discovery, subscriber_transport, qos)
            .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    const MESSAGE_COUNT: u64 = 100;

    for value in 1..=MESSAGE_COUNT {
        publisher.publish(&TestMessage { value }).unwrap();
    }

    let mut received = Vec::new();

    for _ in 0..2000 {
        if let Some(message) = subscriber.try_receive().unwrap() {
            received.push(message);
        }

        if received.len() == MESSAGE_COUNT as usize {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        received.len(),
        MESSAGE_COUNT as usize,
        "subscriber should receive all burst messages"
    );

    for (index, message) in received.iter().enumerate() {
        assert_eq!(message.value, (index + 1) as u64);
    }
}

#[test]
fn burst_ignores_duplicate_packets() {
    std::thread::spawn(|| {
        let mut server = DiscoveryServer::bind("127.0.0.1:6801".parse().unwrap()).unwrap();

        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let sender_address = "127.0.0.1:7711".parse().unwrap();
    let subscriber_address = "127.0.0.1:7712".parse().unwrap();

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6801".parse().unwrap(),
    )
    .unwrap();

    let sender_participant = Participant::new(1, sender_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    discovery.register_participant(&sender_participant).unwrap();

    discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/stress-duplicates", "TestMessage");

    discovery
        .register_endpoint(
            sender_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    discovery
        .register_endpoint(
            subscriber_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let sender = UdpSocket::bind(sender_address).unwrap();
    let subscriber_transport = UdpTransport::bind(subscriber_address).unwrap();

    let mut subscriber = Subscriber::<TestMessage>::new(
        topic.clone(),
        discovery,
        subscriber_transport,
        QosPolicy::new(100, Reliability::Reliable),
    )
    .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    fn send_message(
        socket: &UdpSocket,
        destination: std::net::SocketAddr,
        topic: &Topic,
        sequence_number: u64,
    ) {
        let message = NetworkMessage::Data(WireMessage {
            sequence_number,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload: serialize_payload(&TestMessage {
                value: sequence_number,
            })
            .unwrap(),
        });

        let bytes = encode_network_message(&message).unwrap();

        socket.send_to(&bytes, destination).unwrap();
    }

    for sequence in 1..=50 {
        send_message(&sender, subscriber_address, &topic, sequence);
    }

    // Deliberately inject duplicates.
    send_message(&sender, subscriber_address, &topic, 25);
    send_message(&sender, subscriber_address, &topic, 10);

    let mut received = Vec::new();

    for _ in 0..2000 {
        if let Some(message) = subscriber.try_receive().unwrap() {
            received.push(message);
        }

        if received.len() == 50 {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        received.len(),
        50,
        "duplicate packets must not be delivered twice"
    );

    for (index, message) in received.iter().enumerate() {
        assert_eq!(message.value, (index + 1) as u64);
    }

    // The duplicate packets should have been consumed and ignored.
    assert!(subscriber.try_receive().unwrap().is_none());
}
#[test]
fn burst_detects_sequence_gap_and_sends_nack() {
    std::thread::spawn(|| {
        let mut server = DiscoveryServer::bind("127.0.0.1:6802".parse().unwrap()).unwrap();

        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let sender_address = "127.0.0.1:7721".parse().unwrap();
    let subscriber_address = "127.0.0.1:7722".parse().unwrap();

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6802".parse().unwrap(),
    )
    .unwrap();

    let sender_participant = Participant::new(1, sender_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    discovery.register_participant(&sender_participant).unwrap();

    discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/stress-gap", "TestMessage");

    discovery
        .register_endpoint(
            sender_participant.id(),
            topic.name(),
            EndpointKind::Publisher,
        )
        .unwrap();

    discovery
        .register_endpoint(
            subscriber_participant.id(),
            topic.name(),
            EndpointKind::Subscriber,
        )
        .unwrap();

    let sender = UdpSocket::bind(sender_address).unwrap();

    sender
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let subscriber_transport = UdpTransport::bind(subscriber_address).unwrap();

    let mut subscriber = Subscriber::<TestMessage>::new(
        topic.clone(),
        discovery,
        subscriber_transport,
        QosPolicy::new(100, Reliability::Reliable),
    )
    .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    fn send_message(
        socket: &UdpSocket,
        destination: std::net::SocketAddr,
        topic: &Topic,
        sequence_number: u64,
    ) {
        let message = NetworkMessage::Data(WireMessage {
            sequence_number,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload: serialize_payload(&TestMessage {
                value: sequence_number,
            })
            .unwrap(),
        });

        let bytes = encode_network_message(&message).unwrap();

        socket.send_to(&bytes, destination).unwrap();
    }

    // Send 1..=24.
    for sequence in 1..=24 {
        send_message(&sender, subscriber_address, &topic, sequence);
    }

    // Deliberately skip sequence 25.
    for sequence in 26..=50 {
        send_message(&sender, subscriber_address, &topic, sequence);
    }

    let mut received = Vec::new();

    for _ in 0..2000 {
        if let Some(message) = subscriber.try_receive().unwrap() {
            received.push(message);
        }

        if received.len() == 49 {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        received.len(),
        49,
        "subscriber should receive all messages except the missing sequence"
    );

    assert!(
        received.iter().all(|message| message.value != 25),
        "missing sequence 25 must not be delivered"
    );

    // The subscriber should have sent a NACK for sequence 25.
    let mut buffer = [0u8; 65_535];

    let mut nack_received = false;

    for _ in 0..60 {
        let (size, sender_address) = sender.recv_from(&mut buffer).unwrap();

        assert_eq!(sender_address, subscriber_address);

        let message = decode_network_message(&buffer[..size]).unwrap();

        match message {
            NetworkMessage::Control(ControlMessage::Nack {
                topic: nack_topic,
                missing_sequences,
            }) => {
                assert_eq!(nack_topic, topic.name());
                assert_eq!(missing_sequences, vec![25]);

                nack_received = true;
                break;
            }

            NetworkMessage::Control(ControlMessage::Ack { .. }) => {
                // ACKs are expected for the successfully received messages.
            }

            NetworkMessage::Control(ControlMessage::Heartbeat { .. }) => {
                // Not relevant to this test.
            }

            NetworkMessage::Data(_) => {
                // The sender socket should not receive DATA.
            }
        }
    }

    assert!(
        nack_received,
        "subscriber should send a NACK for missing sequence 25"
    );
}
#[test]
fn reliable_burst_recovers_from_dropped_packet() {
    let publisher_address: SocketAddr = "127.0.0.1:7731".parse().unwrap();
    let relay_address: SocketAddr = "127.0.0.1:7732".parse().unwrap();
    let subscriber_address: SocketAddr = "127.0.0.1:7733".parse().unwrap();
    let mut server = DiscoveryServer::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let discovery_server_address = server.local_addr().unwrap();

    std::thread::spawn(move || {
        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    publisher_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/stress-recovery", "TestMessage");

    /*
     * The publisher advertises its endpoint through the relay.
     *
     * The relay behaves as the network path between publisher and subscriber.
     */
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

    let relay = UdpSocket::bind(relay_address).unwrap();

    relay
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();

    let stop_relay = Arc::new(AtomicBool::new(false));

    let relay_thread_stop = Arc::clone(&stop_relay);

    let relay_thread = std::thread::spawn(move || {
        run_relay(
            relay,
            publisher_address,
            subscriber_address,
            relay_thread_stop,
        );
    });

    let qos = QosPolicy::new(100, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        qos.clone(),
    )
    .unwrap();

    let mut subscriber =
        Subscriber::<TestMessage>::new(topic, subscriber_discovery, subscriber_transport, qos)
            .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    /*
     * Send a burst.
     *
     * The relay will deliberately drop sequence 25.
     */
    for value in 1..=50 {
        publisher.publish(&TestMessage { value }).unwrap();
    }

    let mut received = Vec::new();

    /*
     * First receive the burst and allow the subscriber
     * to generate the NACK for sequence 25.
     */
    for _ in 0..2000 {
        if let Some(message) = subscriber.try_receive().unwrap() {
            received.push(message);
        }

        /*
         * Process the NACK at the publisher.
         *
         * The publisher's reliability logic will retransmit
         * sequence 25 from its history.
         */
        publisher.try_receive_control().unwrap();

        if received.len() == 50 {
            break;
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    stop_relay.store(true, Ordering::Relaxed);

    relay_thread.join().unwrap();

    assert_eq!(
        received.len(),
        50,
        "all 50 messages should eventually be delivered"
    );

    for (index, message) in received.iter().enumerate() {
        assert_eq!(
            message.value,
            (index + 1) as u64,
            "unexpected message at position {}",
            index
        );
    }
}
#[test]
fn reliable_recovers_from_dropped_packet() {
    let mut server = DiscoveryServer::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let discovery_server_address = server.local_addr().unwrap();

    std::thread::spawn(move || {
        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let publisher_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let subscriber_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let relay_address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let publisher_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let publisher_address = publisher_socket.local_addr().unwrap();
    drop(publisher_socket);

    let subscriber_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let subscriber_address = subscriber_socket.local_addr().unwrap();
    drop(subscriber_socket);

    let relay = UdpSocket::bind(relay_address).unwrap();
    let relay_address = relay.local_addr().unwrap();

    relay
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    let relay_stop = Arc::clone(&stop);

    let relay_thread = std::thread::spawn(move || {
        run_relay(relay, publisher_address, subscriber_address, relay_stop);
    });

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let publisher_participant = Participant::new(1, publisher_address);

    // Advertise the relay as the subscriber's endpoint.
    let subscriber_participant = Participant::new(2, relay_address);

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/reliable-drop", "TestMessage");

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
    let subscriber_transport = UdpTransport::bind(subscriber_address).unwrap();

    let subscriber_qos = QosPolicy::new(100, Reliability::Reliable);

    let mut subscriber = Subscriber::<TestMessage>::new(
        topic.clone(),
        subscriber_discovery,
        subscriber_transport,
        subscriber_qos,
    )
    .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    let publisher_transport = UdpTransport::bind(publisher_address).unwrap();
    let publisher_qos = QosPolicy::new(100, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        publisher_qos,
    )
    .unwrap();

    for sequence_number in 1..=50 {
        publisher
            .publish(&TestMessage {
                value: sequence_number,
            })
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received = Vec::new();

    while Instant::now() < deadline && received.len() < 50 {
        match subscriber.try_receive() {
            Ok(Some(message)) => {
                received.push(message.value);
            }

            Ok(None) => {
                publisher
                    .reliability_tick(Duration::from_millis(100))
                    .unwrap();
                std::thread::sleep(Duration::from_millis(1));
            }

            Err(error) => panic!("subscriber receive failed: {error}"),
        }

        publisher.try_receive_control().unwrap();

        publisher
            .reliability_tick(Duration::from_millis(100))
            .unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    relay_thread.join().unwrap();

    let mut sorted = received.clone();
    sorted.sort_unstable();

    assert_eq!(
        sorted,
        (1..=50).collect::<Vec<_>>(),
        "subscriber should recover the dropped packet and receive every message exactly once"
    );

    assert_eq!(
        received.len(),
        50,
        "subscriber should receive exactly 50 messages"
    );
}

#[test]
fn reliable_recovers_from_multiple_dropped_packets() {
    std::thread::spawn(|| {
        let mut server = DiscoveryServer::bind("127.0.0.1:6804".parse().unwrap()).unwrap();

        server.run().unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));

    let publisher_address = "127.0.0.1:7741".parse().unwrap();
    let relay_address = "127.0.0.1:7742".parse().unwrap();
    let subscriber_address = "127.0.0.1:7743".parse().unwrap();

    let relay = UdpSocket::bind(relay_address).unwrap();

    relay
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    let relay_stop = Arc::clone(&stop);

    let relay_thread = std::thread::spawn(move || {
        run_relay_drops_range(relay, publisher_address, subscriber_address, relay_stop);
    });

    let publisher_discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6804".parse().unwrap(),
    )
    .unwrap();

    let subscriber_discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6804".parse().unwrap(),
    )
    .unwrap();

    let publisher_participant = Participant::new(1, publisher_address);

    // Advertise the relay as the subscriber endpoint.
    let subscriber_participant = Participant::new(2, relay_address);

    publisher_discovery
        .register_participant(&publisher_participant)
        .unwrap();

    subscriber_discovery
        .register_participant(&subscriber_participant)
        .unwrap();

    let topic = Topic::new("test/reliable-multiple-drop", "TestMessage");

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

    let subscriber_transport = UdpTransport::bind(subscriber_address).unwrap();

    let subscriber_qos = QosPolicy::new(100, Reliability::Reliable);

    let mut subscriber = Subscriber::<TestMessage>::new(
        topic.clone(),
        subscriber_discovery,
        subscriber_transport,
        subscriber_qos,
    )
    .unwrap();

    subscriber.set_nonblocking(true).unwrap();

    let publisher_transport = UdpTransport::bind(publisher_address).unwrap();

    let publisher_qos = QosPolicy::new(100, Reliability::Reliable);

    let mut publisher = Publisher::<TestMessage>::new(
        topic.clone(),
        publisher_discovery,
        publisher_transport,
        publisher_qos,
    )
    .unwrap();

    publisher.set_nonblocking(true).unwrap();

    for sequence_number in 1..=50 {
        publisher
            .publish(&TestMessage {
                value: sequence_number,
            })
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received = Vec::new();

    while Instant::now() < deadline && received.len() < 50 {
        match subscriber.try_receive() {
            Ok(Some(message)) => {
                received.push(message.value);
            }

            Ok(None) => {
                std::thread::sleep(Duration::from_millis(1));
            }

            Err(error) => {
                panic!("subscriber receive failed: {error}");
            }
        }

        while publisher.try_receive_control().unwrap() {}

        publisher
            .reliability_tick(Duration::from_millis(100))
            .unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    relay_thread.join().unwrap();

    let mut sorted = received.clone();
    sorted.sort_unstable();

    assert_eq!(
        sorted,
        (1..=50).collect::<Vec<_>>(),
        "subscriber should recover all dropped packets exactly once"
    );

    assert_eq!(
        received.len(),
        50,
        "subscriber should receive exactly 50 messages"
    );
}
