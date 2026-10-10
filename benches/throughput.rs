use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use dds_lite_rust::{
    DiscoveryServer, EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy,
    Reliability, Subscriber, Topic, UdpTransport,
};
use serde::{Deserialize, Serialize};

const MESSAGE_COUNT: u64 = 10_000;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BenchmarkMessage {
    sequence: u64,
    value: u64,
}

fn main() -> io::Result<()> {
    // Start the discovery server on an available local port.
    let mut server = DiscoveryServer::bind("127.0.0.1:0".parse().unwrap())?;
    let discovery_address = server.local_addr()?;

    thread::spawn(move || {
        if let Err(error) = server.run() {
            eprintln!("Discovery server stopped: {error}");
        }
    });

    let publisher_address: SocketAddr = "127.0.0.1:7811".parse().unwrap();
    let subscriber_address: SocketAddr = "127.0.0.1:7812".parse().unwrap();

    let topic = Topic::new("benchmark/throughput", "BenchmarkMessage");

    let publisher_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_address)?;

    let subscriber_discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_address)?;

    let publisher_participant = Participant::new(1, publisher_address);
    let subscriber_participant = Participant::new(2, subscriber_address);

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

    let deadline = Instant::now() + DISCOVERY_TIMEOUT;

    loop {
        let endpoints = publisher_discovery.lookup(topic.name(), EndpointKind::Subscriber)?;

        if !endpoints.is_empty() {
            break;
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "subscriber discovery timed out",
            ));
        }

        thread::sleep(Duration::from_millis(10));
    }

    let subscriber_transport = UdpTransport::bind(subscriber_address)?;
    subscriber_transport.set_nonblocking(true)?;

    let mut subscriber = Subscriber::<BenchmarkMessage>::new(
        topic.clone(),
        subscriber_discovery,
        subscriber_transport,
        QosPolicy::new(100, Reliability::BestEffort),
    )?;

    let publisher_transport = UdpTransport::bind(publisher_address)?;
    publisher_transport.set_nonblocking(true)?;

    let mut publisher = Publisher::<BenchmarkMessage>::new(
        topic,
        publisher_discovery,
        publisher_transport,
        QosPolicy::new(100, Reliability::BestEffort),
    )?;

    // Drain incoming messages concurrently while publishing.
    let start = Instant::now();
    let mut received = 0_u64;
    let mut expected_sequence = 1_u64;

    while received < MESSAGE_COUNT {
        if expected_sequence <= MESSAGE_COUNT {
            publisher.publish(&BenchmarkMessage {
                sequence: expected_sequence,
                value: expected_sequence * 10,
            })?;

            expected_sequence += 1;
        }

        while let Some(message) = subscriber.try_receive()? {
            if message.sequence == 0 || message.sequence > MESSAGE_COUNT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "received an invalid benchmark sequence",
                ));
            }

            received += 1;
        }

        if start.elapsed() > DELIVERY_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("received {received}/{MESSAGE_COUNT} messages"),
            ));
        }
    }

    let elapsed = start.elapsed();
    let throughput = received as f64 / elapsed.as_secs_f64();

    println!("Benchmark: UDP BestEffort throughput");
    println!("Messages received: {received}");
    println!("Elapsed:           {:.3} s", elapsed.as_secs_f64());
    println!("Throughput:        {:.0} messages/s", throughput);

    Ok(())
}
