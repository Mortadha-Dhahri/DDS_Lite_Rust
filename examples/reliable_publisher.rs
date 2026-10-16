use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use dds_lite_rust::{
    EndpointKind, NetworkDiscovery, Participant, Publisher, QosPolicy, Reliability, Topic,
    UdpTransport,
};

const SERVER_ADDRESS: &str = "127.0.0.1:6000";
const PUBLISHER_ADDRESS: &str = "127.0.0.1:7001";
const PARTICIPANT_ID: u64 = 1;
const TOPIC_NAME: &str = "vehicle/state";
const SAMPLE_COUNT: u32 = 5;

const ACK_TIMEOUT: Duration = Duration::from_millis(250);
const ACK_DEADLINE: Duration = Duration::from_secs(3);
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct VehicleState {
    sequence: u32,
    speed_kph: f32,
}

fn main() -> io::Result<()> {
    let server_address: SocketAddr = SERVER_ADDRESS.parse().unwrap();
    let publisher_address: SocketAddr = PUBLISHER_ADDRESS.parse().unwrap();

    let transport = UdpTransport::bind(publisher_address)?;
    let local_address = transport.local_addr()?;

    let discovery = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address)?;

    let participant = Participant::new(PARTICIPANT_ID, local_address);

    discovery.register_participant(&participant)?;
    discovery.register_endpoint(PARTICIPANT_ID, TOPIC_NAME, EndpointKind::Publisher)?;

    println!("Publisher registered on {local_address}.");
    println!("Waiting for a subscriber on topic '{TOPIC_NAME}'...");

    // Do not publish until discovery has found the subscriber.
    let deadline = Instant::now() + DISCOVERY_DEADLINE;

    loop {
        let subscribers = discovery.lookup(TOPIC_NAME, EndpointKind::Subscriber)?;

        if !subscribers.is_empty() {
            for subscriber in &subscribers {
                println!(
                    "Discovered subscriber {} at {}",
                    subscriber.participant_id, subscriber.address
                );
            }
            break;
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "No subscriber discovered within 10 seconds",
            ));
        }

        thread::sleep(Duration::from_millis(200));
    }

    let topic = Topic::new(TOPIC_NAME, "VehicleState");
    let qos = QosPolicy::new(10, Reliability::Reliable);

    let mut publisher = Publisher::<VehicleState>::new(topic, discovery, transport, qos)?;

    // try_receive_control() must not block when there is no ACK to read.
    publisher.set_nonblocking(true)?;

    println!("\nPublishing {SAMPLE_COUNT} samples...\n");

    for sequence in 1..=SAMPLE_COUNT {
        let state = VehicleState {
            sequence,
            speed_kph: 30.0 + sequence as f32 * 5.0,
        };

        publisher.publish(&state)?;
        let subscribers: Vec<SocketAddr> = publisher
            .discovered_subscribers()
            .iter()
            .map(|subscriber| subscriber.address)
            .collect();

        println!(
            "Published sample {}: speed = {:.1} km/h",
            state.sequence, state.speed_kph
        );

        // Keep processing ACKs and checking for expired acknowledgements
        // until this sample has been acknowledged or the deadline expires.
        let deadline = Instant::now() + ACK_DEADLINE;

        while publisher.pending_ack_count() > 0 && Instant::now() < deadline {
            publisher.try_receive_control()?;
            publisher.reliability_tick(ACK_TIMEOUT)?;
            thread::sleep(Duration::from_millis(10));
        }

        let failed = subscribers
            .iter()
            .any(|&subscriber| publisher.delivery_failed(subscriber, sequence as u64));

        if publisher.pending_ack_count() > 0 || failed {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Delivery not acknowledged by all subscribers for sample {sequence}"),
            ));
        }

        println!("ACK confirmed for sample {sequence}.\n");
    }

    println!("All {SAMPLE_COUNT} samples acknowledged.");
    Ok(())
}
