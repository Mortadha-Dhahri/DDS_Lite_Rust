use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use dds_lite_rust::{
    EndpointKind, NetworkDiscovery, Participant, QosPolicy, Reliability, Subscriber, Topic,
    UdpTransport,
};

const SERVER_ADDRESS: &str = "127.0.0.1:6000";
const SUBSCRIBER_ADDRESS: &str = "127.0.0.1:7002";
const PARTICIPANT_ID: u64 = 2;
const TOPIC_NAME: &str = "vehicle/state";
const SAMPLE_COUNT: u32 = 5;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct VehicleState {
    sequence: u32,
    speed_kph: f32,
}

fn main() -> io::Result<()> {
    let server_address: SocketAddr = SERVER_ADDRESS.parse().unwrap();
    let subscriber_address: SocketAddr = SUBSCRIBER_ADDRESS.parse().unwrap();

    // Bind the UDP transport first so discovery advertises its actual address.
    let transport = UdpTransport::bind(subscriber_address)?;
    let local_address = transport.local_addr()?;

    let discovery = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address)?;

    let participant = Participant::new(PARTICIPANT_ID, local_address);

    discovery.register_participant(&participant)?;
    discovery.register_endpoint(PARTICIPANT_ID, TOPIC_NAME, EndpointKind::Subscriber)?;

    let subscribers = discovery.lookup(TOPIC_NAME, EndpointKind::Subscriber)?;

    println!(
        "Discovery self-check: found {} subscriber(s): {:?}",
        subscribers.len(),
        subscribers
    );

    println!("Subscriber registered.");
    println!("Listening on {local_address}");
    println!("Topic: {TOPIC_NAME}");
    println!("Reliability: Reliable");
    println!("Waiting for {SAMPLE_COUNT} samples...\n");

    // Keep the participant alive while receive() blocks waiting for data.
    thread::spawn(move || {
        let heartbeat_discovery =
            match NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address) {
                Ok(discovery) => discovery,
                Err(error) => {
                    eprintln!("Failed to create heartbeat client: {error}");
                    return;
                }
            };

        loop {
            if let Err(error) = heartbeat_discovery.heartbeat(PARTICIPANT_ID) {
                eprintln!("Subscriber heartbeat failed: {error}");
            }

            thread::sleep(Duration::from_secs(2));
        }
    });

    let topic = Topic::new(TOPIC_NAME, "VehicleState");
    let qos = QosPolicy::new(10, Reliability::Reliable);

    let mut subscriber = Subscriber::<VehicleState>::new(topic, discovery, transport, qos)?;

    for _ in 0..SAMPLE_COUNT {
        // receive() also processes control messages while waiting for data.
        let state = subscriber.receive()?;

        println!(
            "Received sample {}: speed = {:.1} km/h",
            state.sequence, state.speed_kph
        );
    }

    println!("\nSubscriber received all {SAMPLE_COUNT} samples.");
    Ok(())
}
