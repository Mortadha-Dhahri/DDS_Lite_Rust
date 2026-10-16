use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::Duration;

use dds_lite_rust::{EndpointKind, NetworkDiscovery, Participant};

const SERVER_ADDRESS: &str = "127.0.0.1:6000";
const SUBSCRIBER_ADDRESS: &str = "127.0.0.1:7002";
const PARTICIPANT_ID: u64 = 2;
const TOPIC_NAME: &str = "vehicle/state";

fn main() -> io::Result<()> {
    let server_address: SocketAddr = SERVER_ADDRESS.parse().unwrap();
    let subscriber_address: SocketAddr = SUBSCRIBER_ADDRESS.parse().unwrap();

    // Bind the socket, but deliberately do not create a Subscriber.
    // Consequently, no ACKs will be generated.
    let socket = UdpSocket::bind(subscriber_address)?;
    let local_address = socket.local_addr()?;

    let discovery = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address)?;

    let participant = Participant::new(PARTICIPANT_ID, local_address);

    discovery.register_participant(&participant)?;
    discovery.register_endpoint(PARTICIPANT_ID, TOPIC_NAME, EndpointKind::Subscriber)?;

    println!("Silent subscriber registered at {local_address}.");
    println!("Topic: {TOPIC_NAME}");
    println!("ACKs are intentionally disabled.");

    // Keep the participant alive while the socket waits for UDP packets.
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
                eprintln!("Heartbeat failed: {error}");
            }

            thread::sleep(Duration::from_secs(2));
        }
    });

    let mut buffer = [0u8; 65_535];

    loop {
        let (size, sender) = socket.recv_from(&mut buffer)?;
        println!("Received {size} bytes from {sender}; deliberately sending no ACK.");
    }
}
