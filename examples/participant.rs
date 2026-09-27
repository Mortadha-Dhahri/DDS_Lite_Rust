use std::env;
use std::io;
use std::net::SocketAddr;

use dds_lite_rust::{
    EndpointKind,
    NetworkDiscovery,
    Participant,
};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!(
            "Usage: cargo run --example participant -- <id> <port> <publisher|subscriber>"
        );
        std::process::exit(1);
    }

    let participant_id: u64 = args[1].parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "participant ID must be a number",
        )
    })?;

    let port: u16 = args[2].parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "port must be a number",
        )
    })?;

    let kind = match args[3].as_str() {
        "publisher" => EndpointKind::Publisher,
        "subscriber" => EndpointKind::Subscriber,
        _ => {
            eprintln!("Role must be either 'publisher' or 'subscriber'");
            std::process::exit(1);
        }
    };

    let participant_address: SocketAddr =
        format!("127.0.0.1:{port}").parse().unwrap();

    let server_address: SocketAddr =
        "127.0.0.1:6000".parse().unwrap();

    let participant =
        Participant::new(participant_id, participant_address);

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_address,
    )?;

    println!(
        "Participant {} starting on {}",
        participant.id(),
        participant.address()
    );

    discovery.register_participant(&participant)?;

    println!("Participant registered.");

    let topic = "vehicle/state";

    discovery.register_endpoint(
        participant.id(),
        topic,
        kind,
    )?;

    println!(
        "Registered {:?} endpoint for topic '{}'.",
        kind,
        topic
    );

    println!("Participant running...");

    loop {
        std::thread::park();
    }
}