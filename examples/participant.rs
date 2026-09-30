use std::env;
use std::io;
use std::net::SocketAddr;

use dds_lite_rust::{
    decode_message,
    deserialize_payload,
    encode_message,
    serialize_payload,
    EndpointKind,
    NetworkDiscovery,
    Participant,
    Transport,
    UdpTransport,
    WireMessage,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct VehicleState {
    speed: f32,
    steering_angle: f32,
}

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

    let transport = UdpTransport::bind(participant_address)?;

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_address,
    )?;

    println!(
        "Participant {} starting on {}",
        participant.id(),
        participant.address()
    );

    println!(
        "Data transport listening on {}",
        transport.local_addr()?
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

    if kind == EndpointKind::Publisher {
        println!();
        println!("Looking up subscribers...");

        let subscribers = discovery.lookup(
            topic,
            EndpointKind::Subscriber,
        )?;

        println!("Discovered subscribers:");

        for subscriber in &subscribers {
            println!(
                "Participant {} at {}",
                subscriber.participant_id,
                subscriber.address
            );
        }

        if let Some(subscriber) = subscribers.first() {

            let vehicle_state = VehicleState {
                speed: 42.5,
                steering_angle: 1.2,
            };

            let payload = serialize_payload(&vehicle_state)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        error,
                    )
                })?;

            let message = WireMessage {
                topic: topic.to_string(),
                type_name: "VehicleState".to_string(),
                payload,
            };

            let bytes = encode_message(&message)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        error,
                    )
                })?;

            transport.send(&bytes, subscriber.address)?;

            println!();
            println!(
                "Sent VehicleState to {}",
                subscriber.address
            );

            println!();

        } else {
            println!("No subscribers discovered.");
        }
    }

    if kind == EndpointKind::Subscriber {
        println!();
        println!("Waiting for data...");

        loop {

            let (data, sender) = transport.receive()?;

            let message = decode_message(&data)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        error,
                    )
                })?;

            println!();
            println!("Received message from {}", sender);
            println!("Topic: {}", message.topic);
            println!("Type: {}", message.type_name);

            let vehicle_state: VehicleState =
                deserialize_payload(&message.payload)
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            error,
                        )
                    })?;

            println!("Data: {vehicle_state:?}");
    }
}
    Ok(())
}