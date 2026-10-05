use std::env;
use std::io;
use std::net::SocketAddr;

use dds_lite_rust::{
    EndpointKind, ParticipantRuntime, Publisher, QosPolicy, Reliability, Subscriber, Topic,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VehicleState {
    speed: f32,
    steering_angle: f32,
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!(
            "Usage: cargo run --example participant -- \
             <id> <port> <publisher|subscriber>"
        );
        std::process::exit(1);
    }

    let participant_id: u64 = args[1].parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "participant ID must be a number",
        )
    })?;

    let port: u16 = args[2]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "port must be a number"))?;

    let kind = match args[3].as_str() {
        "publisher" => EndpointKind::Publisher,
        "subscriber" => EndpointKind::Subscriber,
        _ => {
            eprintln!("Role must be either 'publisher' or 'subscriber'");
            std::process::exit(1);
        }
    };

    let topic = Topic::new("vehicle/state", "VehicleState");

    let discovery_server: SocketAddr = "127.0.0.1:6000".parse().unwrap();

    let runtime =
        ParticipantRuntime::new(participant_id, port, kind, topic.name(), discovery_server)?;

    println!(
        "Participant {} starting on {}",
        runtime.participant.id(),
        runtime.participant.address()
    );

    match kind {
        EndpointKind::Publisher => {
            let qos = QosPolicy::new(10, Reliability::BestEffort);

            let mut publisher =
                Publisher::<VehicleState>::new(topic, runtime.discovery, runtime.transport, qos)?;

            let vehicle_state = VehicleState {
                speed: 42.5,
                steering_angle: 1.2,
            };

            publisher.publish(&vehicle_state)?;

            println!("VehicleState published.");

            loop {
                publisher.receive_control()?;
            }
        }

        EndpointKind::Subscriber => {
            let mut subscriber = Subscriber::<VehicleState>::new(
                topic,
                runtime.discovery,
                runtime.transport,
                QosPolicy::new(10, Reliability::Reliable),
            )?;

            println!("Subscriber waiting for messages...");

            loop {
                let vehicle_state = subscriber.receive()?;

                println!("Received VehicleState: {:?}", vehicle_state);
            }
        }
    }
}
