use std::io;
use std::net::SocketAddr;

use dds_lite_rust::{
    EndpointKind,
    NetworkDiscovery,
    Participant,
};

fn main() -> io::Result<()> {
    let server_address: SocketAddr =
        "127.0.0.1:6000".parse().unwrap();

    let participant_a = Participant::new(
        1,
        "127.0.0.1:7001".parse().unwrap(),
    );

    let participant_b = Participant::new(
        2,
        "127.0.0.1:7002".parse().unwrap(),
    );

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_address,
    )?;

    println!("Registering participants...");

    discovery.register_participant(&participant_a)?;
    discovery.register_participant(&participant_b)?;

    println!("Participants registered.");

    println!("Registering endpoints...");

    discovery.register_endpoint(
        participant_a.id(),
        "vehicle/state",
        EndpointKind::Publisher,
    )?;

    discovery.register_endpoint(
        participant_b.id(),
        "vehicle/state",
        EndpointKind::Subscriber,
    )?;

    println!("Endpoints registered.");

    println!();
    println!("Looking up subscribers for vehicle/state...");

    let subscribers = discovery.lookup(
        "vehicle/state",
        EndpointKind::Subscriber,
    )?;

    println!();
    println!("Discovered subscribers:");

    for subscriber in subscribers {
        println!("{subscriber:#?}");
    }

    Ok(())
}