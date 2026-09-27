use std::io;
use std::net::SocketAddr;

use dds_lite_rust::DiscoveryServer;

fn main() -> io::Result<()> {
    let address: SocketAddr =
        "127.0.0.1:6000".parse().unwrap();

    let mut server = DiscoveryServer::bind(address)?;

    println!(
        "Discovery server listening on {}",
        server.local_addr()?
    );

    server.run()
}