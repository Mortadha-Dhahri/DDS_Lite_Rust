use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dds_lite_rust::{NetworkDiscovery, Participant};

#[test]
fn discovery_server_unavailable_returns_error() {
    let discovery_server_address: SocketAddr = "127.0.0.1:6799".parse().unwrap();

    let discovery =
        NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server_address).unwrap();

    let participant = Participant::new(1, "127.0.0.1:7601".parse().unwrap());

    let start = Instant::now();

    let result = discovery.register_participant(&participant);

    let elapsed = start.elapsed();

    assert!(
        result.is_err(),
        "registration should fail when discovery server is unavailable"
    );

    assert!(
        elapsed >= Duration::from_millis(400),
        "discovery failed too quickly: {:?}",
        elapsed
    );

    assert!(
        elapsed < Duration::from_secs(2),
        "discovery took too long to fail: {:?}",
        elapsed
    );
}
