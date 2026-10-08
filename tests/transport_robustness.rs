use std::net::SocketAddr;

use dds_lite_rust::UdpTransport;

#[test]
fn nonblocking_receive_returns_none_when_no_packet_is_available() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport = UdpTransport::bind(address).unwrap();

    transport.set_nonblocking(true).unwrap();

    let result = transport.try_receive().unwrap();

    assert!(
        result.is_none(),
        "non-blocking receive should return None when no packet is available"
    );
}

#[test]
fn bind_returns_error_when_address_is_already_in_use() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let first = UdpTransport::bind(address).unwrap();
    let bound_address = first.local_addr().unwrap();

    let result = UdpTransport::bind(bound_address);

    assert!(
        result.is_err(),
        "binding an already-used UDP address should return an error"
    );
}
