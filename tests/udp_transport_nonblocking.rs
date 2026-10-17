use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use dds_lite_rust::UdpTransport;

fn localhost_ephemeral_addr() -> SocketAddr {
    "127.0.0.1:0".parse().expect("valid localhost socket address")
}

#[test]
fn try_receive_returns_none_when_nonblocking_socket_is_idle() {
    let transport = UdpTransport::bind(localhost_ephemeral_addr()).expect("bind transport");
    transport
        .set_nonblocking(true)
        .expect("set transport to non-blocking");

    let started = Instant::now();
    let received = transport.try_receive().expect("poll receive");

    assert!(received.is_none(), "no datagram should be available");
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "non-blocking receive should return promptly"
    );
}

#[test]
fn try_receive_reads_a_queued_datagram() {
    let transport = UdpTransport::bind(localhost_ephemeral_addr()).expect("bind transport");
    transport
        .set_nonblocking(true)
        .expect("set transport to non-blocking");
    let destination = transport.local_addr().expect("transport local address");

    let sender = UdpSocket::bind(localhost_ephemeral_addr()).expect("bind sender");
    let payload = b"queued datagram";
    sender
        .send_to(payload, destination)
        .expect("send datagram to transport");

    // Loopback delivery can be scheduled just after send_to returns. Poll briefly
    // rather than making the test depend on a fixed sleep.
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if let Some((received, source)) = transport.try_receive().expect("poll receive") {
            assert_eq!(received, payload);
            assert_eq!(source, sender.local_addr().expect("sender local address"));
            break;
        }

        assert!(
            Instant::now() < deadline,
            "queued datagram was not received before the deadline"
        );
        std::thread::yield_now();
    }
}
