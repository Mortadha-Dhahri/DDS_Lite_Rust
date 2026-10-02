use std::time::Duration;

use dds_lite_rust::{
    NetworkDiscovery, Participant, Publisher, QosPolicy, Reliability, Topic, UdpTransport,
};

fn main() -> std::io::Result<()> {
    let participant = Participant::new(1, "127.0.0.1:7001".parse().unwrap());

    let transport = UdpTransport::bind(participant.address())?;

    let discovery = NetworkDiscovery::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    )?;

    discovery.register_participant(&participant)?;
    discovery.register_endpoint(
        participant.id(),
        "vehicle/state",
        dds_lite_rust::EndpointKind::Publisher,
    )?;

    let topic = Topic::new("vehicle/state", "VehicleState");

    let qos = QosPolicy::new(10, Reliability::BestEffort);

    let mut publisher = Publisher::<String>::new(topic, discovery, transport, qos)?;

    publisher.publish(&"hello".to_string())?;

    std::thread::sleep(Duration::from_millis(200));

    publisher.retransmit_expired_acknowledgements(Duration::from_millis(100))?;

    Ok(())
}
