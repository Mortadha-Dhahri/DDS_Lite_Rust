//! Unit tests for control message roundtrip.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

use super::{ControlMessage, decode_control_message, encode_control_message};

#[test]
fn nack_round_trip() {
    let message = ControlMessage::Nack {
        topic: "vehicle/state".to_string(),
        missing_sequences: vec![3, 4, 5],
    };

    let encoded = encode_control_message(&message).unwrap();

    let decoded = decode_control_message(&encoded).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn empty_nack_round_trip() {
    let message = ControlMessage::Nack {
        topic: "vehicle/state".to_string(),
        missing_sequences: Vec::new(),
    };

    let encoded = encode_control_message(&message).unwrap();

    let decoded = decode_control_message(&encoded).unwrap();

    assert_eq!(decoded, message);
}
#[test]
fn ack_round_trip() {
    let message = ControlMessage::Ack {
        topic: "vehicle/state".to_string(),
        sequence_number: 42,
    };

    let encoded = encode_control_message(&message).unwrap();

    let decoded = decode_control_message(&encoded).unwrap();

    assert_eq!(decoded, message);
}
#[test]
fn heartbeat_round_trip() {
    let message = ControlMessage::Heartbeat { participant_id: 42 };

    let encoded = encode_control_message(&message).unwrap();
    let decoded = decode_control_message(&encoded).unwrap();

    assert_eq!(decoded, message);
}
