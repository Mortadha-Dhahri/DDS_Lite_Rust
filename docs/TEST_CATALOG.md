# Test catalog

This catalog covers the named tests under `unit_tests/` and `tests/`. Unit tests are attached to implementation modules through `#[path]` so they can inspect private invariants without widening production visibility.

Run all tests with `cargo test --all-targets`. Integration tests use real local sockets and exercise multiple components; unit tests focus on individual state transitions.

## Unit tests

### `unit_tests/reliability/retry_and_acknowledgement.rs`

| Test | Purpose / invariant |
|---|---|
| `expired_acknowledgement_is_retransmitted` | An unacknowledged reliable sample remains eligible for retry after timeout. |
| `best_effort_does_not_track_acknowledgements` | Best-effort mode creates no pending ACK state. |
| `best_effort_ignores_nack` | NACK does not trigger retransmission in best-effort mode. |
| `late_and_duplicate_acknowledgements_preserve_pending_state` | Late/duplicate ACKs do not corrupt unrelated delivery state. |
| `nack_for_missing_history_entry_does_not_block_available_retransmission` | An evicted sample does not prevent another retained sample from being resent. |
| `retransmission_stops_after_maximum_attempts` | Retry attempts are bounded. |
| `retransmission_limit_is_independent_per_subscriber` | Retry budgets are isolated per subscriber. |
| `missing_history_removes_only_the_unretransmittable_pending_ack` | Cleanup affects only the unavailable sample. |
| `acknowledgement_from_unknown_subscriber_preserves_pending_state` | An unknown sender cannot clear another subscriber's ACK state. |
| `acknowledgement_clears_only_the_matching_subscribers_pending_state` | ACKs match subscriber and sequence. |
| `only_subscriber_with_pending_ack_receives_retransmission` | Retransmission is directed to the subscriber with the relevant state. |
| `reliable_delivery_recovers_a_dropped_sample` | NACK recovers a sample still in history. |
| `publishing_and_acknowledgement_clear_pending_state` | Normal publish/ACK path creates then clears pending state. |
| `nack_retransmissions_respect_maximum_attempts` | Repeated NACKs cannot exceed the retry limit. |
| `nack_cannot_reset_retry_budget_after_pending_ack_expires` | Pending-ACK cleanup does not reset retry budget. |
| `acknowledgement_clears_retry_state_after_pending_ack_is_removed` | Late ACK clears residual retry state. |
| `publisher_heartbeat_is_handled_without_panicking` | Heartbeat control is handled safely by publisher. |
| `send_failure_for_one_subscriber_does_not_block_other_subscribers` | Fan-out continues after an individual send failure. |

Flow: arrange publisher/history/pending state and local sockets → inject ACK/NACK from a known or unknown sender → inspect datagrams and bookkeeping → assert the intended state changes only.

### `unit_tests/reliability/receive_semantics.rs`

| Test | Purpose / invariant |
|---|---|
| `heartbeat_marks_participant_alive` | Heartbeat updates subscriber liveliness. |
| `duplicate_reliable_message_is_acknowledged_without_redelivery` | Duplicate reliable data is ACKed but not delivered twice. |
| `messages_from_different_publishers_have_independent_sequences` | Sequence state is isolated per publisher stream. |
| `corrupt_payload_does_not_advance_sequence_tracker` | Invalid payload does not advance sequence state. |
| `sequence_one_after_high_water_mark_starts_a_new_stream` | Exercises restart/session handling; retired-session packets must not reactivate old state. |

Flow: construct subscriber → inject data/control from a sender/session → run receive handling → inspect returned value, ACK/NACK, sequence state, and liveliness.

### `unit_tests/discovery/endpoint_registry.rs`

| Test | Purpose / invariant |
|---|---|
| `removing_participant_cleans_up_its_endpoints` | Participant removal cleans only its own endpoints. |
| `duplicate_participant_and_endpoint_registration_is_idempotent` | Repeated registrations do not duplicate registry records. |

### `unit_tests/discovery/server_lifecycle.rs`

| Test | Purpose / invariant |
|---|---|
| `heartbeat_keeps_participant_alive` | Known heartbeat refreshes expiry deadline. |
| `expired_participant_and_endpoints_are_removed` | Expired participant and endpoints are removed. |
| `heartbeat_from_unknown_participant_is_rejected` | Heartbeat does not implicitly register an unknown participant. |
| `expiration_removes_only_inactive_participant` | Expiry is scoped per participant. |
| `malformed_datagram_does_not_stop_discovery_server` | Malformed request does not kill the server loop. |

### `unit_tests/qos/history_capacity.rs`

| Test | Purpose / invariant |
|---|---|
| `history_keeps_latest_samples` | Retains latest entries up to configured depth. |
| `zero_depth_keeps_nothing` | Depth zero stores no samples. |
| `finds_sample_by_sequence_number` | Retained sample is found by sequence. |
| `returns_none_for_missing_sequence` | Missing sequence returns no entry. |
| `evicted_sequence_is_no_longer_available` | Evicted samples cannot be retransmitted from history. |

### `unit_tests/qos/liveliness_expiry.rs`

| Test | Purpose / invariant |
|---|---|
| `observed_participant_is_alive` | Recently observed participant is alive. |
| `unknown_participant_is_expired` | Unknown participant does not default to alive. |
| `participant_expires_after_timeout` | Participant expires after configured timeout. |
| `expired_participants_returns_expired_ids` | Expiry scan returns inactive participant IDs. |

### `unit_tests/qos/sequence_tracking.rs`

| Test | Purpose / invariant |
|---|---|
| `first_sequence` | Initializes sequence state. |
| `contiguous_sequences` | Contiguous samples create no gaps. |
| `detects_gap` | Forward jump identifies missing sequence numbers. |
| `late_sequence_fills_gap` | Late sample removes a missing sequence. |
| `duplicate_sequence` | Duplicate is classified correctly. |
| `late_sequence_after_all_gaps_are_filled` | Tracker remains consistent after repair. |
| `huge_gap_is_bounded` | Missing state is capped at 128 entries. |
| `tracker_can_reset_for_a_new_publisher_stream` | New stream can start with fresh sequence state. |

### `unit_tests/qos/policy_defaults.rs`

| Test | Purpose / invariant |
|---|---|
| `default_liveliness_timeout_is_three_seconds` | Confirms default timeout. |
| `liveliness_timeout_can_be_configured` | Confirms timeout override. |

### `unit_tests/serialization/control_message_roundtrip.rs`

| Test | Purpose / invariant |
|---|---|
| `nack_round_trip` | NACK fields survive encoding/decoding. |
| `empty_nack_round_trip` | Empty missing-sequence list is supported. |
| `ack_round_trip` | ACK fields survive encoding/decoding. |
| `heartbeat_round_trip` | Heartbeat participant ID survives encoding/decoding. |

## Integration tests

### `tests/reliable_delivery.rs`

| Test | Purpose / flow |
|---|---|
| `reliable_delivery_clears_pending_ack` | Publish over UDP, receive/ACK, confirm pending state clears. |
| `reliable_delivery_recovers_from_lost_ack` | Lost ACK triggers retry and eventual reconciliation. |
| `reliable_tracks_acknowledgements` | Reliable path records outstanding deliveries. |
| `reliable_responds_to_nack` | NACK causes retained sample retransmission. |
| `best_effort_does_not_retransmit_after_timeout` | Best-effort bypasses reliable retry. |
| `reliable_process_reliability_retransmits_expired_ack` | Reliability processing retries expired pending ACKs. |
| `late_ack_clears_delivery_failure_after_retry_exhaustion` | Late ACK reconciles state after retry exhaustion. |
| `reliable_delivery_recovers_from_a_sequence_gap_end_to_end` | Missing sequence is detected, NACKed, retransmitted, and received across components. |

### Other integration tests

| File / test | Purpose / flow |
|---|---|
| `tests/subscriber_cache.rs::publisher_refresh_discovers_late_subscriber` | Register a subscriber after publisher startup; validate discovery refresh finds it. |
| `tests/liveliness.rs::subscriber_detects_publisher_liveliness_expiration` | Stop publisher heartbeats and verify subscriber-side expiry. |
| `tests/discovery_liveliness.rs::expired_participant_is_removed_while_active_participant_remains` | Expire one participant while another remains active. |
| `tests/discovery_liveliness.rs::publisher_stops_sending_to_expired_subscriber_after_refresh` | Refresh discovery after subscriber expiry and verify it is no longer targeted. |
| `tests/runtime_concurrency.rs::runtime_processes_data_and_heartbeat_without_blocking` | Check data handling and heartbeat processing do not starve each other. |
| `tests/stress.rs::reliable_burst_delivers_all_messages` | Burst delivery preserves expected messages. |
| `tests/stress.rs::burst_ignores_duplicate_packets` | Duplicate burst packets are not delivered as new data. |
| `tests/stress.rs::burst_detects_sequence_gap_and_sends_nack` | Burst gap produces NACK. |
| `tests/stress.rs::reliable_burst_recovers_from_dropped_packet` | Recovery works during a burst. |
| `tests/stress.rs::reliable_recovers_from_dropped_packet` | Single dropped sample is recovered. |
| `tests/stress.rs::reliable_recovers_from_multiple_dropped_packets` | Multiple dropped samples are recovered within configured bounds. |
| `tests/robustness.rs::malformed_packet_does_not_stop_receiver` | Malformed bytes do not stop later receive processing. |
| `tests/sequence_robustness.rs::duplicate_and_late_messages_are_handled_correctly` | Duplicate/late traffic does not corrupt sequence behavior. |
| `tests/discover_failure.rs::discovery_server_unavailable_returns_error` | Unavailable discovery server is surfaced as an error. |
| `tests/unsupported_messages.rs::unsupported_type_is_ignored_and_receiver_continues` | Undecodable payload is ignored; receiver continues. |
| `tests/transport_robustness.rs::nonblocking_receive_returns_none_when_no_packet_is_available` | Empty nonblocking socket returns `Ok(None)`. |
| `tests/transport_robustness.rs::bind_returns_error_when_address_is_already_in_use` | Bind failure is propagated. |

## Commands and interpretation

```bash
cargo test --all-targets
cargo test --lib
cargo test --test reliable_delivery
cargo test <substring-of-test-name>
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
git diff --check
```

Unit failures usually point to a local invariant; integration failures may involve sockets, discovery, timing, or component interaction. Tests using fixed ports can conflict with other processes. A passing suite validates these selected cases, not full DDS/RTPS conformance or delivery under arbitrary packet loss.
