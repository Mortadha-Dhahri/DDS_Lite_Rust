use std::collections::BTreeSet;

/// Maximum number of missing sequence numbers tracked and sent in a NACK.
pub const MAX_TRACKED_GAP: u64 = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceEvent {
    First,
    Contiguous,
    Gap { missing: Vec<u64> },
    Late,
    Duplicate,
}

#[derive(Debug, Default)]
pub struct SequenceTracker {
    last_sequence: u64,
    missing_sequences: BTreeSet<u64>,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, sequence: u64) -> SequenceEvent {
        // First sequence received.
        if self.last_sequence == 0 {
            self.last_sequence = sequence;
            return SequenceEvent::First;
        }

        // A sequence that was previously missing arrived.
        if self.missing_sequences.contains(&sequence) {
            self.missing_sequences.remove(&sequence);
            return SequenceEvent::Late;
        }

        // A new sequence arrived after the current one.
        if sequence > self.last_sequence {
            let gap_start = self.last_sequence.saturating_add(1);
            let gap_size = sequence.saturating_sub(gap_start);

            // Keep the most recent missing samples only. Never allocate proportional
            // to an untrusted sequence jump.
            let tracked_start = if gap_size > MAX_TRACKED_GAP {
                sequence.saturating_sub(MAX_TRACKED_GAP)
            } else {
                gap_start
            };
            let missing: Vec<u64> = (tracked_start..sequence).collect();

            for missing_sequence in &missing {
                self.missing_sequences.insert(*missing_sequence);
            }

            // Bound retained state too: older gaps can otherwise accumulate forever.
            while self.missing_sequences.len() > MAX_TRACKED_GAP as usize {
                if let Some(oldest) = self.missing_sequences.iter().next().copied() {
                    self.missing_sequences.remove(&oldest);
                } else {
                    break;
                }
            }

            self.last_sequence = sequence;

            if gap_size == 0 {
                SequenceEvent::Contiguous
            } else {
                SequenceEvent::Gap { missing }
            }
        } else {
            // The sequence was already processed.
            SequenceEvent::Duplicate
        }
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn missing_sequences(&self) -> impl Iterator<Item = &u64> {
        self.missing_sequences.iter()
    }

    pub fn has_missing(&self) -> bool {
        !self.missing_sequences.is_empty()
    }
}

#[cfg(test)]
#[path = "../../unit_tests/qos/sequence_tracking.rs"]
mod sequence_tracking_tests;
