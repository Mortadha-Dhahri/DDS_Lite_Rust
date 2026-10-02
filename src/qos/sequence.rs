use std::collections::BTreeSet;

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
            let missing: Vec<u64> =
                ((self.last_sequence + 1)..sequence).collect();

            for sequence in &missing {
                self.missing_sequences.insert(*sequence);
            }

            self.last_sequence = sequence;

            if missing.is_empty() {
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
mod tests {
    use super::{SequenceEvent, SequenceTracker};

    #[test]
    fn first_sequence() {
        let mut tracker = SequenceTracker::new();

        let event = tracker.observe(1);

        assert_eq!(event, SequenceEvent::First);
        assert_eq!(tracker.last_sequence(), 1);
    }

    #[test]
    fn contiguous_sequences() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);

        assert_eq!(
            tracker.observe(2),
            SequenceEvent::Contiguous
        );

        assert_eq!(
            tracker.observe(3),
            SequenceEvent::Contiguous
        );

        assert!(!tracker.has_missing());
    }

    #[test]
    fn detects_gap() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);

        assert_eq!(
            tracker.observe(4),
            SequenceEvent::Gap {
                missing: vec![2, 3],
            }
        );

        assert_eq!(tracker.last_sequence(), 4);
    }

    #[test]
    fn late_sequence_fills_gap() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);
        tracker.observe(4);

        assert_eq!(
            tracker.observe(2),
            SequenceEvent::Late
        );

        let missing: Vec<_> =
            tracker.missing_sequences().copied().collect();

        assert_eq!(missing, vec![3]);
    }

    #[test]
    fn duplicate_sequence() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);
        tracker.observe(2);

        assert_eq!(
            tracker.observe(2),
            SequenceEvent::Duplicate
        );

        assert_eq!(tracker.last_sequence(), 2);
    }

    #[test]
    fn late_sequence_after_all_gaps_are_filled() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);
        tracker.observe(4);

        tracker.observe(2);
        tracker.observe(3);

        assert!(!tracker.has_missing());

        assert_eq!(
            tracker.observe(3),
            SequenceEvent::Duplicate
        );
    }
}