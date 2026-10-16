//! Unit tests for sequence tracking.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

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

        assert_eq!(tracker.observe(2), SequenceEvent::Contiguous);

        assert_eq!(tracker.observe(3), SequenceEvent::Contiguous);

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

        assert_eq!(tracker.observe(2), SequenceEvent::Late);

        let missing: Vec<_> = tracker.missing_sequences().copied().collect();

        assert_eq!(missing, vec![3]);
    }

    #[test]
    fn duplicate_sequence() {
        let mut tracker = SequenceTracker::new();

        tracker.observe(1);
        tracker.observe(2);

        assert_eq!(tracker.observe(2), SequenceEvent::Duplicate);

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

        assert_eq!(tracker.observe(3), SequenceEvent::Duplicate);
    }
