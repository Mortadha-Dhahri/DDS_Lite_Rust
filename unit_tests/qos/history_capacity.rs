//! Unit tests for history capacity.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

    use super::History;

    #[test]
    fn history_keeps_latest_samples() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");
        history.push(3, "three");
        history.push(4, "four");

        let sequences: Vec<_> = history
            .samples()
            .map(|entry| entry.sequence_number())
            .collect();

        assert_eq!(sequences, vec![2, 3, 4]);
    }

    #[test]
    fn zero_depth_keeps_nothing() {
        let mut history = History::new(0);

        history.push(1, "one");

        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }

    #[test]
    fn finds_sample_by_sequence_number() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");
        history.push(3, "three");

        let entry = history.find(2).unwrap();

        assert_eq!(entry.sequence_number(), 2);
        assert_eq!(entry.data(), &"two");
    }

    #[test]
    fn returns_none_for_missing_sequence() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");

        assert!(history.find(3).is_none());
    }
    #[test]
    fn evicted_sequence_is_no_longer_available() {
        let mut history = History::new(2);

        history.push(1, "one");
        history.push(2, "two");
        history.push(3, "three");

        assert!(history.find(1).is_none());
        assert!(history.find(2).is_some());
        assert!(history.find(3).is_some());
    }
