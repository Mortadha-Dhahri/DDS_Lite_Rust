//! Unit tests for liveliness expiry.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

    use super::*;

    #[test]
    fn observed_participant_is_alive() {
        let mut tracker = LivelinessTracker::new(Duration::from_millis(100));

        tracker.observe(42);

        assert_eq!(tracker.state(42), LivelinessState::Alive);
    }

    #[test]
    fn unknown_participant_is_expired() {
        let tracker = LivelinessTracker::new(Duration::from_millis(100));

        assert_eq!(tracker.state(42), LivelinessState::Expired);
    }

    #[test]
    fn participant_expires_after_timeout() {
        let mut tracker = LivelinessTracker::new(Duration::from_millis(10));

        tracker.observe(42);

        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(tracker.state(42), LivelinessState::Expired);
    }

    #[test]
    fn expired_participants_returns_expired_ids() {
        let mut tracker = LivelinessTracker::new(Duration::from_millis(10));

        tracker.observe(42);
        tracker.observe(43);

        std::thread::sleep(Duration::from_millis(20));

        let expired = tracker.expired_participants();

        assert!(expired.contains(&42));
        assert!(expired.contains(&43));
    }
