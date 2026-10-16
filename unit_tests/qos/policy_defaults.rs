//! Unit tests for policy defaults.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

    use super::*;

    #[test]
    fn default_liveliness_timeout_is_three_seconds() {
        let policy = QosPolicy::new(10, Reliability::BestEffort);

        assert_eq!(policy.liveliness_timeout(), Duration::from_secs(3));
    }

    #[test]
    fn liveliness_timeout_can_be_configured() {
        let policy = QosPolicy::new(10, Reliability::BestEffort)
            .with_liveliness_timeout(Duration::from_secs(5));

        assert_eq!(policy.liveliness_timeout(), Duration::from_secs(5));
    }
