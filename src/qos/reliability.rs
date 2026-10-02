/*
BestEffort
    │
    └── send once, no retransmission

Reliable
    │
    └── future:
         sequence numbers
         ACK/NACK
         retransmission
 */

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reliability {
    BestEffort,
    Reliable,
}
