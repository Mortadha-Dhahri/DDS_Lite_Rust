use std::io;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{Publisher, Subscriber};

/// An endpoint owned by a cooperative, non-blocking polling loop.
///
/// The managed wrapper owns the endpoint, so callers cannot accidentally create
/// a second receiver for its UDP socket. Each call to `poll_once` handles at
/// most one incoming datagram and checks due reliability work.
pub enum ManagedEndpoint<T> {
    Publisher(Publisher<T>),
    Subscriber(Subscriber<T>),
}

/// Schedules bounded endpoint polling and publisher reliability checks.
///
/// This wrapper deliberately does not spawn a thread. The application owns the
/// loop and can stop it deterministically by dropping the wrapper.
pub struct EndpointRuntime<T> {
    endpoint: ManagedEndpoint<T>,
    reliability_timeout: Duration,
    reliability_interval: Duration,
    next_reliability_check: Instant,
    heartbeat_interval: Option<Duration>,
    next_heartbeat: Option<Instant>,
    heartbeat_action: Option<Box<dyn FnMut() -> io::Result<()> + 'static>>,
}

impl<T> EndpointRuntime<T>
where
    T: Serialize + Clone + DeserializeOwned,
{
    pub fn new(mut endpoint: ManagedEndpoint<T>) -> io::Result<Self> {
        match &mut endpoint {
            ManagedEndpoint::Publisher(publisher) => publisher.set_nonblocking(true)?,
            ManagedEndpoint::Subscriber(subscriber) => subscriber.set_nonblocking(true)?,
        }

        let reliability_interval = Duration::from_millis(100);

        Ok(Self {
            endpoint,
            reliability_timeout: Duration::from_millis(250),
            reliability_interval,
            next_reliability_check: Instant::now() + reliability_interval,
            heartbeat_interval: None,
            next_heartbeat: None,
            heartbeat_action: None,
        })
    }

    /// Configure how long a reliable delivery may wait for an ACK before retrying.
    pub fn with_reliability_timeout(mut self, timeout: Duration) -> Self {
        self.reliability_timeout = timeout;
        self
    }

    /// Configure the maximum interval between reliability checks.
    pub fn with_reliability_interval(mut self, interval: Duration) -> Self {
        self.reliability_interval = interval;
        self.next_reliability_check = Instant::now() + interval;
        self
    }

    /// Schedule a heartbeat action independently from receive polling.
    ///
    /// The action must be non-blocking: it runs on the polling thread. In
    /// particular, do not perform synchronous discovery requests in this
    /// callback. The callback can send heartbeats using an already-known peer
    /// list or enqueue work for a separate worker.
    pub fn with_heartbeat<F>(mut self, interval: Duration, action: F) -> Self
    where
        F: FnMut() -> io::Result<()> + 'static,
    {
        self.heartbeat_interval = Some(interval);
        self.next_heartbeat = Some(Instant::now() + interval);
        self.heartbeat_action = Some(Box::new(action));
        self
    }

    /// Poll at most one datagram, then run due timer actions. Returns a sample
    /// only when the managed endpoint is a subscriber.
    pub fn poll_once(&mut self) -> io::Result<Option<T>> {
        let mut first_error = self.run_due_timers().err();

        let result = match &mut self.endpoint {
            ManagedEndpoint::Publisher(publisher) => publisher.try_receive_control().map(|_| None),
            ManagedEndpoint::Subscriber(subscriber) => subscriber.try_receive(),
        };

        // Check the deadline again after polling. Even if traffic is continuous,
        // every call remains bounded to one receive attempt and timer work runs.
        if let Err(error) = self.run_due_timers() {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        result
    }

    fn run_due_timers(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let mut first_error = None;

        if now >= self.next_reliability_check {
            if let ManagedEndpoint::Publisher(publisher) = &mut self.endpoint {
                if let Err(error) = publisher.reliability_tick(self.reliability_timeout) {
                    first_error = Some(error);
                }
            }
            self.next_reliability_check = now + self.reliability_interval;
        }

        if self.next_heartbeat.is_some_and(|deadline| now >= deadline) {
            if let Some(action) = self.heartbeat_action.as_mut() {
                if let Err(error) = action() {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
            if let Some(interval) = self.heartbeat_interval {
                self.next_heartbeat = Some(now + interval);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Returns the endpoint's local UDP address.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        match &self.endpoint {
            ManagedEndpoint::Publisher(publisher) => publisher.local_addr(),
            ManagedEndpoint::Subscriber(subscriber) => subscriber.local_addr(),
        }
    }
}
