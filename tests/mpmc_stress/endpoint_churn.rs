//! Randomized sender and receiver churn checked against an exact model of
//! pending values.

use std::collections::BTreeSet;

use fanring::mpmc::{Receiver, Sender, TryRecvError, TrySendError, channel};

#[cfg(miri)]
const STATE_MACHINE_STEPS: usize = 512;
#[cfg(not(miri))]
const STATE_MACHINE_STEPS: usize = 100_000;

fn next_random(state: &mut u64) -> usize {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state as usize
}

#[test]
fn randomized_endpoint_churn_preserves_exact_reachability() {
    let mut churn = EndpointChurn::new();
    for _ in 0..STATE_MACHINE_STEPS {
        churn.step();
    }
    churn.drop_senders();
    churn.drain_pending();
    churn.assert_receivers_disconnected();
}

/// Live endpoints of one channel and the values sent but not yet received.
struct EndpointChurn {
    senders: Vec<Sender<usize>>,
    receivers: Vec<Receiver<usize>>,
    pending: BTreeSet<usize>,
    next_value: usize,
    random: u64,
}

impl EndpointChurn {
    /// Opens a channel with 65 values queued on the root sender.
    fn new() -> Self {
        let (mut root, rx0) = channel(128);
        let mut pending = BTreeSet::new();
        for value in 0..65 {
            root.try_send(value).unwrap();
            pending.insert(value);
        }
        Self {
            senders: vec![root],
            receivers: vec![rx0],
            pending,
            next_value: 65,
            random: 0xd1b5_4a32_d192_ed03,
        }
    }

    /// Returns a pseudo-random index below `len`.
    fn pick(&mut self, len: usize) -> usize {
        next_random(&mut self.random) % len
    }

    /// Applies one random action: clone or drop an endpoint, send, or
    /// receive.
    fn step(&mut self) {
        match next_random(&mut self.random) % 8 {
            0 if self.senders.len() < 8 => {
                let source = self.pick(self.senders.len());
                let tx = self.senders[source].try_clone().unwrap();
                self.senders.push(tx);
            }
            1 if self.senders.len() > 1 => {
                let sender = self.pick(self.senders.len());
                drop(self.senders.swap_remove(sender));
            }
            2 if self.receivers.len() < 8 => {
                let source = self.pick(self.receivers.len());
                let rx = self.receivers[source].clone();
                self.receivers.push(rx);
            }
            3 if self.receivers.len() > 1 => {
                let receiver = self.pick(self.receivers.len());
                drop(self.receivers.swap_remove(receiver));
            }
            4 | 5 => self.try_send_next(),
            _ => self.try_recv_pending(),
        }
    }

    /// Sends the next value on a random sender. A full lane must return the
    /// same value.
    fn try_send_next(&mut self) {
        let sender = self.pick(self.senders.len());
        match self.senders[sender].try_send(self.next_value) {
            Ok(()) => {
                assert!(self.pending.insert(self.next_value));
                self.next_value += 1;
            }
            Err(TrySendError::Full(returned)) => {
                assert_eq!(returned, self.next_value);
            }
            Err(TrySendError::Disconnected(_)) => {
                panic!("at least one receiver remains alive")
            }
        }
    }

    /// Receives on a random receiver. An empty poll while values are pending
    /// must be followed by a reachable pending value.
    fn try_recv_pending(&mut self) {
        let receiver = self.pick(self.receivers.len());
        match self.receivers[receiver].try_recv() {
            Ok(value) => assert!(self.pending.remove(&value), "duplicate value {value}"),
            Err(TryRecvError::Empty) if !self.pending.is_empty() => self.reach_pending_value(),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                panic!("at least one sender remains alive")
            }
        }
    }

    /// Asserts that random receivers reach a pending value within 128 polls.
    fn reach_pending_value(&mut self) {
        let mut reached = false;
        for _ in 0..128 {
            let receiver = self.pick(self.receivers.len());
            match self.receivers[receiver].try_recv() {
                Ok(value) => {
                    assert!(self.pending.remove(&value), "duplicate value {value}");
                    reached = true;
                    break;
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => {
                    panic!("{0} values remain unconsumed", self.pending.len())
                }
            }
        }
        assert!(reached, "{0} values remain unreachable", self.pending.len());
    }

    /// Drops every sender, in order.
    fn drop_senders(&mut self) {
        self.senders.clear();
    }

    /// Receives every pending value after the senders are gone, failing if
    /// the receivers stop making progress.
    fn drain_pending(&mut self) {
        let mut empty_polls = 0;
        while !self.pending.is_empty() {
            let mut progress = false;
            for rx in &mut self.receivers {
                match rx.try_recv() {
                    Ok(value) => {
                        assert!(self.pending.remove(&value), "duplicate value {value}");
                        progress = true;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) if !self.pending.is_empty() => {
                        panic!("{0} values remain unreachable", self.pending.len())
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            if progress {
                empty_polls = 0;
            } else {
                empty_polls += 1;
                assert!(
                    empty_polls < 1_000,
                    "{0} values remain unreachable",
                    self.pending.len()
                );
                std::thread::yield_now();
            }
        }
    }

    /// Asserts that every receiver reports disconnect with no value left.
    fn assert_receivers_disconnected(&mut self) {
        for rx in &mut self.receivers {
            loop {
                match rx.try_recv() {
                    Ok(value) => panic!("unexpected value {value}"),
                    Err(TryRecvError::Empty) => std::thread::yield_now(),
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }
    }
}
