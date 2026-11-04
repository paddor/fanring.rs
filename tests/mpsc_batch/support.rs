//! Values and harnesses shared by several bulk receive modules.

use fanring::mpsc::{Receiver, Sender, TryRecvError, channel_with_policy};
use fanring::teardown::Teardown;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A value tagged with its lane and its sequence number on that lane.
pub(crate) type LaneValue = (usize, usize);

/// Asserts that it drops at most once, in its own counter.
#[derive(Debug)]
pub(crate) struct Counted {
    pub(crate) id: usize,
    pub(crate) drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        assert_eq!(self.drops[self.id].fetch_add(1, Ordering::Relaxed), 0);
    }
}

/// Two channels driven with identical sends, lane drops, and registrations,
/// so that two receive strategies can be compared value for value.
pub(crate) struct MirroredLanes<P: Teardown> {
    single: Vec<Option<Sender<LaneValue, P>>>,
    bulk: Vec<Option<Sender<LaneValue, P>>>,
    next: Vec<usize>,
    state: u64,
}

impl<P: Teardown> MirroredLanes<P> {
    /// Opens two channels with capacity 128 and four lanes each. Returns the
    /// lanes and the receivers of the single and bulk sides.
    pub(crate) fn open() -> (Self, Receiver<LaneValue, P>, Receiver<LaneValue, P>) {
        let (root_single, rx_single) = channel_with_policy::<_, P>(128);
        let (root_bulk, rx_bulk) = channel_with_policy::<_, P>(128);
        let mut lanes = Self {
            single: vec![Some(root_single)],
            bulk: vec![Some(root_bulk)],
            next: vec![0],
            state: 0x9E37_79B9_7F4A_7C15,
        };
        for _ in 1..4 {
            let (single, bulk) = lanes.clone_first_lane();
            lanes.push_lane(single, bulk);
        }
        (lanes, rx_single, rx_bulk)
    }

    /// Sends a pseudo-random number of values on every live lane of both
    /// channels. A lane stops early once it is full on both sides.
    pub(crate) fn send_round(&mut self, context: &str) {
        for lane in 0..self.single.len() {
            self.state = self
                .state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let count = ((self.state >> 33) % 90) as usize;
            let (Some(single), Some(bulk)) = (self.single[lane].as_mut(), self.bulk[lane].as_mut())
            else {
                continue;
            };
            for _ in 0..count {
                let value = (lane, self.next[lane]);
                let sent_single = single.try_send(value).is_ok();
                let sent_bulk = bulk.try_send(value).is_ok();
                assert_eq!(sent_single, sent_bulk, "{context}");
                if !sent_single {
                    break;
                }
                self.next[lane] += 1;
            }
        }
    }

    /// Drops `lane` on both channels.
    pub(crate) fn drop_lane(&mut self, lane: usize) {
        self.single[lane] = None;
        self.bulk[lane] = None;
    }

    /// Registers one more lane on both channels and checks that both get the
    /// same lane ID.
    pub(crate) fn register_lane(&mut self) {
        let (single, bulk) = self.clone_first_lane();
        assert_eq!(single.lane_id(), bulk.lane_id());
        self.push_lane(single, bulk);
    }

    /// Drops every sender, single side first, and returns how many values
    /// each channel accepted.
    pub(crate) fn disconnect(self) -> usize {
        let Self {
            single, bulk, next, ..
        } = self;
        drop(single);
        drop(bulk);
        next.iter().sum()
    }

    fn clone_first_lane(&self) -> (Sender<LaneValue, P>, Sender<LaneValue, P>) {
        let single = self.single[0].as_ref().unwrap().try_clone().unwrap();
        let bulk = self.bulk[0].as_ref().unwrap().try_clone().unwrap();
        (single, bulk)
    }

    fn push_lane(&mut self, single: Sender<LaneValue, P>, bulk: Sender<LaneValue, P>) {
        self.single.push(Some(single));
        self.bulk.push(Some(bulk));
        self.next.push(0);
    }
}

/// Values that the single and bulk receive strategies delivered, for the
/// current pass and in total.
pub(crate) struct MirroredReceives {
    pub(crate) single_batch: Vec<LaneValue>,
    pub(crate) bulk_batch: Vec<LaneValue>,
    single_sequence: Vec<LaneValue>,
    bulk_sequence: Vec<LaneValue>,
}

impl MirroredReceives {
    /// Starts with empty batches that can hold `capacity` values.
    pub(crate) fn with_batch_capacity(capacity: usize) -> Self {
        Self {
            single_batch: Vec::with_capacity(capacity),
            bulk_batch: Vec::with_capacity(capacity),
            single_sequence: Vec::new(),
            bulk_sequence: Vec::new(),
        }
    }

    /// Checks that both strategies took the same values in this pass, then
    /// moves the batches into the delivered sequences.
    pub(crate) fn commit(&mut self, single: usize, bulk: usize, context: &str) {
        assert_eq!(single, bulk, "{context}");
        assert_eq!(self.single_batch, self.bulk_batch, "{context}");
        self.single_sequence.append(&mut self.single_batch);
        self.bulk_sequence.append(&mut self.bulk_batch);
    }

    /// Returns the delivered single and bulk sequences.
    pub(crate) fn into_sequences(self) -> (Vec<LaneValue>, Vec<LaneValue>) {
        (self.single_sequence, self.bulk_sequence)
    }
}

/// Returns the count of a nonblocking receive while senders are alive:
/// empty counts as zero, disconnect fails the test.
pub(crate) fn count_while_senders_alive(result: Result<usize, TryRecvError>) -> usize {
    match result {
        Ok(count) => count,
        Err(TryRecvError::Empty) => 0,
        Err(TryRecvError::Disconnected) => panic!("senders alive"),
    }
}

/// Moves the values left on `rx` after disconnect into `sequence`. Returns
/// what a bulk drain of the mirrored channel must report.
pub(crate) fn drain_after_disconnect<P: Teardown>(
    rx: &mut Receiver<LaneValue, P>,
    sequence: &mut Vec<LaneValue>,
) -> Result<usize, TryRecvError> {
    let before = sequence.len();
    sequence.extend(rx.try_iter());
    let remaining = sequence.len() - before;
    if remaining == 0 {
        Err(TryRecvError::Disconnected)
    } else {
        Ok(remaining)
    }
}
