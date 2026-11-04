use std::ops::{Deref, DerefMut};

use crate::compat::Arc;
use crate::mpmc::{LaneKey, PREFETCH_LIMIT};
use crate::ready::LaneSignal;
use crate::teardown::Teardown;

pub(in crate::mpmc) struct Lane<T, P: Teardown> {
    pub(in crate::mpmc) key: LaneKey,
    pub(in crate::mpmc) signal: Arc<LaneSignal>,
    pub(in crate::mpmc) consumer: crate::ring::Consumer<T, P>,
    pub(in crate::mpmc) cached_available: usize,
    pub(in crate::mpmc) unreleased: usize,
    pub(in crate::mpmc) release_batch: usize,
}

pub(in crate::mpmc) struct LaneToken<T, P: Teardown>(Box<Lane<T, P>>);

impl<T, P: Teardown> LaneToken<T, P> {
    pub(in crate::mpmc) fn new(lane: Lane<T, P>) -> Self {
        Self(Box::new(lane))
    }
}

impl<T, P: Teardown> Deref for LaneToken<T, P> {
    type Target = Lane<T, P>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, P: Teardown> DerefMut for LaneToken<T, P> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T, P: Teardown> Lane<T, P> {
    pub(in crate::mpmc) fn new(
        key: LaneKey,
        signal: Arc<LaneSignal>,
        consumer: crate::ring::Consumer<T, P>,
    ) -> Self {
        let release_batch = consumer.capacity().min(PREFETCH_LIMIT);
        Self {
            key,
            signal,
            consumer,
            cached_available: 0,
            unreleased: 0,
            release_batch,
        }
    }

    #[inline]
    pub(super) fn release_pending(&mut self) -> bool {
        if self.unreleased == 0 {
            return false;
        }
        self.consumer.release();
        self.unreleased = 0;
        true
    }
}

pub(in crate::mpmc) enum FinishLane<T, P: Teardown> {
    Ready(LaneToken<T, P>),
    Parked,
    Retired { wake_all: bool },
}

pub(in crate::mpmc) fn close_lane<T, P: Teardown>(mut lane: LaneToken<T, P>) {
    lane.consumer.close();
    lane.signal.notify_space();
}
