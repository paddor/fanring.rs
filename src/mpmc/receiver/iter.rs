use crate::teardown::{Deferred, Teardown};

use super::Receiver;

impl<T, P: Teardown> Receiver<T, P> {
    /// Iterate until every sender disconnects and buffered values are drained.
    #[inline]
    #[must_use]
    pub const fn iter(&mut self) -> Iter<'_, T, P> {
        Iter { receiver: self }
    }

    /// Iterate over values immediately available without blocking.
    #[inline]
    #[must_use]
    pub const fn try_iter(&mut self) -> TryIter<'_, T, P> {
        TryIter { receiver: self }
    }
}

/// Blocking iterator over a borrowed receiver.
#[derive(Debug)]
pub struct Iter<'a, T, P: Teardown = Deferred> {
    receiver: &'a mut Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for Iter<'_, T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T, P: Teardown> std::iter::FusedIterator for Iter<'_, T, P> {}

/// Nonblocking iterator over a borrowed receiver.
#[derive(Debug)]
pub struct TryIter<'a, T, P: Teardown = Deferred> {
    receiver: &'a mut Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for TryIter<'_, T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.try_recv().ok()
    }
}

/// Blocking iterator that owns its receiver.
#[derive(Debug)]
pub struct IntoIter<T, P: Teardown = Deferred> {
    receiver: Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for IntoIter<T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T, P: Teardown> std::iter::FusedIterator for IntoIter<T, P> {}

#[allow(
    clippy::into_iter_without_iter,
    reason = "channel convention names the blocking iterator iter"
)]
impl<'a, T, P: Teardown> IntoIterator for &'a mut Receiver<T, P> {
    type Item = T;
    type IntoIter = Iter<'a, T, P>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T, P: Teardown> IntoIterator for Receiver<T, P> {
    type Item = T;
    type IntoIter = IntoIter<T, P>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter { receiver: self }
    }
}
