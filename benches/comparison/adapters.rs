//! Uniform send and receive adapters over the compared MPSC channels.

use std::sync::Arc;

pub(crate) enum SendAttempt {
    Sent,
    Full,
    Disconnected,
}

pub(crate) enum RecvAttempt<T> {
    Item(T),
    Empty,
    Disconnected,
}

pub(crate) trait BenchSender<T>: Send + 'static {
    fn try_send(&mut self, value: T) -> SendAttempt;
    fn send(&mut self, value: T) -> bool;
}

pub(crate) trait BenchReceiver<T> {
    fn try_recv(&mut self) -> RecvAttempt<T>;
    fn recv(&mut self) -> Option<T>;
    fn close_for_drain(&mut self) {}
}

pub(crate) struct ConcurrentSender<T>(pub(crate) Arc<concurrent_queue::ConcurrentQueue<T>>);

pub(crate) struct ConcurrentReceiver<T>(pub(crate) Arc<concurrent_queue::ConcurrentQueue<T>>);

impl<T: Send + 'static, P: fanring::teardown::Teardown> BenchSender<T>
    for fanring::mpsc::Sender<T, P>
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.try_send(value) {
            Ok(()) => SendAttempt::Sent,
            Err(fanring::mpsc::TrySendError::Full(_)) => SendAttempt::Full,
            Err(fanring::mpsc::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        self.send(value).is_ok()
    }
}

impl<T, P: fanring::teardown::Teardown> BenchReceiver<T> for fanring::mpsc::Receiver<T, P> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.try_recv() {
            Ok(value) => RecvAttempt::Item(value),
            Err(fanring::mpsc::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(fanring::mpsc::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for crossbeam_channel::Sender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match crossbeam_channel::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(crossbeam_channel::TrySendError::Full(_)) => SendAttempt::Full,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        crossbeam_channel::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for crossbeam_channel::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match crossbeam_channel::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(crossbeam_channel::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(crossbeam_channel::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        crossbeam_channel::Receiver::recv(self).ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for crossfire::MTx<crossfire::mpsc::Array<T>> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match crossfire::BlockingTxTrait::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(crossfire::TrySendError::Full(_)) => SendAttempt::Full,
            Err(crossfire::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        crossfire::BlockingTxTrait::send(self, value).is_ok()
    }
}

impl<T: Send + 'static> BenchReceiver<T> for crossfire::Rx<crossfire::mpsc::Array<T>> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match crossfire::BlockingRxTrait::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(crossfire::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(crossfire::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        crossfire::BlockingRxTrait::recv(self).ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for flume::Sender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match flume::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(flume::TrySendError::Full(_)) => SendAttempt::Full,
            Err(flume::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        flume::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for flume::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match flume::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(flume::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(flume::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        flume::Receiver::recv(self).ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for kanal::Sender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match kanal::Sender::try_send(self, value) {
            Ok(true) => SendAttempt::Sent,
            Ok(false) => SendAttempt::Full,
            Err(_) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        kanal::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for kanal::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match kanal::Receiver::try_recv(self) {
            Ok(Some(value)) => RecvAttempt::Item(value),
            Ok(None) => RecvAttempt::Empty,
            Err(_) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        kanal::Receiver::recv(self).ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for ConcurrentSender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.0.push(value) {
            Ok(()) => SendAttempt::Sent,
            Err(concurrent_queue::PushError::Full(_)) => SendAttempt::Full,
            Err(concurrent_queue::PushError::Closed(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, _value: T) -> bool {
        unreachable!("concurrent-queue has no blocking benchmark")
    }
}

impl<T> BenchReceiver<T> for ConcurrentReceiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.0.pop() {
            Ok(value) => RecvAttempt::Item(value),
            Err(concurrent_queue::PopError::Empty) => RecvAttempt::Empty,
            Err(concurrent_queue::PopError::Closed) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        unreachable!("concurrent-queue has no blocking benchmark")
    }

    fn close_for_drain(&mut self) {
        self.0.close();
    }
}

impl<T: Copy + Default + Send + Sync + 'static> BenchSender<T>
    for thingbuf::mpsc::blocking::Sender<T>
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match thingbuf::mpsc::blocking::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(thingbuf::mpsc::errors::TrySendError::Full(_)) => SendAttempt::Full,
            Err(thingbuf::mpsc::errors::TrySendError::Closed(_)) => SendAttempt::Disconnected,
            Err(_) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        thingbuf::mpsc::blocking::Sender::send(self, value).is_ok()
    }
}

impl<T: Clone + Default> BenchReceiver<T> for thingbuf::mpsc::blocking::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match thingbuf::mpsc::blocking::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(thingbuf::mpsc::errors::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(thingbuf::mpsc::errors::TryRecvError::Closed) => RecvAttempt::Disconnected,
            Err(_) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        thingbuf::mpsc::blocking::Receiver::recv(self)
    }
}
