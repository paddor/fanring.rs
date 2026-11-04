//! Uniform send and receive adapters over the compared MPMC channels.

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

pub(crate) trait BenchReceiver<T>: Send + 'static {
    fn try_recv(&mut self) -> RecvAttempt<T>;
    fn recv(&mut self) -> Option<T>;
}

impl<T, P: fanring::teardown::Teardown> BenchSender<T> for fanring::mpmc::Sender<T, P>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.try_send(value) {
            Ok(()) => SendAttempt::Sent,
            Err(fanring::mpmc::TrySendError::Full(_)) => SendAttempt::Full,
            Err(fanring::mpmc::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        self.send(value).is_ok()
    }
}

impl<T, P: fanring::teardown::Teardown> BenchReceiver<T> for fanring::mpmc::Receiver<T, P>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.try_recv() {
            Ok(value) => RecvAttempt::Item(value),
            Err(fanring::mpmc::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(fanring::mpmc::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T> BenchSender<T> for crossbeam_channel::Sender<T>
where
    T: Send + 'static,
{
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

impl<T> BenchReceiver<T> for crossbeam_channel::Receiver<T>
where
    T: Send + 'static,
{
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

impl<T> BenchSender<T> for crossfire::MTx<crossfire::mpmc::Array<T>>
where
    T: Send + 'static,
{
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

impl<T> BenchReceiver<T> for crossfire::MRx<crossfire::mpmc::Array<T>>
where
    T: Send + 'static,
{
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

impl<T> BenchSender<T> for flume::Sender<T>
where
    T: Send + 'static,
{
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

impl<T> BenchReceiver<T> for flume::Receiver<T>
where
    T: Send + 'static,
{
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

impl<T> BenchSender<T> for kanal::Sender<T>
where
    T: Send + 'static,
{
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

impl<T> BenchReceiver<T> for kanal::Receiver<T>
where
    T: Send + 'static,
{
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
