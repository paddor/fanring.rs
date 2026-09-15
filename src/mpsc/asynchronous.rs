//! Runtime-independent futures over the synchronous MPSC rings.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::teardown::{Deferred, Teardown};
use crate::wait::WaitCell;

use super::{Receiver, RecvError, SendError, Sender, TryRecvError};

impl<T, P: Teardown> Sender<T, P> {
    /// Wait asynchronously for capacity and publish one value.
    ///
    /// Success publishes immediately, as with `try_send`. No unpublished
    /// batch can prevent the receiver from making progress. Canceling before
    /// completion drops the unsent value and releases the waker registration;
    /// it consumes no capacity. Returns the value if the receiver disconnects.
    pub fn send_async(&mut self, value: T) -> SendFuture<'_, T, P> {
        SendFuture {
            sender: self,
            value: Some(value),
        }
    }

    /// Poll this sender's capacity without reserving a slot.
    ///
    /// Registers only this producer's waker. A subsequent poll replaces the
    /// previous registration; `&mut self` excludes simultaneous send futures.
    /// Use [`cancel_send_wait`](Self::cancel_send_wait) when abandoning a
    /// manual poll. Returns `Err` when the receiver has disconnected.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<()>>> {
        if !self.is_disconnected() && self.producer.is_full() {
            self.signal.space_waiter.register_async(cx.waker());
            if !self.is_disconnected() && self.producer.is_full() {
                return Poll::Pending;
            }
        }
        self.cancel_send_wait();
        Poll::Ready(if self.is_disconnected() {
            Err(SendError(()))
        } else {
            Ok(())
        })
    }

    /// Remove the waker retained by a pending manual `poll_ready` call.
    pub fn cancel_send_wait(&mut self) {
        self.signal.space_waiter.cancel_async();
    }
}

/// Future returned by [`Sender::send_async`].
pub struct SendFuture<'a, T, P: Teardown = Deferred> {
    sender: &'a mut Sender<T, P>,
    value: Option<T>,
}

impl<T, P: Teardown> std::fmt::Debug for SendFuture<'_, T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture")
            .field("pending_value", &self.value.is_some())
            .finish_non_exhaustive()
    }
}

// No field is structurally pinned; values move into the ring on completion.
impl<T, P: Teardown> Unpin for SendFuture<'_, T, P> {}

impl<T, P: Teardown> Future for SendFuture<'_, T, P> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.sender.poll_ready(cx).is_pending() {
            return Poll::Pending;
        }
        let value = this.value.take().expect("send polled after completion");
        Poll::Ready(
            this.sender
                .try_send(value)
                .map_err(|error| SendError(error.into_inner())),
        )
    }
}

impl<T, P: Teardown> Drop for SendFuture<'_, T, P> {
    fn drop(&mut self) {
        self.sender.cancel_send_wait();
    }
}

struct Registration<'a>(&'a WaitCell);

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.0.cancel_async();
    }
}

impl<T, P: Teardown> Receiver<T, P> {
    /// Receive one value asynchronously, releasing its capacity before return.
    ///
    /// Cancellation before completion consumes no value and removes the
    /// receiver's waker. Final disconnect is reported only after all rings drain.
    pub async fn recv_async(&mut self) -> Result<T, RecvError> {
        let shared = self.shared.clone();
        let _registration = Registration(&shared.data_waiter);
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Poll a receive, registering the channel's single receiver waker.
    ///
    /// Releases consumed slots before returning or waiting. When abandoning
    /// a manual poll, call [`cancel_recv_wait`](Self::cancel_recv_wait).
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut result = self.try_recv();
        if matches!(result, Err(TryRecvError::Empty)) {
            self.release_consumed();
            self.shared.data_waiter.register_async(cx.waker());
            result = self.try_recv();
        }
        self.release_consumed();
        match result {
            Ok(value) => {
                self.cancel_recv_wait();
                Poll::Ready(Ok(value))
            }
            Err(TryRecvError::Disconnected) => {
                self.cancel_recv_wait();
                Poll::Ready(Err(RecvError))
            }
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }

    /// Remove the waker retained by a pending manual `poll_recv` call.
    pub fn cancel_recv_wait(&mut self) {
        self.shared.data_waiter.cancel_async();
    }

    /// Append at most `limit` values, waiting only for the first value.
    ///
    /// Releases consumed slots in batches, including before returning. A zero
    /// limit succeeds even after disconnect. Reserve output capacity to avoid
    /// allocations while receiving. Cancellation before the first value leaves
    /// `output` unchanged; after that value this future completes without waiting.
    pub async fn recv_batch_into_async(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
    ) -> Result<usize, RecvError> {
        if limit == 0 {
            self.release_consumed();
            return Ok(0);
        }
        output.push(self.recv_async().await?);
        let mut received = 1;
        while received < limit {
            let Ok(value) = self.try_recv() else { break };
            output.push(value);
            received += 1;
        }
        self.release_consumed();
        Ok(received)
    }
}
