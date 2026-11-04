use super::{
    Attempt, Blocking, Recheck, block, block_until, recv_timeout_result, send_timeout_result,
};
use crate::config::{SpinWait, WaitStrategy};
use crate::error::{RecvError, RecvTimeoutError, SendTimeoutError};
use crate::wait::{Park, WaitSource};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Fake wait cell. Counts registrations and waits and tracks whether a
/// registration is alive.
#[derive(Default)]
struct FakeCell {
    registered: Cell<usize>,
    active: Cell<bool>,
    waits: Cell<usize>,
    timed_waits: Cell<usize>,
    time_out: Cell<bool>,
}

struct Registration<'a>(&'a FakeCell);

impl WaitSource for FakeCell {
    type Registration<'a> = Registration<'a>;

    fn register(&self) -> Registration<'_> {
        assert!(!self.active.replace(true), "one registration at a time");
        self.registered.set(self.registered.get() + 1);
        Registration(self)
    }
}

impl Park for Registration<'_> {
    fn wait(self) {
        self.0.waits.set(self.0.waits.get() + 1);
    }

    fn wait_timeout(self, _timeout: Duration) -> bool {
        self.0.timed_waits.set(self.0.timed_waits.get() + 1);
        self.0.time_out.get()
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.0.active.set(false);
    }
}

/// Fake endpoint. Tries fail `pending` times, then finish with `outcome`.
/// The first `progressed` rechecks report progress.
struct Endpoint {
    cell: Rc<FakeCell>,
    pending: usize,
    progressed: usize,
    outcome: Result<u32, RecvError>,
    tries: usize,
    notified: usize,
}

impl Endpoint {
    fn new(pending: usize) -> Self {
        Self {
            cell: Rc::default(),
            pending,
            progressed: 0,
            outcome: Ok(7),
            tries: 0,
            notified: 0,
        }
    }

    fn try_once(&mut self) -> Option<Result<u32, RecvError>> {
        self.tries += 1;
        if self.pending == 0 {
            return Some(self.outcome);
        }
        self.pending -= 1;
        None
    }
}

impl Blocking<u8> for Endpoint {
    type Output = Result<u32, RecvError>;
    type Deferred = ();
    type Waiter = Rc<FakeCell>;

    fn waiter(&self) -> Rc<FakeCell> {
        self.cell.clone()
    }

    fn attempt(&mut self, input: u8) -> Attempt<u8, Self::Output> {
        match self.try_once() {
            Some(output) => Attempt::Done(output),
            None => Attempt::Pending(input),
        }
    }

    fn recheck(&mut self, input: u8) -> Recheck<u8, Self::Output, ()> {
        match self.try_once() {
            Some(output) => Recheck::Done(output, ()),
            None if self.progressed != 0 => {
                self.progressed -= 1;
                Recheck::Progressed(input, ())
            }
            None => Recheck::Pending(input),
        }
    }

    fn notify(&mut self, (): ()) {
        assert!(!self.cell.active.get(), "notified while registered");
        self.notified += 1;
    }
}

fn later() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

fn no_spin() -> SpinWait {
    SpinWait::blocking(WaitStrategy::SpinFor(Duration::ZERO))
}

#[test]
fn success_while_spinning_never_registers() {
    let mut endpoint = Endpoint::new(2);
    let spin = SpinWait::blocking(WaitStrategy::SpinFor(Duration::from_secs(60)));
    assert_eq!(block(&mut endpoint, 0, spin), Ok(7));
    assert_eq!(endpoint.cell.registered.get(), 0);
}

#[test]
fn each_park_registers_and_rechecks_before_waiting() {
    // Attempt, recheck, wait; attempt, recheck succeeds.
    let mut endpoint = Endpoint::new(3);
    assert_eq!(block(&mut endpoint, 0, no_spin()), Ok(7));
    assert_eq!(endpoint.cell.registered.get(), 2);
    assert_eq!(endpoint.cell.waits.get(), 1);
    assert_eq!(endpoint.notified, 1);
}

#[test]
fn progress_while_registered_notifies_after_unregistering_and_skips_the_wait() {
    let mut endpoint = Endpoint::new(3);
    endpoint.progressed = 1;
    assert_eq!(block(&mut endpoint, 0, no_spin()), Ok(7));
    assert_eq!(endpoint.cell.waits.get(), 0);
    assert_eq!(endpoint.notified, 2);
}

#[test]
fn disconnect_finishes_the_wait() {
    let mut endpoint = Endpoint::new(1);
    endpoint.outcome = Err(RecvError);
    assert_eq!(block(&mut endpoint, 0, no_spin()), Err(RecvError));
    assert_eq!(endpoint.cell.waits.get(), 0);
}

#[test]
fn timed_out_wait_makes_one_final_attempt() {
    let mut endpoint = Endpoint::new(usize::MAX);
    endpoint.cell.time_out.set(true);
    let deadline = later();
    let spin = SpinWait::deadline(WaitStrategy::Park, deadline);
    assert_eq!(block_until(&mut endpoint, 5, spin, deadline), Err(5));
    assert_eq!(endpoint.tries, 3);
    assert_eq!(endpoint.cell.timed_waits.get(), 1);
}

#[test]
fn passed_deadline_times_out_without_waiting() {
    let mut endpoint = Endpoint::new(usize::MAX);
    let deadline = Instant::now();
    let spin = SpinWait::deadline(WaitStrategy::Park, deadline);
    assert_eq!(block_until(&mut endpoint, 5, spin, deadline), Err(5));
    assert_eq!(endpoint.cell.timed_waits.get(), 0);
}

#[test]
fn woken_timed_wait_retries_until_done() {
    let mut endpoint = Endpoint::new(3);
    let deadline = later();
    let spin = SpinWait::deadline(WaitStrategy::Park, deadline);
    assert_eq!(block_until(&mut endpoint, 5, spin, deadline), Ok(Ok(7)));
    assert_eq!(endpoint.cell.timed_waits.get(), 1);
}

#[test]
fn timed_results_map_to_public_errors() {
    assert_eq!(send_timeout_result::<u8>(Ok(Ok(()))), Ok(()));
    assert_eq!(
        send_timeout_result(Ok(Err(4))),
        Err(SendTimeoutError::Disconnected(4))
    );
    assert_eq!(
        send_timeout_result::<u8>(Err(4)),
        Err(SendTimeoutError::Timeout(4))
    );
    assert_eq!(recv_timeout_result::<u8, ()>(Ok(Ok(3))), Ok(3));
    assert_eq!(
        recv_timeout_result::<u8, ()>(Ok(Err(RecvError))),
        Err(RecvTimeoutError::Disconnected)
    );
    assert_eq!(
        recv_timeout_result::<u8, ()>(Err(())),
        Err(RecvTimeoutError::Timeout)
    );
}
