///|
/// Internal state of a `OneShot`. The prefixed name keeps it clear of the
/// actor vocabulary (`Lifecycle`, `StopReason`, ...) in this package.
/// Package-private: consumers only observe the cell through `complete`
/// and `wait`.
priv enum OneShotState[X] {
  Pending
  Completed(X)
}

///|
/// A single-assignment cell for the same cooperative async runtime the
/// Fuwaroid actors live on — the non-actor primitive when a caller just
/// needs to hand ONE value to one or more waiting tasks and have every
/// successful wait observe the same value. No mailbox, no actor, no loop:
/// private state plus a `moonbitlang/async` `CondVar` is the whole
/// mechanism.
///
/// - `complete` is synchronous (a plain function — it cannot suspend by
///   type): it publishes the value and broadcasts, first-wins. Later
///   submissions are rejected with `Err` carrying the submitted value.
/// - `wait` is async: it returns the saved value immediately once
///   completed, otherwise parks on the CondVar. Repeated and late waits
///   are fine; waiting never consumes the value and never copies it —
///   every waiter observes the same `X`.
/// - Cancelling a waiting task removes only that waiter (the CondVar
///   unlinks it); the cell and the other waiters are unaffected. A waiter
///   woken by the broadcast but cancelled before it resumes still
///   observes the value — the CondVar's woken-then-cancelled protection
///   keeps an already-delivered value from being lost.
///
/// This is a cooperative-concurrency primitive INSIDE one async runtime:
/// not thread-safe, not an FFI callback target. Events from other threads
/// must enter the runtime first (timers, `TaskGroup` spawn, ...).
///
/// # Example
/// ```mbt check
/// async test {
///   @async.with_task_group(group => {
///     let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
///     let waiter = group.spawn(() => cell.wait())
///     assert_eq(cell.complete(5), Ok(()))
///     assert_eq(cell.complete(6), Err(6))
///     assert_eq(waiter.wait(), 5)
///     assert_eq(cell.wait(), 5)
///   })
/// }
/// ```
pub struct OneShot[X] {
  priv mut state : OneShotState[X]
  priv wake : @cond_var.Cond
}

///|
/// Create an empty cell. No timeout, error state, reset or close: to make
/// a result-carrying cell, instantiate `X` as `Result`/an outcome type and
/// bound the wait with `@async.with_timeout` on the caller side.
pub fn[X] OneShot::OneShot() -> OneShot[X] {
  { state: Pending, wake: @cond_var.Cond(), }
}

///|
/// Publish `value`. The FIRST call on a cell returns `Ok(())` after
/// storing the value and waking every current waiter; every later call is
/// synchronous, changes nothing, and returns `Err(value)` — the value
/// THIS call submitted and the cell did not accept (the saved value is
/// not reported back). The reject path is a no-waiter fast path: it does
/// not broadcast and cannot suspend.
pub fn[X] OneShot::complete(self : OneShot[X], value : X) -> Result[Unit, X] {
  guard self.state is Pending else { return Err(value) }
  // Cooperative-runtime premise: publish-then-broadcast runs without a
  // suspension point, so no waiter can observe the window in between —
  // a waiter either registered before the broadcast (it is woken and
  // finds `Completed`) or checks `wait`'s state afterwards.
  self.state = Completed(value)
  self.wake.broadcast()
  Ok(())
}

///|
/// Wait for the value. Suspends until some task completes the cell, then
/// returns the saved value; on an already-completed cell it returns
/// immediately. Any number of waiters may call this at any time (before
/// or after completion, repeatedly) and every successful wait observes
/// the same value. If the WAITING task is cancelled, the cancellation
/// propagates and the waiter is removed; the cell keeps its state for
/// everyone else. A cancellation that lands after the wake but before
/// this waiter resumes does NOT remove the delivered value — the wait
/// still returns it.
///
/// Lost-wakeup safety rests on the cooperative premise documented on
/// `complete`: between the state check and the CondVar registration below
/// there is no suspension point, so a concurrent `complete` either
/// broadcast before the registration (this loop's re-check sees
/// `Completed`) or can only run after this waiter is already parked.
pub async fn[X] OneShot::wait(self : OneShot[X]) -> X {
  while true {
    match self.state {
      Completed(value) => return value
      Pending => self.wake.wait()
    }
  } nobreak {
    // Unreachable: the loop above has no `break`, it only exits through
    // the completed-state return.
    abort("fuwaroid: OneShot::wait exited its wait loop without a value")
  }
}
