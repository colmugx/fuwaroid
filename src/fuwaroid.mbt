///|
/// Single-writer concurrency over `moonbitlang/async`: private state, a
/// typed mailbox, one serial loop. A Fuwaroid is a light entity that
/// floats suspended until a message arrives, settles it in one
/// synchronous step, and drifts back to sleep — no OS threads, no locks,
/// no blocking (see README for the name).
///
/// The execution model:
/// - Handlers (`on_cmd` / `on_query`) are SYNCHRONOUS by signature, so the
///   loop never awaits between callback entry and return; callback
///   invocations are serialized and no other message can interleave them.
///   This does not make a generic `State` immutable or prevent aliases.
///   Callers must treat the state as actor-owned and must not retain or
///   mutate it from spawned work or replies.
/// - Async work (IO, subprocesses, timers) never runs inside a handler:
///   spawn it on the host group from the handler and report the outcome
///   back with `tell`. The loop is the only place where a handler return
///   value advances its state; the ownership rule is a caller contract,
///   not a compiler-enforced property of the generic API.
/// - Between complete messages (handler returned, state committed) the
///   loop yields the scheduler every `fuwaroid_yield_batch` messages, so
///   an instance that keeps its own mailbox non-empty cannot monopolize
///   the cooperative runtime: other instances, timers and cancellation
///   all get scheduled. A handler is never suspended midway.
/// - Every stop — graceful closed-and-empty drain, host cancellation
///   observed at a yield/`get` boundary, or any other terminal mailbox
///   error — goes through ONE cleanup path (`fuwaroid_cleanup`): admission
///   is closed, stranded asks are failed with the stop marker, the
///   termination reason is recorded, and only then does the loop task
///   terminate (returning normally after a graceful drain, re-raising the
///   original error for an ordinary failure, and terminating CANCELLED —
///   observable as `@async.TaskCancelled` to `Task::wait` — after a host
///   cancellation, whose signal in async 0.22.x is distinct from `Error`).
/// - `join` waits for the loop task itself to terminate and returns the
///   recorded stop reason; it never waits for or cancels background work
///   a handler spawned on the host group.
/// - The loop is spawned `no_wait` on the HOST's long-lived task group.
///   There is no actor system, no registry, no supervision tree: the host
///   owns the lifetime (structured concurrency). Handlers cannot raise
///   (their types say so); failure is an ordinary message or terminal
///   state — never a restart that resets state, which is what ledgers of
///   in-flight work require.
pub struct Fuwaroid[Cmd, Query, Reply] {
  priv mailbox : @aqueue.Queue[Envelope[Cmd, Query, Reply]]
  /// Every handle shares its loop's stop record and diagnostics.
  priv stop : LoopStop
}

///|
/// Internal mailbox vocabulary. Public only because it appears in the
/// (private) field type of the public `Fuwaroid` handle — visibility
/// rule: consumers never construct these; the mailbox itself is private.
pub enum Envelope[Cmd, Query, Reply] {
  Tell(Cmd)
  Ask(Query, @aqueue.Queue[Reply])
}

///|
/// What a handler may touch: the host group (to spawn async work) and
/// this Fuwaroid's own address (to report work outcomes back). Never the
/// raw mailbox, never another Fuwaroid's state.
pub struct Ctx[Cmd, Query, Reply] {
  /// Host task group — spawn async side work here; it outlives the
  /// message that started it but not the host.
  group : @async.TaskGroup[Unit]
  /// This Fuwaroid's address. Use `tell` only. A handler is a SYNCHRONOUS
  /// function, so calling the async `ask` from inside a handler is already
  /// a compile error (E4149): the classic self-ask scenario — the loop busy
  /// serving the current message until the ask timeout fires — is excluded
  /// at the type level, not merely discouraged.
  address : Fuwaroid[Cmd, Query, Reply]
}

///|
/// Why a `tell` was not accepted.
pub(all) enum SendRefusal {
  MailboxClosed
  MailboxFull
} derive(Eq, Debug)

///|
pub extend SendRefusal with Eq::{equal, not_equal}

///|
pub extend SendRefusal with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Why an `ask` did not produce a reply.
pub(all) enum AskFailure {
  /// The request never entered the mailbox.
  NotDelivered(SendRefusal)
  /// No reply within the caller's timeout. The request is NOT withdrawn —
  /// the Fuwaroid may still process it later; the abandoned reply is
  /// dropped.
  TimedOut
  /// The loop stopped abnormally (host cancellation) while this request
  /// was queued; its reply queue was closed by the drain.
  Stopped
} derive(Eq, Debug)

///|
pub extend AskFailure with Eq::{equal, not_equal}

///|
pub extend AskFailure with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Why the loop task terminated. Recorded by the unified stop path
/// (`fuwaroid_cleanup`) before the loop task terminates. The three
/// constructors distinguish the terminal dispositions the async 0.22.x
/// runtime separates at the language level:
///
/// - `Graceful`: `close` was requested and the closed-and-empty drain
///   completed.
/// - `Cancelled`: the host (task group) cancelled the loop. In
///   `moonbitlang/async` 0.22.x cancellation is a distinct runtime signal,
///   NOT an `Error` value, so it is reported as its own constructor —
///   the loop never fabricates an error to stand in for it.
/// - `Failed(Error)`: an ordinary terminal error (e.g. a foreign mailbox
///   error); the ORIGINAL error identity is kept, never a compressed copy.
pub(all) enum StopReason {
  Graceful
  Cancelled
  Failed(Error)
} derive(Debug)

///|
pub extend StopReason with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Internal marker closed into reply queues of asks stranded by an
/// abnormal loop stop, so their waiters fail fast instead of timing out.
pub(all) suberror StoppedError {
  StoppedError
}

///|
/// Internal per-instance stop record, stored on the `Fuwaroid` handle.
/// The loop writes the termination reason and any secondary cleanup
/// failure here; `join` (and a later snapshot) read the recorded outcome
/// after — or while — waiting on the loop task.
priv struct LoopStop {
  /// Why the loop terminated (written once, by `fuwaroid_cleanup`).
  mut reason : StopReason?
  /// Secondary failure evidence: if the cleanup itself fails (e.g. a
  /// foreign drain error) while an original stop cause exists, the
  /// original cause still wins propagation and the secondary failure is
  /// preserved here instead of being swallowed.
  mut cleanup_error : Error?
  /// The loop task handle, obtained via `TaskGroup::spawn`. Termination
  /// waiters wait on this task; the recorded `reason` is read from this
  /// cell after termination.
  mut task : @async.Task[Unit]?
  /// Per-instance diagnostics and lifecycle phase: the
  /// admission side (`tell`/`ask`/`close`), the serving loop
  /// (`processed`) and the unified cleanup (`abandoned`, the final
  /// `Stopped` phase) all write it; `Fuwaroid::snapshot` reads it.
  diag : Diag
}

///|
/// Mailbox configuration for `Fuwaroid::spawn`:
/// this library owns the mailbox vocabulary, `@aqueue.Kind` is no longer
/// part of any public signature.
///
/// - `Unbounded`: admits every message; the queue grows without bound.
/// - `Bounded(n)`: at most `n` messages are buffered; a send that finds
///   the mailbox full is refused SYNCHRONOUSLY with `MailboxFull` — it
///   never waits for capacity. `n` must be a positive integer: zero
///   (rendezvous) and negative capacities are a configuration error and
///   fail-fast (`abort`) at spawn time, they are not clamped or remapped.
///
/// Measured admission-window semantics: while the loop
/// is parked in `queue.get()` — its steady state while idle — the first
/// send is handed to that parked reader point-to-point, bypassing the
/// buffer. One uninterrupted producer turn can therefore admit up to
/// `capacity + 1` messages in flight (1 handoff slot + `capacity`
/// buffered); the next send is the one refused with `MailboxFull`.
pub(all) enum Mailbox {
  Unbounded
  Bounded(Int)
} derive(Eq, Debug)

///|
pub extend Mailbox with Eq::{equal, not_equal}

///|
pub extend Mailbox with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Pure validation of a `Bounded` capacity: returns the failure
/// message for `spawn` to abort with, `None` if valid. Kept a pure,
/// package-internal function so the white-box tests can pin the acceptance
/// boundary directly.
fn mailbox_capacity_error(n : Int) -> String? {
  if n <= 0 {
    Some(
      "Bounded mailbox capacity must be a positive integer (zero/negative capacities are rejected fail-fast, not clamped), got \{n}",
    )
  } else {
    None
  }
}

///|
fn LoopStop::fresh() -> LoopStop {
  { reason: None, cleanup_error: None, task: None, diag: Diag::fresh(), }
}

///|
/// No-op query handler for `spawn_fold`.
#inline
fn[State, Cmd] fold_receipt_query(
  _ctx : Ctx[Cmd, Unit, Unit],
  state : State,
  _query : Unit,
) -> (State, Unit) {
  (state, ())
}

///|
/// Shared admission path for `tell` and `ask`.
#inline
fn[Cmd, Query, Reply] Fuwaroid::admit(
  self : Fuwaroid[Cmd, Query, Reply],
  envelope : Envelope[Cmd, Query, Reply],
) -> Result[Unit, SendRefusal] {
  try {
    if self.mailbox.try_put(envelope) {
      self.stop.diag.accepted += 1L
      Ok(())
    } else {
      self.stop.diag.rejected_full += 1L
      Err(SendRefusal::MailboxFull)
    }
  } catch {
    _ => {
      self.stop.diag.rejected_closed += 1L
      Err(SendRefusal::MailboxClosed)
    }
  }
}

///|
/// Spawn a Fuwaroid on the host's long-lived task group. Commands and
/// queries are SEPARATE types: `on_cmd` folds a command into the state,
/// `on_query` folds a query and produces its reply — neither handler has
/// unreachable arms for the other flow. Both run on the single loop,
/// serially, in mailbox FIFO order, and may not raise. Use
/// `Fuwaroid::spawn_fold` when there is no query flow at all.
///
/// `mailbox` configures admission with this library's own `Mailbox`
/// vocabulary (see its documentation for bounded refusal and the measured
/// capacity+1 admission window); an invalid `Bounded` capacity aborts
/// synchronously here — spawn stays a non-raising API.
pub fn[State, Cmd, Query, Reply] Fuwaroid::spawn(
  group~ : @async.TaskGroup[Unit],
  init~ : State,
  on_cmd~ : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query~ : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  mailbox? : Mailbox = Mailbox::Unbounded,
) -> Fuwaroid[Cmd, Query, Reply] {
  let kind : @aqueue.Kind = match mailbox {
    Unbounded => @aqueue.Unbounded
    Bounded(n) =>
      match mailbox_capacity_error(n) {
        Some(message) =>
          abort("fuwaroid: invalid mailbox configuration: \{message}")
        None => @aqueue.Blocking(n)
      }
  }
  let queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]] = @aqueue.Queue(kind~)
  let stop : LoopStop = LoopStop::fresh()
  let fuwaroid : Fuwaroid[Cmd, Query, Reply] = { mailbox: queue, stop, }
  let ctx : Ctx[Cmd, Query, Reply] = { group, address: fuwaroid, }
  // TaskGroup::spawn (not spawn_bg) so the loop task handle exists for
  // termination waiters (`join`); no_wait stays true: the host group does
  // not wait for the loop, it cancels it when the scope ends.
  // allow_failure keeps its default (false): a cleanup-path bug that lets
  // a non-cancellation error escape still fails the host group.
  // spawn and task installation have no suspension between them; no
  // caller can observe this bootstrap-only empty task slot.
  let task = group.spawn(no_wait=true, () => {
    fuwaroid_loop(ctx, init, on_cmd, on_query, queue, stop~)
  })
  stop.task = Some(task)
  fuwaroid
}

///|
/// `Fuwaroid::spawn` without a query flow: the state simply folds over
/// the command stream. The handle is `Fuwaroid[Cmd, Unit, Unit]`:
/// `ask((), timeout_ms=...)` degenerates to a served-receipt
/// (`Ok(())`) through a no-op query handler — the barrier is NOT a
/// command, it never reaches `on_cmd`; its only value is FIFO proof: the
/// reply certifies every message enqueued before it was already served.
/// (The legacy `ask(cmd)` barrier misuse is now a compile error: the
/// query type is `Unit`.)
pub fn[State, Cmd] Fuwaroid::spawn_fold(
  group~ : @async.TaskGroup[Unit],
  init~ : State,
  on_cmd~ : (Ctx[Cmd, Unit, Unit], State, Cmd) -> State,
  mailbox? : Mailbox = Mailbox::Unbounded,
) -> Fuwaroid[Cmd, Unit, Unit] {
  Fuwaroid::spawn(
    group~,
    init~,
    on_cmd~,
    on_query=fold_receipt_query,
    mailbox~,
  )
}

///|
/// Complete messages processed between scheduler yields.
let fuwaroid_yield_batch : Int = 64

///|
/// Stranded-message cleanup keeps the original smaller cadence.
let fuwaroid_cleanup_yield_batch : Int = 32

///|
#inline
fn[State, Cmd, Query, Reply] settle_envelope(
  ctx : Ctx[Cmd, Query, Reply],
  state : State,
  envelope : Envelope[Cmd, Query, Reply],
  on_cmd : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
) -> State {
  match envelope {
    Tell(cmd) => on_cmd(ctx, state, cmd)
    Ask(query, reply) => {
      let (next_state, answer) = on_query(ctx, state, query)
      let _ = reply.try_put(answer) catch { _ => false }
      next_state
    }
  }
}

///|
#inline
fn[Cmd, Query, Reply] abandon_envelope(
  envelope : Envelope[Cmd, Query, Reply],
) -> Unit {
  match envelope {
    Ask(_, reply) => reply.close(error=StoppedError)
    Tell(_) => ()
  }
}

///|
/// Cancellation is handled separately from ordinary loop errors.
async fn[State, Cmd, Query, Reply] fuwaroid_loop(
  ctx : Ctx[Cmd, Query, Reply],
  init : State,
  on_cmd : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  stop~ : LoopStop,
) -> Unit {
  match @async.handle_cancellation(() => {
    fuwaroid_serve(ctx, init, on_cmd, on_query, queue, stop~)
  }) {
    Some(_) => ()
    None => {
      fuwaroid_cleanup(queue, Cancelled, stop)
      @async.pause()
    }
  }
}

///|
/// Drain ready messages synchronously; publish diagnostics before suspension.
async fn[State, Cmd, Query, Reply] fuwaroid_serve(
  ctx : Ctx[Cmd, Query, Reply],
  init : State,
  on_cmd : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  stop~ : LoopStop,
) -> Unit {
  let reason : StopReason = try {
    for envelope = queue.get(),
        state = init,
        served = 0 {
      let state = settle_envelope(ctx, state, envelope, on_cmd, on_query)
      let served = served + 1
      stop.diag.processed += 1L

      if served >= fuwaroid_yield_batch {
        @async.pause()
        continue queue.get(), state, 0
      }

      match queue.try_get() {
        Some(next) => continue next, state, served
        None => continue queue.get(), state, served
      }
    }
    Graceful
  } catch {
    @aqueue.QueueAlreadyClosed => Graceful
    original => Failed(original)
  }
  fuwaroid_cleanup(queue, reason, stop)
}

///|
/// THE single stop/cleanup path for the loop. Runs exactly
/// once per stop, for every disposition (`StopReason`):
///
/// 1. Close admission (`queue.close`): idempotent — safe when the mailbox
///    was already closed (graceful `close`, or a foreign close). From here
///    on the handle refuses new messages.
/// 2. Fail every accepted-but-unserved ask via `fail_leftover_asks` so
///    its waiter gets `AskFailure::Stopped` immediately. The drain is
///    shielded with `@async.protect_from_cancel`, so it also completes
///    when called from the sticky cancelled state (the cancellation
///    branch of `fuwaroid_loop`).
/// 3. Record the termination reason on `stop` and keep any secondary
///    cleanup failure as evidence on `stop.cleanup_error` instead of
///    swallowing it.
/// 4. Propagate, with the PRIMARY cause winning:
///    - `Graceful` returns normally unless the cleanup itself failed.
///    - `Failed(original)` re-raises the ORIGINAL error — a secondary
///      cleanup failure never replaces it.
///    - `Cancelled` returns normally into the still-cancelled caller,
///      which re-propagates the pending cancellation signal; a secondary
///      cleanup failure is recorded as evidence only, so the actor is
///      never disguised as an ordinary failure.
async fn[Cmd, Query, Reply] fuwaroid_cleanup(
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  reason : StopReason,
  stop : LoopStop,
) -> Unit {
  // The default close error is intentional: it only decides what NEW
  // attempts to touch the mailbox see; the recorded reason is retained
  // below and stranded asks get `StoppedError` in step 2.
  queue.close()
  stop.diag.transition(Closing)
  // Counting point: every envelope the stranded-ask drain removes
  // was accepted but is provably unserved — commands and queries alike
  // are counted as abandoned. Admission is closed before any yield.
  // Shield the whole drain so cancellation cannot strand remaining asks.
  let secondary : Error? = try {
    @async.protect_from_cancel(() => fail_leftover_asks(queue, diag=stop.diag))
    None
  } catch {
    error => Some(error)
  }
  stop.reason = Some(reason)
  stop.cleanup_error = secondary
  // Counting point: the unified cleanup is complete — the phase moves to
  // Stopped carrying the recorded reason (forward-only); `abandoned` was
  // fixed by the drain above.
  stop.diag.transition(Lifecycle::Stopped(reason))
  match reason {
    // Cancellation is re-propagated by the caller (the cancellation
    // branch of `fuwaroid_loop`); secondary evidence stays recorded.
    Cancelled => ()
    Graceful =>
      match secondary {
        Some(error) => raise error
        None => ()
      }
    // Ordinary failure: the original error identity wins propagation.
    Failed(original) => raise original
  }
}

///|
/// Drain accepted-but-unserved messages after admission closes.
async fn[Cmd, Query, Reply] fail_leftover_asks(
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  diag? : Diag,
) -> Unit {
  for abandoned = 0 {
    let next = try {
      queue.try_get()
    } catch {
      @aqueue.QueueAlreadyClosed => {
        match diag {
          Some(d) => d.add_abandoned(abandoned)
          None => ()
        }
        break ()
      }
      error => {
        match diag {
          Some(d) => d.add_abandoned(abandoned)
          None => ()
        }
        raise error
      }
    }

    match next {
      Some(envelope) => {
        abandon_envelope(envelope)
        let abandoned = abandoned + 1
        if abandoned >= fuwaroid_cleanup_yield_batch {
          match diag {
            Some(d) => d.add_abandoned(abandoned)
            None => ()
          }
          @async.pause()
          continue 0
        }
        continue abandoned
      }
      None => {
        match diag {
          Some(d) => d.add_abandoned(abandoned)
          None => ()
        }
        break ()
      }
    }
  }
}

///|
/// Fire-and-forget send; returns immediately after the mailbox accept
/// decision. `Ok` only means the command was enqueued. Order of
/// processing is mailbox FIFO; the interleaving of concurrent `tell`s is
/// the caller's scheduling. Every outcome is counted on the instance's
/// diagnostics: an accepted admission bumps `accepted`, a full
/// refusal `rejected_full`, a closed refusal `rejected_closed`.
pub fn[Cmd, Query, Reply] Fuwaroid::tell(
  self : Fuwaroid[Cmd, Query, Reply],
  cmd : Cmd,
) -> Result[Unit, SendRefusal] {
  self.admit(Envelope::Tell(cmd))
}

///|
/// Pure classification of the errors `ask` can see on its reply queue:
/// mapped failures return `Some`; `None` means "not ours" — the caller
/// must propagate (notably host cancellation of the asker).
#inline
fn classify_ask_error(error : Error) -> AskFailure? {
  match error {
    StoppedError => Some(AskFailure::Stopped)
    @async.TimeoutError => Some(AskFailure::TimedOut)
    _ => None
  }
}

///|
/// Send one query and wait for the handler's reply, bounded by
/// `timeout_ms`. Calling `ask` from inside a handler of the same Fuwaroid
/// is a compile error (E4149): handlers are synchronous functions and
/// `ask` is async, so the self-ask scenario — the loop busy serving the
/// current message until the timeout fires — is excluded by the type
/// system rather than merely discouraged. Cancellation of the CALLER
/// propagates (it is not converted into a failure value).
pub async fn[Cmd, Query, Reply] Fuwaroid::ask(
  self : Fuwaroid[Cmd, Query, Reply],
  query : Query,
  timeout_ms~ : Int,
) -> Result[Reply, AskFailure] {
  let reply : @aqueue.Queue[Reply] = @aqueue.Queue(kind=@aqueue.Unbounded)
  match self.admit(Envelope::Ask(query, reply)) {
    Ok(_) => ()
    Err(refusal) => return Err(AskFailure::NotDelivered(refusal))
  }
  try {
    match @async.with_timeout_opt(timeout_ms, () => reply.get()) {
      Some(answer) => Ok(answer)
      None => Err(AskFailure::TimedOut)
    }
  } catch {
    error =>
      match classify_ask_error(error) {
        Some(failure) => Err(failure)
        None => raise error
      }
  }
}

///|
/// Graceful mailbox stop: the mailbox accepts no new messages, everything
/// already queued (commands AND queries) is still processed in FIFO order,
/// then the loop exits. A reply to an `ask` issued just before `close` proves
/// every earlier message was already served (FIFO). `close` does not cancel
/// or await work that a handler already spawned on the host group; such work
/// may observe `MailboxClosed` when it reports back. The host group owns the
/// lifetime and waits for all of its children when it terminates.
///
/// Closing twice is idempotent (the second close is a no-op on the
/// already-closed queue). Returning does NOT mean the drain completed:
/// the backlog is drained by the loop task, which yields between batches
/// while doing so. To wait for the loop task to actually terminate and
/// learn why, use `join`. The lifecycle phase moves to `Closing` here
/// (forward-only: a late `close` cannot un-stop an already `Stopped`
/// instance) and to `Stopped(reason)` when the unified cleanup completes.
pub fn[Cmd, Query, Reply] Fuwaroid::close(
  self : Fuwaroid[Cmd, Query, Reply],
) -> Unit {
  // Counting point: the close REQUEST moves Running → Closing.
  self.stop.diag.transition(Closing)
  self.mailbox.close()
}

///|
/// Wait for this Fuwaroid's loop task to terminate and return the
/// recorded stop reason.
///
/// - Waiting uses `Task::wait`, so it is multi-waiter safe and returns
///   immediately once the loop has terminated. Only the loop task is
///   awaited: background work a handler spawned on the host group is
///   neither waited for nor cancelled by `join`.
/// - Waiter cancellation (A): the waiter's own cancellation is the
///   runtime's cancellation signal, NOT an `Error` — `catch` cannot
///   capture it, so it propagates out of `join` unchanged and is never
///   converted into a `StopReason`. The pre-check below makes even an
///   ALREADY-cancelled waiter observe itself when the loop already
///   terminated: `Task::wait` has a synchronous fast path for terminated
///   targets that performs no cancellation check, so the pre-check raises
///   the pending signal first via `@async.pause()`.
/// - Cancelled loop task (B): `Task::wait` reports a cancelled TARGET as
///   the ordinary `@async.TaskCancelled` error. That value describes the
///   waited task, not this actor's stop, so it is never stored or
///   returned as the reason: the loop's own cancellation branch already
///   recorded `Cancelled` during its protected cleanup, and `join`
///   returns exactly that.
/// - Failed loop task (C): the target's ordinary error maps to the
///   recorded `Failed(original)` — the original identity wins; a
///   recorded-but-unexplained failure falls back to the secondary
///   cleanup evidence, then to the raw task error (only reachable
///   through a cleanup-path bug).
/// - Graceful completion (D): the task ending `Done` returns the
///   recorded reason, `Graceful`.
/// - A missing internal task is an invariant violation and aborts.
///
/// Note: when the loop task fails with a NON-cancellation error, the
/// host task group's fail-fast still applies independently of `join` —
/// a joiner living in the same group is cancelled with the group before
/// it can observe `Failed`; a joiner in a different scope observes it.
pub async fn[Cmd, Query, Reply] Fuwaroid::join(
  self : Fuwaroid[Cmd, Query, Reply],
) -> StopReason {
  // Task::wait skips cancellation checks for already completed targets.
  // Surface the waiter's own pending cancellation before taking that
  // fast path: `pause` raises the cancellation signal, which propagates
  // out of `join` unchanged (it is not an `Error` and cannot be caught
  // into a `StopReason`).
  if @async.is_being_cancelled() {
    @async.pause()
  }
  let cell = self.stop
  let task = match cell.task {
    Some(task) => task
    None => abort("fuwaroid: join before loop task initialization")
  }
  let outcome : Result[Unit, Error] = Ok(task.wait()) catch {
    // Only errors of the TARGET task arrive here: `catch` cannot capture
    // the waiter's own cancellation signal, so no waiter-cancellation
    // branch exists anymore.
    error => Err(error)
  }
  match outcome {
    Ok(_) =>
      match cell.reason {
        Some(reason) => reason
        None =>
          abort("fuwaroid: loop completed without recording its stop reason")
      }
    Err(error) =>
      match error {
        // The loop TASK was cancelled; its cancellation branch completed
        // the unified cleanup before re-propagating, so `Cancelled` is
        // the recorded outcome. `TaskCancelled` itself is never used as
        // the reason — it describes the waited task, not this actor.
        @async.TaskCancelled => Cancelled
        other =>
          match cell.reason {
            Some(Failed(_) as reason) => reason
            Some(Graceful) | Some(Cancelled) | None =>
              match cell.cleanup_error {
                Some(secondary) => Failed(secondary)
                None => Failed(other)
              }
          }
      }
  }
}
