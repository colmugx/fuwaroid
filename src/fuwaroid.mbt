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
///   original error otherwise).
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
  /// This instance's loop stop record (loop task handle, termination
  /// reason, secondary cleanup evidence). `None` only for handles built
  /// directly (white-box, same package) without `spawn`: such a handle
  /// has no loop of its own.
  priv stop : LoopStop?
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
/// Why the loop task terminated. Recorded by the unified stop path
/// (`fuwaroid_cleanup`) before the loop task terminates; abnormal stops
/// keep the ORIGINAL error identity (the host's cancellation error, or
/// whatever terminal mailbox error occurred), never a compressed copy.
pub(all) enum StopReason {
  /// `close` was requested and the closed-and-empty drain completed.
  Graceful
  /// The loop stopped abnormally: host cancellation observed at a yield
  /// or `queue.get` boundary, or another terminal mailbox error.
  Stopped(Error)
} derive(Debug)

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
  mut reason : StopReason
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
  { reason: Graceful, cleanup_error: None, task: None, diag: Diag::fresh(), }
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
  let fuwaroid : Fuwaroid[Cmd, Query, Reply] = {
    mailbox: queue,
    stop: Some(stop),
  }
  let ctx : Ctx[Cmd, Query, Reply] = { group, address: fuwaroid, }
  // TaskGroup::spawn (not spawn_bg) so the loop task handle exists for
  // termination waiters (`join`); no_wait stays true: the host group does
  // not wait for the loop, it cancels it when the scope ends.
  // allow_failure keeps its default (false): a cleanup-path bug that lets
  // a non-cancellation error escape still fails the host group.
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
    on_query=(_, state, _) => (state, ()),
    mailbox~,
  )
}

///|
/// Complete messages processed between two scheduler yields. A batch of 1
/// maximizes fairness at the cost of a scheduling round trip per message;
/// larger batches amortize the yield but delay other ready tasks and
/// cancellation observation. 32 is a starting point — retune it with the
/// benchmark harness, not by intuition.
let fuwaroid_yield_batch : Int = 32

///|
/// The serial drain. `queue.get()` suspends until a message arrives, so
/// the loop never busy-polls. A closed-and-empty mailbox (graceful stop
/// after `close`) ends the loop gracefully; task cancellation observed at
/// a suspension boundary (`queue.get`, or the batch yield between
/// complete messages) or any other terminal mailbox error ends it
/// abnormally. ALL of these flow into the single cleanup path
/// (`fuwaroid_cleanup`): admission is closed, stranded asks are failed,
/// the termination reason is recorded — and only then does this task
/// terminate, re-raising the ORIGINAL cause for abnormal stops (so a
/// cancelled loop task still terminates with the cancellation identity
/// and a foreign stop error stays observable) and returning normally
/// after a graceful drain.
///
/// Cancellation outranks draining: the loop stops at the next
/// observable cancellation boundary — the batch yield after the message
/// currently being served, or the next blocking `get` — instead of
/// finishing the whole backlog. A message that `queue.get` already
/// delivered when cancellation lands is still served: `try_get` does not
/// observe cancellation, so one accepted message is never dropped
/// mid-batch.
async fn[State, Cmd, Query, Reply] fuwaroid_loop(
  ctx : Ctx[Cmd, Query, Reply],
  init : State,
  on_cmd : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  stop? : LoopStop = LoopStop::fresh(),
) -> Unit {
  let mut state = init
  let mut served_in_batch = 0
  let mut cause : Error? = None
  while cause is None {
    let next : Result[Envelope[Cmd, Query, Reply], Error] = Ok(queue.get()) catch {
      error => Err(error)
    }
    match next {
      Ok(Tell(cmd)) => {
        state = on_cmd(ctx, state, cmd)
        // Counting point: the handler returned, the state is committed.
        stop.diag.processed = stop.diag.processed + 1L
        served_in_batch += 1
      }
      Ok(Ask(query, reply)) => {
        let pair = on_query(ctx, state, query)
        state = pair.0
        let _ = reply.try_put(pair.1) catch { _ => false }
        // Counting point: the handler returned, the state is committed
        // (the reply handoff is best-effort and not part of the criterion).
        stop.diag.processed = stop.diag.processed + 1L
        served_in_batch += 1
      }
      Err(error) => cause = Some(error)
    }
    // Yield point BETWEEN complete messages only: the handler above ran
    // entry-to-return with no suspension inside. `pause` re-queues this
    // task at the tail of the ready queue and raises the host's
    // cancellation here if it was requested since the last boundary, so a
    // cancelled loop cannot drain an unbounded backlog.
    if cause is None && served_in_batch >= fuwaroid_yield_batch {
      served_in_batch = 0
      let yielded : Result[Unit, Error] = Ok(@async.pause()) catch {
        error => Err(error)
      }
      match yielded {
        Ok(_) => ()
        Err(error) => cause = Some(error)
      }
    }
  }
  fuwaroid_cleanup(queue, cause, stop)
}

///|
/// THE single stop/cleanup path for the loop. Runs exactly
/// once, after the message loop exits, for every kind of stop:
///
/// 1. Close admission (`queue.close`): idempotent — safe when the mailbox
///    was already closed (graceful `close`, or a foreign close). From here
///    on the handle refuses new messages.
/// 2. Fail every accepted-but-unserved ask via `fail_leftover_asks` so
///    its waiter gets `AskFailure::Stopped` immediately.
/// 3. Record the termination reason on `stop`: `Graceful` for a
///    closed-and-empty drain, `Stopped(original cause)` otherwise, and
///    keep any secondary cleanup failure as evidence on
///    `stop.cleanup_error` instead of swallowing it.
/// 4. Propagate: a graceful stop returns normally unless the cleanup
///    itself failed; an abnormal stop re-raises the ORIGINAL cause — the
///    cleanup error never replaces it (a cancelled loop task keeps the
///    cancellation identity; a foreign stop error stays observable).
fn[Cmd, Query, Reply] fuwaroid_cleanup(
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  cause : Error?,
  stop : LoopStop,
) -> Unit raise {
  // The default close error is intentional: it only decides what NEW
  // attempts to touch the mailbox see; the original stop cause is
  // retained below and stranded asks get `StoppedError` in step 2.
  queue.close()
  // Counting point: every envelope the stranded-ask drain removes
  // was accepted but is provably unserved — commands and queries alike
  // are counted as abandoned (the drain runs in one uninterrupted
  // scheduling turn, so nothing can be admitted or served in between).
  let drained : Result[Unit, Error] = Ok(
    fail_leftover_asks(queue, diag=stop.diag),
  ) catch {
    error => Err(error)
  }
  let secondary : Error? = match drained {
    Ok(_) => None
    Err(error) => Some(error)
  }
  match cause {
    // Graceful: closed-and-empty drain completed (or the loop exited
    // without a cause — same disposition).
    Some(@aqueue.QueueAlreadyClosed) | None => {
      stop.reason = Graceful
      stop.cleanup_error = secondary
      // Counting point: the unified cleanup is complete — the phase
      // moves to Stopped carrying the recorded reason (forward-only).
      stop.diag.transition(Lifecycle::Stopped(Graceful))
      match secondary {
        Some(error) => raise error
        None => ()
      }
    }
    // Abnormal: keep the original error identity; the secondary cleanup
    // failure is preserved as evidence, never used as the replacement.
    Some(original) => {
      stop.reason = Stopped(original)
      stop.cleanup_error = secondary
      // Counting point: the unified cleanup is complete — the phase
      // moves to Stopped carrying the recorded reason (forward-only);
      // `abandoned` was fixed by the drain above.
      stop.diag.transition(Lifecycle::Stopped(Stopped(original)))
      raise original
    }
  }
}

///|
/// After an abnormal stop (host cancellation), asks may still sit in the
/// mailbox; their callers wait on reply queues that will never be filled.
/// Close each so every waiter fails fast with `StoppedError`. On a
/// graceful `close` the mailbox is already empty-and-closed here, and the
/// first `try_get` raises `QueueAlreadyClosed` — a no-op. Any other queue
/// error is propagated so cleanup cannot hide a failure.
///
/// Every envelope removed here (commands AND queries) is counted as
/// `abandoned` on the given diagnostics cell, when one is supplied: the
/// drain is the exact truncation point between "accepted and served" and
/// "accepted but unserved". Direct white-box callers that pass
/// no cell only get the draining behavior.
fn[Cmd, Query, Reply] fail_leftover_asks(
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
  diag? : Diag,
) -> Unit raise {
  let mut draining = true
  while draining {
    let next : Result[Envelope[Cmd, Query, Reply]?, Error] = Ok(queue.try_get()) catch {
      error => Err(error)
    }
    match next {
      Ok(Some(Ask(_, reply))) => {
        match diag {
          Some(d) => d.abandoned = d.abandoned + 1L
          None => ()
        }
        reply.close(error=StoppedError)
      }
      Ok(Some(Tell(_))) =>
        match diag {
          Some(d) => d.abandoned = d.abandoned + 1L
          None => ()
        }
      Ok(None) => draining = false
      Err(@aqueue.QueueAlreadyClosed) => draining = false
      Err(error) => raise error
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
  let delivered : Result[Bool, Error] = Ok(
    self.mailbox.try_put(Envelope::Tell(cmd)),
  ) catch {
    error => Err(error)
  }
  match delivered {
    Ok(true) => {
      // Counting point: admission accepted.
      match self.stop {
        Some(cell) => cell.diag.accepted = cell.diag.accepted + 1L
        None => ()
      }
      Ok(())
    }
    Ok(false) => {
      // Counting point: admission refused, mailbox full.
      match self.stop {
        Some(cell) => cell.diag.rejected_full = cell.diag.rejected_full + 1L
        None => ()
      }
      Err(SendRefusal::MailboxFull)
    }
    Err(_) => {
      // Counting point: admission refused, mailbox closed.
      match self.stop {
        Some(cell) => cell.diag.rejected_closed = cell.diag.rejected_closed + 1L
        None => ()
      }
      Err(SendRefusal::MailboxClosed)
    }
  }
}

///|
/// Pure classification of the errors `ask` can see on its reply queue:
/// mapped failures return `Some`; `None` means "not ours" — the caller
/// must propagate (notably host cancellation of the asker).
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
  let delivered : Result[Bool, Error] = Ok(
    self.mailbox.try_put(Envelope::Ask(query, reply)),
  ) catch {
    error => Err(error)
  }
  match delivered {
    Ok(true) =>
      // Counting point: admission accepted (queries count too).
      match self.stop {
        Some(cell) => cell.diag.accepted = cell.diag.accepted + 1L
        None => ()
      }
    Ok(false) => {
      // Counting point: admission refused, mailbox full.
      match self.stop {
        Some(cell) => cell.diag.rejected_full = cell.diag.rejected_full + 1L
        None => ()
      }
      return Err(AskFailure::NotDelivered(SendRefusal::MailboxFull))
    }
    Err(_) => {
      // Counting point: admission refused, mailbox closed.
      match self.stop {
        Some(cell) => cell.diag.rejected_closed = cell.diag.rejected_closed + 1L
        None => ()
      }
      return Err(AskFailure::NotDelivered(SendRefusal::MailboxClosed))
    }
  }
  let outcome : Result[Reply?, Error] = Ok(
    @async.with_timeout_opt(timeout_ms, () => reply.get()),
  ) catch {
    error => Err(error)
  }
  match outcome {
    Ok(Some(answer)) => Ok(answer)
    Ok(None) => Err(AskFailure::TimedOut)
    Err(error) =>
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
  match self.stop {
    Some(cell) => cell.diag.transition(Closing)
    None => ()
  }
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
/// - The loop task ending `Done` (graceful closed-and-empty drain)
///   returns the recorded reason, `Graceful`. The task failing returns
///   the recorded `Stopped(original cause)` — notably host cancellation,
///   which the unified stop path records before re-raising the cancel
///   identity, so a joiner that is NOT itself cancelled observes
///   `Stopped(cancel error)` instead of the raw cancellation.
/// - If the WAITER itself is cancelled, that cancellation propagates: the
///   caught wait error is re-raised after checking
///   `@async.is_being_cancelled()`, never converted into a `StopReason`.
/// - Fallback boundary: a task failure whose recorded
///   reason is still `Graceful` means an error escaped outside the
///   recorded original cause — the recorded secondary cleanup evidence is
///   mapped to `Stopped` when present (the task did fail; the evidence
///   must not vanish), and the escaped error itself otherwise (only
///   reachable through a cleanup-path bug).
/// - A handle not obtained from `spawn` (white-box construction) has no
///   loop of its own; `join` returns `Graceful` immediately for it.
///
/// Note: when the loop task fails with a NON-cancellation error, the
/// host task group's fail-fast still applies independently of `join` —
/// a joiner living in the same group is cancelled with the group before
/// it can observe `Stopped`; a joiner in a different scope observes it.
pub async fn[Cmd, Query, Reply] Fuwaroid::join(
  self : Fuwaroid[Cmd, Query, Reply],
) -> StopReason {
  let cell = match self.stop {
    Some(cell) => cell
    None => return Graceful
  }
  let task = match cell.task {
    Some(task) => task
    None => return Graceful
  }
  let outcome : Result[Unit, Error] = Ok(task.wait()) catch {
    error => Err(error)
  }
  match outcome {
    Ok(_) => cell.reason
    Err(error) =>
      if @async.is_being_cancelled() {
        raise error
      } else {
        match cell.reason {
          Stopped(_) => cell.reason
          Graceful =>
            match cell.cleanup_error {
              Some(secondary) => Stopped(secondary)
              None => Stopped(error)
            }
        }
      }
  }
}
