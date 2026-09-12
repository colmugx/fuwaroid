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
/// - The loop is spawned `no_wait` on the HOST's long-lived task group.
///   There is no actor system, no registry, no supervision tree: the host
///   owns the lifetime (structured concurrency). Handlers cannot raise
///   (their types say so); failure is an ordinary message or terminal
///   state — never a restart that resets state, which is what ledgers of
///   in-flight work require.
pub struct Fuwaroid[Cmd, Query, Reply] {
  priv mailbox : @aqueue.Queue[Envelope[Cmd, Query, Reply]]
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
  /// This Fuwaroid's address. Use `tell` only: a self-`ask` from inside a
  /// handler deadlocks (the loop is busy serving the current message)
  /// until the ask timeout fires.
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
/// Internal marker closed into reply queues of asks stranded by an
/// abnormal loop stop, so their waiters fail fast instead of timing out.
pub(all) suberror StoppedError {
  StoppedError
}

///|
/// Spawn a Fuwaroid on the host's long-lived task group. Commands and
/// queries are SEPARATE types: `on_cmd` folds a command into the state,
/// `on_query` folds a query and produces its reply — neither handler has
/// unreachable arms for the other flow. Both run on the single loop,
/// serially, in mailbox FIFO order, and may not raise. Use
/// `Fuwaroid::spawn_fold` when there is no query flow at all.
pub fn[State, Cmd, Query, Reply] Fuwaroid::spawn(
  group~ : @async.TaskGroup[Unit],
  init~ : State,
  on_cmd~ : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query~ : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  mailbox? : @aqueue.Kind = @aqueue.Unbounded,
) -> Fuwaroid[Cmd, Query, Reply] {
  let queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]] = @aqueue.Queue(
    kind=mailbox,
  )
  let fuwaroid : Fuwaroid[Cmd, Query, Reply] = { mailbox: queue, }
  let ctx : Ctx[Cmd, Query, Reply] = { group, address: fuwaroid, }
  group.spawn_bg(no_wait=true, () => {
    fuwaroid_loop(ctx, init, on_cmd, on_query, queue)
  })
  fuwaroid
}

///|
/// `Fuwaroid::spawn` without a query flow: the state simply folds over
/// the command stream. `ask` degenerates to a served-receipt (`Ok(())`)
/// through a no-op query handler — it does NOT fold through `on_cmd`;
/// its value is FIFO proof: the reply certifies every message enqueued
/// before it was already served.
pub fn[State, Cmd] Fuwaroid::spawn_fold(
  group~ : @async.TaskGroup[Unit],
  init~ : State,
  on_cmd~ : (Ctx[Cmd, Cmd, Unit], State, Cmd) -> State,
  mailbox? : @aqueue.Kind = @aqueue.Unbounded,
) -> Fuwaroid[Cmd, Cmd, Unit] {
  Fuwaroid::spawn(
    group~,
    init~,
    on_cmd~,
    on_query=(_, state, _) => (state, ()),
    mailbox~,
  )
}

///|
/// The serial drain. `queue.get()` suspends until a message arrives, so
/// the loop never busy-polls; a closed-and-empty mailbox (graceful stop
/// after `close`) or task cancellation wakes it with an error, which ends
/// the loop. The loop closes the mailbox before cleanup on every stop, so
/// a stopped address cannot accept new messages. Reply delivery to an
/// asker that already timed out or went away is a best-effort `try_put` —
/// nobody is listening, the queue is garbage.
async fn[State, Cmd, Query, Reply] fuwaroid_loop(
  ctx : Ctx[Cmd, Query, Reply],
  init : State,
  on_cmd : (Ctx[Cmd, Query, Reply], State, Cmd) -> State,
  on_query : (Ctx[Cmd, Query, Reply], State, Query) -> (State, Reply),
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
) -> Unit {
  let mut state = init
  let mut stopped = false
  let mut stop_error : Error? = None
  while !stopped {
    let next : Result[Envelope[Cmd, Query, Reply], Error] = Ok(queue.get()) catch {
      error => Err(error)
    }
    match next {
      Ok(Tell(cmd)) => state = on_cmd(ctx, state, cmd)
      Ok(Ask(query, reply)) => {
        let pair = on_query(ctx, state, query)
        state = pair.0
        let _ = reply.try_put(pair.1) catch { _ => false }
      }
      Err(error) => {
        stop_error = Some(error)
        stopped = true
      }
    }
  }
  // Close admission before draining. The default close error is intentional:
  // stranded reply queues get StoppedError, while the original loop error is
  // retained below for propagation.
  queue.close()
  let cleanup : Result[Unit, Error] = Ok(fail_leftover_asks(queue)) catch {
    error => Err(error)
  }
  match stop_error {
    Some(error) =>
      match error {
        @aqueue.QueueAlreadyClosed =>
          match cleanup {
            Ok(_) => ()
            Err(cleanup_error) => raise cleanup_error
          }
        _ =>
          // Cleanup must not replace a cancellation or foreign stop cause.
          raise error
      }
    None =>
      match cleanup {
        Ok(_) => ()
        Err(error) => raise error
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
fn[Cmd, Query, Reply] fail_leftover_asks(
  queue : @aqueue.Queue[Envelope[Cmd, Query, Reply]],
) -> Unit raise {
  let mut draining = true
  while draining {
    let next : Result[Envelope[Cmd, Query, Reply]?, Error] = Ok(queue.try_get()) catch {
      error => Err(error)
    }
    match next {
      Ok(Some(Ask(_, reply))) => reply.close(error=StoppedError)
      Ok(Some(Tell(_))) => ()
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
/// the caller's scheduling.
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
    Ok(true) => Ok(())
    Ok(false) => Err(SendRefusal::MailboxFull)
    Err(_) => Err(SendRefusal::MailboxClosed)
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
/// `timeout_ms`. Never call `ask` from inside a handler of the same
/// Fuwaroid: the loop is busy serving that handler and would deadlock
/// until this timeout fires. Cancellation of the CALLER propagates (it is
/// not converted into a failure value).
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
    Ok(true) => ()
    Ok(false) => return Err(AskFailure::NotDelivered(SendRefusal::MailboxFull))
    Err(_) => return Err(AskFailure::NotDelivered(SendRefusal::MailboxClosed))
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
pub fn[Cmd, Query, Reply] Fuwaroid::close(
  self : Fuwaroid[Cmd, Query, Reply],
) -> Unit {
  self.mailbox.close()
}
