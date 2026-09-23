///|
/// Black-box API tests. Public API only.
///
/// Under test:
/// - A bounded mailbox at full load refuses WITHOUT changing the
///   already-accepted messages, and capacity recovers after the loop
///   consumed them (T05).
/// - The fold barrier: `tell(cmd); ask(())` proves every message enqueued
///   before the ask was already served, and the barrier never folds through
///   `on_cmd` — a barrier is not a command (T12). The compile-rejection
///   half of T12 (the legacy `ask(cmd)` misuse) is enforced by
///   tools/concurrency-api-guard.mbtx.
///
/// Admission windows are deterministic, not timing-based: `try_put` is a
/// synchronous, non-suspending call, so consecutive sends without a
/// suspension point form ONE uninterrupted scheduling turn whose outcome is
/// decided by the queue alone. When the loop is parked in `queue.get()`
/// (its steady state while idle), the first send is handed to that parked
/// reader point-to-point, bypassing the buffer — one uninterrupted turn
/// admits at most `capacity + 1` messages (1 handoff slot + `capacity`
/// buffered) and the NEXT send is refused with `MailboxFull`. The windows
/// below send at most `capacity + 2` messages and assert the refusal
/// arrived within that bound: the mailbox is pinned as BOUNDED without
/// pinning @aqueue's exact handoff arithmetic. Every wait is bounded; a
/// regression degrades into a fast assertion failure, never an unobserved
/// infinite stall.

///|
/// The bounded capacity used by T05: small, so the loop cannot drain the
/// window mid-turn (it cannot run during the window at all).
let t05_capacity : Int = 1

///|
/// Sends per admission window: `capacity + 1` may be admitted (handoff slot
/// + buffer, see the file comment), so one more send is guaranteed to hit
/// the full boundary within the same turn.
let t05_window_sends : Int = t05_capacity + 2

///|
/// Spin budget for the barrier retries: one pause lets the loop serve the
/// queued message, so the barrier is admitted on the next attempt.
let t05_spin_budget : Int = 10000

///|
/// Bounded retry of the barrier ask until the mailbox has room for it, i.e.
/// until every previously accepted message has been consumed. Only
/// `NotDelivered(MailboxFull)` is retried — that outcome admits nothing into
/// the mailbox, so retrying cannot reorder or duplicate anything. Any other
/// outcome is returned immediately for the caller to assert.
async fn t05_barrier(
  fuwaroid : @fuwaroid.Fuwaroid[Int, Unit, Int],
) -> Result[Int, @fuwaroid.AskFailure] {
  for _ in 0..<t05_spin_budget {
    match fuwaroid.ask((), timeout_ms=10000) {
      Err(
        @fuwaroid.AskFailure::NotDelivered(@fuwaroid.SendRefusal::MailboxFull)
      ) =>
        // Not admitted (mailbox still full): let the loop run, then retry.
        @async.pause()
      other => return other
    }
  }
  fail("T05: barrier ask never got mailbox room within the spin budget")
}

///|
/// ONE admission window: starting from an idle loop (parked on an empty
/// mailbox), send distinct commands back to back WITHOUT a suspension point
/// until the first refusal. Every send is synchronous, so the loop cannot
/// run between them and the outcome is decided by the queue alone. Returns
/// the accepted commands (in order) and the refusal that ended the window.
fn t05_admission_window(
  fuwaroid : @fuwaroid.Fuwaroid[Int, Unit, Int],
  first_cmd : Int,
) -> (Array[Int], @fuwaroid.SendRefusal?) {
  let accepted : Array[Int] = []
  let mut refusal : @fuwaroid.SendRefusal? = None
  for i in 0..<t05_window_sends {
    match fuwaroid.tell(first_cmd + i) {
      Ok(_) => accepted.push(first_cmd + i)
      Err(send_refusal) => {
        refusal = Some(send_refusal)
        break
      }
    }
  }
  (accepted, refusal)
}

///|
fn t05_sum(commands : Array[Int]) -> Int {
  let mut total = 0
  for command in commands {
    total = total + command
  }
  total
}

///|
/// T05: bounded full load and capacity recovery, through the
/// PUBLIC spawn API. Assertions:
/// 1. control: the loop serves the mailbox (barrier reply on empty, count 0),
/// 2. window 1 hits the full boundary in ONE turn: at least one accept, the
///    refusal is exactly `MailboxFull`, and at most `capacity + 1` messages
///    were admitted (the mailbox is genuinely bounded),
/// 3. the refusal changed nothing about accepted messages: the barrier count
///    is EXACTLY the sum of the accepted commands — the refused command (and
///    only it) never applied,
/// 4. capacity recovered after consumption: window 2 accepts again, hits the
///    same bounded boundary, and the final barrier count is the exact total.
async test "T05 bounded full load refuses without changing accepted messages and recovers capacity" {
  @async.with_task_group(group => {
    let fuwaroid : @fuwaroid.Fuwaroid[Int, Unit, Int] = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, n) => state + n,
      on_query=(_, state, _query) => (state, state),
      mailbox=@fuwaroid.Mailbox::Bounded(t05_capacity),
    )
    // Control: the loop serves this mailbox and the ask left it empty —
    // the reply proves the ask itself was processed (count 0).
    let empty = t05_barrier(fuwaroid)
    debug_inspect(empty, content="Ok(0)")
    // ---- Admission window 1: one uninterrupted scheduling turn. ----
    let (accepted1, refusal1) = t05_admission_window(fuwaroid, 1)
    if accepted1.is_empty() {
      fail(
        "T05: the bounded mailbox refused a send although the loop was idle and the buffer empty",
      )
    }
    guard refusal1 is Some(@fuwaroid.MailboxFull) else {
      fail(
        "T05: window 1 did not end with MailboxFull within \{t05_window_sends} sends: refusal=\{Repr(refusal1)}, accepted=\{Repr(accepted1)}",
      )
    }
    if accepted1.length() > t05_capacity + 1 {
      fail(
        "T05: the mailbox admitted \{accepted1.length()} messages in one turn at capacity \{t05_capacity} — it is not bounded",
      )
    }
    // The refusal changed nothing about the accepted messages: exactly the
    // accepted commands are folded, the refused one never applies.
    let after_full = t05_barrier(fuwaroid)
    assert_eq(after_full, Ok(t05_sum(accepted1)))
    // ---- Capacity recovered after consumption: same-turn boundary probe.
    let (accepted2, refusal2) = t05_admission_window(fuwaroid, 100)
    if accepted2.is_empty() {
      fail(
        "T05: capacity did not recover after the loop consumed the accepted messages",
      )
    }
    guard refusal2 is Some(@fuwaroid.MailboxFull) else {
      fail(
        "T05: window 2 did not end with MailboxFull within \{t05_window_sends} sends: refusal=\{Repr(refusal2)}, accepted=\{Repr(accepted2)}",
      )
    }
    if accepted2.length() > t05_capacity + 1 {
      fail(
        "T05: the mailbox admitted \{accepted2.length()} messages in one turn at capacity \{t05_capacity} — it is not bounded",
      )
    }
    // Final barrier: exactly the accepted commands of both windows were
    // applied; both refusals left no trace.
    let final_count = t05_barrier(fuwaroid)
    assert_eq(final_count, Ok(t05_sum(accepted1) + t05_sum(accepted2)))
  })
}

///|
/// T12 command vocabulary: each command carries its ordinal so FIFO service
/// order is observable through the handler-side `seen` log.
priv enum T12Cmd {
  T12Add(Int)
}

///|
/// T12: the fold barrier, through the public spawn_fold API. `tell(cmd);
/// ask(())` proves every message enqueued before the ask was already
/// served: the Unit query rides the same FIFO mailbox, so its reply is
/// only produced after the loop served all five commands. The barrier must
/// NOT fold through `on_cmd` (a barrier is not a command): the `seen` log
/// holds exactly the told commands, never one more. The compile-rejection
/// half of T12 (the legacy `ask(cmd)` misuse) is enforced by
/// tools/concurrency-api-guard.mbtx, not here.
async test "T12 fold barrier tell-then-ask-unit proves prior processing" {
  @async.with_task_group(group => {
    let seen : Array[Int] = []
    let fuwaroid = @fuwaroid.Fuwaroid::spawn_fold(group~, init=0, on_cmd=(
      _,
      state,
      cmd,
    ) => {
      match cmd {
        T12Add(n) => {
          seen.push(n)
          state + n
        }
      }
    })
    for i in 0..<5 {
      let _ = fuwaroid.tell(T12Cmd::T12Add(i + 1))
    }
    // Barrier: the reply certifies all five commands above were served
    // (FIFO), and the ask did not execute any command itself.
    let barrier = fuwaroid.ask((), timeout_ms=10000)
    debug_inspect(barrier, content="Ok(())")
    assert_eq(seen.length(), 5)
    assert_eq(seen[4], 5)
    // A post-barrier command is still processed normally, and a second
    // barrier certifies it too: the barrier is repeatable, not one-shot.
    let _ = fuwaroid.tell(T12Cmd::T12Add(100))
    let again = fuwaroid.ask((), timeout_ms=10000)
    debug_inspect(again, content="Ok(())")
    assert_eq(seen.length(), 6)
    assert_eq(seen[5], 100)
  })
}
