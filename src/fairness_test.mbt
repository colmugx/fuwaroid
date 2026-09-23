///|
/// Black-box fairness and lifecycle tests. Public API only.
///
/// Under test:
/// - An instance that keeps its own mailbox non-empty must yield the
///   scheduler between complete messages, so other instances make progress
///   before a hot chain finishes on its own (T01/T02).
/// - Cancelling the host task group while the mailbox is still non-empty
///   must stop the loop at an observable cancellation boundary (cancellation
///   outranks draining the backlog) and the handle must refuse new messages
///   afterwards (T03).
///
/// Coordination is deterministic, with no sleep-based timing guesses: a
/// self-perpetuating chain cannot be barriered by a pre-enqueued ask (the
/// chain's later links only exist while it runs), so completion is observed
/// through a flag the handler sets on its last step, awaited with a bounded
/// spin-yield loop. Fairness ordering is asserted from handler-side
/// execution logs — the true service order; waiter wakeup order is
/// scheduler noise, not evidence. Every wait is bounded (generous ask
/// timeouts, bounded spin trip counts), so a regression degrades into a
/// fast assertion failure, never an unobserved infinite stall.

///|
/// Chain length for T01. Large enough that a yielding implementation is
/// forced to yield many times, and a no-yield one demonstrably monopolizes
/// a scheduling quantum.
let t01_chain_len : Int = 2000

///|
/// Per-instance chain length (three instances).
let t02_chain_len : Int = 1000

///|
/// Chain length for T03's busy mailbox.
let t03_chain_len : Int = 2000

///|
/// Spin budget for completion flags: one spin-yield advances every ready
/// loop by one batch, so `chain_len` iterations always suffice; the margin
/// covers the auxiliary ask tasks.
let spin_budget : Int = 10000

///|
priv enum T01HotCmd {
  T01Step
}

///|
priv enum T01HotQuery {
  T01Barrier
}

///|
priv enum T01ColdQuery {
  T01Ping
}

///|
/// T01: a hot instance keeps feeding itself commands while a cold instance
/// must get a query served. Assertions: the chain fully executed (the
/// read-back count equals the chain length); the cold ask succeeded; and
/// the handler-side log shows the cold query was SERVED before the last
/// hot chain step.
async test "T01 cold instance progresses while hot instance self-chains" {
  let log : Array[String] = []
  let hot_done : Ref[Bool] = Ref(false)
  @async.with_task_group(group => {
    let hot = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(ctx, state, cmd) => {
        match cmd {
          T01Step => {
            log.push("hot")
            let next = state + 1
            if next < t01_chain_len {
              let _ = ctx.address.tell(T01Step)
            } else {
              hot_done.val = true
            }
            next
          }
        }
      },
      on_query=(_, state, _query) => (state, state),
    )
    // The cold instance has no command flow in this test; only its query
    // path is exercised.
    let cold : @fuwaroid.Fuwaroid[Unit, T01ColdQuery, Int] = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, _cmd) => state,
      on_query=(_, state, _query) => {
        log.push("cold")
        (state + 1, state + 1)
      },
    )
    let _ = hot.tell(T01Step)
    let cold_task = group.spawn(() => cold.ask(T01Ping, timeout_ms=10000))
    // Bounded spin until the chain has demonstrably completed (the flag is
    // set by the handler itself, never by wall-clock time).
    let mut completed = false
    for _ in 0..<spin_budget {
      if hot_done.val {
        completed = true
        break
      }
      @async.pause()
    }
    if !completed {
      fail("T01: hot chain did not complete within the spin budget")
    }
    let cold_result = cold_task.wait()
    guard cold_result is Ok(reply) else {
      fail("T01: cold instance was not served: \{Repr(cold_result)}")
    }
    assert_eq(reply, 1)
    // Read back the final count now that the chain is done: the reply
    // certifies every link was folded.
    let barrier = hot.ask(T01Barrier, timeout_ms=10000)
    guard barrier is Ok(count) else {
      fail("T01: chain count read-back failed: \{Repr(barrier)}")
    }
    assert_eq(count, t01_chain_len)
    let mut cold_index = -1
    let mut hot_total = 0
    for i, entry in log {
      if entry == "cold" && cold_index < 0 {
        cold_index = i
      }
      if entry == "hot" {
        hot_total += 1
      }
    }
    if hot_total != t01_chain_len {
      fail(
        "T01: chain did not execute exactly once per link: hot_total=\{hot_total}",
      )
    }
    if cold_index < 0 {
      fail("T01: cold instance made no progress at all")
    }
    if cold_index >= hot_total {
      fail(
        "T01: cold instance only progressed after the hot chain finished: cold_index=\{cold_index}, hot_total=\{hot_total}",
      )
    }
  })
}

///|
priv enum T02Cmd {
  T02Step
}

///|
priv enum T02Query {
  T02Stats
}

///|
priv struct T02State {
  mut count : Int
  mut sum : Int
}

///|
/// One T02 instance: a self-chaining counter that records every processed
/// step in the shared execution log, folds a checksum over its own
/// sequence, and raises its completion flag on the last link.
fn t02_hot_instance(
  group : @async.TaskGroup[Unit],
  tag : String,
  log : Array[String],
  done : Ref[Bool],
) -> @fuwaroid.Fuwaroid[T02Cmd, T02Query, (Int, Int)] {
  @fuwaroid.Fuwaroid::spawn(
    group~,
    init={ count: 0, sum: 0, },
    on_cmd=(ctx, state, _cmd) => {
      log.push(tag)
      state.count = state.count + 1
      state.sum = state.sum + state.count
      if state.count < t02_chain_len {
        let _ = ctx.address.tell(T02Step)
      } else {
        done.val = true
      }
      state
    },
    on_query=(_, state, _query) => (state, (state.count, state.sum)),
  )
}

///|
/// T02: several hot instances run self-chains concurrently. Every chain
/// must fully execute with its per-instance FIFO intact (count/checksum
/// read back after completion), and the instance that finishes first must
/// have seen interleaved progress from the others: under a yielding
/// scheduler the chains advance in interleaved batches, while a
/// one-quantum implementation runs them strictly one after another.
async test "T02 hot instances progress together with per-instance FIFO" {
  let log : Array[String] = []
  let done_a : Ref[Bool] = Ref(false)
  let done_b : Ref[Bool] = Ref(false)
  let done_c : Ref[Bool] = Ref(false)
  @async.with_task_group(group => {
    let a = t02_hot_instance(group, "A", log, done_a)
    let b = t02_hot_instance(group, "B", log, done_b)
    let c = t02_hot_instance(group, "C", log, done_c)
    let _ = a.tell(T02Step)
    let _ = b.tell(T02Step)
    let _ = c.tell(T02Step)
    // Bounded spin until all three chains have demonstrably completed.
    let mut completed = false
    for _ in 0..<(spin_budget * 3) {
      if done_a.val && done_b.val && done_c.val {
        completed = true
        break
      }
      @async.pause()
    }
    if !completed {
      fail("T02: chains did not complete within the spin budget")
    }
    // Read back the final count and checksum of each chain now that the
    // chains are done: the replies certify every ordinal was folded exactly
    // once, in order (per-instance FIFO).
    let expected_sum = t02_chain_len * (t02_chain_len + 1) / 2
    let ra = a.ask(T02Stats, timeout_ms=10000)
    let rb = b.ask(T02Stats, timeout_ms=10000)
    let rc = c.ask(T02Stats, timeout_ms=10000)
    guard ra is Ok((count_a, sum_a)) else {
      fail("T02: instance A read-back failed: \{Repr(ra)}")
    }
    guard rb is Ok((count_b, sum_b)) else {
      fail("T02: instance B read-back failed: \{Repr(rb)}")
    }
    guard rc is Ok((count_c, sum_c)) else {
      fail("T02: instance C read-back failed: \{Repr(rc)}")
    }
    assert_eq(count_a, t02_chain_len)
    assert_eq(sum_a, expected_sum)
    assert_eq(count_b, t02_chain_len)
    assert_eq(sum_b, expected_sum)
    assert_eq(count_c, t02_chain_len)
    assert_eq(sum_c, expected_sum)
    // Service-log completeness per instance.
    let mut total_a = 0
    let mut total_b = 0
    let mut total_c = 0
    for entry in log {
      match entry {
        "A" => total_a += 1
        "B" => total_b += 1
        "C" => total_c += 1
        _ => ()
      }
    }
    if total_a != t02_chain_len ||
      total_b != t02_chain_len ||
      total_c != t02_chain_len {
      fail(
        "T02: per-instance service counts are incomplete: A=\{total_a}, B=\{total_b}, C=\{total_c}",
      )
    }
    // Fairness distinction: when the FIRST chain completed, the other two
    // instances must already have made progress. Deterministic because all
    // three loops are ready before any of them runs (the three tells
    // precede the test task's first suspension) and a yielding
    // implementation re-queues at the tail on every batch boundary, so the
    // other instances run ahead of a resumed batch.
    let mut last_a = -1
    let mut last_b = -1
    let mut last_c = -1
    for i, entry in log {
      match entry {
        "A" => last_a = i
        "B" => last_b = i
        "C" => last_c = i
        _ => ()
      }
    }
    let finishes : Array[(String, Int)] = [
      ("A", last_a),
      ("B", last_b),
      ("C", last_c),
    ]
    let mut first_tag = ""
    let mut first_index = -1
    for pair in finishes {
      if pair.1 >= 0 && (first_index < 0 || pair.1 < first_index) {
        first_tag = pair.0
        first_index = pair.1
      }
    }
    if first_index < 0 {
      fail("T02: no instance recorded any processed step")
    }
    let mut others_before = 0
    for i, entry in log {
      if i < first_index && entry != first_tag {
        others_before += 1
      }
    }
    if others_before == 0 {
      fail(
        "T02: the first finished chain (\{first_tag}) saw no interleaved progress from the others: log_len=\{log.length()}",
      )
    }
  })
}

///|
priv enum T03Cmd {
  T03Step
}

///|
/// T03: the host task group is cancelled by an external observer while the
/// mailbox is kept non-empty by a self-perpetuating chain. The observer
/// lives in the test task's scope, independent of the actor's host group.
///
/// Assertions: the host group terminates after cancellation; the handle
/// refuses new tells and asks afterwards (fail fast, no hang); the loop
/// stops at an observable cancellation boundary instead of finishing the
/// whole backlog; and `join` from the uncancelled observer returns
/// `Cancelled` — the loop recorded the host's cancellation signal (which
/// is distinct from any `Error` in async 0.22.x) and terminated
/// cancelled.
async test "T03 cancelling a busy host stops the loop and closes admission" {
  let processed : Ref[Int] = Ref(0)
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T03Cmd, Unit, Unit]?] = Ref(None)
  let group_cell : Ref[@async.TaskGroup[Unit]?] = Ref(None)
  @async.with_task_group(outer => {
    // Host owner task: creates the host group and the actor, triggers the
    // chain, then parks. It is cancelled from outside by the observer.
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        group_cell.val = Some(host)
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          ctx,
          _,
          _cmd,
        ) => {
          processed.val = processed.val + 1
          if processed.val < t03_chain_len {
            let _ = ctx.address.tell(T03Step)
          }
        })
        handle_cell.val = Some(handle)
        let _ = handle.tell(T03Step)
        // Park until the observer cancels the group; bounded so a broken
        // observer still lets the test terminate.
        @async.sleep(60000)
      })
    })
    // Observer: wait until the chain has demonstrably started (the loop has
    // processed at least one message), then cancel the host group while its
    // mailbox still holds the chain backlog.
    let mut started = false
    for _ in 0..<spin_budget {
      if processed.val > 0 {
        started = true
        break
      }
      @async.pause()
    }
    if !started {
      fail("T03: chain never started")
    }
    guard group_cell.val is Some(host) else {
      fail("T03: host group not visible to the observer")
    }
    guard handle_cell.val is Some(handle) else {
      fail("T03: handle not visible to the observer")
    }
    host.return_immediately(())
    // with_task_group returns only after every child of the host group has
    // terminated — including the cancelled actor loop and the cancelled
    // group body. The cancellation of those children is not a group
    // failure, so the owner task completes normally with the group result;
    // any raised error would be an abnormal termination worth failing on.
    let host_outcome : Result[Unit, Error] = Ok(host_task.wait()) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T03: host group terminated abnormally: \{error}")
    }
    // The loop is gone; the handle must refuse new messages instead of
    // accepting them into a mailbox nobody serves.
    let refused = handle.tell(T03Step)
    debug_inspect(refused, content="Err(MailboxClosed)")
    let late_ask = handle.ask((), timeout_ms=10000)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
    // Cancellation outranks draining: the loop must not have finished
    // the whole backlog after the cancellation was requested mid-chain.
    if processed.val >= t03_chain_len {
      fail(
        "T03: loop drained the whole backlog after cancellation: processed=\{processed.val} of \{t03_chain_len}",
      )
    }
    // Cancellation through join: the recorded stop reason is `Cancelled`
    // — the loop's cancellation branch completed its protected cleanup
    // and recorded the cancellation before the task terminated cancelled.
    // The observer is not itself cancelled, so `join` surfaces the
    // recorded reason rather than its own signal; `with_timeout` is only
    // a hang guard.
    let reason = @async.with_timeout(10000, () => handle.join())
    guard reason is @fuwaroid.Cancelled else {
      fail(
        "T03: join after host cancellation returned \{Repr(reason)}, expected Cancelled",
      )
    }
  })
}
