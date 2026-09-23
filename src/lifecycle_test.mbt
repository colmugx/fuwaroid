///|
/// Black-box lifecycle tests for close/join semantics. Public API only.
///
/// Under test:
/// - `close` is idempotent, and every termination waiter — concurrent or
///   late, inside the host scope or outside it — observes the SAME
///   recorded stop reason (`Graceful` after a full drain), while the loop
///   keeps serving the backlog unperturbed (T07).
/// - The loop is spawned `no_wait` on the host group, so the two closing
///   disciplines are observably different: `close` followed by an
///   IMMEDIATE host-scope return cancels the still-running loop
///   (cancellation outranks draining; `join` from an independent scope
///   reports `Cancelled`), while `close(); join()`
///   before the scope ends drains the whole backlog in FIFO order and
///   reports `Graceful` (T08).
///
/// Coordination is deterministic, with no sleep-based timing guesses:
/// backlogs are pre-enqueued before `close` (a closed self-chain stops
/// growing, which would let the loop reach the closed-and-empty boundary
/// first and make the outcomes racy instead of contract-determined), T07's
/// joiners synchronize through a counting barrier entered in one
/// uninterrupted turn, and every potentially long wait carries a generous
/// `with_timeout` guard. Handler-side counters and logs record the true
/// service order; waiter wakeup order is never used as evidence.

///|
/// Backlog size for T07: large enough that the drain spans many scheduler
/// yields.
let t07_backlog : Int = 2000

///|
/// Backlog size for T08 (both sequences).
let t08_backlog : Int = 2000

///|
/// Number of concurrent external joiners in T07.
let t07_joiners : Int = 3

///|
/// Spin budget for coordination counters: one spin-yield advances every
/// ready loop by one batch, so the backlog always fits.
let lc_spin_budget : Int = 10000

///|
priv enum T07Cmd {
  T07Step
}

///|
/// T07: close idempotence and uniform waiter termination.
///
/// Sequence: the host owner spawns the actor, pre-enqueues the backlog,
/// and waits on a counter barrier until three external joiners (living in
/// the test task's scope, independent of the host group) are parked inside
/// `join`. Only then does it call `close` TWICE (idempotence) and join the
/// loop itself, keeping the host scope open until the drain completed.
///
/// Assertions:
/// 1. all three concurrent joiners and the host owner observe `Graceful`
///    (waiter consistency across scopes),
/// 2. a LATE joiner — after every earlier waiter returned — still gets
///    `Graceful` immediately (bounded by a with_timeout hang guard),
/// 3. the backlog was fully processed (count exact) while the waiters were
///    parked: `join` waits for the loop task only and never blocks the
///    handler,
/// 4. the second `close` changed nothing: the drained handle still refuses
///    new tells, and new asks fail fast with a delivery rejection instead
///    of hanging until their timeout.
async test "T07 close is idempotent and every join waiter agrees on Graceful" {
  let processed : Ref[Int] = Ref(0)
  let joining : Ref[Int] = Ref(0)
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T07Cmd, Unit, Unit]?] = Ref(None)
  let owner_outcome : Ref[Result[@fuwaroid.StopReason, Error]?] = Ref(None)
  let join_results : Array[Result[@fuwaroid.StopReason, Error]] = []
  @async.with_task_group(outer => {
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          _,
          _,
          _cmd,
        ) => processed.val = processed.val + 1)
        handle_cell.val = Some(handle)
        for _ in 0..<t07_backlog {
          let _ = handle.tell(T07Step)
        }
        // Barrier: close only after every external joiner is parked inside
        // join (each joiner increments the counter and enters join in the
        // same uninterrupted turn, so this observation is exact).
        let mut all_joining = false
        for _ in 0..<lc_spin_budget {
          if joining.val >= t07_joiners {
            all_joining = true
            break
          }
          @async.pause()
        }
        if !all_joining {
          fail("T07: external joiners never initiated join before close")
        }
        // Idempotent close: the second call is a no-op on the already
        // closed mailbox and must not change any outcome below.
        handle.close()
        handle.close()
        // Keep the host scope open until the loop terminated: the owner is
        // itself a waiter and records its outcome for the test task.
        let owner_reason : Result[@fuwaroid.StopReason, Error] = Ok(
          @async.with_timeout(30000, () => handle.join()),
        ) catch {
          error => Err(error)
        }
        owner_outcome.val = Some(owner_reason)
      })
    })
    // External joiners, initiated before close (guaranteed by the barrier
    // above). They live in the test task's scope, independent of the host
    // group, and wait on the loop task via join.
    for _ in 0..<t07_joiners {
      let _ = outer.spawn(() => {
        let mut visible = false
        for _ in 0..<lc_spin_budget {
          if handle_cell.val is Some(_) {
            visible = true
            break
          }
          @async.pause()
        }
        if !visible {
          fail("T07: a joiner never saw the handle")
        }
        guard handle_cell.val is Some(handle) else {
          fail("T07: joiner handle vanished")
        }
        joining.val = joining.val + 1
        let outcome : Result[@fuwaroid.StopReason, Error] = Ok(
          @async.with_timeout(30000, () => handle.join()),
        ) catch {
          error => Err(error)
        }
        join_results.push(outcome)
      })
    }
    // Bounded spin until every joiner and the owner have settled.
    let mut settled = false
    for _ in 0..<(lc_spin_budget * 2) {
      if join_results.length() >= t07_joiners && owner_outcome.val is Some(_) {
        settled = true
        break
      }
      @async.pause()
    }
    if !settled {
      fail("T07: waiters did not settle within the spin budget")
    }
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(60000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T07: host owner terminated abnormally: \{error}")
    }
    // 1. Waiter consistency: every concurrent joiner and the owner got the
    //    same reason.
    for outcome in join_results {
      guard outcome is Ok(@fuwaroid.Graceful) else {
        fail("T07: a concurrent joiner did not get Graceful: \{Repr(outcome)}")
      }
    }
    guard owner_outcome.val is Some(Ok(@fuwaroid.Graceful)) else {
      fail(
        "T07: the host owner's join did not get Graceful: \{Repr(owner_outcome.val)}",
      )
    }
    guard handle_cell.val is Some(handle) else {
      fail("T07: handle not visible to the test task")
    }
    // 2. Late joiner: after every earlier waiter returned, join still
    //    returns the same reason immediately.
    let late = @async.with_timeout(10000, () => handle.join())
    guard late is @fuwaroid.Graceful else {
      fail("T07: late join returned \{Repr(late)}, expected Graceful")
    }
    // 3. The backlog was fully processed while the waiters were parked.
    assert_eq(processed.val, t07_backlog)
    // 4. The (idempotently closed) handle still refuses new messages.
    debug_inspect(handle.tell(T07Step), content="Err(MailboxClosed)")
    let late_ask = handle.ask((), timeout_ms=10000)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
  })
}

///|
priv enum T08Cmd {
  T08Step(Int)
}

///|
/// T08: close then immediately leave the host scope vs
/// `close(); join()`. Both sequences run in this one test, each in its own
/// pair of task groups; the observer/joiner lifecycle is independent of
/// the actor's host group in both.
async test "T08 scope-end cancellation vs explicit join after close" {
  // ---- Sequence (a): close, then the host scope returns immediately. ----
  //
  // The host owner cancels nothing explicitly: it starts the drain,
  // observes the loop running with almost the whole backlog still queued,
  // closes, and returns — the scope end cancels the no_wait loop.
  //
  // Assertions (a):
  // 1. the host owner task still terminates normally (child cancellation
  //    during scope teardown is not a group failure),
  // 2. `join` from the independent observer scope returns `Cancelled` —
  //    the loop's cleanup recorded the host's cancellation signal (a
  //    distinct signal, not an `Error`, in async 0.22.x),
  // 3. the backlog was NOT drained: the processed count stays below the
  //    backlog size (cancellation won at an observable boundary).
  @async.with_task_group(outer => {
    let processed : Ref[Int] = Ref(0)
    let handle_cell : Ref[@fuwaroid.Fuwaroid[T08Cmd, Unit, Unit]?] = Ref(None)
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          _,
          _,
          _cmd,
        ) => processed.val = processed.val + 1)
        handle_cell.val = Some(handle)
        for _ in 0..<t08_backlog {
          let _ = handle.tell(T08Step(0))
        }
        // Observe the loop actually draining so the scope end cancels a
        // RUNNING loop with most of the backlog still queued.
        let mut started = false
        for _ in 0..<lc_spin_budget {
          if processed.val > 0 {
            started = true
            break
          }
          @async.pause()
        }
        if !started {
          fail("T08a: the loop never started draining")
        }
        handle.close()
        // Return immediately: no join, no park. The host scope ends here
        // and cancels the no_wait loop mid-backlog.
      })
    })
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(60000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T08a: host owner terminated abnormally: \{error}")
    }
    guard handle_cell.val is Some(handle) else {
      fail("T08a: handle not visible to the observer")
    }
    // The loop terminated during scope teardown; join returns the recorded
    // reason (with_timeout is only a hang guard).
    let reason = @async.with_timeout(10000, () => handle.join())
    guard reason is @fuwaroid.Cancelled else {
      fail(
        "T08a: join after scope-end cancellation returned \{Repr(reason)}, expected Cancelled",
      )
    }
    // Cancellation outranks draining: the loop stopped long before the
    // backlog was exhausted.
    if processed.val >= t08_backlog {
      fail(
        "T08a: the loop drained the whole backlog despite the scope-end cancellation: processed=\{processed.val} of \{t08_backlog}",
      )
    }
  })
  // ---- Sequence (b): close, then an explicit join before leaving. ----
  //
  // Same-scale backlog, but the host owner calls `close(); join()` and
  // only THEN returns, so the host scope ends after a graceful drain.
  //
  // Assertions (b):
  // 1. the owner's `join` returns `Graceful`,
  // 2. the whole backlog was processed exactly once, in FIFO order (the
  //    handler logs each command's index; the log must be the identity
  //    permutation),
  // 3. a late joiner in the observer scope also gets `Graceful`.
  @async.with_task_group(outer => {
    let processed : Ref[Int] = Ref(0)
    let handle_cell : Ref[@fuwaroid.Fuwaroid[T08Cmd, Unit, Unit]?] = Ref(None)
    let owner_outcome : Ref[Result[@fuwaroid.StopReason, Error]?] = Ref(None)
    let served : Array[Int] = []
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          _,
          _,
          cmd,
        ) => {
          match cmd {
            T08Step(i) => {
              served.push(i)
              processed.val = processed.val + 1
            }
          }
          ()
        })
        handle_cell.val = Some(handle)
        for i in 0..<t08_backlog {
          let _ = handle.tell(T08Step(i))
        }
        let mut started = false
        for _ in 0..<lc_spin_budget {
          if processed.val > 0 {
            started = true
            break
          }
          @async.pause()
        }
        if !started {
          fail("T08b: the loop never started draining")
        }
        handle.close()
        // Explicit join BEFORE the scope ends: wait for the drain and take
        // its reason (with_timeout is only a hang guard).
        let reason : Result[@fuwaroid.StopReason, Error] = Ok(
          @async.with_timeout(60000, () => handle.join()),
        ) catch {
          error => Err(error)
        }
        owner_outcome.val = Some(reason)
      })
    })
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(60000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T08b: host owner terminated abnormally: \{error}")
    }
    guard owner_outcome.val is Some(Ok(@fuwaroid.Graceful)) else {
      fail(
        "T08b: explicit join after close did not get Graceful: \{Repr(owner_outcome.val)}",
      )
    }
    guard handle_cell.val is Some(handle) else {
      fail("T08b: handle not visible to the observer")
    }
    // The whole backlog, exactly once, in FIFO order.
    if served.length() != t08_backlog {
      fail("T08b: served \{served.length()} of \{t08_backlog} backlog messages")
    }
    for i, index in served {
      if index != i {
        fail("T08b: FIFO violated at position \{i}: served index \{index}")
      }
    }
    assert_eq(processed.val, t08_backlog)
    // Late joiner agrees.
    let late = @async.with_timeout(10000, () => handle.join())
    guard late is @fuwaroid.Graceful else {
      fail("T08b: late join returned \{Repr(late)}, expected Graceful")
    }
  })
}

///|
/// Lifecycle/observability coverage. Same coordination discipline as the
/// T07/T08 tests above: mailbox POSITION (pre-enqueued backlogs), counting
/// barriers and flag-set-in-the-same-turn-as-admission provide
/// deterministic edges — never sleep-based timing guesses. The one
/// deliberate exception is T11, whose SUBJECT is the timeout timer: there
/// the backlog's drain time is made orders of magnitude larger than the
/// timer by handler-side arithmetic work, so the timer firing mid-drain is
/// machine-independent. Producer/asker/observer tasks live in the observer
/// scope, independent of the actor's host group, wherever the test
/// requires it.

///|
/// T04: producers x unique-ID stride per producer (4 x 250 = 1000 messages).
let t04_producers : Int = 4

///|
let t04_stride : Int = 250

///|
/// T06: commands queued before the closing query.
let t06_backlog : Int = 2000

///|
/// T09: commands queued before the external asker's query. Far larger than
/// the batch size, so the query sits at a mailbox position the loop cannot
/// reach before its first batch-yield boundary — the deterministic edge
/// where the host cancellation lands.
let t09_backlog : Int = 2000

///|
/// T10: commands queued ahead of the ask that will be cancelled. Gives the
/// canceller several batch boundaries of headroom, so the ask is provably
/// still parked when the asker task is cancelled.
let t10_backlog : Int = 200

///|
/// T11: backlog size, per-command handler work and the ask timeout. The
/// drain takes orders of magnitude longer than `t11_timeout_ms` of real
/// time on any plausible machine, so the timeout deterministically fires
/// while the query is still queued and its later execution is observed
/// after the drain.
let t11_backlog : Int = 1000

///|
let t11_burn_iters : Int = 200000

///|
let t11_timeout_ms : Int = 10

///|
priv enum T04Cmd {
  T04Msg(Int, Int)
}

///|
/// T04: multi-producer unique-ID delivery through the public
/// API. Four producer tasks — independent of the actor's host group — each
/// deliver their own disjoint range of unique IDs after a counted start
/// barrier. EVERY tell result is recorded (refusals are collected and
/// asserted empty, never silently ignored); the mailbox is Unbounded so
/// bounded-admission refusals cannot interfere (that is T05's subject).
///
/// Assertions, all from handler/producer-side records (no waiter-wakeup
/// order, no timing):
/// 1. no tell was refused (with an Unbounded mailbox a refusal is a bug),
/// 2. the graceful drain processed every accepted message, certified by a
///    FIFO barrier ask after the producers finished,
/// 3. the processed set equals the accepted set: per producer the served
///    subsequence is exactly its own ID range in sending order — strictly
///    increasing, complete, no duplicates, no losses.
async test "T04 multi-producer unique IDs: accepted set equals drained processed set" {
  let served : Array[(Int, Int)] = []
  let refusals : Array[String] = []
  let ready_count : Ref[Int] = Ref(0)
  let go_flag : Ref[Bool] = Ref(false)
  let done_count : Ref[Int] = Ref(0)
  let drain_done : Ref[Bool] = Ref(false)
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T04Cmd, Unit, Unit]?] = Ref(None)
  let owner_outcome : Ref[Result[@fuwaroid.StopReason, Error]?] = Ref(None)
  @async.with_task_group(outer => {
    // Host owner: the actor's lifecycle lives here; the producers below are
    // outer-scope tasks, independent of it.
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          _,
          _,
          cmd,
        ) => {
          match cmd {
            T04Msg(producer, id) => served.push((producer, id))
          }
        })
        handle_cell.val = Some(handle)
        let mut finished = false
        for _ in 0..<(lc_spin_budget * 8) {
          if drain_done.val {
            finished = true
            break
          }
          @async.pause()
        }
        if !finished {
          fail("T04: producers/drain never finished within the spin budget")
        }
        handle.close()
        let reason : Result[@fuwaroid.StopReason, Error] = Ok(
          @async.with_timeout(30000, () => handle.join()),
        ) catch {
          error => Err(error)
        }
        owner_outcome.val = Some(reason)
      })
    })
    // Producers: each owns a disjoint ID range and records every send
    // result — a refusal is collected, never ignored.
    for p in 0..<t04_producers {
      let _ = outer.spawn(() => {
        ready_count.val = ready_count.val + 1
        let mut go = false
        for _ in 0..<lc_spin_budget {
          if go_flag.val {
            go = true
            break
          }
          @async.pause()
        }
        if !go {
          fail("T04: producer \{p} never got the start signal")
        }
        guard handle_cell.val is Some(handle) else {
          fail("T04: producer \{p} never saw the handle")
        }
        for id in 0..<t04_stride {
          match handle.tell(T04Msg(p, id)) {
            Ok(_) => ()
            Err(refusal) =>
              refusals.push("producer \{p} id \{id}: \{Repr(refusal)}")
          }
        }
        done_count.val = done_count.val + 1
      })
    }
    // Coordinator: synchronized start (counted barrier), then wait for every
    // producer to finish sending.
    let mut all_ready = false
    for _ in 0..<lc_spin_budget {
      if ready_count.val >= t04_producers {
        all_ready = true
        break
      }
      @async.pause()
    }
    if !all_ready {
      fail("T04: producers never became ready")
    }
    go_flag.val = true
    let mut all_done = false
    for _ in 0..<(lc_spin_budget * 2) {
      if done_count.val >= t04_producers {
        all_done = true
        break
      }
      @async.pause()
    }
    if !all_done {
      fail("T04: producers never finished sending")
    }
    let total = t04_producers * t04_stride
    // 1. On an Unbounded mailbox every send must have been accepted; any
    //    refusal was recorded above and is reported here.
    if refusals.length() != 0 {
      fail("T04: \{refusals.length()} sends were refused: \{refusals[0]} ...")
    }
    // 2. The handler-side log holds every accepted message; a FIFO barrier
    //    ask certifies the drain completed.
    let mut drained = false
    for _ in 0..<(lc_spin_budget * 4) {
      if served.length() >= total {
        drained = true
        break
      }
      @async.pause()
    }
    if !drained {
      fail(
        "T04: only \{served.length()} of \{total} accepted messages were processed within the spin budget",
      )
    }
    guard handle_cell.val is Some(handle) else {
      fail("T04: handle not visible to the coordinator")
    }
    let barrier = handle.ask((), timeout_ms=30000)
    debug_inspect(barrier, content="Ok(())")
    // 3. The processed set equals the accepted set: per producer, exactly
    //    its own range in sending order (strictly increasing => no
    //    duplicates; complete => no losses; disjoint ranges => globally
    //    unique IDs).
    for p in 0..<t04_producers {
      let mut expected = 0
      for pair in served {
        if pair.0 == p {
          if pair.1 != expected {
            fail(
              "T04: producer \{p} served id \{pair.1} where \{expected} was expected (per-producer order violated)",
            )
          }
          expected = expected + 1
        }
      }
      if expected != t04_stride {
        fail(
          "T04: producer \{p} served \{expected} of \{t04_stride} unique IDs (lost or duplicated)",
        )
      }
    }
    if served.length() != total {
      fail("T04: processed \{served.length()} messages, expected \{total}")
    }
    // Release the host owner to close and join; assert the graceful end.
    drain_done.val = true
    let mut owner_settled = false
    for _ in 0..<(lc_spin_budget * 2) {
      if owner_outcome.val is Some(_) {
        owner_settled = true
        break
      }
      @async.pause()
    }
    if !owner_settled {
      fail("T04: the host owner never settled")
    }
    guard owner_outcome.val is Some(Ok(@fuwaroid.Graceful)) else {
      fail(
        "T04: the graceful drain did not end Graceful: \{Repr(owner_outcome.val)}",
      )
    }
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(30000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T04: host owner terminated abnormally: \{error}")
    }
  })
}

///|
priv enum T06Cmd {
  T06Add(Int)
}

///|
priv enum T06Query {
  T06Count
}

///|
/// T06: closing while commands AND a query are still queued.
/// The host owner pre-enqueues a large command backlog; the external asker
/// then enqueues one query BEHIND the whole backlog (its admission is proven
/// by a flag set in the same uninterrupted turn as the ask's try_put, so
/// observing the flag implies the query is queued); only then does the
/// owner close. The close happens with the backlog demonstrably still
/// queued (the processed count at close time is recorded and asserted).
///
/// Assertions:
/// 1. after `close` returns, a new tell is refused immediately (recorded
///    synchronously in the owner's turn — Closing refuses new admission),
/// 2. the backlog drains in FIFO order: the queued query is served after
///    every command and its reply reflects ALL of them (count == backlog) —
///    the query is NOT awaited before close,
/// 3. every queued command was processed exactly once,
/// 4. `join` returns `Graceful` (the closed-and-empty drain completed),
/// 5. the drained handle still refuses new tells and asks fail-fast.
async test "T06 close with queued commands and a query drains FIFO and answers Graceful" {
  let processed : Ref[Int] = Ref(0)
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T06Cmd, T06Query, Int]?] = Ref(None)
  let backlog_enqueued : Ref[Bool] = Ref(false)
  let ask_initiated : Ref[Bool] = Ref(false)
  let ask_outcome : Ref[Result[Result[Int, @fuwaroid.AskFailure], Error]?] = Ref(
    None,
  )
  let close_time_processed : Ref[Int] = Ref(-1)
  let post_close_refusal : Ref[@fuwaroid.SendRefusal?] = Ref(None)
  let owner_outcome : Ref[Result[@fuwaroid.StopReason, Error]?] = Ref(None)
  @async.with_task_group(outer => {
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        let handle = @fuwaroid.Fuwaroid::spawn(
          group=host,
          init=0,
          on_cmd=(_, state, cmd) => {
            match cmd {
              T06Add(n) => {
                processed.val = processed.val + 1
                state + n
              }
            }
          },
          on_query=(_, state, _query) => (state, state),
        )
        handle_cell.val = Some(handle)
        for _ in 0..<t06_backlog {
          let _ = handle.tell(T06Add(1))
        }
        backlog_enqueued.val = true
        // Deterministic edge: close only after the asker's query was
        // admitted behind the whole backlog.
        let mut initiated = false
        for _ in 0..<lc_spin_budget {
          if ask_initiated.val {
            initiated = true
            break
          }
          @async.pause()
        }
        if !initiated {
          fail("T06: the query was never initiated")
        }
        // Backlog evidence at the stopping point: only a few batches can
        // have been served so far, far short of the backlog.
        close_time_processed.val = processed.val
        if close_time_processed.val >= t06_backlog {
          fail(
            "T06: no backlog was left at close time: processed=\{close_time_processed.val} of \{t06_backlog}",
          )
        }
        handle.close()
        // New admission is refused the moment close returns (same turn —
        // the decision is the closed mailbox's alone).
        match handle.tell(T06Add(-1)) {
          Ok(_) => fail("T06: a tell after close was accepted")
          Err(refusal) => post_close_refusal.val = Some(refusal)
        }
        let reason : Result[@fuwaroid.StopReason, Error] = Ok(
          @async.with_timeout(60000, () => handle.join()),
        ) catch {
          error => Err(error)
        }
        owner_outcome.val = Some(reason)
      })
    })
    // External asker: its query is queued behind the whole backlog and it
    // parks there — close does NOT wait for it first.
    let _ = outer.spawn(() => {
      let mut ready = false
      for _ in 0..<lc_spin_budget {
        if backlog_enqueued.val && handle_cell.val is Some(_) {
          ready = true
          break
        }
        @async.pause()
      }
      if !ready {
        fail("T06: the backlog never became visible to the asker")
      }
      guard handle_cell.val is Some(handle) else {
        fail("T06: handle not visible to the asker")
      }
      ask_initiated.val = true
      let outcome : Result[Result[Int, @fuwaroid.AskFailure], Error] = Ok(
        handle.ask(T06Query::T06Count, timeout_ms=30000),
      ) catch {
        error => Err(error)
      }
      ask_outcome.val = Some(outcome)
    })
    let mut settled = false
    for _ in 0..<(lc_spin_budget * 8) {
      if ask_outcome.val is Some(_) && owner_outcome.val is Some(_) {
        settled = true
        break
      }
      @async.pause()
    }
    if !settled {
      fail("T06: the closing drain did not settle within the spin budget")
    }
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(60000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T06: host owner terminated abnormally: \{error}")
    }
    // 1. Closing refused the new admission.
    guard post_close_refusal.val is Some(@fuwaroid.MailboxClosed) else {
      fail(
        "T06: the tell after close was not refused with MailboxClosed: \{Repr(post_close_refusal.val)}",
      )
    }
    // 2. The queued query was served after ALL backlog commands and its
    //    reply reflects every one of them (FIFO drain of the backlog).
    match ask_outcome.val {
      Some(Ok(Ok(count))) => assert_eq(count, t06_backlog)
      other =>
        fail(
          "T06: the queued query did not get a correct reply: \{Repr(other)}",
        )
    }
    // 3. Every queued command was processed exactly once.
    assert_eq(processed.val, t06_backlog)
    // 4. The closed-and-empty drain completed gracefully.
    guard owner_outcome.val is Some(Ok(@fuwaroid.Graceful)) else {
      fail(
        "T06: join after the closing drain did not return Graceful: \{Repr(owner_outcome.val)}",
      )
    }
    // 5. The drained handle still refuses new messages (fail fast, no hang).
    guard handle_cell.val is Some(handle) else {
      fail("T06: handle not visible to the coordinator")
    }
    debug_inspect(handle.tell(T06Add(1)), content="Err(MailboxClosed)")
    let late_ask = handle.ask(T06Query::T06Count, timeout_ms=10000)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
  })
}

///|
priv enum T09Cmd {
  T09Step
}

///|
/// T09: an ACCEPTED ask is stranded by a loop cancellation and
/// its external waiter fails fast with `AskFailure::Stopped` instead of
/// hanging until its timeout. The backlog (2000
/// commands) is enqueued in one uninterrupted turn, then the external
/// asker's query is admitted BEHIND the whole backlog (its flag-set and the
/// ask's try_put are one uninterrupted turn). The host cancellation is
/// requested in the coordinator's turn immediately after that flag is
/// observed — i.e. after admission but long before the loop can reach the
/// query, which sits ~2000 positions deep, dozens of batch-yield boundaries
/// away. The cancelled loop's unified cleanup path closes every stranded
/// ask's reply queue with the stop marker (`fail_leftover_asks`), which
/// `classify_ask_error` maps to `AskFailure::Stopped` at the waiter. The
/// asker lives in the observer scope, independent of the cancelled host
/// group.
///
/// Assertions:
/// 1. the asker gets `Err(AskFailure::Stopped)` — NOT `TimedOut` (the
///    regression this test guards) and NOT a reply (a served ask would have
///    returned `Ok`),
/// 2. cancellation outranks draining: the processed count stays far below
///    the backlog (the loop stopped at a batch-yield boundary),
/// 3. the host group terminates cleanly despite the cancellation,
/// 4. the cancelled handle refuses new messages fail-fast (the unified
///    cleanup path closed admission).
async test "T09 accepted ask stranded by loop cancellation fails fast as Stopped" {
  let processed : Ref[Int] = Ref(0)
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T09Cmd, Unit, Unit]?] = Ref(None)
  let group_cell : Ref[@async.TaskGroup[Unit]?] = Ref(None)
  let backlog_enqueued : Ref[Bool] = Ref(false)
  let ask_initiated : Ref[Bool] = Ref(false)
  let ask_outcome : Ref[Result[Result[Unit, @fuwaroid.AskFailure], Error]?] = Ref(
    None,
  )
  @async.with_task_group(outer => {
    let host_task = outer.spawn(() => {
      @async.with_task_group(host => {
        group_cell.val = Some(host)
        let handle = @fuwaroid.Fuwaroid::spawn_fold(group=host, init=(), on_cmd=(
          _,
          _,
          _cmd,
        ) => processed.val = processed.val + 1)
        handle_cell.val = Some(handle)
        for _ in 0..<t09_backlog {
          let _ = handle.tell(T09Step)
        }
        backlog_enqueued.val = true
        @async.sleep(60000)
      })
    })
    // External asker, independent of the host group: its query is admitted
    // behind the whole backlog and parks there.
    let _ = outer.spawn(() => {
      let mut ready = false
      for _ in 0..<lc_spin_budget {
        if backlog_enqueued.val && handle_cell.val is Some(_) {
          ready = true
          break
        }
        @async.pause()
      }
      if !ready {
        fail("T09: the backlog never became visible to the asker")
      }
      guard handle_cell.val is Some(handle) else {
        fail("T09: handle not visible to the asker")
      }
      ask_initiated.val = true
      let outcome : Result[Result[Unit, @fuwaroid.AskFailure], Error] = Ok(
        handle.ask((), timeout_ms=30000),
      ) catch {
        error => Err(error)
      }
      ask_outcome.val = Some(outcome)
    })
    // Coordinator: cancel the host group as soon as the ask is admitted —
    // after acceptance, long before the loop can reach the query.
    let mut initiated = false
    for _ in 0..<lc_spin_budget {
      if ask_initiated.val {
        initiated = true
        break
      }
      @async.pause()
    }
    if !initiated {
      fail("T09: the ask was never initiated")
    }
    guard group_cell.val is Some(host) else {
      fail("T09: host group not visible to the coordinator")
    }
    host.return_immediately(())
    // The loop is cancelled at a batch-yield boundary; its cleanup path
    // fails the stranded ask. Wait for the asker to record the outcome.
    let mut settled = false
    for _ in 0..<(lc_spin_budget * 4) {
      if ask_outcome.val is Some(_) {
        settled = true
        break
      }
      @async.pause()
    }
    if !settled {
      fail("T09: the stranded ask never settled within the spin budget")
    }
    // 1. THE assertion: Stopped, not TimedOut, not a reply.
    match ask_outcome.val {
      Some(Ok(Err(@fuwaroid.AskFailure::Stopped))) => ()
      other =>
        fail(
          "T09: the stranded asker did not get AskFailure::Stopped: \{Repr(other)}",
        )
    }
    // 2. Cancellation outranks draining.
    if processed.val >= t09_backlog {
      fail(
        "T09: the loop drained the whole backlog despite the cancellation: processed=\{processed.val}",
      )
    }
    // 3. The host group terminated cleanly.
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(60000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T09: host group terminated abnormally: \{error}")
    }
    // 4. The cancelled handle refuses new messages fail-fast.
    guard handle_cell.val is Some(handle) else {
      fail("T09: handle not visible to the coordinator")
    }
    debug_inspect(handle.tell(T09Step), content="Err(MailboxClosed)")
    let late_ask = handle.ask((), timeout_ms=10000)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
  })
}

///|
priv enum T10Cmd {
  T10Add(Int)
}

///|
priv enum T10Query {
  T10Count
}

///|
/// T10: cancelling the ASKER task — ONLY the asker; the actor
/// and its host group stay untouched — while its ask waits behind a backlog.
/// `ask` is a cancellation checkpoint: the caller's own cancellation is
/// the runtime's cancellation signal — NOT an `Error` in async 0.22.x —
/// so it propagates through `ask` untouched (`catch` cannot capture it,
/// and `ask` never converts it into an `AskFailure`). The asker task
/// therefore ends cancelled, which the uncancelled test task observes as
/// `@async.TaskCancelled` from `wait`. The actor is unaffected and keeps
/// serving: the backlog completes exactly once and a fresh command+query
/// afterwards gets the correct reply, so the interrupted ask left no
/// state damage behind.
async test "T10 asker cancellation propagates to the call site and the actor keeps serving" {
  let processed : Ref[Int] = Ref(0)
  let ask_started : Ref[Bool] = Ref(false)
  let ask_reply : Ref[Result[Int, @fuwaroid.AskFailure]?] = Ref(None)
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, cmd) => {
        match cmd {
          T10Add(n) => {
            processed.val = processed.val + 1
            state + n
          }
        }
      },
      on_query=(_, state, _query) => (state, state),
    )
    // A backlog parks the asker behind many messages, so the cancel lands
    // while the ask is still waiting (the asker's flag-set and its ask's
    // try_put are one uninterrupted turn).
    for _ in 0..<t10_backlog {
      let _ = handle.tell(T10Add(1))
    }
    // The asker is an independent task in this scope; only IT is cancelled.
    // The asker cannot (and must not try to) catch its own cancellation:
    // it calls `ask` and lets the signal propagate out of the task.
    let asker = group.spawn(() => {
      ask_started.val = true
      let reply : Result[Int, @fuwaroid.AskFailure] = handle.ask(
        T10Query::T10Count,
        timeout_ms=30000,
      )
      ask_reply.val = Some(reply)
    })
    let mut started = false
    for _ in 0..<lc_spin_budget {
      if ask_started.val {
        started = true
        break
      }
      @async.pause()
    }
    if !started {
      fail("T10: the asker never initiated its ask")
    }
    asker.cancel()
    // The test task (uncancelled) waits for the asker — waiting suspends
    // this task, so the cancellation is actually delivered at the ask's
    // suspension point. A cancelled TARGET is reported to the observer as
    // the ordinary `@async.TaskCancelled` error.
    let asker_outcome : Result[Unit, Error] = Ok(asker.wait()) catch {
      error => Err(error)
    }
    guard asker_outcome is Err(@async.TaskCancelled) else {
      fail(
        "T10: the cancelled asker did not end cancelled: \{Repr(asker_outcome)}",
      )
    }
    // The cancellation aborted the ask: no reply was recorded (an
    // AskFailure value would mean the cancellation was converted).
    if ask_reply.val is Some(_) {
      fail(
        "T10: the cancelled asker recorded a reply instead of propagating its cancellation",
      )
    }
    // The actor keeps serving: the backlog completes exactly once, then a
    // fresh command + query gets the correct reply (state unpolluted: the
    // interrupted ask was a read-only query and applied nothing).
    let mut drained = false
    for _ in 0..<(lc_spin_budget * 4) {
      if processed.val >= t10_backlog {
        drained = true
        break
      }
      @async.pause()
    }
    if !drained {
      fail(
        "T10: the actor did not finish the backlog after the asker was cancelled",
      )
    }
    let _ = handle.tell(T10Add(1000))
    let after = handle.ask(T10Query::T10Count, timeout_ms=10000)
    assert_eq(after, Ok(t10_backlog + 1000))
  })
}

///|
/// T11 handler work: a pure arithmetic burn sized so one backlog command
/// costs far more real time than the ask timeout below. No sleeps: the
/// delay the timeout fires against is created by the mailbox POSITION of the
/// query behind the slow backlog.
fn t11_burn() -> Int {
  let mut acc = 0
  for i in 0..<t11_burn_iters {
    acc = (acc + i) % 1000003
  }
  acc
}

///|
/// T11: an ask timeout does NOT withdraw the accepted request.
/// The query is enqueued behind a long, deliberately slow backlog in the
/// same uninterrupted turn as its ask; the short timeout fires mid-drain and
/// the caller observes `Err(TimedOut)` while the request stays queued. After
/// the drain, the handler-side query-execution counter proves the request
/// executed EXACTLY once (no automatic retry, no loss, no double
/// execution), and the actor keeps serving: a fresh barrier query — served
/// after the timed-out one (FIFO) — returns the full count with the counter
/// at exactly +1.
async test "T11 ask timeout does not withdraw the request and it executes exactly once" {
  let processed : Ref[Int] = Ref(0)
  let query_runs : Ref[Int] = Ref(0)
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, _n) => {
        let _ = t11_burn()
        processed.val = processed.val + 1
        state + 1
      },
      on_query=(_, state, _query) => {
        query_runs.val = query_runs.val + 1
        (state, state)
      },
    )
    // One uninterrupted turn: the whole slow backlog, then the query behind
    // it; the ask then parks and its (short) timeout fires mid-drain.
    for _ in 0..<t11_backlog {
      let _ = handle.tell(1)
    }
    let timed_out : Result[Result[Int, @fuwaroid.AskFailure], Error] = Ok(
      handle.ask((), timeout_ms=t11_timeout_ms),
    ) catch {
      error => Err(error)
    }
    debug_inspect(timed_out, content="Ok(Err(TimedOut))")
    // The request is NOT withdrawn: wait for the drain to reach the end of
    // the backlog; the timed-out query is served right after it (FIFO).
    let mut drained = false
    for _ in 0..<(lc_spin_budget * 8) {
      if processed.val >= t11_backlog {
        drained = true
        break
      }
      @async.pause()
    }
    if !drained {
      fail("T11: the backlog was not drained within the spin budget")
    }
    // Fresh barrier query: served AFTER the timed-out request, so its reply
    // certifies the timed-out request's execution is already counted.
    let barrier : Result[Result[Int, @fuwaroid.AskFailure], Error] = Ok(
      handle.ask((), timeout_ms=30000),
    ) catch {
      error => Err(error)
    }
    guard barrier is Ok(Ok(count)) else {
      fail("T11: the post-timeout barrier query failed: \{Repr(barrier)}")
    }
    assert_eq(count, t11_backlog)
    // Exactly once: the timed-out request (+1) and the barrier (+1) — the
    // timeout triggered no retry and lost no execution.
    assert_eq(query_runs.val, 2)
    assert_eq(processed.val, t11_backlog)
  })
}

///|
priv enum T13Cmd {
  T13Start
  T13Report(Int)
}

///|
priv enum T13Query {
  T13Applied
}

///|
/// T13: background work a handler spawned on the HOST group
/// (via `ctx.group`) completes AFTER `close` and its report-back to
/// `ctx.address` is refused by the closed mailbox. The refusal is collected
/// INDEPENDENTLY by the background task itself (into a shared cell) — never
/// silently ignored, never retried through the closed mailbox. The ordering
/// is deterministic: the background task spins on a flag that the test sets
/// only after `close` returned, so the report-back provably happens after
/// the close and the refusal must be `MailboxClosed`.
///
/// Assertions:
/// 1. the background work ran to completion under the host group's
///    lifetime (its done flag is observed before the scope ends),
/// 2. the refusal was collected: exactly `Err(MailboxClosed)`,
/// 3. the actor ended `Graceful` (the closed-and-empty drain completed),
/// 4. the ledger was not polluted by the late report: the handler-side
///    applied-log is empty (the report never entered the mailbox).
async test "T13 background work reports back after close and the refusal is independently collected" {
  let applied_log : Array[Int] = []
  let refusals : Array[Result[Unit, @fuwaroid.SendRefusal]] = []
  let closed_flag : Ref[Bool] = Ref(false)
  let bg_done : Ref[Bool] = Ref(false)
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(ctx, state, cmd) => {
        match cmd {
          T13Start => {
            // Background work on the HOST group: it outlives this message
            // but not the host, and it is not awaited by close/join.
            ctx.group.spawn_bg(() => {
              let mut closed = false
              for _ in 0..<lc_spin_budget {
                if closed_flag.val {
                  closed = true
                  break
                }
                @async.pause()
              }
              if !closed {
                fail("T13: the background work never observed the close")
              }
              // Independent refusal collection: the tell result is stored,
              // not ignored and not retried.
              let result = ctx.address.tell(T13Report(1))
              refusals.push(result)
              bg_done.val = true
            })
            state
          }
          T13Report(n) => {
            applied_log.push(n)
            state + n
          }
        }
      },
      on_query=(_, state, _query) => (state, state),
    )
    let _ = handle.tell(T13Start)
    let before = handle.ask(T13Query::T13Applied, timeout_ms=10000)
    debug_inspect(before, content="Ok(0)")
    handle.close()
    // From here the background work provably reports back into a CLOSED
    // mailbox (the flag is set strictly after close returned).
    closed_flag.val = true
    let mut done = false
    for _ in 0..<lc_spin_budget {
      if bg_done.val {
        done = true
        break
      }
      @async.pause()
    }
    if !done {
      fail("T13: the background work never completed its report attempt")
    }
    // 2. The refusal was collected independently of the closed mailbox.
    debug_inspect(refusals, content="[Err(MailboxClosed)]")
    // 3. The actor ended gracefully.
    let reason : Result[@fuwaroid.StopReason, Error] = Ok(
      @async.with_timeout(30000, () => handle.join()),
    ) catch {
      error => Err(error)
    }
    guard reason is Ok(@fuwaroid.Graceful) else {
      fail(
        "T13: join after the closing drain did not return Graceful: \{Repr(reason)}",
      )
    }
    // 4. The late report never entered the ledger.
    if applied_log.length() != 0 {
      fail(
        "T13: the late background report polluted the ledger: \{Repr(applied_log)}",
      )
    }
  })
}
