///|
/// Black-box diagnostics tests. Public API only, including `snapshot()` /
/// `Snapshot` / `Lifecycle`.
///
/// Under test:
/// - Conservation at EVERY observation point:
///   `accepted == processed + abandoned + outstanding`.
/// - `abandoned == 0` for a graceful stop; after an abnormal stop,
///   `abandoned == accepted - processed` exactly (the stranded-envelope
///   drain is the truncation point) and `outstanding` closes to zero.
/// - The lifecycle phase only moves FORWARD (Running → Closing → Stopped);
///   a late `close` after the stop cannot regress it.
/// - Refusal attribution: a full/closed refusal counts in
///   `rejected_full`/`rejected_closed`, never in `accepted`, and never
///   changes the fate of already-accepted messages.
/// - Stop reasons are distinguishable: `Stopped(Graceful)` vs
///   `Stopped(Cancelled)` vs `Stopped(Failed(original error))`.
///
/// `snapshot()` never suspends, so a snapshot taken in the SAME
/// uninterrupted scheduling turn as the events it observes has exact,
/// machine-independent values (the loop cannot run inside the test task's
/// turn). Every potentially long wait carries a generous with_timeout hang
/// guard.

///|
/// Spin budget for coordination counters: one pause advances every ready
/// loop by one batch.
let t14_spin_budget : Int = 10000

///|
/// Monotonicity ordering for the lifecycle assertions (Running=0,
/// Closing=1, Stopped=2).
fn t14_lifecycle_rank(lifecycle : @fuwaroid.Lifecycle) -> Int {
  match lifecycle {
    @fuwaroid.Running => 0
    @fuwaroid.Closing => 1
    @fuwaroid.Stopped(_) => 2
  }
}

///|
/// T14-A: graceful close — snapshot conservation, exact
/// counter values at deterministic observation points, lifecycle
/// monotonicity, and a distinguishable `Stopped(Graceful)` end state.
async test "T14-A snapshot conservation and lifecycle monotonicity under graceful close" {
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, n) => state + n,
      on_query=(_, state, _q) => (state, state),
    )
    // Turn 1 (uninterrupted): the loop cannot run inside it.
    let s0 = handle.snapshot()
    debug_inspect(s0.lifecycle, content="Running")
    assert_eq(s0.accepted, 0L)
    assert_eq(s0.processed, 0L)
    assert_eq(s0.rejected_full, 0L)
    assert_eq(s0.rejected_closed, 0L)
    assert_eq(s0.abandoned, 0L)
    assert_eq(s0.outstanding, 0L)
    for _ in 0..<10 {
      let _ = handle.tell(1)
    }
    let s1 = handle.snapshot()
    assert_eq(s1.accepted, 10L)
    assert_eq(s1.processed, 0L)
    assert_eq(s1.outstanding, 10L)
    assert_eq(s1.abandoned, 0L)
    // Conservation at every observation point.
    assert_eq(s1.accepted, s1.processed + s1.abandoned + s1.outstanding)
    assert_true(
      t14_lifecycle_rank(s1.lifecycle) >= t14_lifecycle_rank(s0.lifecycle),
    )
    // The barrier ask suspends; its reply proves the 10 commands AND the
    // query itself were served (FIFO).
    let barrier = handle.ask((), timeout_ms=30000)
    debug_inspect(barrier, content="Ok(10)")
    // Turn 2 (same turn as the reply): everything accepted is processed.
    let s2 = handle.snapshot()
    assert_eq(s2.accepted, 11L)
    assert_eq(s2.processed, 11L)
    assert_eq(s2.outstanding, 0L)
    assert_eq(s2.abandoned, 0L)
    assert_eq(s2.accepted, s2.processed + s2.abandoned + s2.outstanding)
    // Backlog + close + snapshot in ONE turn: the loop cannot run inside
    // it, so the values are exact; the phase shows Closing deterministically
    // (the close request moves the phase synchronously).
    for _ in 0..<2000 {
      let _ = handle.tell(7)
    }
    handle.close()
    let s3 = handle.snapshot()
    guard s3.lifecycle is @fuwaroid.Closing else {
      fail("T14-A: lifecycle right after close is \{Repr(s3.lifecycle)}")
    }
    assert_eq(s3.accepted, 2011L)
    assert_eq(s3.processed, 11L)
    assert_eq(s3.outstanding, 2000L)
    assert_eq(s3.abandoned, 0L)
    assert_eq(s3.accepted, s3.processed + s3.abandoned + s3.outstanding)
    assert_true(
      t14_lifecycle_rank(s3.lifecycle) > t14_lifecycle_rank(s2.lifecycle),
    )
    // The closed-and-empty drain completes gracefully.
    let reason : Result[@fuwaroid.StopReason, Error] = Ok(
      @async.with_timeout(30000, () => handle.join()),
    ) catch {
      error => Err(error)
    }
    guard reason is Ok(@fuwaroid.Graceful) else {
      fail("T14-A: join after the closing drain returned \{Repr(reason)}")
    }
    let s4 = handle.snapshot()
    guard s4.lifecycle is @fuwaroid.Stopped(@fuwaroid.Graceful) else {
      fail("T14-A: final lifecycle is \{Repr(s4.lifecycle)}")
    }
    assert_eq(s4.accepted, 2011L)
    assert_eq(s4.processed, 2011L)
    assert_eq(s4.abandoned, 0L)
    assert_eq(s4.outstanding, 0L)
    assert_eq(s4.accepted, s4.processed + s4.abandoned + s4.outstanding)
    assert_true(
      t14_lifecycle_rank(s4.lifecycle) > t14_lifecycle_rank(s3.lifecycle),
    )
  })
}

///|
/// A handler snapshot sees all previously completed messages immediately.
async test "T14-D self-snapshot sees prior processed messages" {
  let seen : Ref[Int64] = Ref(-1L)
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn_fold(group~, init=0, on_cmd=(
      ctx,
      state,
      _cmd,
    ) => {
      if state == 1 {
        seen.val = ctx.address.snapshot().processed
      }
      state + 1
    })
    let _ = handle.tell(())
    let _ = handle.tell(())
    let barrier = handle.ask((), timeout_ms=30000)
    debug_inspect(barrier, content="Ok(())")
    assert_eq(seen.val, 1L)
    handle.close()
    debug_inspect(handle.join(), content="Graceful")
  })
}

///|
/// T14-B: refusal attribution — full/closed refusals count in
/// their own counters, never in `accepted`, and never change the fate of
/// already-accepted messages. The bounded window is EXACT: the control
/// barrier leaves the loop parked in `queue.get()`, so one uninterrupted
/// turn admits exactly `capacity + 1 = 3` messages (1 handoff + 2 buffered)
/// and the 4th send is the single `MailboxFull` refusal. The drain barrier
/// afterwards may ITSELF be refused full while the window messages are still
/// queued (the loop cannot run inside the test task's turn); each such
/// refusal admits nothing but IS counted in `rejected_full`, so every
/// attempt is recorded and the final count is asserted as window refusal +
/// recorded retries — exact accounting, no weakening.
async test "T14-B snapshot attributes refusals correctly and keeps conservation" {
  @async.with_task_group(group => {
    let handle = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=(_, state, n) => state + n,
      on_query=(_, state, _q) => (state, state),
      mailbox=@fuwaroid.Mailbox::Bounded(2),
    )
    let s0 = handle.snapshot()
    debug_inspect(s0.lifecycle, content="Running")
    assert_eq(s0.accepted, 0L)
    assert_eq(s0.rejected_full, 0L)
    assert_eq(s0.rejected_closed, 0L)
    assert_eq(s0.accepted, s0.processed + s0.abandoned + s0.outstanding)
    // Control: the loop serves the mailbox; the reply (count 0) proves the
    // barrier itself was processed. When this ask returns, the loop is
    // parked on the now-empty mailbox (it parks before the asker resumes).
    let control = handle.ask((), timeout_ms=30000)
    debug_inspect(control, content="Ok(0)")
    // ONE uninterrupted turn: sends 1-3 are admitted (1 parked-reader
    // handoff + 2 buffered), send 4 is the single MailboxFull refusal.
    let mut window_accepted = 0
    let mut full_refusals = 0
    for _ in 0..<4 {
      match handle.tell(1) {
        Ok(_) => window_accepted += 1
        Err(@fuwaroid.MailboxFull) => full_refusals += 1
        Err(other) => fail("T14-B: unexpected refusal \{Repr(other)}")
      }
    }
    let s1 = handle.snapshot()
    assert_eq(window_accepted, 3)
    assert_eq(full_refusals, 1)
    assert_eq(s1.accepted, 4L) // control barrier + 3 window sends
    assert_eq(s1.processed, 1L) // only the control barrier so far
    assert_eq(s1.rejected_full, 1L)
    assert_eq(s1.rejected_closed, 0L)
    assert_eq(s1.outstanding, 3L)
    assert_eq(s1.abandoned, 0L)
    assert_eq(s1.accepted, s1.processed + s1.abandoned + s1.outstanding)
    // The refusal changed nothing about accepted messages: drain with a
    // bounded retry barrier until the mailbox has room. A barrier attempt
    // refused full admits nothing (retrying is side-effect free) but IS
    // counted in rejected_full, so every attempt is recorded and the
    // final count is asserted exactly: window refusal + recorded retries.
    let mut barrier_refusals = 0
    let mut after_full : Result[Int, @fuwaroid.AskFailure] = Err(
      @fuwaroid.AskFailure::TimedOut,
    )
    for _ in 0..<t14_spin_budget {
      match handle.ask((), timeout_ms=30000) {
        Err(@fuwaroid.AskFailure::NotDelivered(@fuwaroid.MailboxFull)) => {
          barrier_refusals += 1
          @async.pause()
        }
        other => {
          after_full = other
          break
        }
      }
    }
    guard after_full is Ok(count) else {
      fail("T14-B: the post-refusal barrier failed: \{Repr(after_full)}")
    }
    assert_eq(count, 3)
    let s2 = handle.snapshot()
    assert_eq(s2.accepted, 5L) // control + 3 window + the successful barrier
    assert_eq(s2.processed, 5L)
    assert_eq(s2.outstanding, 0L)
    assert_eq(s2.rejected_full, Int64::from_int(1 + barrier_refusals))
    assert_eq(s2.accepted, s2.processed + s2.abandoned + s2.outstanding)
    // close: every subsequent admission is refused closed, one count each;
    // the phase shows Closing deterministically in the same turn.
    handle.close()
    debug_inspect(handle.tell(1), content="Err(MailboxClosed)")
    debug_inspect(handle.tell(1), content="Err(MailboxClosed)")
    let s3 = handle.snapshot()
    guard s3.lifecycle is @fuwaroid.Closing else {
      fail("T14-B: lifecycle right after close is \{Repr(s3.lifecycle)}")
    }
    assert_eq(s3.rejected_closed, 2L)
    // No admission is attempted between s2 and here except the two closed
    // refusals above, so the full-refusal count is unchanged.
    assert_eq(s3.rejected_full, Int64::from_int(1 + barrier_refusals))
    assert_eq(s3.accepted, 5L)
    assert_eq(s3.accepted, s3.processed + s3.abandoned + s3.outstanding)
    let stop_reason = handle.join()
    debug_inspect(stop_reason, content="Graceful")
    let s4 = handle.snapshot()
    guard s4.lifecycle is @fuwaroid.Stopped(@fuwaroid.Graceful) else {
      fail("T14-B: final lifecycle is \{Repr(s4.lifecycle)}")
    }
    assert_eq(s4.processed, 5L)
    assert_eq(s4.abandoned, 0L)
    assert_eq(s4.outstanding, 0L)
    assert_eq(s4.accepted, s4.processed + s4.abandoned + s4.outstanding)
    assert_true(
      t14_lifecycle_rank(s4.lifecycle) > t14_lifecycle_rank(s3.lifecycle),
    )
  })
}

///|
priv enum T14Cmd {
  T14Step
}

///|
/// T14-C: abnormal stop — the phase jumps to Stopped carrying
/// `Cancelled` (the host's cancellation signal is distinct from any
/// `Error` in async 0.22.x), `abandoned` counts exactly the
/// accepted-but-unserved messages (commands AND queries; the stranded-
/// envelope drain is the truncation point), `outstanding` closes to zero,
/// conservation holds, and a late `close` cannot regress the phase.
async test "T14-C snapshot after abnormal stop counts abandoned and keeps conservation" {
  let handle_cell : Ref[@fuwaroid.Fuwaroid[T14Cmd, Unit, Unit]?] = Ref(None)
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
        ) => ())
        handle_cell.val = Some(handle)
        for _ in 0..<2000 {
          let _ = handle.tell(T14Cmd::T14Step)
        }
        backlog_enqueued.val = true
        @async.sleep(60000)
      })
    })
    // The query is admitted behind the whole backlog (flag-set and the
    // ask's try_put are one uninterrupted turn); it parks there.
    let _ = outer.spawn(() => {
      let mut ready = false
      for _ in 0..<t14_spin_budget {
        if backlog_enqueued.val && handle_cell.val is Some(_) {
          ready = true
          break
        }
        @async.pause()
      }
      if !ready {
        fail("T14-C: the backlog never became visible to the asker")
      }
      guard handle_cell.val is Some(handle) else {
        fail("T14-C: handle not visible to the asker")
      }
      ask_initiated.val = true
      let outcome : Result[Result[Unit, @fuwaroid.AskFailure], Error] = Ok(
        handle.ask((), timeout_ms=30000),
      ) catch {
        error => Err(error)
      }
      ask_outcome.val = Some(outcome)
    })
    // Coordinator: snapshot BEFORE the cancel (same turn as the admission
    // flag, so accepted is exact), then cancel the host group.
    let mut initiated = false
    for _ in 0..<t14_spin_budget {
      if ask_initiated.val {
        initiated = true
        break
      }
      @async.pause()
    }
    if !initiated {
      fail("T14-C: the ask was never initiated")
    }
    guard handle_cell.val is Some(handle) else {
      fail("T14-C: handle not visible to the coordinator")
    }
    let s_running = handle.snapshot()
    guard s_running.lifecycle is @fuwaroid.Running else {
      fail("T14-C: pre-cancel lifecycle is \{Repr(s_running.lifecycle)}")
    }
    assert_eq(s_running.accepted, 2001L) // 2000 commands + 1 query
    assert_eq(s_running.abandoned, 0L)
    assert_eq(s_running.rejected_full, 0L)
    assert_eq(s_running.rejected_closed, 0L)
    if s_running.processed >= s_running.accepted {
      fail(
        "T14-C: no backlog before the cancel: processed=\{s_running.processed}",
      )
    }
    assert_eq(
      s_running.accepted,
      s_running.processed + s_running.abandoned + s_running.outstanding,
    )
    guard group_cell.val is Some(host) else {
      fail("T14-C: host group not visible to the coordinator")
    }
    host.return_immediately(())
    // The loop stops at its first batch-yield boundary; the unified cleanup
    // counts the stranded envelopes and records the phase. join waits for
    // the loop task and returns the recorded reason.
    let reason : Result[@fuwaroid.StopReason, Error] = Ok(
      @async.with_timeout(30000, () => handle.join()),
    ) catch {
      error => Err(error)
    }
    guard reason is Ok(@fuwaroid.Cancelled) else {
      fail("T14-C: join after the cancellation returned \{Repr(reason)}")
    }
    let s_stopped = handle.snapshot()
    guard s_stopped.lifecycle is @fuwaroid.Stopped(@fuwaroid.Cancelled) else {
      fail("T14-C: post-stop lifecycle is \{Repr(s_stopped.lifecycle)}")
    }
    assert_eq(s_stopped.accepted, 2001L)
    // The closing law for an abnormal stop: abandoned == accepted -
    // processed EXACTLY, and outstanding has closed to zero (the same fact,
    // spelled out both ways).
    assert_eq(s_stopped.abandoned, s_stopped.accepted - s_stopped.processed)
    assert_eq(s_stopped.outstanding, 0L)
    assert_eq(
      s_stopped.accepted,
      s_stopped.processed + s_stopped.abandoned + s_stopped.outstanding,
    )
    assert_true(
      t14_lifecycle_rank(s_stopped.lifecycle) >
      t14_lifecycle_rank(s_running.lifecycle),
    )
    // The stranded query failed fast with Stopped (it is among the
    // abandoned envelopes).
    match ask_outcome.val {
      Some(Ok(Err(@fuwaroid.AskFailure::Stopped))) => ()
      other =>
        fail(
          "T14-C: the stranded asker did not get AskFailure::Stopped: \{Repr(other)}",
        )
    }
    // A late close cannot regress the phase.
    handle.close()
    let s_late = handle.snapshot()
    guard s_late.lifecycle is @fuwaroid.Stopped(@fuwaroid.Cancelled) else {
      fail("T14-C: late close regressed the phase: \{Repr(s_late.lifecycle)}")
    }
    assert_eq(
      s_late.accepted,
      s_late.processed + s_late.abandoned + s_late.outstanding,
    )
    // The host group terminated cleanly despite the cancellation.
    let host_outcome : Result[Unit, Error] = Ok(
      @async.with_timeout(30000, () => host_task.wait()),
    ) catch {
      error => Err(error)
    }
    match host_outcome {
      Ok(_) => ()
      Err(error) => fail("T14-C: host group terminated abnormally: \{error}")
    }
  })
}
