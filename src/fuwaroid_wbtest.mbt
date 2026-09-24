///|
/// Counter fixtures shared by the tests below: commands fold the count,
/// queries read it. Cmd/Query separation means neither handler has an
/// arm for the other flow.
priv struct CounterState {
  mut count : Int
}

///|
priv enum CounterCmd {
  Add(Int)
}

///|
priv enum CounterQuery {
  GetCount
}

///|
fn counter_fuwaroid(
  group : @async.TaskGroup[Unit],
) -> Fuwaroid[CounterCmd, CounterQuery, Int] {
  Fuwaroid::spawn(
    group~,
    init={ count: 0, },
    on_cmd=(_, state, cmd) => {
      match cmd {
        Add(n) => {
          state.count = state.count + n
          state
        }
      }
    },
    on_query=(_, state, _query) => (state, state.count),
  )
}

///|
async fn counter_loop(
  ctx : Ctx[CounterCmd, CounterQuery, Int],
  queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]],
) -> Unit {
  fuwaroid_loop(
    ctx,
    { count: 0, },
    (_, state, cmd) => {
      match cmd {
        Add(n) => {
          state.count = state.count + n
          state
        }
      }
    },
    (_, state, _query) => (state, state.count),
    queue,
    stop=ctx.address.stop,
  )
}

///|
/// FIFO + serialization: an ask enqueued after a batch of tells observes
/// the whole batch; every concurrent tell applies exactly once.
async test "concurrent tells all apply exactly once" {
  @async.with_task_group(group => {
    let fuwaroid = counter_fuwaroid(group)
    let sends : Array[async () -> Unit] = []
    for i = 0; i < 10; i = i + 1 {
      sends.push(() => {
        let _ = fuwaroid.tell(CounterCmd::Add(1))
      })
    }
    let _ = @async.all(sends[:])
    let count = fuwaroid.ask(CounterQuery::GetCount, timeout_ms=1000)
    debug_inspect(count, content="Ok(10)")
  })
}

///|
/// The async-work pattern this library exists for: a sync handler spawns
/// the slow part on the host group and the outcome returns as a `tell` —
/// state still changes only on the Fuwaroid's own loop.
priv enum WorkCmd {
  Start
  Finished
}

///|
priv enum WorkQuery {
  DoneCount
}

///|
priv struct WorkState {
  mut done : Bool
}

///|
async test "handler spawns async work and reports back via tell" {
  @async.with_task_group(group => {
    let fuwaroid : Fuwaroid[WorkCmd, WorkQuery, Int] = Fuwaroid::spawn(
      group~,
      init={ done: false, },
      on_cmd=(ctx, state, cmd) => {
        match cmd {
          Start => {
            ctx.group.spawn_bg(() => {
              @async.sleep(5)
              let _ = ctx.address.tell(WorkCmd::Finished)
            })
            state
          }
          Finished => {
            state.done = true
            state
          }
        }
      },
      on_query=(_, state, _query) => (state, if state.done { 1 } else { 0 }),
    )
    let _ = fuwaroid.tell(WorkCmd::Start)
    @async.sleep(30)
    let done = fuwaroid.ask(WorkQuery::DoneCount, timeout_ms=1000)
    debug_inspect(done, content="Ok(1)")
  })
}

///|
/// Graceful close: everything queued before `close` (tells and asks
/// alike) is still served in order; afterwards the mailbox refuses new
/// messages.
async test "close drains queued messages then refuses new ones" {
  @async.with_task_group(group => {
    let fuwaroid = counter_fuwaroid(group)
    let _ = fuwaroid.tell(CounterCmd::Add(7))
    // ask waits on the spot: the reply proves the earlier tell was served
    let count = fuwaroid.ask(CounterQuery::GetCount, timeout_ms=1000)
    fuwaroid.close()
    debug_inspect(count, content="Ok(7)")
    let refused = fuwaroid.tell(CounterCmd::Add(1))
    debug_inspect(refused, content="Err(MailboxClosed)")
  })
}

///|
/// A cancelled loop closes its public address before re-raising cancellation.
/// The late send and ask must be refused by the actual handle, rather than
/// being accepted into a mailbox with no consumer.
async test "cancelled actor closes its handle" {
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop: LoopStop::fresh(),
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }
    let loop_task = group.spawn(() => counter_loop(ctx, queue))
    @async.pause()
    loop_task.cancel()
    let loop_outcome : Result[Unit, Error] = Ok(loop_task.wait()) catch {
      error => Err(error)
    }
    // This test task is NOT cancelled, so a failed wait can only report
    // the TARGET loop task's cancellation — the ordinary
    // `@async.WaitedTaskAlreadyCancelled` error. (The loop's own cancellation signal
    // is not an `Error` and never appears here.)
    match loop_outcome {
      Err(@async.WaitedTaskAlreadyCancelled) => ()
      Err(other) => fail("expected TaskCancelled, got \{other}")
      Ok(_) => fail("expected the actor loop to end cancelled")
    }
    debug_inspect(
      fuwaroid.tell(CounterCmd::Add(1)),
      content="Err(MailboxClosed)",
    )
    let late_ask = fuwaroid.ask(CounterQuery::GetCount, timeout_ms=10)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
  })
}

///|
/// An ask that is already accepted by the mailbox is exercised through the
/// public API while the loop is cancelled. The loop must still terminate with
/// the cancellation identity and close the same handle used by the ask.
async test "queued ask and loop cancellation close the public handle" {
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop: LoopStop::fresh(),
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }

    // Start the real ask before the loop so its envelope is queued through
    // Fuwaroid::ask rather than inserted by the test.
    let ask_task = group.spawn(() => {
      fuwaroid.ask(CounterQuery::GetCount, timeout_ms=1000)
    })
    @async.pause()

    // Cancellation is requested before the loop gets its first scheduling
    // turn. Queue::get still delivers the accepted envelope, then the loop
    // observes cancellation at its next suspension point.
    let loop_task = group.spawn(() => counter_loop(ctx, queue))
    loop_task.cancel()
    let loop_outcome : Result[Unit, Error] = Ok(loop_task.wait()) catch {
      error => Err(error)
    }
    // Uncancelled observer: the cancelled loop task is reported as the
    // ordinary `@async.WaitedTaskAlreadyCancelled` error.
    match loop_outcome {
      Err(@async.WaitedTaskAlreadyCancelled) => ()
      Err(other) => fail("expected TaskCancelled, got \{other}")
      Ok(_) => fail("expected the actor loop to end cancelled")
    }

    let ask_outcome : Result[Result[Int, AskFailure], Error] = Ok(
      ask_task.wait(),
    ) catch {
      error => Err(error)
    }
    match ask_outcome {
      Ok(Ok(count)) => assert_eq(count, 0)
      Ok(Err(_)) => fail("queued ask was rejected")
      Err(error) => fail("queued ask task raised: \{error}")
    }
    debug_inspect(
      fuwaroid.tell(CounterCmd::Add(1)),
      content="Err(MailboxClosed)",
    )
    let late_ask = fuwaroid.ask(CounterQuery::GetCount, timeout_ms=10)
    debug_inspect(late_ask, content="Err(NotDelivered(MailboxClosed))")
  })
}

///|
/// Bounded mailbox backpressure: with no loop draining it, a
/// `Blocking(1)` mailbox accepts one tell and reports the second as
/// full. The Fuwaroid is built directly (white-box) so the queue state is
/// deterministic.
async test "full bounded mailbox reports MailboxFull" {
  let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
    mailbox: @aqueue.Queue(kind=@aqueue.Blocking(1)),
    stop: LoopStop::fresh(),
  }
  debug_inspect(fuwaroid.tell(CounterCmd::Add(1)), content="Ok(())")
  debug_inspect(fuwaroid.tell(CounterCmd::Add(2)), content="Err(MailboxFull)")
}

///|
/// An ask nobody serves times out as `TimedOut` (white-box Fuwaroid with
/// no loop — the request sits in the mailbox forever).
async test "unserved ask times out" {
  let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
    mailbox: @aqueue.Queue(kind=@aqueue.Unbounded),
    stop: LoopStop::fresh(),
  }
  let outcome = fuwaroid.ask(CounterQuery::GetCount, timeout_ms=10)
  debug_inspect(outcome, content="Err(TimedOut)")
}

///|
/// After an abnormal stop, `fail_leftover_asks` closes every stranded
/// ask's reply queue, so waiters see `StoppedError` immediately instead
/// of hanging until their timeout.
async test "stranded asks fail fast after abnormal stop" {
  let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
    kind=@aqueue.Unbounded,
  )
  let reply : @aqueue.Queue[Int] = @aqueue.Queue(kind=@aqueue.Unbounded)
  let _ = queue.try_put(Envelope::Ask(CounterQuery::GetCount, reply))
  fail_leftover_asks(queue)
  let outcome : Result[Int, Error] = Ok(reply.get()) catch {
    error => Err(error)
  }
  match outcome {
    Err(StoppedError) => ()
    Err(other) => fail("expected StoppedError, got \{other}")
    Ok(_) => fail("expected the reply queue to be closed")
  }
}

///|
/// A foreign queue close error is not a graceful actor stop and must remain
/// observable after cleanup.
async test "foreign mailbox error propagates" {
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    queue.close(error=ForeignError)
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop: LoopStop::fresh(),
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }
    let outcome : Result[Unit, Error] = Ok(counter_loop(ctx, queue)) catch {
      error => Err(error)
    }
    match outcome {
      Err(ForeignError) => ()
      Err(other) => fail("expected ForeignError, got \{other}")
      Ok(_) => fail("expected foreign mailbox error")
    }
  })
}

///|
/// `fail_leftover_asks` also propagates a foreign drain error instead of
/// treating it as an empty mailbox.
async test "foreign drain error propagates" {
  let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
    kind=@aqueue.Unbounded,
  )
  queue.close(error=ForeignError)
  let outcome : Result[Unit, Error] = Ok(fail_leftover_asks(queue)) catch {
    error => Err(error)
  }
  match outcome {
    Err(ForeignError) => ()
    Err(other) => fail("expected ForeignError, got \{other}")
    Ok(_) => fail("expected foreign drain error")
  }
}

///|
/// `spawn_fold` needs no query flow; `ask` degenerates to a
/// processed-receipt for the command.
priv enum PingCmd {
  Ping
}

///|
async test "spawn_fold ask degenerates to processed receipt" {
  @async.with_task_group(group => {
    let seen : Array[String] = []
    let fuwaroid = Fuwaroid::spawn_fold(group~, init=(), on_cmd=(_, _, cmd) => {
      match cmd {
        Ping => seen.push("ping")
      }
      ()
    })
    let _ = fuwaroid.tell(PingCmd::Ping)
    let receipt = fuwaroid.ask((), timeout_ms=1000)
    debug_inspect(receipt, content="Ok(())")
    // the ask rides the (no-op) query path, not on_cmd: only the tell folded
    debug_inspect(seen.length(), content="1")
  })
}

///|
/// `close` only closes the mailbox. Work already spawned on the host group
/// still runs and observes the closed address when it reports back.
async test "close leaves spawned work host-owned" {
  @async.with_task_group(group => {
    let refusals : Array[Result[Unit, SendRefusal]] = []
    let fuwaroid : Fuwaroid[WorkCmd, WorkQuery, Int] = Fuwaroid::spawn(
      group~,
      init={ done: false, },
      on_cmd=(ctx, state, cmd) => {
        match cmd {
          Start => {
            ctx.group.spawn_bg(() => {
              @async.sleep(5)
              refusals.push(ctx.address.tell(WorkCmd::Finished))
            })
            state
          }
          Finished => {
            state.done = true
            state
          }
        }
      },
      on_query=(_, state, _query) => (state, if state.done { 1 } else { 0 }),
    )
    let _ = fuwaroid.tell(WorkCmd::Start)
    @async.pause()
    fuwaroid.close()
    @async.sleep(30)
    debug_inspect(refusals, content="[Err(MailboxClosed)]")
  })
}

///|
/// Pure table: which reply-queue errors map to an `AskFailure` and which
/// must propagate (None).
priv suberror ForeignError {
  ForeignError
}

///|
test "classify_ask_error table" {
  debug_inspect(classify_ask_error(StoppedError), content="Some(Stopped)")
  debug_inspect(classify_ask_error(ForeignError), content="None")
}

///|
/// T15: stop/cleanup disposition, white-box fault injection. Two
/// sections:
///
/// (a) A FOREIGN stop error keeps its ORIGINAL identity through the unified
///     cleanup path. The "foreign mailbox error propagates" test above
///     already proves the raw propagation out of the loop; this section
///     extends that evidence to the recorded-reason contract: the loop task
///     fails with the foreign error itself and `join` — reading the
///     recorded reason after the task failed — returns
///     `Failed(ForeignError)`, the same constructor the
///     raw error carried. `allow_failure=true` only keeps the EXPECTED loop
///     failure contained inside this test group; the task itself still
///     fails with the original error, which is exactly what join maps.
///
/// (b) A CANCELLED loop strands a queued ask: the ask is queued behind one
///     full batch (`fuwaroid_yield_batch` commands), so the loop's first
///     suspension is the batch-yield boundary where the cancellation lands
///     and the ask is provably still queued. The recorded reason is
///     `Cancelled` — in async 0.22.x the cancellation signal is distinct
///     from `Error`, so no error value is fabricated for it — the stranded
///     ask's reply queue is closed with the stop marker (`StoppedError`) by
///     `fail_leftover_asks`, the loop task terminates CANCELLED (the
///     uncancelled observer sees `@async.WaitedTaskAlreadyCancelled` from `wait`), and
///     `join` reports the same recorded `Cancelled`.
///
/// NOTE on secondary cleanup evidence (`LoopStop.cleanup_error`): injecting
/// a secondary cleanup failure into `fuwaroid_cleanup` is NOT reachable
/// with the current queue semantics: `Queue::close` is
/// infallible (it overwrites the close error and never raises), and
/// `fuwaroid_cleanup` closes the queue with the DEFAULT error before
/// draining, so `fail_leftover_asks` can only ever observe
/// `QueueAlreadyClosed`, which it treats as the normal end marker. No fake
/// secondary failure is asserted here.
async test "T15 stop reason keeps its original identity through cleanup and join" {
  // ---- (a) foreign stop error: raw identity survives into join. ----
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    queue.close(error=ForeignError)
    let stop : LoopStop = LoopStop::fresh()
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop,
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }
    let task = group.spawn(allow_failure=true, () => {
      fuwaroid_loop(
        ctx,
        { count: 0, },
        (_, state, cmd) => {
          match cmd {
            Add(n) => {
              state.count = state.count + n
              state
            }
          }
        },
        (_, state, _query) => (state, state.count),
        queue,
        stop~,
      )
    })
    stop.task = Some(task)
    // The loop task fails with the ORIGINAL foreign error (re-raised by the
    // unified cleanup path after recording the reason).
    let outcome : Result[Unit, Error] = Ok(task.wait()) catch {
      error => Err(error)
    }
    match outcome {
      Err(ForeignError) => ()
      Err(other) => fail("T15a: expected ForeignError, got \{other}")
      Ok(_) =>
        fail(
          "T15a: the loop task completed normally despite the foreign mailbox error",
        )
    }
    // join maps the failed task to the RECORDED reason: the original
    // identity, never a replacement.
    let reason = fuwaroid.join()
    match reason {
      Failed(error) =>
        match error {
          ForeignError => ()
          other =>
            fail(
              "T15a: join's Failed holds \{other}, expected the original ForeignError",
            )
        }
      Graceful => fail("T15a: join returned Graceful for a foreign stop")
      Cancelled => fail("T15a: join returned Cancelled for a foreign stop")
    }
  })
  // ---- (b) cancelled loop: stranded ask + original cancellation identity. ----
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    let reply : @aqueue.Queue[Int] = @aqueue.Queue(kind=@aqueue.Unbounded)
    // One full batch of commands, then the ask: the loop's first suspension
    // is the batch-yield boundary, so the ask is provably still queued when
    // the cancellation lands there.
    for _ in 0..<fuwaroid_yield_batch {
      let _ = queue.try_put(Tell(Add(1)))
    }
    let _ = queue.try_put(Ask(GetCount, reply))
    let stop : LoopStop = LoopStop::fresh()
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop,
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }
    let task = group.spawn(() => {
      fuwaroid_loop(
        ctx,
        { count: 0, },
        (_, state, cmd) => {
          match cmd {
            Add(n) => {
              state.count = state.count + n
              state
            }
          }
        },
        (_, state, _query) => (state, state.count),
        queue,
        stop~,
      )
    })
    stop.task = Some(task)
    // Cancel before the loop's first turn: the first suspension it reaches
    // is the batch-yield after one full batch — the ask stays queued.
    task.cancel()
    let outcome : Result[Unit, Error] = Ok(task.wait()) catch {
      error => Err(error)
    }
    // The uncancelled test task observes the cancelled loop task as the
    // ordinary `@async.WaitedTaskAlreadyCancelled` error — never as a normal Done.
    match outcome {
      Err(@async.WaitedTaskAlreadyCancelled) => ()
      Err(other) => fail("T15b: expected TaskCancelled, got \{other}")
      Ok(_) => fail("T15b: the cancelled loop ended as a normal completion")
    }
    // The recorded reason is `Cancelled`: the cancellation signal is not
    // an `Error`, so no error value is fabricated for it.
    match stop.reason {
      Some(Cancelled) => ()
      Some(Graceful) | Some(Failed(_)) | None =>
        fail("T15b: the recorded reason is not Cancelled for a cancelled loop")
    }
    // The stranded ask's reply queue was closed with the stop marker by the
    // cleanup path's fail_leftover_asks.
    let reply_outcome : Result[Int, Error] = Ok(reply.get()) catch {
      error => Err(error)
    }
    match reply_outcome {
      Err(StoppedError) => ()
      Err(other) =>
        fail(
          "T15b: expected StoppedError on the stranded reply queue, got \{other}",
        )
      Ok(_) => fail("T15b: the stranded reply queue was not closed")
    }
    // join agrees with the recorded reason.
    let reason = fuwaroid.join()
    match reason {
      Cancelled => ()
      Failed(_) | Graceful =>
        fail("T15b: join did not return Cancelled for a cancelled loop")
    }
  })
}

///|
/// Pure capacity-validation boundary: a `Bounded` capacity
/// must be a POSITIVE integer. Zero — which `@aqueue` would silently accept
/// as a rendezvous — and negative values are rejected; `spawn` aborts with
/// the returned message (that end-to-end abort path is accepted separately,
/// in an independent process, by tools/concurrency-api-guard.mbtx — an
/// in-process `moon test` cannot observe an abort without killing the whole
/// test executable).
test "mailbox_capacity_error accepts only positive Bounded capacities" {
  for n in [0, -1, -100] {
    match mailbox_capacity_error(n) {
      Some(message) => {
        assert_true(message.contains("positive integer"))
        assert_true(message.contains("got \{n}"))
      }
      None =>
        fail("mailbox_capacity_error(\{n}) must reject a non-positive capacity")
    }
  }
  for n in [1, 64, 1073741824] {
    match mailbox_capacity_error(n) {
      Some(message) =>
        fail(
          "mailbox_capacity_error(\{n}) must accept a positive capacity, got: \{message}",
        )
      None => ()
    }
  }
}

///|
/// A cancelled join waiter observes its OWN cancellation — the runtime's
/// cancellation signal, which `catch` cannot capture and `join` cannot
/// convert into a `StopReason`. The waiter task therefore ends cancelled,
/// and the uncancelled test task (the observer) verifies that by waiting
/// on the waiter and seeing `@async.WaitedTaskAlreadyCancelled`. The actor's outcome
/// is unaffected: it stays `Graceful`.
async test "cancelled joiner propagates cancellation after loop completion" {
  @async.with_task_group(group => {
    let actor : Fuwaroid[Unit, Unit, Unit] = Fuwaroid::spawn_fold(
      group~,
      init=(),
      on_cmd=(_, _, _) => (),
    )
    actor.close()
    guard actor.join() is Graceful else { fail("expected graceful stop") }
    let entered = Ref(false)
    let waiter = group.spawn(() => {
      // The waiter is cancelled before it runs; `join`'s pre-check
      // surfaces the pending cancellation even though the loop already
      // terminated (Task::wait's fast path performs no cancellation
      // check). The signal propagates out of the task, so the join value
      // is never produced.
      assert_true(@async.is_being_cancelled())
      entered.val = true
      let _ = actor.join()
    })
    waiter.cancel()
    // This test task is NOT cancelled: waiting on the cancelled waiter
    // reports the TARGET's cancellation as the ordinary
    // `@async.WaitedTaskAlreadyCancelled` error.
    let waiter_outcome : Result[Unit, Error] = Ok(waiter.wait()) catch {
      error => Err(error)
    }
    guard waiter_outcome is Err(@async.WaitedTaskAlreadyCancelled) else {
      fail(
        "the cancelled join waiter did not end cancelled: \{Repr(waiter_outcome)}",
      )
    }
    assert_true(entered.val)
    guard actor.join() is Graceful else { fail("waiter changed actor outcome") }
  })
}

///|
async test "cancelled cleanup yields with admission closed and settles every ask" {
  @async.with_task_group(group => {
    let queue : @aqueue.Queue[Envelope[CounterCmd, CounterQuery, Int]] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    let stop = LoopStop::fresh()
    let actor : Fuwaroid[CounterCmd, CounterQuery, Int] = {
      mailbox: queue,
      stop,
    }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: actor, }
    for _ in 0..<fuwaroid_yield_batch {
      assert_eq(actor.tell(Add(1)), Ok(()))
    }
    let replies : Array[@aqueue.Queue[Int]] = []
    for _ in 0..<(fuwaroid_yield_batch * 4) {
      let reply = @aqueue.Queue(kind=@aqueue.Unbounded)
      assert_true(queue.try_put(Ask(GetCount, reply)))
      stop.diag.accepted += 1L
      replies.push(reply)
    }
    let task = group.spawn(() => counter_loop(ctx, queue))
    stop.task = Some(task)
    task.cancel()
    let mut observed_partial = false
    for _ in 0..<100 {
      let snapshot = actor.snapshot()
      if snapshot.abandoned > 0L && snapshot.outstanding > 0L {
        guard snapshot.lifecycle is Closing else {
          fail("cleanup must publish Closing before yielding")
        }
        assert_eq(actor.tell(Add(1)), Err(MailboxClosed))
        observed_partial = true
        break
      }
      @async.pause()
    }
    assert_true(observed_partial, msg="cleanup monopolized the scheduler")
    // The uncancelled test task observes the cancelled loop task through
    // join: the recorded reason is `Cancelled`.
    let reason = actor.join()
    guard reason is Cancelled else {
      fail("cancelled cleanup lost the cancellation: \{Repr(reason)}")
    }
    for reply in replies {
      let outcome : Result[Int, Error] = Ok(reply.get()) catch {
        error => Err(error)
      }
      guard outcome is Err(StoppedError) else {
        fail("stranded ask was not closed")
      }
    }
    let final_snapshot = actor.snapshot()
    assert_eq(final_snapshot.processed, fuwaroid_yield_batch.to_int64())
    assert_eq(final_snapshot.abandoned, (fuwaroid_yield_batch * 4).to_int64())
    assert_eq(final_snapshot.outstanding, 0L)
    guard final_snapshot.lifecycle is Stopped(Cancelled) else {
      fail("cleanup did not publish the cancelled terminal state")
    }
  })
}

///|
test "fresh stop record has no fabricated termination and tracks admissions" {
  let stop = LoopStop::fresh()
  guard stop.reason is None else {
    fail("fresh loop already has a stop reason")
  }
  let actor : Fuwaroid[Unit, Unit, Unit] = {
    mailbox: @aqueue.Queue(kind=@aqueue.Unbounded),
    stop,
  }
  assert_eq(actor.tell(()), Ok(()))
  assert_eq(actor.snapshot().accepted, 1L)
  actor.close()
  guard actor.snapshot().lifecycle is Closing else {
    fail("missing real lifecycle")
  }
}
