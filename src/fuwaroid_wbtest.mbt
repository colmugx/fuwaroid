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
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = { mailbox: queue, }
    let ctx : Ctx[CounterCmd, CounterQuery, Int] = { group, address: fuwaroid, }
    let loop_task = group.spawn(() => counter_loop(ctx, queue))
    @async.pause()
    loop_task.cancel()
    let loop_outcome : Result[Unit, Error] = Ok(loop_task.wait()) catch {
      error => Err(error)
    }
    match loop_outcome {
      Err(error) => assert_true(@async.is_cancellation_error(error))
      Ok(_) => fail("expected the actor loop to propagate cancellation")
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
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = { mailbox: queue, }
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
    match loop_outcome {
      Err(error) => assert_true(@async.is_cancellation_error(error))
      Ok(_) => fail("expected the actor loop to propagate cancellation")
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
    let fuwaroid : Fuwaroid[CounterCmd, CounterQuery, Int] = { mailbox: queue, }
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
test "foreign drain error propagates" {
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
    let receipt = fuwaroid.ask(PingCmd::Ping, timeout_ms=1000)
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
