///|
/// Spin budget for flag barriers and completion counters.
let os_spin_budget : Int32 = 10000

///|
/// A waiter observed before completion still receives the value, and a
/// completed cell serves late and repeated waits from the saved value.
async test "OS01 complete before wait serves late and repeated waiters" {
  let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
  assert_eq(cell.complete(41), Ok(()))
  assert_eq(cell.wait(), 41)
  assert_eq(cell.wait(), 41)
}

///|
async test "OS02 wait before complete suspends until the value is published" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let got = Ref(-1)
    let _ = group.spawn(() => {
      parked.val = true
      got.val = cell.wait()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS02: the waiter never reached wait")
    }
    assert_eq(cell.complete(7), Ok(()))
    let mut delivered = false
    for _ in 0..<os_spin_budget {
      if got.val == 7 {
        delivered = true
        break
      }
      @async.pause()
    }
    if !delivered {
      fail("OS02: the parked waiter was not woken by complete")
    }
  })
}

///|
async test "OS03 broadcast wakes every parked waiter and late waiters agree" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let waiters = 4
    let parked = Ref(0)
    let results : Array[Int32] = []
    for _ in 0..<waiters {
      let _ = group.spawn(() => {
        parked.val = parked.val + 1
        results.push(cell.wait())
      })
    }
    let mut all_parked = false
    for _ in 0..<os_spin_budget {
      if parked.val >= waiters {
        all_parked = true
        break
      }
      @async.pause()
    }
    if !all_parked {
      fail("OS03: waiters never parked")
    }
    assert_eq(cell.complete(99), Ok(()))
    let mut settled = false
    for _ in 0..<os_spin_budget {
      if results.length() >= waiters {
        settled = true
        break
      }
      @async.pause()
    }
    if !settled {
      fail("OS03: broadcast did not settle every waiter")
    }
    for got in results {
      assert_eq(got, 99)
    }
    // Late arrival and a repeated wait read the same saved value.
    assert_eq(cell.wait(), 99)
    assert_eq(cell.wait(), 99)
  })
}

///|
/// The rejection carries the SECOND submission, not the saved value.
async test "OS04 the second completion is rejected with its own value" {
  let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
  assert_eq(cell.complete(1), Ok(()))
  assert_eq(cell.complete(2), Err(2))
  assert_eq(cell.wait(), 1)
}

///|
/// `Option` and `Result` payloads are stored as-is: `None` and `Err(..)`
/// are ordinary values, so the cell never borrows the payload type as a
/// completion sentinel.
async test "OS05 None and Err payloads are first-class values" {
  let opt : @fuwaroid.OneShot[Int32?] = @fuwaroid.OneShot()
  assert_eq(opt.complete(None), Ok(()))
  assert_eq(opt.wait(), None)
  assert_eq(opt.complete(Some(1)), Err(Some(1)))
  assert_eq(opt.wait(), None)
  let res : @fuwaroid.OneShot[Result[Int32, String]] = @fuwaroid.OneShot()
  assert_eq(res.complete(Err("no")), Ok(()))
  assert_eq(res.wait(), Err("no"))
  assert_eq(res.complete(Ok(5)), Err(Ok(5)))
  assert_eq(res.wait(), Err("no"))
}

///|
/// Cancelling the only waiter is reported to an UNCANCELLED observer as
/// the ordinary `@async.WaitedTaskAlreadyCancelled` target error, and the
/// cell stays fully usable.
async test "OS06 cancelling the only waiter leaves the cell completable" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let waiter = group.spawn(() => {
      parked.val = true
      cell.wait()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS06: the waiter never parked")
    }
    waiter.cancel()
    let outcome : Result[Int32, Error] = Ok(waiter.wait()) catch {
      error => Err(error)
    }
    guard outcome is Err(@async.WaitedTaskAlreadyCancelled) else {
      fail("OS06: the cancelled waiter did not end cancelled: \{Repr(outcome)}")
    }
    assert_eq(cell.complete(3), Ok(()))
    assert_eq(cell.wait(), 3)
  })
}

///|
/// Cancelling a subset of waiters removes exactly those: the survivors are
/// woken by the broadcast, repeated cancellation requests do not disturb
/// the cell, and fresh waiters after completion read the saved value.
async test "OS07 partial cancellation spares the other waiters and the cell" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(0)
    let cancelled : Array[@async.Task[Int32]] = []
    let mut survivor : @async.Task[Int32]? = None
    for i in 0..<3 {
      let task = group.spawn(() => {
        parked.val = parked.val + 1
        cell.wait()
      })
      if i < 2 {
        cancelled.push(task)
      } else {
        survivor = Some(task)
      }
    }
    let mut all_parked = false
    for _ in 0..<os_spin_budget {
      if parked.val >= 3 {
        all_parked = true
        break
      }
      @async.pause()
    }
    if !all_parked {
      fail("OS07: waiters never parked")
    }
    // Repeated requests are idempotent; both targets must actually
    // TERMINATE cancelled (Task::wait suspends this task, so the cancelled
    // waiters run and dequeue before complete is reached).
    cancelled[0].cancel()
    cancelled[0].cancel()
    cancelled[1].cancel()
    for task in cancelled {
      let outcome : Result[Int32, Error] = Ok(task.wait()) catch {
        error => Err(error)
      }
      guard outcome is Err(@async.WaitedTaskAlreadyCancelled) else {
        fail("OS07: a cancelled waiter did not end cancelled: \{Repr(outcome)}")
      }
    }
    assert_eq(cell.complete(11), Ok(()))
    guard survivor is Some(kept) else { fail("OS07: survivor vanished") }
    assert_eq(kept.wait(), 11)
    assert_eq(cell.wait(), 11)
  })
}

///|
/// The CondVar woken-then-cancelled boundary: the waiter is parked, the
/// broadcast has already popped and woken it, and the cancellation request
/// lands BEFORE the waiter ever resumes. The protection swallows the
/// signal and the waiter still observes the saved value — cancellation
/// must not lose a value that was already handed to the waiter.
async test "OS08 cancellation after the wake still delivers the value" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let waiter = group.spawn(() => {
      parked.val = true
      cell.wait()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS08: the waiter never parked")
    }
    // Broadcast wakes (dequeues) the parked waiter without resuming it.
    assert_eq(cell.complete(31), Ok(()))
    // The cancellation lands in the wake-to-resume window.
    waiter.cancel()
    assert_eq(waiter.wait(), 31)
  })
}

///|
/// A waiter cancelled while parked is dequeued BEFORE the completion; it
/// consumes nothing, and a completion issued afterwards is not lost.
async test "OS09 a dequeued waiter does not consume the completion" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let waiter = group.spawn(() => {
      parked.val = true
      cell.wait()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS09: the waiter never parked")
    }
    waiter.cancel()
    let outcome : Result[Int32, Error] = Ok(waiter.wait()) catch {
      error => Err(error)
    }
    guard outcome is Err(@async.WaitedTaskAlreadyCancelled) else {
      fail("OS09: the cancelled waiter did not end cancelled: \{Repr(outcome)}")
    }
    // The waiter has fully terminated (dequeued); the completion survives.
    assert_eq(cell.complete(5), Ok(()))
    assert_eq(cell.wait(), 5)
  })
}

///|
/// A `with_timeout` caller that gives up neither completes nor breaks the
/// cell: the abandoned waiter is still served, and later waiters read the
/// saved value.
async test "OS10 a with_timeout caller leaves the cell intact" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let waiter = group.spawn(() => {
      parked.val = true
      cell.wait()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS10: the waiter never parked")
    }
    // Nobody completes the cell, so the outcome is None whenever the timer
    // fires; the wait is bounded by construction, not by a sleep guess.
    assert_eq(@async.with_timeout_opt(5, () => cell.wait()), None)
    assert_eq(cell.complete(21), Ok(()))
    assert_eq(waiter.wait(), 21)
    assert_eq(cell.wait(), 21)
  })
}

///|
/// Several producer tasks race to complete: exactly one is accepted, every
/// rejected producer gets its OWN value back, and the saved value is the
/// winner's. First-wins only — which producer wins is scheduler order and
/// is deliberately not asserted.
async test "OS11 concurrent producers first-wins, losers keep their value" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let producers = 4
    let outcomes : Array[Result[Unit, Int32]] = []
    for i in 0..<producers {
      let value = 100 + i
      let _ = group.spawn(() => outcomes.push(cell.complete(value)))
    }
    let mut settled = false
    for _ in 0..<os_spin_budget {
      if outcomes.length() >= producers {
        settled = true
        break
      }
      @async.pause()
    }
    if !settled {
      fail("OS11: producers did not settle")
    }
    let mut accepted = 0
    let mut losers = 0
    let mut rejected_sum = 0
    for outcome in outcomes {
      match outcome {
        Ok(_) => accepted = accepted + 1
        Err(rejected) => {
          guard rejected >= 100 && rejected < 100 + producers else {
            fail(
              "OS11: rejection does not carry a submitted value: \{rejected}",
            )
          }
          losers = losers + 1
          rejected_sum = rejected_sum + rejected
        }
      }
    }
    // Exactly one producer wins; every loser keeps its own value.
    assert_eq(accepted, 1)
    assert_eq(losers, producers - 1)
    let saved = cell.wait()
    // The accepted value plus every rejected value account for each of the
    // distinct submissions exactly once: no rejection reports the saved
    // value, and no submission is reported twice.
    let submitted_sum = producers * (producers - 1) / 2 + producers * 100
    assert_eq(saved + rejected_sum, submitted_sum)
  })
}

///|
/// The two cancellation views are distinct: the waiter's OWN coroutine
/// observes its cancellation signal (caught with handle_cancellation,
/// then re-raised at an unprotected suspension point so the task ends
/// cancelled), while any uncancelled observer sees the ordinary
/// `@async.WaitedTaskAlreadyCancelled` target error from `Task::wait`.
async test "OS12 own cancellation is observable in the cancelled waiter" {
  @async.with_task_group(group => {
    let cell : @fuwaroid.OneShot[Int32] = @fuwaroid.OneShot()
    let parked = Ref(false)
    let caught = Ref(false)
    let waiter = group.spawn(() => {
      parked.val = true
      let rescued : Int32? = @async.handle_cancellation(() => cell.wait())
      caught.val = rescued is None
      // The cancellation was observed and recorded; re-enter an
      // unprotected cancellation point so this task terminates cancelled
      // instead of masquerading as a normal completion.
      @async.pause()
    })
    let mut registered = false
    for _ in 0..<os_spin_budget {
      if parked.val {
        registered = true
        break
      }
      @async.pause()
    }
    if !registered {
      fail("OS12: the waiter never parked")
    }
    waiter.cancel()
    let outcome : Result[Unit, Error] = Ok(waiter.wait()) catch {
      error => Err(error)
    }
    guard outcome is Err(@async.WaitedTaskAlreadyCancelled) else {
      fail("OS12: the waiter did not end cancelled: \{Repr(outcome)}")
    }
    assert_true(
      caught.val,
      msg="OS12: the waiter never saw its own cancellation",
    )
    // The cell is untouched by the cancelled waiter.
    assert_eq(cell.complete(8), Ok(()))
    assert_eq(cell.wait(), 8)
  })
}
