///|
/// Supervisor black-box tests: task lifecycle, selected settlement,
/// deadlines, shutdown, host-group compatibility, and cancellation races.
fn[T] s_spawn_ok(
  sup : @fuwaroid.Supervisor[T],
  f : async () -> T,
  label? : String,
) -> @fuwaroid.SupervisedTask[T] {
  match sup.spawn(f, label?) {
    Ok(handle) => handle
    Err(_) => abort("test bug: unexpected spawn refusal")
  }
}

///|
fn[T] s_parked(park : @aqueue.Queue[Unit], value : T) -> async () -> T {
  () => {
    park.get()
    value
  }
}

///|
fn[T] s_gated(gate : @aqueue.Queue[Unit], value : T) -> async () -> T {
  () => {
    @async.protect_from_cancel(() => gate.get())
    @async.pause()
    value
  }
}

///|
fn[T] s_describe(result : Result[T, Error]) -> String {
  match result {
    Ok(_) => "ok"
    Err(@async.TaskCancelled) => "task-cancelled"
    Err(_) => "other-error"
  }
}

///|
/// Successful task completes, returns its value, and leaves the live registry.
async test "S01 successful task completes, delivers value, leaves registry" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let handle = s_spawn_ok(
      sup,
      () => {
        gate.get()
        42
      },
      label="answer",
    )
    assert_eq(handle.snapshot().status, @fuwaroid.SupervisedTaskStatus::Running)
    let _ = gate.try_put(())
    assert_eq(handle.wait(), 42)
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Completed,
    )
    assert_eq(handle.snapshot().error_text, None)
    assert_eq(sup.snapshot().tasks.length(), 0)
  })
}

///|
priv suberror S02Boom {
  S02Boom(String)
}

///|
/// Ordinary failure keeps original error propagation and host-group fail-fast.
async test "S02 failure propagates original error and host group semantics" {
  let sup_cell : Ref[@fuwaroid.Supervisor[Int]?] = Ref(None)
  let handle_cell : Ref[@fuwaroid.SupervisedTask[Int]?] = Ref(None)
  let inner : Result[Unit, Error] = Ok(
    @async.with_task_group((group : @async.TaskGroup[Unit]) => {
      let sup = @fuwaroid.Supervisor(group~)
      sup_cell.val = Some(sup)
      match sup.spawn(() => raise S02Boom("original"), label="failing") {
        Ok(handle) => handle_cell.val = Some(handle)
        Err(_) => abort("S02: unexpected spawn refusal")
      }
    }),
  ) catch {
    error => Err(error)
  }
  // The host group's fail-fast is preserved: the scope fails with the
  // worker's original error value, not a supervisor-made substitute.
  match inner {
    Err(S02Boom(msg)) => assert_eq(msg, "original")
    _ => abort("S02: the original S02Boom must escape the inner scope")
  }
  let sup = sup_cell.val.unwrap()
  let handle = handle_cell.val.unwrap()
  assert_eq(handle.snapshot().status, @fuwaroid.SupervisedTaskStatus::Failed)
  match handle.snapshot().error_text {
    Some(text) =>
      // The contract: the ORIGINAL error is propagated unchanged (matched
      // above); the snapshot additionally carries it rendered as text.
      assert_true(text.length() > 0, msg="S02: error text must be non-empty")
    None => abort("S02: a Failed task must record error text")
  }
  assert_eq(sup.snapshot().tasks.length(), 0)
}

///|
/// Cancellation records Cancelling then Cancelled; wait sees TaskCancelled.
async test "S03 cancel requests, worker terminates cancelled, waiter sees TaskCancelled" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let park : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let handle = s_spawn_ok(sup, s_parked(park, 7), label="worker")
    handle.cancel()

    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelling,
    )
    let waited : Result[Int, Error] = Ok(handle.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(waited), "task-cancelled")
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelled,
    )
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// Immediate cancellation is race-safe and leaves no registry leak.
async test "S04 immediate cancellation race ends cancelled without registry leak" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let park : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let handle = s_spawn_ok(sup, s_parked(park, 7), label="early")
    handle.cancel()
    let waited : Result[Int, Error] = Ok(handle.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(waited), "task-cancelled")
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelled,
    )
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// Completion may beat cancellation; terminal state never moves backward.
async test "S05 completion beats cancellation and state never moves backwards" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let handle = s_spawn_ok(sup, () => 42, label="quick")
    // One scheduler turn: the fully synchronous worker runs to
    // completion (eagerly during spawn, or during this pause).
    @async.pause()
    handle.cancel()
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Completed,
    )
    assert_eq(handle.wait(), 42)
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// Selected cancellation leaves unselected tasks running.
async test "S06 selected cancellation leaves unselected tasks running" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let park_a : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let park_b : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let a = s_spawn_ok(sup, s_parked(park_a, 1), label="a")
    let b = s_spawn_ok(sup, s_parked(park_b, 2), label="b")
    let park_c : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let c = s_spawn_ok(sup, s_parked(park_c, 6), label="c")
    @async.pause()
    let outcome = sup.cancel_and_wait(
      tasks=[a, b].clamped_view(),
      timeout_ms=500,
    )
    assert_eq(outcome, @fuwaroid.Settled)
    assert_eq(a.snapshot().status, @fuwaroid.SupervisedTaskStatus::Cancelled)
    assert_eq(b.snapshot().status, @fuwaroid.SupervisedTaskStatus::Cancelled)
    assert_eq(c.snapshot().status, @fuwaroid.SupervisedTaskStatus::Running)
    assert_eq(sup.snapshot().tasks.length(), 1)
    let _ = park_c.try_put(())
    assert_eq(c.wait(), 6)
    assert_eq(c.snapshot().status, @fuwaroid.SupervisedTaskStatus::Completed)
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// Selected cancellable tasks settle before the deadline.
async test "S07 bounded settlement succeeds before the deadline" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let handles : Array[@fuwaroid.SupervisedTask[Int]] = []
    for i in 0..<3 {
      let park : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
      handles.push(s_spawn_ok(sup, s_parked(park, i), label="w\{i}"))
    }
    @async.pause()
    let outcome = sup.cancel_and_wait(
      tasks=handles.clamped_view(),
      timeout_ms=500,
    )
    assert_eq(outcome, @fuwaroid.Settled)
    for handle in handles {
      assert_eq(
        handle.snapshot().status,
        @fuwaroid.SupervisedTaskStatus::Cancelled,
      )
    }
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// DeadlineExceeded reports a protected unresolved task without reaping it.
async test "S08 deadline exceeds a protected task, reports truth, converges after release" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let handle = s_spawn_ok(sup, s_gated(gate, 9), label="stuck")
    @async.pause()
    let outcome = sup.cancel_and_wait(
      tasks=[handle].clamped_view(),
      timeout_ms=150,
    )
    match outcome {
      @fuwaroid.Settled =>
        abort("S08: the protected task must not settle before the deadline")
      @fuwaroid.DeadlineExceeded(snapshots) => {
        assert_eq(snapshots.length(), 1)
        assert_eq(snapshots[0].id, handle.snapshot().id)

        match snapshots[0].status {
          @fuwaroid.SupervisedTaskStatus::Running
          | @fuwaroid.SupervisedTaskStatus::Cancelling => ()
          _ => abort("S08: unresolved task must stay nonterminal in the report")
        }
      }
    }

    assert_eq(sup.snapshot().tasks.length(), 1)
    let _ = gate.try_put(())
    let waited : Result[Int, Error] = Ok(handle.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(waited), "task-cancelled")
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelled,
    )
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// One global deadline is not multiplied by the task count.
async test "S09 deadline is global, not multiplied by the task count" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gates : Array[@aqueue.Queue[Unit]] = []
    let handles : Array[@fuwaroid.SupervisedTask[Int]] = []
    for i in 0..<8 {
      let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
      gates.push(gate)
      handles.push(s_spawn_ok(sup, s_gated(gate, i), label="g\{i}"))
    }
    @async.pause()
    let start = @async.now()
    let outcome = sup.cancel_and_wait(
      tasks=handles.clamped_view(),
      timeout_ms=200,
    )
    let elapsed = @async.now() - start
    match outcome {
      @fuwaroid.Settled => abort("S09: expected the global deadline to fire")
      @fuwaroid.DeadlineExceeded(snapshots) => assert_eq(snapshots.length(), 8)
    }
    assert_true(elapsed >= 150L, msg="S09: deadline fired early")
    assert_true(
      elapsed < 1_000L,
      msg="S09: deadline scaled with the task count",
    )
    for gate in gates {
      let _ = gate.try_put(())
    }
    ignore(group)
  })
}

///|
/// Cancelling the settlement caller does not add target-side cancellation.
async test "S10 caller cancellation does not cancel targets beyond the request" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let target = s_spawn_ok(sup, s_gated(gate, 4), label="target")
    @async.pause()
    let caller = group.spawn(() => {
      let outcome = sup.cancel_and_wait(
        tasks=[target].clamped_view(),
        timeout_ms=30_000,
      )
      match outcome {
        @fuwaroid.Settled => 0
        @fuwaroid.DeadlineExceeded(_) => 1
      }
    })
    @async.pause()
    caller.cancel()
    let caller_outcome : Result[Int, Error] = Ok(caller.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(caller_outcome), "task-cancelled")
    assert_eq(
      target.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelling,
    )
    assert_eq(sup.snapshot().tasks.length(), 1)
    let _ = gate.try_put(())
    let target_outcome : Result[Int, Error] = Ok(target.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(target_outcome), "task-cancelled")
    assert_eq(sup.snapshot().tasks.length(), 0)
  })
}

///|
/// Shutdown closes admission during and after settlement.
async test "S11 shutdown refuses admission during and after shutdown" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let stuck = s_spawn_ok(sup, s_gated(gate, 1), label="stuck")
    @async.pause()
    let shutdown_task = group.spawn(() => {
      let outcome = sup.shutdown(timeout_ms=5_000)
      match outcome {
        @fuwaroid.Settled => 0
        @fuwaroid.DeadlineExceeded(_) => 1
      }
    })
    @async.pause()
    match sup.spawn(() => 0) {
      Ok(_) => abort("S11: spawn must be refused once shutdown began")
      Err(@fuwaroid.SupervisorClosed) => ()
    }
    let _ = gate.try_put(())
    assert_eq(shutdown_task.wait(), 0)
    assert_eq(
      stuck.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelled,
    )
    assert_eq(sup.snapshot().lifecycle, @fuwaroid.SupervisorLifecycle::Closed)
    match sup.spawn(() => 0) {
      Ok(_) => abort("S11: spawn must stay refused after shutdown")
      Err(@fuwaroid.SupervisorClosed) => ()
    }
  })
}

///|
/// Shutdown is idempotent and can retry after DeadlineExceeded.
async test "S12 shutdown idempotence and deadline retry convergence" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let stuck = s_spawn_ok(sup, s_gated(gate, 8), label="stuck")
    @async.pause()

    match sup.shutdown(timeout_ms=80) {
      @fuwaroid.Settled =>
        abort("S12: protected task must outlive the first deadline")
      @fuwaroid.DeadlineExceeded(snapshots) => assert_eq(snapshots.length(), 1)
    }
    assert_eq(sup.snapshot().tasks.length(), 1)
    match sup.spawn(() => 0) {
      Ok(_) => abort("S12: admission must stay closed after deadline exceeded")
      Err(@fuwaroid.SupervisorClosed) => ()
    }

    match sup.shutdown(timeout_ms=80) {
      @fuwaroid.Settled =>
        abort("S12: protected task must outlive the second deadline")
      @fuwaroid.DeadlineExceeded(snapshots) => assert_eq(snapshots.length(), 1)
    }

    let _ = gate.try_put(())
    let waited : Result[Int, Error] = Ok(stuck.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(waited), "task-cancelled")
    match sup.shutdown(timeout_ms=1_000) {
      @fuwaroid.Settled => ()
      @fuwaroid.DeadlineExceeded(_) =>
        abort("S12: a converged registry must settle")
    }
    assert_eq(sup.snapshot().lifecycle, @fuwaroid.SupervisorLifecycle::Closed)
    assert_eq(sup.snapshot().tasks.length(), 0)
    ignore(group)
  })
}

///|
/// Late and concurrent waiters agree after registry reap.
async test "S13 late and multi waiters agree after the task is reaped" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let handle = s_spawn_ok(sup, () => 7, label="value")
    assert_eq(handle.wait(), 7)

    assert_eq(sup.snapshot().tasks.length(), 0)

    let w1 = group.spawn(() => handle.wait())
    let w2 = group.spawn(() => handle.wait())
    assert_eq(w1.wait(), 7)
    assert_eq(w2.wait(), 7)

    assert_eq(handle.wait(), 7)
    assert_eq(
      handle.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Completed,
    )
  })
}

///|
/// Supervisor accepts a host TaskGroup with a non-Unit result type.
async test "S14 supervisor binds to a non-Unit host task group" {
  let result = @async.with_task_group((group : @async.TaskGroup[String]) => {
    let sup : @fuwaroid.Supervisor[Int] = @fuwaroid.Supervisor(group~)
    let handle = s_spawn_ok(sup, () => 17, label="non-unit-host")
    assert_eq(handle.wait(), 17)
    "host-result"
  })
  assert_eq(result, "host-result")
}

///|
/// A shielded caller still observes the independent settlement deadline.
async test "S15 shielded cancelled caller still observes settlement deadline" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = @fuwaroid.Supervisor(group~)
    let gate : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let target = s_spawn_ok(sup, s_gated(gate, 5), label="shielded-target")
    let caller_entered : @aqueue.Queue[Unit] = @aqueue.Queue(
      kind=@aqueue.Unbounded,
    )
    let deadline_observed : Ref[Bool] = Ref(false)

    let caller = group.spawn(() => {
      @async.protect_from_cancel(() => {
        let _ = caller_entered.try_put(())
        match
          sup.cancel_and_wait(tasks=[target].clamped_view(), timeout_ms=120) {
          @fuwaroid.Settled =>
            abort("S15: protected target must outlive the settlement deadline")
          @fuwaroid.DeadlineExceeded(snapshots) => {
            assert_eq(snapshots.length(), 1)
            assert_eq(
              snapshots[0].status,
              @fuwaroid.SupervisedTaskStatus::Cancelling,
            )
            deadline_observed.val = true
          }
        }
      })

      // cancellation point after the shield.
      @async.pause()
    })

    caller_entered.get()
    caller.cancel()

    let caller_outcome : Result[Unit, Error] = Ok(caller.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(caller_outcome), "task-cancelled")
    assert_true(
      deadline_observed.val,
      msg="S15: deadline did not fire while caller was shielded",
    )
    assert_eq(
      target.snapshot().status,
      @fuwaroid.SupervisedTaskStatus::Cancelling,
    )
    assert_eq(sup.snapshot().tasks.length(), 1)

    let _ = gate.try_put(())
    let target_outcome : Result[Int, Error] = Ok(target.wait()) catch {
      error => Err(error)
    }
    assert_eq(s_describe(target_outcome), "task-cancelled")
    assert_eq(sup.snapshot().tasks.length(), 0)
  })
}
