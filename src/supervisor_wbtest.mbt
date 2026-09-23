///|
/// White-box invariants for status transitions, sticky cancellation, and
/// original error propagation.
///
///|
test "W01 forward-only transition table" {
  let terminals : Array[SupervisedTaskStatus] = [Completed, Cancelled, Failed]
  let statuses : Array[SupervisedTaskStatus] = [
    Running, Cancelling, Completed, Cancelled, Failed,
  ]
  for next in terminals {
    assert_true(
      supervisor_status_transition(Running, next),
      msg="terminal must be recordable from Running",
    )
  }
  for next in terminals {
    assert_true(
      supervisor_status_transition(Cancelling, next),
      msg="terminal must be recordable from Cancelling",
    )
  }
  assert_false(supervisor_status_transition(Running, Running))
  assert_false(supervisor_status_transition(Running, Cancelling))
  assert_false(supervisor_status_transition(Cancelling, Running))
  assert_false(supervisor_status_transition(Cancelling, Cancelling))
  for current in terminals {
    for next in statuses {
      assert_false(
        supervisor_status_transition(current, next),
        msg="terminal states must be final",
      )
    }
  }
}

///|
fn w_parked(park : @aqueue.Queue[Unit], value : Int) -> async () -> Int {
  () => {
    park.get()
    value
  }
}

///|
async test "W02 cancelled invariant: Cancelled status, reaped registry, TaskCancelled waiter" {
  @async.with_task_group((group : @async.TaskGroup[Unit]) => {
    let sup = Supervisor(group~)
    let park : @aqueue.Queue[Unit] = @aqueue.Queue(kind=@aqueue.Unbounded)
    let handle = match sup.spawn(w_parked(park, 9), label="w02") {
      Ok(handle) => handle
      Err(_) => abort("W02: unexpected spawn refusal")
    }
    handle.cancel()
    // An uncancelled observer must see the target task as cancelled.
    let observer = group.spawn(() => {
      let waited : Result[Int, Error] = Ok(handle.wait()) catch {
        error => Err(error)
      }
      match waited {
        Err(@async.TaskCancelled) => 0
        Ok(value) =>
          abort("W02: cancelled task must not deliver a value, got \{value}")
        Err(_) => abort("W02: waiter must see exactly TaskCancelled")
      }
    })
    assert_eq(observer.wait(), 0)
    assert_eq(handle.status, Cancelled)
    assert_eq(handle.error_text, None)
    assert_eq(sup.snapshot().tasks.length(), 0)
  })
}

///|
priv suberror WBoom {
  WBoom(String)
}

///|
async test "W03 failed task records error text and keeps original propagation" {
  let sup_cell : Ref[Supervisor[Int]?] = Ref(None)
  let handle_cell : Ref[SupervisedTask[Int]?] = Ref(None)
  let inner : Result[Unit, Error] = Ok(
    @async.with_task_group((group : @async.TaskGroup[Unit]) => {
      let sup = Supervisor(group~)
      sup_cell.val = Some(sup)
      match sup.spawn(() => raise WBoom("w03-original"), label="boom") {
        Ok(handle) => handle_cell.val = Some(handle)
        Err(_) => abort("W03: unexpected spawn refusal")
      }
    }),
  ) catch { error => Err(error) }
  match inner {
    Err(WBoom(msg)) => assert_eq(msg, "w03-original")
    _ => abort("W03: the original WBoom must escape the inner scope")
  }
  let handle = handle_cell.val.unwrap()
  assert_eq(handle.status, Failed)
  assert_true(handle.error_text is Some(_), msg="W03: error text recorded")
  assert_eq(sup_cell.val.unwrap().snapshot().tasks.length(), 0)
}
