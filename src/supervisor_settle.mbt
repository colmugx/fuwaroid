///|
priv enum SettleEvent {
  Terminal
  Deadline
}

///|
fn[X] supervisor_unresolved(
  tasks : ArrayView[SupervisedTask[X]],
) -> Array[SupervisedTask[X]] {
  let unresolved = []
  for task in tasks {
    match task.status {
      Running | Cancelling => unresolved.push(task)
      Completed | Cancelled | Failed => ()
    }
  }
  unresolved
}

///|
fn[X] supervisor_snapshots(
  tasks : Array[SupervisedTask[X]],
) -> Array[SupervisedTaskSnapshot] {
  tasks.map(task => task.snapshot())
}

///|
/// Cancel selected handles managed by this supervisor and wait under one
/// overall deadline.
///
/// The deadline bounds this settlement operation only. Supervised tasks
/// remain children of the host TaskGroup, whose structured exit still waits
/// for unresolved children.
pub async fn[X] Supervisor::cancel_and_wait(
  _self : Supervisor[X],
  tasks~ : ArrayView[SupervisedTask[X]],
  timeout_ms~ : Int,
) -> SettleOutcome {
  for task in tasks {
    task.cancel()
  }

  let unresolved = supervisor_unresolved(tasks)
  if unresolved.is_empty() {
    return Settled
  }

  let deadline = @async.now() + timeout_ms.to_int64()
  if @async.now() >= deadline {
    return DeadlineExceeded(supervisor_snapshots(unresolved))
  }

  @async.with_task_group(group => {
    let events : @aqueue.Queue[SettleEvent] = @aqueue.Queue(kind=@aqueue.Unbounded)

    for target in unresolved {
      group.spawn_bg(no_wait=true, allow_failure=true, () => {
        match target.task {
          Some(task) => {
            let waited : Result[X, Error] = Ok(task.wait()) catch {
              error => Err(error)
            }
            let _ = waited
          }
          None => abort("fuwaroid: supervisee task missing during settlement")
        }
        let _ = events.try_put(Terminal)
      })
    }

    let remaining = (deadline - @async.now()).to_int()
    group.spawn_bg(no_wait=true, allow_failure=true, () => {
      @async.sleep(remaining)
      let _ = events.try_put(Deadline)
    })

    settle~: {
      let mut settled = 0
      while true {
        match events.get() {
          Terminal => {
            settled += 1
            if settled == unresolved.length() {
              break settle~ Settled
            }
          }
          Deadline => {
            let current = supervisor_unresolved(tasks)
            if current.is_empty() {
              break settle~ Settled
            }
            break settle~ DeadlineExceeded(supervisor_snapshots(current))
          }
        }
      }
      abort("fuwaroid: settlement loop exited without outcome")
    }
  })
}

///|
/// Close admission, cancel the live-task snapshot, and bounded-settle it.
///
/// DeadlineExceeded does not detach unresolved tasks from the host
/// TaskGroup. Repeated shutdown calls may wait for them again.
pub async fn[X] Supervisor::shutdown(
  self : Supervisor[X],
  timeout_ms~ : Int,
) -> SettleOutcome {
  if self.state.lifecycle is Open {
    self.state.lifecycle = Closing
  }
  let selected = self.state.live.copy()
  let outcome = self.cancel_and_wait(tasks=selected.clamped_view(), timeout_ms~)
  if self.state.lifecycle is Closing {
    self.state.lifecycle = Closed
  }
  outcome
}
