///|
/// Task ownership and cancellation on an existing host TaskGroup.
///
/// Supervisor is separate from Fuwaroid message processing. It creates no
/// runtime and does not detach tasks from the host group.
pub struct Supervisor[X] {
  priv spawn_task : (async () -> X) -> @async.Task[X]
  priv state : SupervisorState[X]
}

///|
priv struct SupervisorState[X] {
  mut next_id : Int
  mut lifecycle : SupervisorLifecycle
  live : Array[SupervisedTask[X]]
}

///|
pub struct SupervisedTask[X] {
  priv id : Int
  priv label : String
  priv mut status : SupervisedTaskStatus
  priv mut error_text : String?
  priv mut task : @async.Task[X]?
}

///|
/// Bind to a host TaskGroup of any result type.
pub fn[G, X] Supervisor::Supervisor(
  group~ : @async.TaskGroup[G],
) -> Supervisor[X] {
  {
    spawn_task: fn(worker) { group.spawn(no_wait=true, worker) },
    state: { next_id: 1, lifecycle: Open, live: [], },
  }
}

///|
fn[X] supervisor_reap(
  state : SupervisorState[X],
  task : SupervisedTask[X],
) -> Unit {
  for i in 0..<state.live.length() {
    if state.live[i].id == task.id {
      let _ = state.live.remove(i)
      return
    }
  }
}

///|
fn[X] supervisor_record_terminal(
  state : SupervisorState[X],
  task : SupervisedTask[X],
  status : SupervisedTaskStatus,
  text? : String,
) -> Unit {
  if supervisor_status_transition(task.status, status) {
    task.status = status
    task.error_text = text
    supervisor_reap(state, task)
  }
}

///|
async fn[X] supervised_run(
  state : SupervisorState[X],
  task : SupervisedTask[X],
  f : async () -> X,
) -> X {
  let outcome : Result[X?, Error] = Ok(@async.handle_cancellation(f)) catch {
    error => Err(error)
  }
  match outcome {
    Ok(Some(value)) => {
      supervisor_record_terminal(state, task, Completed)
      value
    }
    Ok(None) => {
      supervisor_record_terminal(state, task, Cancelled)
      // Cancellation is sticky in async 0.22.x. Re-enter an unprotected
      // cancellation point so the underlying task remains Cancelled.
      @async.pause()
      abort("fuwaroid: cancelled supervisee resumed unexpectedly")
    }
    Err(original) => {
      supervisor_record_terminal(state, task, Failed, text=original.to_string())
      raise original
    }
  }
}

///|
/// Spawn a supervised child task. Admission is refused once shutdown starts.
pub fn[X] Supervisor::spawn(
  self : Supervisor[X],
  f : async () -> X,
  label? : String = "",
) -> Result[SupervisedTask[X], SpawnRefusal] {
  if !(self.state.lifecycle is Open) {
    return Err(SupervisorClosed)
  }
  let id = self.state.next_id
  self.state.next_id = id + 1
  let handle : SupervisedTask[X] = {
    id,
    label,
    status: Running,
    error_text: None,
    task: None,
  }
  // Register before spawn because the child may run before spawn returns.
  self.state.live.push(handle)
  let task = (self.spawn_task)(() => supervised_run(self.state, handle, f))
  handle.task = Some(task)
  Ok(handle)
}

///|
/// Request cooperative cancellation. Returning does not imply termination.
pub fn[X] SupervisedTask::cancel(self : SupervisedTask[X]) -> Unit {
  match self.status {
    Running => {
      self.status = Cancelling
      match self.task {
        Some(task) => task.cancel()
        None => abort("fuwaroid: supervisee task missing during cancel")
      }
    }
    Cancelling =>
      match self.task {
        Some(task) => task.cancel()
        None => abort("fuwaroid: supervisee task missing during cancel")
      }
    Completed | Cancelled | Failed => ()
  }
}

///|
/// Wait using the underlying Task semantics.
///
/// A cancelled target raises @async.WaitedTaskAlreadyCancelled. Ordinary worker failures
/// keep their original Error identity. Caller cancellation propagates.
pub async fn[X] SupervisedTask::wait(self : SupervisedTask[X]) -> X {
  match self.task {
    Some(task) => task.wait()
    None => abort("fuwaroid: supervisee task missing during wait")
  }
}

///|
pub fn[X] SupervisedTask::snapshot(
  self : SupervisedTask[X],
) -> SupervisedTaskSnapshot {
  {
    id: self.id,
    label: self.label,
    status: self.status,
    error_text: self.error_text,
  }
}

///|
pub fn[X] Supervisor::snapshot(self : Supervisor[X]) -> SupervisorSnapshot {
  {
    lifecycle: self.state.lifecycle,
    tasks: self.state.live.map(task => task.snapshot()),
  }
}
