///|
/// Supervisor admission lifecycle.
#derive(Eq, Debug)
pub(all) enum SupervisorLifecycle {
  Open
  Closing
  Closed
}

///|
pub extend SupervisorLifecycle with Eq::{equal, not_equal}

///|
pub extend SupervisorLifecycle with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Lifecycle of one supervised task.
#derive(Eq, Debug)
pub(all) enum SupervisedTaskStatus {
  Running
  Cancelling
  Completed
  Cancelled
  Failed
}

///|
pub extend SupervisedTaskStatus with Eq::{equal, not_equal}

///|
pub extend SupervisedTaskStatus with @moonbitlang/core/debug.Debug::{to_repr}

///|
#derive(Eq, Debug)
pub(all) enum SpawnRefusal {
  SupervisorClosed
}

///|
pub extend SpawnRefusal with Eq::{equal, not_equal}

///|
pub extend SpawnRefusal with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Copied diagnostic view of one supervised task.
#derive(Eq, Debug)
pub(all) struct SupervisedTaskSnapshot {
  id : Int32
  label : String
  status : SupervisedTaskStatus
  error_text : String?
}

///|
pub extend SupervisedTaskSnapshot with Eq::{equal, not_equal}

///|
pub extend SupervisedTaskSnapshot with @moonbitlang/core/debug.Debug::{to_repr}

///|
#derive(Eq, Debug)
pub(all) enum SettleOutcome {
  Settled
  DeadlineExceeded(Array[SupervisedTaskSnapshot])
}

///|
pub extend SettleOutcome with Eq::{equal, not_equal}

///|
pub extend SettleOutcome with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Copied diagnostic view of a supervisor and its live tasks.
#derive(Eq, Debug)
pub(all) struct SupervisorSnapshot {
  lifecycle : SupervisorLifecycle
  tasks : Array[SupervisedTaskSnapshot]
}

///|
pub extend SupervisorSnapshot with Eq::{equal, not_equal}

///|
pub extend SupervisorSnapshot with @moonbitlang/core/debug.Debug::{to_repr}

///|
fn supervisor_status_transition(
  current : SupervisedTaskStatus,
  next : SupervisedTaskStatus,
) -> Bool {
  match (current, next) {
    (Running, Completed) | (Running, Cancelled) | (Running, Failed) => true
    (Cancelling, Completed) | (Cancelling, Cancelled) | (Cancelling, Failed) =>
      true
    (Running, _)
    | (Cancelling, _)
    | (Completed, _)
    | (Cancelled, _)
    | (Failed, _) => false
  }
}
