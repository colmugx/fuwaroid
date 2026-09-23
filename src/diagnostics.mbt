///|
/// Lifecycle phase of a Fuwaroid's loop. Phases
/// only move FORWARD: `Running` → `Closing` (admission was closed) →
/// `Stopped` (the unified stop/cleanup path completed). An abnormal stop
/// closes admission and enters `Closing` before yielding during cleanup;
/// `Stopped` carries the recorded `StopReason` after cleanup. The phase never moves
/// backwards: a late `close` cannot un-stop an instance, and a second
/// `close` cannot leave `Closing` once `Stopped` was reached.
pub(all) enum Lifecycle {
  Running
  Closing
  Stopped(StopReason)
} derive(Debug)

///|
pub extend Lifecycle with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Synchronous, read-only diagnostic snapshot of one Fuwaroid. Every
/// field is a copied value: a snapshot never leaks
/// mutable internal state, the business `State`, nor `Cmd`/`Query`/`Reply`
/// values.
///
/// Field semantics:
/// - `lifecycle`: the forward-only phase, see `Lifecycle`.
/// - `accepted`: admissions ACCEPTED by the mailbox — commands and queries
///   alike, one count per successful `tell`/`ask` admission.
/// - `processed`: messages whose handler RETURNED and whose state was
///   committed on the loop. This is NOT background-work completion: work a
///   handler spawned on the host group may still be running (or have been
///   refused by the closing mailbox) long after `processed` was bumped.
/// - `rejected_full` / `rejected_closed`: admission attempts refused
///   because the bounded mailbox was full / because the mailbox was
///   closed — one count per refused `tell` or `ask`.
/// - `abandoned`: messages accepted but never served when the loop stopped
///   abnormally — commands and queries alike, counted by the unified
///   cleanup path over exactly the envelopes its stranded-ask drain
///   removes. Once the loop has terminated the conservation law closes:
///   `abandoned == accepted − processed` for an abnormal stop and
///   `abandoned == 0` for a graceful one.
/// - `outstanding`: derived at snapshot time as
///   `accepted − processed − abandoned` — accepted messages whose fate
///   (served or abandoned) is not decided yet. It counts REQUESTS in
///   flight; it does NOT pretend to be the underlying buffer length (a
///   parked-reader handoff can hold one message beyond the configured
///   capacity, and this field says nothing about buffer occupancy).
///
/// Counter policy: every counter is `Int64` and only ever increases (each
/// counts one monotone event kind); they never silently wrap or reset.
/// Overflow handling is by design "unreachable, documented": at 2^63
/// admissions the cooperative runtime would have exhausted every
/// realizable resource long before, and per-increment checked arithmetic
/// on the message hot path is not worth its cost.
pub(all) struct Snapshot {
  lifecycle : Lifecycle
  accepted : Int64
  processed : Int64
  rejected_full : Int64
  rejected_closed : Int64
  abandoned : Int64
  outstanding : Int64
} derive(Debug)

///|
pub extend Snapshot with @moonbitlang/core/debug.Debug::{to_repr}

///|
/// Mutable per-instance diagnostic state backing `Snapshot`. It lives
/// inside the instance's `LoopStop` cell, which is shared between the
/// handle (admission side: `tell`/`ask`/`close`) and the loop task
/// (serving side: `processed`; cleanup: `abandoned` and the final phase).
/// All updates happen inside synchronous, non-suspending sections, so the
/// cooperative runtime never observes a torn or half-counted state.
priv struct Diag {
  mut lifecycle : Lifecycle
  mut accepted : Int64
  mut processed : Int64
  mut rejected_full : Int64
  mut rejected_closed : Int64
  mut abandoned : Int64
}

///|
fn Diag::fresh() -> Diag {
  {
    lifecycle: Running,
    accepted: 0L,
    processed: 0L,
    rejected_full: 0L,
    rejected_closed: 0L,
    abandoned: 0L,
  }
}

///|
/// Rank used by the forward-only phase move (see `Diag::transition`).
fn lifecycle_rank(lifecycle : Lifecycle) -> Int {
  match lifecycle {
    Running => 0
    Closing => 1
    Stopped(_) => 2
  }
}

///|
/// Forward-only phase move. A backwards request (e.g. a `close` arriving
/// after the loop already stopped) is ignored.
fn Diag::transition(self : Diag, next : Lifecycle) -> Unit {
  if lifecycle_rank(next) > lifecycle_rank(self.lifecycle) {
    self.lifecycle = next
  }
}

///|
/// Synchronous, read-only diagnostic snapshot (field criteria on
/// `Snapshot`). Never raises, never suspends: the read happens in one
/// uninterrupted scheduling turn, so the counters are mutually consistent
/// and `outstanding = accepted − processed − abandoned` holds for every
/// observed snapshot.
pub fn[Cmd, Query, Reply] Fuwaroid::snapshot(
  self : Fuwaroid[Cmd, Query, Reply],
) -> Snapshot {
  let d = self.stop.diag
  {
    lifecycle: d.lifecycle,
    accepted: d.accepted,
    processed: d.processed,
    rejected_full: d.rejected_full,
    rejected_closed: d.rejected_closed,
    abandoned: d.abandoned,
    outstanding: d.accepted - d.processed - d.abandoned,
  }
}
