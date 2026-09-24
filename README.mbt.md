# colmugx/fuwaroid

Lightweight actor-style concurrency primitives for
[`moonbitlang/async`](https://mooncakes.io/docs/moonbitlang/async): private
state, a typed mailbox, and one serial loop. The host owns the lifecycle;
the library owns the mailbox semantics — nothing else. The runtime
underneath is single-threaded and cooperative: many Fuwaroids make
**concurrent progress**, which is **not** multicore parallelism.

## Why "Fuwaroid"

**ふわり (fuwari)** is the Japanese mimetic word for the way something
weightless moves and settles — a feather alighting, no impact, no weight.
**-oid** makes it a creature of that quality (as in *android*, *humanoid*).
A **Fuwaroid** is thus "a thing that floats lightly".

That is precisely this library's concurrency. Tasks never grip OS threads
and never block — they float suspended and drift between suspension
points. A Fuwaroid is one such light entity: a little state and a mailbox.
It sleeps weightlessly until a message arrives (`queue.get()` suspends),
settles the message in **one synchronous step**, and drifts back to sleep.
No locks, no blocking, no machinery — and the library itself stays
featherweight: two small files over one dependency.

## The model

```text
            tell(cmd) ───────────────┐   ask(query) ───────────┐
   callers                             │  (with timeout)         │
                                       ▼                         ▼
                             ┌──────────────────────────────────────┐
   host task group ─────────►│  mailbox (typed, FIFO, bounded or not)│
   (owns the lifetime)       └──────────────────┬───────────────────┘
                                                │ one serial loop
                                                ▼ yields every 32
                                                  complete messages
                            state = on_cmd(state, cmd)              ← sync
                            (state, reply) = on_query(state, query) ← sync
                                                │
                 async work: ctx.group.spawn_bg(work) …
                            work finishes → ctx.address.tell(result)
```

Commands and queries are separate types — `on_cmd` folds state, `on_query`
reads it and replies — so neither handler carries unreachable arms for
the other flow.

## Guarantees

1. **Handlers are synchronous by signature.** A callback runs to return
   without another mailbox message interleaving it, so the loop applies each
   returned state in one serial step. Because handlers are plain
   synchronous functions and `ask` is an `async` method, the compiler
   rejects an `ask` call from inside a handler — report results back with
   `tell` instead. The generic API cannot prove that `State` has no aliases
   or that a callback is pure; callers must keep actor state owned by the
   loop and avoid mutating it from spawned work or replies.
2. **Fair scheduling happens between complete messages.** After every
   `fuwaroid_yield_batch` (64) complete messages the loop yields the
   scheduler, so an instance that keeps its own mailbox non-empty cannot
   monopolize the cooperative runtime: other instances, timers and
   cancellation all get scheduled. This is a scheduling opportunity, not a
   real-time guarantee: no millisecond bound is promised, and one long
   CPU-bound handler still blocks the whole runtime for its duration
   (measured in `docs/concurrency-benchmarks.md` §2.5: a 2,000-iteration
   arithmetic handler drops end-to-end throughput from 1.87M to 423k
   msg/s).
3. **Async work lives outside the Fuwaroid.** Spawn it on the host group
   from a handler; report the outcome back with `tell`. The loop is the
   only place that applies a handler's returned state. The library does
   **not** guarantee reliable delivery of background results: a report
   that arrives after `close`, or into a full bounded mailbox, is refused,
   and the host must collect that refusal itself (see
   [Background work](#background-work)).
4. **Lifecycle: close, cancel, join.** `close` is idempotent: it refuses
   new messages and drains the already-queued ones in FIFO order, then the
   loop exits; `close` returning does not mean the drain completed. Host
   cancellation **outranks draining**: the loop stops at the next
   cancellation boundary instead of finishing the backlog. `join` waits
   for the loop task to terminate and returns the recorded `StopReason`;
   `close(); join()` is the way to confirm a graceful drain.
5. **Stranded asks fail fast.** If the loop stops abnormally (host
   cancellation or another terminal mailbox error), asks that were already
   accepted get `Stopped` immediately instead of hanging until their
   timeout. Cancellation of the asker itself propagates unchanged.
6. **Backpressure is explicit.** The mailbox is this library's own
   `Mailbox`: `Unbounded`, or `Bounded(n)` whose sends are refused
   synchronously with `MailboxFull` — nothing is silently dropped, and
   there is no drop-style mailbox. An invalid capacity (`Bounded(0)` or a
   negative value) aborts at spawn time.

These semantics are also the forward story for MoonBit's eventual
multithreaded runtime: serial processing plus message-only communication
is exactly the shape that survives real threads unchanged.

## API

| Symbol | Meaning |
|---|---|
| `Fuwaroid::spawn(group~, init~, on_cmd~, on_query~, mailbox?)` | Start a Fuwaroid on the host's long-lived task group. `mailbox?` defaults to `Mailbox::Unbounded`. |
| `Fuwaroid::spawn_fold(group~, init~, on_cmd~, mailbox?)` | Command-only variant; the handle is `Fuwaroid[Cmd, Unit, Unit]` and `ask(())` degenerates to a served-receipt barrier. |
| `f.tell(cmd)` | Fire-and-forget; `Result[Unit, SendRefusal]` right after the mailbox decision. |
| `f.ask(query, timeout_ms~)` | Request-response; `TimedOut` / `NotDelivered` / `Stopped` failures; caller cancellation propagates. |
| `f.close()` | Graceful mailbox stop: refuse new, drain queued, exit the loop. Idempotent; returns before the drain completes. |
| `f.join()` | Wait for the loop task to terminate; returns `StopReason` (`Graceful`, `Cancelled`, or `Failed(Error)`). Multi-waiter safe, immediate once terminated. Only waits for the loop — never for or against background work. |
| `f.snapshot()` | Synchronous, read-only diagnostic counters (`Snapshot`); never raises, never suspends. |
| `Mailbox`, `StopReason`, `Snapshot`, `SendRefusal`, `AskFailure` | Configuration and result vocabulary, all owned by this library. |
| `Ctx { group, address }` | All a handler may touch: the host group and its own address. A handler cannot call `ask` at all (handlers are synchronous, `ask` is async); reporting uses `tell`. |
| `Supervisor(group~)` | Bind a supervisor to an existing host task group of any result type. No runtime is created; the host stays the structured-concurrency owner. Opt-in and fully separate from every Fuwaroid path. |
| `sup.spawn(f, label?)` | Spawn an owned task on the host group; `Result[SupervisedTask[X], SpawnRefusal]` — refusal is explicit once shutdown began, never a silent drop. |
| `handle.cancel()` | Cooperative cancellation REQUEST (`Running -> Cancelling`, idempotent, forward-only). Returning does not mean the task stopped. |
| `handle.wait()` | Result delivery delegated to `Task::wait`: the value, the ORIGINAL error, or `@async.WaitedTaskAlreadyCancelled` for a cancelled target. Multi-waiter and late-wait safe. |
| `handle.snapshot()` | Synchronous `{ id, label, status, error_text }` view. |
| `sup.cancel_and_wait(tasks~, timeout_ms~)` | Cancel the SELECTED tasks and wait for exactly them under ONE overall deadline: `Settled`, or `DeadlineExceeded(snapshots)` with the unresolved tasks in their true status. Unselected tasks and admission are untouched. |
| `sup.shutdown(timeout_ms~)` | Close admission forever, then cancel and bounded-settle everything live. Idempotent; a deadline exceeded can be retried and finally reports `Settled`. |
| `sup.snapshot()` | Synchronous `{ lifecycle, tasks }` view (`Open / Closing / Closed`). |
| `SupervisedTaskStatus`, `SettleOutcome`, `SupervisorLifecycle`, `SupervisedTaskSnapshot`, `SupervisorSnapshot`, `SpawnRefusal` | Supervisor vocabulary, all owned by this library. |

## Usage

```mbt nocheck
// A counter Fuwaroid with a query flow.
enum CounterCmd {
  Add(Int)
}

enum CounterQuery {
  GetCount
}

struct Count {
  mut n : Int
}

async test "counter" {
  @async.with_task_group(group => {
    let f = @fuwaroid.Fuwaroid::spawn(
      group~, // the host's long-lived task group — never a per-call one
      init=Count::{ n: 0 },
      on_cmd=(_, state, cmd) => {
        match cmd {
          CounterCmd::Add(k) => { state.n = state.n + k; state }
        }
      },
      on_query=(_, state, _query) => (state, state.n),
    )
    let _ = f.tell(CounterCmd::Add(2))
    // FIFO: the reply certifies Add(2) was already served.
    let count = f.ask(CounterQuery::GetCount, timeout_ms=1000) // Ok(2)
    debug_inspect(count, content="Ok(2)")
  })
}
```

## Background work

The async-work pattern (the reason this library exists). A refused report
is **kept as host-owned evidence** — the library does not guarantee
reliable delivery of background results, and a refusal must never be
retried into the same (possibly closed or full) mailbox:

```mbt nocheck
enum WorkCmd {
  StartWork
  Report(Int)
}

fn run_delegation() -> Int {
  42 // stand-in for IO, a subprocess, anything slow
}

async test "background work" {
  let refusals : Array[@fuwaroid.SendRefusal] = []
  @async.with_task_group(group => {
    let f = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=(ctx, state, cmd) => {
        match cmd {
          StartWork => {
            ctx.group.spawn_bg(() => {
              let outcome = run_delegation() // IO, subprocess, anything slow
              match ctx.address.tell(Report(outcome)) {
                Ok(_) => () // admitted; a later message folds it in
                // Refused: record it somewhere the host owns. The report is
                // lost — state changes only via messages that are admitted.
                Err(refusal) => refusals.push(refusal)
              }
            })
            state // unchanged here — the report arrives as a message
          }
          Report(n) => state + n
        }
      },
    )
    let _ = f.tell(StartWork)
    f.close()
    let _ = f.join() // Graceful — the drain completed
    debug_inspect(refusals, content="[MailboxClosed]")
  })
}
```

After `close`, background work still runs under the host group's lifetime;
its late `tell` observes `Err(MailboxClosed)` — exactly the refusal the
example collects. `join` neither waits for nor cancels such work.

## Lifecycle: close, cancel, join

```mbt nocheck
async test "close then join" {
  @async.with_task_group(group => {
    let f = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=(_, state, _cmd) => state + 1,
    )
    let _ = f.tell(1)
    f.close() // refuse new; drain what is left; the loop then exits
    let reason = f.join() // wait for the loop task and learn why it ended
    debug_inspect(reason, content="Graceful")
  })
}
```

- `close` is idempotent and returns before the drain completes; the loop
  drains the backlog in FIFO order, yielding between batches while doing so.
- `join` returns the recorded `StopReason`: `Graceful` after a
  closed-and-empty drain, `Cancelled` when the host cancelled the loop,
  and `Failed(original error)` after an ordinary terminal error. In
  `moonbitlang/async` 0.22.x cancellation is a runtime signal of its
  own, distinct from `Error` — Fuwaroid never wraps it into a fake
  error. The cancelled loop task ends cancelled (an external
  `Task::wait` observer sees `@async.WaitedTaskAlreadyCancelled`), and `join`
  reports the recorded `Cancelled`. Multiple waiters are fine, and a
  `join` after termination returns immediately. If the waiter itself is
  cancelled, that cancellation propagates unchanged, including when the
  loop has already terminated.
- Abnormal cleanup closes admission and enters `Closing`, then abandons
  queued messages in batches of 32 under cancellation protection. It yields
  between batches so other tasks can run; `join` completes only after all
  stranded reply queues are closed and the stop reason is recorded.
- **Cancellation outranks draining.** The loop is spawned `no_wait` on the
  host group. `close()` followed by an *immediate* return from the host
  scope cancels the still-running loop — the backlog is NOT fully
  processed and `join` (observed from another scope) reports
  `Cancelled`. To confirm a graceful drain, `close();
  join()` **before** leaving the scope.
- An `ask` issued just before `close` proves ordering by FIFO: its reply
  certifies every message enqueued before it was already served.

## Supervisor: bounded task settlement

Supervisor is introduced in Fuwaroid 0.2.0.

`Supervisor` is a separate, lightweight structured-concurrency utility
that lives next to Fuwaroid, not inside it:

> Fuwaroid serializes mutable state. Supervisor owns cancellable async
> work. Neither is an actor system.

It answers one concrete lifecycle need: *cancel these tasks, then bound
this settlement operation by one overall deadline, reporting any
stragglers in their real status instead of waiting on each selected task
indefinitely*. This is the seam a cleanup path needs when it would
otherwise end with an unbounded `cancel -> Task::wait` sequence inside a
cancellation-shielded scope.

```mbt nocheck
async test "supervisor settles a selected foreground set" {
  @async.with_task_group(group => {
    let sup = @fuwaroid.Supervisor(group~)
    let foreground = []
    for i in 0..<2 {
      match sup.spawn(() => i, label="fg\{i}") {
        Ok(handle) => foreground.push(handle)
        Err(_) => abort("open supervisor refuses nothing")
      }
    }
    // Background work is spawned the same way and simply never selected;
    // it keeps running while the foreground set settles.
    let outcome = sup.cancel_and_wait(
      tasks=foreground.clamped_view(),
      timeout_ms=5000,
    )
    debug_inspect(outcome, content="Settled")
    // Later: stop owning tasks altogether.
    debug_inspect(sup.shutdown(timeout_ms=5000), content="Settled")
  })
}
```

Semantics that matter:

- **Ownership without ceremony.** A supervisor is bound to an existing
  host `TaskGroup` and creates no runtime. Spawned tasks are real
  children of the host group: when the host scope ends, remaining tasks
  are cancelled by the group exactly like any other child. Ordinary
  failures keep the host group's own fail-fast behavior — the supervisor
  never swallows them.
- **`cancel` is a request, not a verdict.** It records `Cancelling`
  synchronously and idempotently, and never demotes a terminal status.
  The race outcome is honest: a worker that finishes before observing
  the request ends `Completed` (or `Failed`), not `Cancelled`.
- **`wait` is `Task::wait`.** Results, original error identity and
  `@async.WaitedTaskAlreadyCancelled` for a cancelled target come straight from the
  underlying task; handles stay waitable after the supervisor's live
  registry has reaped their task, with any number of waiters.
- **One deadline for the whole settlement operation.** `cancel_and_wait`
  computes `deadline = now + timeout_ms` once; every selected task shares
  it. Waiting for N tasks never costs N × timeout inside Supervisor. On
  expiry, `DeadlineExceeded` carries snapshots of exactly the
  still-nonterminal selected tasks in their TRUE status (`Running` /
  `Cancelling`) — it never fabricates a terminal status, never clears the
  live registry, and the stragglers may still terminate later.
- **The deadline works from shielded contexts.** Settlement runs in a
  local task group of waiter helpers plus one timer, so it never relies
  on "cancel the caller to stop the wait" — it is safe to call from
  cleanup code running under `@async.protect_from_cancel`. Cancelling
  the settlement caller tears down only the helpers; a waiter helper's
  cancellation never reaches its target.
- **Shutdown is admission control plus settlement.** The first
  `shutdown` closes admission synchronously and forever (`Open ->
  Closing -> Closed`; `spawn` refuses with `SupervisorClosed` from that
  moment), then cancels and bounded-settles exactly the tasks that were
  live at entry. Repeated `shutdown`s are safe; after a deadline
  exceeded they can wait again for the stragglers, and the registry
  converges by itself once those terminate.

**Structured-exit limitation.** Supervisor bounds its own settlement
operation; it does not detach tasks from the host `TaskGroup` and cannot
weaken that group's structured-exit rule. If unresolved supervised tasks
remain when the owning `with_task_group` body returns, the host group
will still wait for those children to terminate. A
`DeadlineExceeded` result therefore does **not** mean the owning task
group itself has a bounded exit time.

**Cooperative-runtime limitation — read this before relying on the
deadline.** MoonBit's async runtime is single-threaded and cooperative:
cancellation and timers are observed only at suspension points, and a
worker that never yields cannot be interrupted at all. Bounded
settlement is therefore not hard real-time preemption, and nothing can
force-kill a coroutine. What `Supervisor` guarantees: as long as the
scheduler still gets execution opportunities, waiting on a suspended or
cancellation-nonresponsive task does not block the lifecycle forever —
the deadline expires and the stragglers are reported as they are.

Not an actor framework: no actor registry, no supervision tree, no
restart, no linking, no remote addressing — and no application metadata
(session ids, receipts, business tags) inside the supervisor. A task is
`id` (unique and monotonic within the supervisor's lifetime), an
optional `label`, and a lifecycle status; that is the whole identity.

## Mailbox configuration

```mbt nocheck
async test "bounded mailbox" {
  @async.with_task_group(group => {
    let f = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=(_, state, _cmd) => state + 1,
      mailbox=@fuwaroid.Mailbox::Bounded(1),
    )
    @async.pause() // let the loop reach its mailbox park (its steady state)
    // A Bounded mailbox never waits for capacity: a send that finds it
    // full is refused synchronously with MailboxFull.
    let mut refused = 0
    for i in 0..<3 {
      match f.tell(i) {
        Ok(_) => ()
        Err(refusal) => {
          debug_inspect(refusal, content="MailboxFull")
          refused = refused + 1
        }
      }
    }
    if refused == 0 {
      fail("Bounded(1) never refused within the window")
    }
    // A refused send admitted nothing, so retrying it is side-effect free;
    // each pause lets the loop serve queued messages and recover capacity.
    let mut served = false
    for _ in 0..<100 {
      match f.ask((), timeout_ms=10000) {
        Ok(_) => { served = true; break }
        Err(NotDelivered(MailboxFull)) => @async.pause()
        Err(other) => fail("unexpected ask failure: \{Repr(other)}")
      }
    }
    if !served {
      fail("the barrier was never admitted")
    }
    // The barrier's reply certifies every admitted message was served.
    let snap = f.snapshot()
    if snap.accepted != snap.processed || snap.abandoned != 0L {
      fail("conservation violated after the barrier")
    }
  })
}
```

Two measured facts matter for capacity planning:

- **The effective admission window is `capacity + 1`.** While the loop is
  parked in `queue.get()` — its steady state while idle — the first send
  is handed to that parked reader point-to-point, bypassing the buffer.
  One uninterrupted producer turn can therefore admit up to
  `capacity + 1` messages in flight (1 handoff slot + `capacity`
  buffered); the next send is the one refused with `MailboxFull`.
- **`Bounded(n)` requires a positive integer `n`.** `Bounded(0)` and
  negative capacities are a configuration error: `spawn` aborts
  synchronously with a fail-fast message — capacities are never clamped,
  remapped, or silently treated as rendezvous.

`Mailbox::Unbounded` (the default) admits every message and grows without
bound; budget your producers (see the queue-growth measurements in
`docs/concurrency-benchmarks.md` §2.4).

## Fair scheduling

The loop yields the scheduler after every 64 complete messages (the
default `fuwaroid_yield_batch`, adjudicated from measured data — see
`docs/concurrency-benchmarks.md`). What the measurements show:

- With 16 hot self-chaining instances, a cold instance's observed latency
  stays in the hundreds of microseconds at batch 32 (P50 212µs, max
  276µs), versus 52µs/77µs at batch 1 and 737µs/1,484µs at batch 128.
- Batch 32 delivers ~24x the drain throughput of batch 1 (1.80M vs 74.5k
  msg/s end-to-end) while loaded `ask` P99 stays at 51µs (batch 1: 3,611µs).

These are measurements of this library on one machine, not promises: the
scheduler is cooperative, there is **no preemption**, and a single long
handler blocks the runtime for its whole duration (§2.5 of the benchmark
report). If you need hard latency bounds, keep handlers short and bounded.

## Diagnostics

`f.snapshot()` is synchronous and read-only; every field is a copied value
(no mutable internal state, no `State`/`Cmd`/`Query`/`Reply` leak). All
counters are `Int64` and only ever increase.

| Field | Meaning |
|---|---|
| `lifecycle` | `Running` → `Closing` (a `close` was requested) → `Stopped(StopReason)`; forward-only, never moves back. |
| `accepted` | Admissions ACCEPTED by the mailbox — commands and queries alike. |
| `processed` | Messages whose handler RETURNED and whose state was committed. Not background-work completion. |
| `rejected_full` / `rejected_closed` | Admissions refused because the bounded mailbox was full / the mailbox was closed. |
| `abandoned` | Messages accepted but never served when the loop stopped abnormally; 0 after a graceful drain. |
| `outstanding` | `accepted − processed − abandoned` at snapshot time: requests in flight. NOT the underlying buffer length. |

Once the loop has terminated the conservation law closes:
`accepted == processed + abandoned + outstanding` holds for every observed
snapshot.

## Migrating from the pre-R1 API

The R1 API narrowing is breaking. R1 is not yet released (`moon.mod` still
says `0.1.0`); there is no compatibility layer.

| Pre-R1 | Now |
|---|---|
| `mailbox? : @aqueue.Kind` | `mailbox? : Mailbox` — this library's own vocabulary. `@aqueue.Kind` is no longer accepted in any public signature (compile error). |
| `@aqueue.Blocking(n)` | `Mailbox::Bounded(n)`. `n` must be positive; `Bounded(0)` / negative values abort at spawn. |
| `@aqueue.DiscardOldest(n)` / `DiscardLatest(n)` | **No equivalent and no wrapper.** There is no drop-style mailbox: choose refusal semantics explicitly (`Bounded` refuses; handle `SendRefusal::MailboxFull`). |
| fold handle `Fuwaroid[Cmd, Cmd, Unit]`, barrier `ask(cmd)` | Handle is `Fuwaroid[Cmd, Unit, Unit]`; barrier is `tell(cmd); ask(())`. The query type is `Unit`, so passing a command as the barrier argument is a compile error. |
| no termination wait | `join()` returns `StopReason`; `close(); join()` confirms the drain. |
| no diagnostics | `snapshot()` returns `Snapshot` counters. |

## Migrating to 0.2.0 (`moonbitlang/async` 0.22.x)

0.2.0 tracks `moonbitlang/async` 0.22.x, whose cancellation is a compiler
cancellation signal — propagated like an error but NOT capturable by
`catch` and NOT an `Error` value. `StopReason` therefore changes shape
(breaking, no compatibility layer):

| 0.1.x | 0.2.0 |
|---|---|
| `StopReason::Stopped(error)` — one abnormal constructor; host cancellation surfaced as a stored cancellation `Error` | `StopReason::Cancelled` for a host-cancelled loop (the cancelled loop task ends cancelled; `Task::wait` observers see `@async.WaitedTaskAlreadyCancelled`), and `StopReason::Failed(original error)` for ordinary terminal errors |
| `join` on a cancelled loop returned `Stopped(cancel error)` | `join` returns `Cancelled`; `TaskCancelled` itself is never stored or returned as the reason |

## Purity

Error classification (`classify_ask_error`) and capacity validation
(`mailbox_capacity_error`) are pure and table-tested. The lifecycle itself
performs queue close/drain operations and preserves the terminal
disposition: only `QueueAlreadyClosed` is a graceful mailbox stop; a
foreign queue error keeps its original identity as
`StopReason::Failed(original)`, while the host's cancellation — a runtime
signal distinct from `Error` in async 0.22.x — is recorded as
`StopReason::Cancelled`, never disguised as an error. Callback
serialization does not make generic state ownership or callback purity a
type-enforced property.

## License

Apache-2.0
