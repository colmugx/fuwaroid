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
   `fuwaroid_yield_batch` (32) complete messages the loop yields the
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
| `f.join()` | Wait for the loop task to terminate; returns `StopReason` (`Graceful` or `Stopped(Error)`). Multi-waiter safe, immediate once terminated. Only waits for the loop — never for or against background work. |
| `f.snapshot()` | Synchronous, read-only diagnostic counters (`Snapshot`); never raises, never suspends. |
| `Mailbox`, `StopReason`, `Snapshot`, `SendRefusal`, `AskFailure` | Configuration and result vocabulary, all owned by this library. |
| `Ctx { group, address }` | All a handler may touch: the host group and its own address. A handler cannot call `ask` at all (handlers are synchronous, `ask` is async); reporting uses `tell`. |

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
  closed-and-empty drain, `Stopped(original error)` after an abnormal stop
  (host cancellation keeps its original error identity). Multiple waiters
  are fine, and a `join` after termination returns immediately. If the
  waiter itself is cancelled, that cancellation propagates unchanged.
- **Cancellation outranks draining.** The loop is spawned `no_wait` on the
  host group. `close()` followed by an *immediate* return from the host
  scope cancels the still-running loop — the backlog is NOT fully
  processed and `join` (observed from another scope) reports
  `Stopped(cancellation error)`. To confirm a graceful drain, `close();
  join()` **before** leaving the scope.
- An `ask` issued just before `close` proves ordering by FIFO: its reply
  certifies every message enqueued before it was already served.

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

The loop yields the scheduler after every 32 complete messages (the
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

## Purity

Error classification (`classify_ask_error`) and capacity validation
(`mailbox_capacity_error`) are pure and table-tested. The lifecycle itself
performs queue close/drain operations and preserves the original stop
error: only `QueueAlreadyClosed` is a graceful mailbox stop, while
cancellation and foreign queue errors propagate as the recorded
`StopReason::Stopped` cause. Callback serialization does not make generic
state ownership or callback purity a type-enforced property.

## License

Apache-2.0
