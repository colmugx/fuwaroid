# colmugx/fuwaroid

Lightweight single-writer concurrency over
[`moonbitlang/async`](https://mooncakes.io/docs/moonbitlang/async): private
state, a typed mailbox, and one serial loop — nothing else.

## Why "Fuwaroid"

**ふわり (fuwari)** is the Japanese mimetic word for the way something
weightless moves and settles — a feather alighting, no impact, no weight.
**-oid** makes it a creature of that quality (as in *android*, *humanoid*).
A **Fuwaroid** is thus "a thing that floats lightly".

That is precisely this library's concurrency. The runtime underneath is
single-threaded and cooperative: tasks never grip OS threads and never
block — they float suspended and drift between suspension points. A
Fuwaroid is one such light entity: a little state and a mailbox. It sleeps
weightlessly until a message arrives (`queue.get()` suspends), settles the
message in **one synchronous step**, and drifts back to sleep. No locks,
no blocking, no machinery — and the library itself is featherweight: a
few hundred lines over one dependency.

## The model

```text
            tell(cmd) ───────────────┐   ask(query) ───────────┐
   callers                             │  (with timeout)         │
                                       ▼                         ▼
                             ┌──────────────────────────────────────┐
   host task group ─────────►│  mailbox (typed, FIFO, backpressure) │
   (owns the lifetime)       └──────────────────┬───────────────────┘
                                                │ one serial loop
                                                ▼
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
   returned state in one serial step. The generic API cannot prove that
   `State` has no aliases or that a callback is pure; callers must keep actor
   state owned by the loop and avoid mutating it from spawned work or replies.
2. **Async work lives outside the Fuwaroid.** Spawn it on the host group
   from a handler; report the outcome back with `tell`. The loop is the only
   place that applies a handler's returned state. State ownership and alias
   discipline remain the caller's responsibility.
3. **No restarts.** Handlers cannot raise (their types say so); failure
   is data. Ledgers of in-flight work are never reset by a supervisor.
4. **`close` drains the mailbox.** Messages already queued (commands and
   queries alike) are processed in FIFO order before the loop exits; new
   messages are refused. `close` does not cancel or await work already
   spawned on the host group.
5. **Stranded asks fail fast.** If the loop stops abnormally (host
   cancellation), waiting askers get `Stopped` immediately instead of
   hanging until their timeout. Cancellation itself propagates.
6. **Backpressure is explicit.** A `Blocking(n)` mailbox reports
   `MailboxFull` on overflow; nothing is silently dropped.

These semantics are also the forward story for MoonBit's eventual
multithreaded runtime: serial processing plus message-only communication
is exactly the shape that survives real threads unchanged.

## API

| Symbol | Meaning |
|---|---|
| `Fuwaroid::spawn(group~, init~, on_cmd~, on_query~, mailbox?)` | Start a Fuwaroid on the host's long-lived task group. |
| `Fuwaroid::spawn_fold(group~, init~, on_cmd~, mailbox?)` | Command-only variant; `ask` becomes a processed-receipt. |
| `f.tell(cmd)` | Fire-and-forget; `Result[Unit, SendRefusal]` right after the mailbox decision. |
| `f.ask(query, timeout_ms~)` | Request-response; `TimedOut` / `NotDelivered` / `Stopped` failures; caller cancellation propagates. |
| `f.close()` | Graceful mailbox stop: refuse new, drain queued, exit the loop; host-group work has its own lifetime. |
| `Ctx { group, address }` | All a handler may touch: the host group and its own address (self-`ask` from a handler deadlocks until timeout — use `tell`). |

## Usage

```mbt nocheck
// A counter Fuwaroid with async work kicked off from a sync handler.
enum CounterCmd {
  Add(Int)
}

enum CounterQuery {
  GetCount
}

struct Count { mut n : Int }

let f = @fuwaroid.Fuwaroid::spawn(
  group~, // the host's long-lived task group — never a per-call one
  init={ n: 0 },
  on_cmd=(_, state, cmd) => {
    match cmd {
      Add(k) => { state.n = state.n + k; state }
    }
  },
  on_query=(_, state, _query) => (state, state.n),
)
let _ = f.tell(CounterCmd::Add(2))
let count = f.ask(CounterQuery::GetCount, timeout_ms=1000) // Ok(2)
```

The async-work pattern (the reason this library exists):

```mbt nocheck
on_cmd=(ctx, state, cmd) => {
  match cmd {
    StartHeavyWork => {
      ctx.group.spawn_bg(() => {
        let outcome = run_delegation() // IO, subprocess, anything slow
        let _ = ctx.address.tell(Report(outcome)) // state changes only via tell
      })
      state // unchanged here — the report arrives as a message
    }
    Report(outcome) => record(state, outcome)
  }
}
```

Ordering proof at shutdown: an `ask` issued just before `close` waits for
its reply on the spot (async calls wait implicitly), and FIFO means the
reply certifies that every message enqueued before it was already served.
The loop's own termination is guaranteed by structured concurrency —
`with_task_group` returns only after every task, the Fuwaroid loop included,
has ended. Work spawned by a handler remains host-owned and is awaited by the
host group independently of `close`:

```mbt nocheck
let count = f.ask(CounterQuery::GetCount, timeout_ms=1000) // Ok(n) — everything earlier is served
f.close() // refuse new; drain what is left; the loop then exits
```

If that work reports back after `close`, its `tell` receives
`Err(MailboxClosed)`; the host decides whether and how to record that
refusal.

## Mailbox kinds

`mailbox? : @aqueue.Kind` accepts `Unbounded` (default), `Blocking(n)`
(tell reports `MailboxFull` when full), `DiscardOldest(n)`,
`DiscardLatest(n)`. Backpressure policy is the composer's choice.

## Purity

Error classification is pure and table-tested. The lifecycle itself performs
queue close/drain operations and preserves the original stop error: only
`QueueAlreadyClosed` is a graceful mailbox stop, while cancellation and
foreign queue errors propagate. Callback serialization does not make generic
state ownership or callback purity a type-enforced property.

## License

Apache-2.0
