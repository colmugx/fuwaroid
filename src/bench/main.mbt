///|
/// src/bench/main.mbt — R1 benchmark driver (plan W4, benchmark-agent).
///
/// Public-API-driven load program. It never touches library internals:
/// every workload drives `Fuwaroid::spawn` / `spawn_fold` / `tell` / `ask` /
/// `close` / `join` / `snapshot` exactly like an external consumer would.
///
/// Invocation (parameters reach the program through `@env.args()`):
///
///
/// Workloads: `selftest`, `smoke`, `tell`, `ask`, `hotcold`, `producers`,
/// `handler`. Orchestration (scenario table, batch-variant isolated copies,
/// deadlines, raw-output files) lives in `tools/concurrency-bench.mbtx`.
///
/// Measurement discipline (plan section "性能基准设计"):
/// - ALL durations use `@bench.monotonic_clock_start/end` (CLOCK_MONOTONIC;
///   `monotonic_clock_end` returns MICROSECONDS). `@async.now()` is wall
///   clock and is never used here.
/// - Every mailbox gets an explicit message budget; an Unbounded mailbox is
///   never written without bound.
/// - Latency percentiles are computed over per-request samples (nearest
///   rank), never from aggregated means. Timeouts and refusals are always
///   reported separately and never folded into the success statistics.
/// - Warmup rounds run before measured rounds; both are reported.
/// - Peak memory is not measured: every report prints `RSS: not measured`.
///   `outstanding` is a count of requests in flight and is never presented
///   as memory.
/// - The batch scheduling constant is private to the library; the harness
///   passes `note_batch=<n>` purely as an annotation of which isolated-copy
///   variant produced this run. The program never reads the constant.
///
/// Exit code: non-zero on any conservation violation (its own bookkeeping
/// or the diagnostic snapshot counts) — a benchmark that loses messages
/// measures nothing. In measurement runs a slow request (timeout) or a
/// bounded-mailbox refusal is a RESULT, not an error; in `smoke`/`selftest`
/// they fail the run (small scale must be clean).

///|
/// Unit scale of `@bench.monotonic_clock_end` (microseconds).
let us_per_ms : Double = 1000.0

///|
let us_per_s : Double = 1000000.0

///|
/// One workload run's parameters (parsed from `key=value` arguments).
priv struct Params {
  mut mode : String
  mut messages : Int
  mut warmup : Int
  mut samples : Int
  mut hot : Int
  mut chain : Int
  mut producers : Int
  mut per_producer : Int
  mut burst : Int
  mut capacity : String
  mut burn : Int
  mut timeout_ms : Int
  mut producer_per_sample : Int
  mut sample_interval_ms : Int
  mut note_batch : String
  mut note : String
  /// True for `selftest`/`smoke`: ask timeouts/refusals and any conservation
  /// deviation fail the run instead of being reported as results.
  mut strict : Bool
}

///|
fn Params::default() -> Params {
  {
    mode: "idle",
    messages: 200000,
    warmup: 10000,
    samples: 2000,
    hot: 1,
    chain: 100000,
    producers: 1,
    per_producer: 5000,
    burst: 32,
    capacity: "unbounded",
    burn: 0,
    timeout_ms: 60000,
    producer_per_sample: 20,
    sample_interval_ms: 1,
    note_batch: "unknown",
    note: "",
    strict: false,
  }
}

///|
/// Parse a non-negative decimal integer, or None. Local helper so the bench
/// package needs no extra core imports.
fn parse_int_bench(s : String) -> Int? {
  let mut value = 0
  let mut digits = 0
  for ch in s {
    guard ch is ('0'..='9') else { return None }
    value = value * 10 + (ch.to_int() - '0'.to_int())
    digits = digits + 1
  }
  if digits == 0 {
    None
  } else {
    Some(value)
  }
}

///|
/// Apply one `key=value` argument. Returns false for an unknown key or a
/// malformed/unparseable value so `main` can fail visibly.
fn apply_param(p : Params, kv : String) -> Bool {
  let parts : Array[String] = []
  for token in kv.split("=") {
    parts.push(token.to_owned())
  }
  if parts.length() != 2 {
    return false
  }
  let key = parts[0]
  let value = parts[1]
  match key {
    "mode" => {
      p.mode = value
      true
    }
    "messages" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.messages = n
          true
        }
        None => false
      }
    "warmup" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.warmup = n
          true
        }
        None => false
      }
    "samples" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.samples = n
          true
        }
        None => false
      }
    "hot" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.hot = n
          true
        }
        None => false
      }
    "chain" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.chain = n
          true
        }
        None => false
      }
    "producers" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.producers = n
          true
        }
        None => false
      }
    "per_producer" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.per_producer = n
          true
        }
        None => false
      }
    "burst" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.burst = n
          true
        }
        None => false
      }
    "capacity" => {
      p.capacity = value
      true
    }
    "burn" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.burn = n
          true
        }
        None => false
      }
    "timeout_ms" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.timeout_ms = n
          true
        }
        None => false
      }
    "producer_per_sample" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.producer_per_sample = n
          true
        }
        None => false
      }
    "sample_interval_ms" =>
      match parse_int_bench(value) {
        Some(n) => {
          p.sample_interval_ms = n
          true
        }
        None => false
      }
    "note_batch" => {
      p.note_batch = value
      true
    }
    "note" => {
      p.note = value
      true
    }
    _ => false
  }
}

///|
/// Format a microsecond count as an integer string.
fn fmt0(us : Double) -> String {
  (us + 0.5).to_int().to_string()
}

///|
/// Format a millisecond value (given in microseconds) with one decimal.
fn fmt1_ms(us : Double) -> String {
  let tenths = (us / us_per_ms * 10.0 + 0.5).to_int()
  "\{tenths / 10}.\{tenths % 10}"
}

///|
/// Nearest-rank percentile of an ALREADY SORTED sample array (microseconds).
fn percentile(sorted : Array[Double], p : Double) -> Double? {
  let n = sorted.length()
  if n == 0 {
    return None
  }
  let mut idx = (p / 100.0 * n.to_double()).ceil().to_int() - 1
  if idx < 0 {
    idx = 0
  }
  if idx >= n {
    idx = n - 1
  }
  Some(sorted[idx])
}

///|
fn percentile_text(sorted : Array[Double], p : Double) -> String {
  match percentile(sorted, p) {
    Some(value) => fmt0(value)
    None => "n/a"
  }
}

///|
/// Line with P50/P95/P99/max over a sorted microsecond sample array.
fn latency_lines(label : String, sorted : Array[Double]) -> Unit {
  println("latency_us_\{label}_p50: \{percentile_text(sorted, 50.0)}")
  println("latency_us_\{label}_p95: \{percentile_text(sorted, 95.0)}")
  println("latency_us_\{label}_p99: \{percentile_text(sorted, 99.0)}")
  if sorted.length() > 0 {
    println("latency_us_\{label}_max: \{fmt0(sorted[sorted.length() - 1])}")
  } else {
    println("latency_us_\{label}_max: n/a")
  }
}

///|
/// Header every report starts with: measurement method and the parameters
/// that produced the numbers (plan: 参数/时钟/预热写进输出).
fn print_common_header(p : Params) -> Unit {
  println(
    "clock: @bench.monotonic_clock_start/end (CLOCK_MONOTONIC, microseconds; core/bench imported as @clock)",
  )
  println(
    "build: native, build mode recorded by invoking harness; public API only",
  )
  println("note_batch(build constant annotation): \{p.note_batch}")
  if p.note != "" {
    println("note: \{p.note}")
  }
  println("RSS: not measured")
}

///|
/// One-line rendering of a snapshot (diagnostics may be disabled in the
/// diag-off isolated-copy variant; the zero view then is the evidence).
fn snapshot_line(tag : String, s : @fuwaroid.Snapshot) -> Unit {
  println(
    "snapshot[\{tag}]: lifecycle=\{Repr(s.lifecycle)} accepted=\{s.accepted} processed=\{s.processed} rejected_full=\{s.rejected_full} rejected_closed=\{s.rejected_closed} abandoned=\{s.abandoned} outstanding=\{s.outstanding}",
  )
}

///|
/// Conservation verdict shared by every workload.
///
/// - Own bookkeeping must always hold (it is what the driver counted).
/// - The diagnostic snapshot must satisfy
///   `accepted == processed + abandoned` and match the driver's counts —
///   unless the snapshot is entirely zeroed while the driver did admit
///   messages, which is exactly how the diag-off variant (counting points
///   stripped in an isolated copy) presents itself; then the snapshot part
///   is skipped and the fact is reported.
fn check_conservation(
  tag : String,
  snap : @fuwaroid.Snapshot,
  own_accepted : Int64,
  own_processed_min : Int64,
) -> Unit raise {
  let counting_enabled = snap.accepted != 0L ||
    snap.processed != 0L ||
    snap.rejected_full != 0L ||
    snap.rejected_closed != 0L ||
    snap.abandoned != 0L
  if !counting_enabled {
    if own_accepted > 0L {
      println(
        "conservation[\{tag}]: SKIPPED — snapshot counters are all zero while the driver admitted \{own_accepted} messages (diagnostics counting disabled: diag-off variant); driver-side counts are the only evidence in this build",
      )
    }
    return
  }
  let conserved = snap.accepted ==
    snap.processed + snap.abandoned + snap.outstanding
  let matches_own = snap.accepted >= own_accepted &&
    snap.processed >= own_processed_min
  if !conserved || !matches_own {
    fail(
      "conservation violation[\{tag}]: accepted=\{snap.accepted} processed=\{snap.processed} abandoned=\{snap.abandoned} outstanding=\{snap.outstanding} own_accepted=\{own_accepted} own_processed_min=\{own_processed_min}",
    )
  }
  println(
    "conservation[\{tag}]: OK (accepted == processed + abandoned + outstanding; accepted=\{snap.accepted})",
  )
}

///|
/// Aggregate counters shared by the ask-based workloads.
priv struct AskStats {
  samples : Array[Double]
  mut timeouts : Int
  mut refusals : Int
  mut stopped : Int
}

///|
fn AskStats::new() -> AskStats {
  { samples: [], timeouts: 0, refusals: 0, stopped: 0, }
}

///|
/// Record one timed ask outcome. A failure is recorded — never folded into
/// the latency samples (plan: 超时/拒绝单独报告).
fn AskStats::record(
  self : AskStats,
  r : Result[Int, @fuwaroid.AskFailure],
  us : Double,
  p : Params,
) -> Unit raise {
  match r {
    Ok(_) => self.samples.push(us)
    Err(failure) =>
      match failure {
        @fuwaroid.AskFailure::TimedOut => self.timeouts += 1
        @fuwaroid.AskFailure::NotDelivered(_) => self.refusals += 1
        @fuwaroid.AskFailure::Stopped => self.stopped += 1
      }
  }
  if p.strict && self.timeouts + self.refusals + self.stopped > 0 {
    fail(
      "strict mode: ask failure observed (timeout/refusal/stopped) — small-scale runs must be clean",
    )
  }
}

///|
fn AskStats::report(self : AskStats, label : String) -> Unit {
  self.samples.sort()
  println(
    "samples_\{label}: \{self.samples.length()} successful / \{self.timeouts} timed out / \{self.refusals} refused / \{self.stopped} stopped",
  )
  latency_lines(label, self.samples)
}

///|
/// The short handler used across workloads: one integer increment. Pure
/// arithmetic, no IO, no suspension inside the handler.
fn short_handler(
  _ctx : @fuwaroid.Ctx[Int, Unit, Unit],
  state : Int,
  _cmd : Int,
) -> Int {
  state + 1
}

///|
/// Command handler for instances that also serve queries (Reply = Int): the
/// command counter still advances by one per message.
fn query_actor_cmd(
  _ctx : @fuwaroid.Ctx[Int, Unit, Int],
  state : Int,
  _cmd : Int,
) -> Int {
  state + 1
}

///|
/// Query handler for the cold/query-serving instance: replies with the
/// counter value.
fn counting_query(
  _ctx : @fuwaroid.Ctx[Int, Unit, Int],
  state : Int,
  _query : Unit,
) -> (Int, Int) {
  (state, state)
}

///|
/// Self-chaining hot handler: re-tells itself while more budget remains, so
/// the instance keeps its own mailbox non-empty (plan workload "热实例自发链").
/// The chain lives entirely inside synchronous handlers.
fn hot_chain_handler(
  ctx : @fuwaroid.Ctx[HotCmd, Unit, Unit],
  state : Int,
  _cmd : HotCmd,
) -> Int {
  if state > 1 {
    let _ = ctx.address.tell(HotCmd::Chain)
  }
  state - 1
}

///|
enum HotCmd {
  Chain
} derive(Eq)

///|
/// Tiny end-to-end self-check on the REAL counting path: a small tell budget
/// with a FIFO barrier, then full conservation. Used by the harness to prove
/// each isolated-copy variant actually runs and does not lose messages.
async fn run_selftest(p : Params) -> Unit {
  println("=== workload: selftest ===")
  print_common_header(p)
  @async.with_task_group(group => {
    let fu = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=short_handler,
    )
    let mut own_ok = 0
    for i in 0..<p.messages {
      match fu.tell(i) {
        Ok(_) => own_ok += 1
        Err(refusal) => fail("selftest: tell refused: \{Repr(refusal)}")
      }
    }
    let barrier = fu.ask((), timeout_ms=p.timeout_ms)
    guard barrier is Ok(_) else { fail("selftest: barrier did not complete") }
    let snap = fu.snapshot()
    fu.close()
    let reason = fu.join()
    snapshot_line("selftest", snap)
    println("lifecycle_after_join: \{Repr(reason)}")
    check_conservation(
      "selftest",
      snap,
      (own_ok + 1).to_int64(),
      (own_ok + 1).to_int64(),
    )
  })
  println("selftest: OK")
}

///|
/// Count-conservation smoke over every workload shape with small budgets.
/// Strict mode: any ask timeout/refusal fails the run. This is the
/// "计数正确性自检" the W4 plan requires before any measurement.
async fn run_smoke(p : Params) -> Unit {
  println(
    "=== workload: smoke (count-conservation self-check, all workloads, small budgets) ===",
  )
  // 1. tell-throughput shape.
  {
    let small = Params::default()
    small.messages = 2000
    small.warmup = 200
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/tell"
    run_tell_body(small)
  }
  // 2. ask round-trip, idle and loaded.
  {
    let small = Params::default()
    small.mode = "idle"
    small.samples = 100
    small.warmup = 50
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/ask-idle"
    run_ask_body(small)
  }
  {
    let small = Params::default()
    small.mode = "loaded"
    small.samples = 100
    small.warmup = 50
    small.producer_per_sample = 20
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/ask-loaded"
    run_ask_body(small)
  }
  // 3. hot-cold with two hot chains.
  {
    let small = Params::default()
    small.hot = 2
    small.chain = 1000
    small.samples = 100
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/hotcold"
    run_hotcold_body(small)
  }
  // 4. producers against a small bounded mailbox (exercises refusals).
  {
    let small = Params::default()
    small.producers = 2
    small.per_producer = 500
    small.burst = 8
    small.capacity = "4"
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/producers-bounded"
    run_producers_body(small)
  }
  // 5. producers against Unbounded (conservation with a big queue).
  {
    let small = Params::default()
    small.producers = 2
    small.per_producer = 500
    small.burst = 8
    small.capacity = "unbounded"
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/producers-unbounded"
    run_producers_body(small)
  }
  // 6. handler cost with a burn.
  {
    let small = Params::default()
    small.burn = 50
    small.messages = 1000
    small.warmup = 100
    small.timeout_ms = 30000
    small.strict = true
    small.note_batch = p.note_batch
    small.note = "smoke/handler"
    run_handler_body(small)
  }
  println("smoke: OK (all workloads small-budget clean, conservation verified)")
}

///|
/// tell-throughput: one instance, short handler, explicit message budget.
/// The driver enqueues the whole budget synchronously (admission phase),
/// then a FIFO barrier `ask(())` proves every earlier message was served
/// (end-to-end phase includes the drain).
async fn run_tell(p : Params) -> Unit {
  println("=== workload: tell-throughput ===")
  print_common_header(p)
  run_tell_body(p)
}

///|
async fn run_tell_body(p : Params) -> Unit {
  @async.with_task_group(group => {
    let fu = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=short_handler,
    )
    // Warmup round: own budget + FIFO barrier.
    let mut warm_ok = 0
    for i in 0..<p.warmup {
      match fu.tell(i) {
        Ok(_) => warm_ok += 1
        Err(_) => ()
      }
    }
    let warm_barrier = fu.ask((), timeout_ms=p.timeout_ms)
    guard warm_barrier is Ok(_) else {
      fail("warmup barrier did not complete within \{p.timeout_ms}ms")
    }
    // Measured round.
    let mut own_ok = 0
    let t0 = @clock.monotonic_clock_start()
    for i in 0..<p.messages {
      match fu.tell(i) {
        Ok(_) => own_ok += 1
        Err(_) => ()
      }
    }
    let send_us = @clock.monotonic_clock_end(t0)
    let barrier = fu.ask((), timeout_ms=p.timeout_ms)
    let total_us = @clock.monotonic_clock_end(t0)
    guard barrier is Ok(_) else {
      fail(
        "drain barrier did not complete within \{p.timeout_ms}ms (budget \{p.messages})",
      )
    }
    let snap = fu.snapshot()
    fu.close()
    let reason = fu.join()
    println("messages_budget: \{p.messages}")
    println("warmup: \{p.warmup} messages + 1 barrier ask")
    println("accepted_by_driver: \{own_ok}")
    println("send_phase_ms: \{fmt1_ms(send_us)}")
    println("total_incl_drain_ms: \{fmt1_ms(total_us)}")
    let send_s = if send_us > 0.0 { send_us / us_per_s } else { 1.0 }
    let total_s = if total_us > 0.0 { total_us / us_per_s } else { 1.0 }
    println(
      "throughput_admission_msg_per_s: \{fmt0(own_ok.to_double() / send_s)}",
    )
    println(
      "throughput_end_to_end_msg_per_s: \{fmt0(own_ok.to_double() / total_s)}",
    )
    snapshot_line("final", snap)
    println("lifecycle_after_join: \{Repr(reason)}")
    check_conservation(
      "tell",
      snap,
      (own_ok + warm_ok + 1 + 1).to_int64(),
      (own_ok + warm_ok + 1 + 1).to_int64(),
    )
  })
}

///|
/// ask-roundtrip: per-request timed `ask` samples; mode `idle` measures a
/// quiet instance, mode `loaded` adds one background producer that tells
/// with an explicit budget (one message per scheduler yield) for the whole
/// sampling window. Timeouts/refusals are reported, never averaged in.
async fn run_ask(p : Params) -> Unit {
  println("=== workload: ask-roundtrip (mode=\{p.mode}) ===")
  print_common_header(p)
  run_ask_body(p)
}

///|
async fn run_ask_body(p : Params) -> Unit {
  if p.mode != "idle" && p.mode != "loaded" {
    fail("ask: unknown mode \{p.mode} (idle|loaded)")
  }
  @async.with_task_group(group => {
    let fu = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=query_actor_cmd,
      on_query=counting_query,
    )
    // Warmup.
    for _i in 0..<p.warmup {
      let _ = fu.ask((), timeout_ms=p.timeout_ms)
    }
    let stats = AskStats::new()
    let mut producer_accepted = 0
    let mut producer_refused = 0
    let t0 = @clock.monotonic_clock_start()
    if p.mode == "loaded" {
      let budget = p.samples * p.producer_per_sample
      let producer = group.spawn(() => {
        let mut acc = 0
        let mut rej = 0
        for i in 0..<budget {
          match fu.tell(i) {
            Ok(_) => acc += 1
            Err(_) => rej += 1
          }
          // One message per scheduler turn: a steady load, not a burst.
          @async.pause()
        }
        (acc, rej)
      })
      for _i in 0..<p.samples {
        let ts = @clock.monotonic_clock_start()
        let r = fu.ask((), timeout_ms=p.timeout_ms)
        let us = @clock.monotonic_clock_end(ts)
        stats.record(r, us, p)
      }
      let pair = producer.wait()
      producer_accepted = pair.0
      producer_refused = pair.1
    } else {
      for _i in 0..<p.samples {
        let ts = @clock.monotonic_clock_start()
        let r = fu.ask((), timeout_ms=p.timeout_ms)
        let us = @clock.monotonic_clock_end(ts)
        stats.record(r, us, p)
      }
    }
    let window_us = @clock.monotonic_clock_end(t0)
    let snap = fu.snapshot()
    fu.close()
    let reason = fu.join()
    println("mode: \{p.mode}")
    println("requested_samples: \{p.samples}")
    println("timeout_ms_per_request: \{p.timeout_ms}")
    println("warmup: \{p.warmup} asks")
    println("sampling_window_ms: \{fmt1_ms(window_us)}")
    if p.mode == "loaded" {
      println("producer_budget: \{p.samples * p.producer_per_sample}")
      println("producer_tells_accepted: \{producer_accepted}")
      println("producer_tells_refused: \{producer_refused}")
    }
    stats.report("ask")
    snapshot_line("final", snap)
    println("lifecycle_after_join: \{Repr(reason)}")
    check_conservation("ask", snap, snap.accepted, 0L)
  })
}

///|
/// hot-cold: `hot` self-chaining instances under load + one cold instance
/// answering timed queries. Cold percentiles and the maximum observed stall
/// are the fairness evidence; each hot chain's processed count proves the
/// chain really ran to its budget.
async fn run_hotcold(p : Params) -> Unit {
  println("=== workload: hot-cold (hot=\{p.hot} chain=\{p.chain}) ===")
  print_common_header(p)
  run_hotcold_body(p)
}

///|
async fn run_hotcold_body(p : Params) -> Unit {
  @async.with_task_group(group => {
    let hots : Array[@fuwaroid.Fuwaroid[HotCmd, Unit, Unit]] = []
    for _i in 0..<p.hot {
      let h = @fuwaroid.Fuwaroid::spawn_fold(
        group~,
        init=p.chain,
        on_cmd=hot_chain_handler,
      )
      hots.push(h)
    }
    let cold = @fuwaroid.Fuwaroid::spawn(
      group~,
      init=0,
      on_cmd=query_actor_cmd,
      on_query=counting_query,
    )
    // Warmup the cold instance before the chains start.
    let w = cold.ask((), timeout_ms=p.timeout_ms)
    guard w is Ok(_) else { fail("cold warmup ask failed") }
    // Start the chains: one trigger message per hot instance.
    for h in hots {
      let _ = h.tell(HotCmd::Chain)
    }
    // Timed cold queries while the hot chains are running.
    let stats = AskStats::new()
    let t0 = @clock.monotonic_clock_start()
    for _i in 0..<p.samples {
      let ts = @clock.monotonic_clock_start()
      let r = cold.ask((), timeout_ms=p.timeout_ms)
      let us = @clock.monotonic_clock_end(ts)
      stats.record(r, us, p)
    }
    let window_us = @clock.monotonic_clock_end(t0)
    // Let the chains run their full budget before closing: a `close` also
    // refuses a chain's own re-tell, which would truncate it. Poll with a
    // 1ms sleep between snapshots; the wait itself is bounded and reported.
    let expected : Int64 = p.chain.to_int64()
    let wait_deadline_ms = 30000
    let wait_start = @clock.monotonic_clock_start()
    let mut all_done = false
    while !all_done {
      all_done = true
      for h in hots {
        if h.snapshot().processed < expected {
          all_done = false
        }
      }
      if !all_done {
        if @clock.monotonic_clock_end(wait_start) >
          wait_deadline_ms.to_double() * 1000.0 {
          break
        }
        @async.sleep(1)
      }
    }
    let chain_wait_ms = @clock.monotonic_clock_end(wait_start) / 1000.0
    for h in hots {
      h.close()
    }
    let mut chains_completed = 0
    let mut hot_processed_total = 0L
    for h in hots {
      let _reason = h.join()
      let s = h.snapshot()
      hot_processed_total += s.processed
      if s.processed == expected {
        chains_completed += 1
      }
    }
    let cold_snap = cold.snapshot()
    cold.close()
    let cold_reason = cold.join()
    println("hot_instances: \{p.hot}")
    println("chain_budget_per_hot: \{p.chain}")
    println("hot_processed_total: \{hot_processed_total}")
    println("chains_completed_full_budget: \{chains_completed}/\{p.hot}")
    println("chain_completion_wait_ms: \{fmt1_ms(chain_wait_ms)}")
    println("requested_cold_samples: \{p.samples}")
    println("timeout_ms_per_request: \{p.timeout_ms}")
    println("sampling_window_ms: \{fmt1_ms(window_us)}")
    stats.report("cold")
    snapshot_line("cold", cold_snap)
    println("lifecycle_after_join(cold): \{Repr(cold_reason)}")
    check_conservation("hotcold-cold", cold_snap, cold_snap.accepted, 0L)
  })
}

///|
/// producers: N producers x explicit per-producer budget against one
/// instance with the configured mailbox. Refusals are counted, a sampler
/// task tracks the outstanding peak via `snapshot()` at a fixed interval.
/// The sampler period bounds peak resolution — reported alongside.
async fn run_producers(p : Params) -> Unit {
  println("=== workload: producers ===")
  print_common_header(p)
  run_producers_body(p)
}

///|
async fn run_producers_body(p : Params) -> Unit {
  let mailbox : @fuwaroid.Mailbox = if p.capacity == "unbounded" {
    @fuwaroid.Mailbox::Unbounded
  } else {
    match parse_int_bench(p.capacity) {
      Some(n) =>
        if n > 0 {
          @fuwaroid.Mailbox::Bounded(n)
        } else {
          fail(
            "producers: capacity must be a positive integer or 'unbounded', got \{p.capacity}",
          )
        }
      None => fail("producers: unparsable capacity \{p.capacity}")
    }
  }
  @async.with_task_group(group => {
    let fu = @fuwaroid.Fuwaroid::spawn_fold(
      group~,
      init=0,
      on_cmd=short_handler,
      mailbox~,
    )
    let done : Ref[Bool] = { val: false, }
    let peak : Ref[Int64] = { val: 0L, }
    let sample_count : Ref[Int] = { val: 0, }
    let sampler = group.spawn(() => {
      while !done.val {
        @async.sleep(p.sample_interval_ms)
        let s = fu.snapshot()
        if s.outstanding > peak.val {
          peak.val = s.outstanding
        }
        sample_count.val += 1
      }
    })
    let tasks : Array[@async.Task[(Int, Int)]] = []
    let t0 = @clock.monotonic_clock_start()
    for _pi in 0..<p.producers {
      tasks.push(
        group.spawn(() => {
          let mut acc = 0
          let mut rej = 0
          for i in 0..<p.per_producer {
            match fu.tell(i) {
              Ok(_) => acc += 1
              Err(_) => rej += 1
            }
            // Burst then yield: `burst` admissions per scheduler turn.
            if i % p.burst == p.burst - 1 {
              @async.pause()
            }
          }
          (acc, rej)
        }),
      )
    }
    let mut accepted = 0
    let mut refused = 0
    for t in tasks {
      let pair = t.wait()
      accepted += pair.0
      refused += pair.1
    }
    let producers_us = @clock.monotonic_clock_end(t0)
    done.val = true
    let _ = sampler.wait()
    fu.close()
    let reason = fu.join()
    let snap = fu.snapshot()
    let total_budget = p.producers * p.per_producer
    println("producers: \{p.producers}")
    println("per_producer_budget: \{p.per_producer}")
    println("total_budget: \{total_budget}")
    println("mailbox: \{p.capacity}")
    println("burst_per_yield: \{p.burst}")
    println("accepted: \{accepted}")
    println("refused: \{refused}")
    println(
      "accepted_plus_refused: \{accepted + refused} (must equal total_budget)",
    )
    println("producers_phase_ms: \{fmt1_ms(producers_us)}")
    println("outstanding_peak_sampled: \{peak.val}")
    println("outstanding_sampler_interval_ms: \{p.sample_interval_ms}")
    println("outstanding_samples_taken: \{sample_count.val}")
    snapshot_line("final", snap)
    println("lifecycle_after_join: \{Repr(reason)}")
    if accepted + refused != total_budget {
      fail(
        "producers: accepted + refused (\{accepted + refused}) != total budget (\{total_budget})",
      )
    }
    if p.capacity == "unbounded" && refused > 0 {
      fail(
        "producers: an Unbounded mailbox must not refuse, got \{refused} refusals",
      )
    }
    check_conservation(
      "producers",
      snap,
      accepted.to_int64(),
      accepted.to_int64(),
    )
  })
}

///|
/// handler-cost: tell-throughput shape with a configurable pure-arithmetic
/// burn inside the handler (no sleep, no IO). burn=0 is the short-handler
/// baseline; a larger burn shows the synchronous handler's ceiling: while a
/// handler runs, nothing else on the runtime can.
async fn run_handler(p : Params) -> Unit {
  println("=== workload: handler-cost (burn=\{p.burn}) ===")
  print_common_header(p)
  run_handler_body(p)
}

///|
async fn run_handler_body(p : Params) -> Unit {
  let burn = p.burn
  @async.with_task_group(group => {
    let fu = @fuwaroid.Fuwaroid::spawn_fold(group~, init=0, on_cmd=(
      _,
      state,
      _cmd,
    ) => {
      // Pure arithmetic burn; the accumulator feeds the state, so the
      // loop cannot be optimized away. Wrapping overflow is irrelevant
      // to the count bookkeeping.
      let mut acc = state
      for i in 0..<burn {
        acc = acc * 31 + i
      }
      acc
    })
    let mut warm_ok = 0
    for i in 0..<p.warmup {
      match fu.tell(i) {
        Ok(_) => warm_ok += 1
        Err(_) => ()
      }
    }
    let warm_barrier = fu.ask((), timeout_ms=p.timeout_ms)
    guard warm_barrier is Ok(_) else {
      fail("handler-cost: warmup barrier did not complete")
    }
    let mut own_ok = 0
    let t0 = @clock.monotonic_clock_start()
    for i in 0..<p.messages {
      match fu.tell(i) {
        Ok(_) => own_ok += 1
        Err(_) => ()
      }
    }
    let send_us = @clock.monotonic_clock_end(t0)
    let barrier = fu.ask((), timeout_ms=p.timeout_ms)
    let total_us = @clock.monotonic_clock_end(t0)
    guard barrier is Ok(_) else {
      fail("handler-cost: drain barrier did not complete")
    }
    let snap = fu.snapshot()
    fu.close()
    let reason = fu.join()
    println("burn_iterations_per_message: \{p.burn}")
    println("messages_budget: \{p.messages}")
    println("warmup: \{p.warmup} messages + 1 barrier ask")
    println("accepted_by_driver: \{own_ok}")
    println("send_phase_ms: \{fmt1_ms(send_us)}")
    println("total_incl_drain_ms: \{fmt1_ms(total_us)}")
    let total_s = if total_us > 0.0 { total_us / us_per_s } else { 1.0 }
    println(
      "throughput_end_to_end_msg_per_s: \{fmt0(own_ok.to_double() / total_s)}",
    )
    println(
      "avg_end_to_end_us_per_msg: \{fmt0(total_us / (own_ok.to_double() + 1.0))}",
    )
    snapshot_line("final", snap)
    println("lifecycle_after_join: \{Repr(reason)}")
    check_conservation(
      "handler",
      snap,
      (own_ok + warm_ok + 1 + 1).to_int64(),
      (own_ok + warm_ok + 1 + 1).to_int64(),
    )
  })
}

///|
async fn main {
  let args = @env.args()
  let mut workload = ""
  let p = Params::default()
  for i in 1..<args.length() {
    let a = args[i]
    if a.contains("=") {
      if !apply_param(p, a) {
        fail("bench: bad or unknown parameter: \{a}")
      }
    } else if !a.has_prefix("-") && workload == "" {
      workload = a
    }
  }
  if workload == "" {
    println(
      "usage: moon run src/bench --target native <workload> [key=value ...]",
    )
    println("workloads: selftest smoke tell ask hotcold producers handler")
    println(
      "keys: mode messages warmup samples hot chain producers per_producer",
    )
    println("  burst capacity burn timeout_ms producer_per_sample")
    println("  sample_interval_ms note_batch note")
    fail("bench: no workload given")
  }
  match workload {
    "selftest" => run_selftest(p)
    "smoke" => run_smoke(p)
    "tell" => run_tell(p)
    "ask" => run_ask(p)
    "hotcold" => run_hotcold(p)
    "producers" => run_producers(p)
    "handler" => run_handler(p)
    other => fail("bench: unknown workload \{other}")
  }
}
