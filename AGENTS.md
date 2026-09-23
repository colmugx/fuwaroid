# AGENTS.md

## Toolchain

- Validation commands that support `--output-json` must include it:
  `moon test`, `moon check`, and `moon build`. This selects JSON compiler
  diagnostics; it does not make program output JSON. `moon run` is used for
  executable/script output and must not receive this flag. Confirm support
  with the installed command's `--help`; do not add the flag to `moon
  version`, `moon ide`, `moon fmt`, or `moon info` either.

## File layout

- `moon.mod` sets `source = "src"`, so the library package
  `colmugx/fuwaroid` is rooted at `src/`.
- `src/fuwaroid.mbt`, `src/diagnostics.mbt` — library sources.
- `src/bench/` — `is-main` benchmark driver; uses only the public API.
- `tools/` — `.mbtx` script-mode automation (three entries below).
- `docs/benchmarks/r1/<run-id>/` — verbatim raw benchmark outputs; never
  edit them by hand.
- `README.md` is a symlink to `README.mbt.md`. Keep the symlink.

## Tests

- `src/*_test.mbt` — black-box tests (public API only; the tested package
  is auto-imported as `@fuwaroid`): `fairness_test.mbt` (T01—T03),
  `api_test.mbt` (T05/T12), `lifecycle_test.mbt` (T04, T06—T11, T13),
  `diagnostics_test.mbt` (T14).
- `src/fuwaroid_wbtest.mbt` — white-box tests (same package, private
  members, fault injection, pure-function tables, T15).
- Run one file: `moon test src/fairness_test.mbt --target native --output-json`
  (file-level filter works; `-p` package and `-f` name-glob also exist).
- Full suite: `moon test --target native --output-json` (36 tests:
  34 library tests plus 2 benchmark-statistics tests; 31 at R1 closeout).
  Anything that could hang must also pass under the watchdog
  tool below — the watchdog, not the test framework, is the starvation
  safety net.
- Tests are deterministic by design: bounded spin-yield and barrier
  coordination, no sleep-based timing guesses. Do not weaken or delete
  assertions to get green; record the evidence and escalate instead.
- Stop-boundary regressions in `fuwaroid_wbtest.mbt` cover already-completed
  joins under caller cancellation, cooperative cancellation-shielded cleanup,
  and mandatory per-handle state. Cleanup publishes `Closing` before yielding;
  every 32 abandoned envelopes it yields, then records `Stopped` on completion.

## Tool entries (`.mbtx` script mode)

- `.github/workflows/concurrency-benchmark.yml` runs on manual dispatch and
  pushes to `main` on Ubuntu 24.04. `moon run tools/concurrency-ci.mbtx`
  builds the native release bench and runs eight scenarios five times in
  serial fresh processes; `[smoke]` reduces budgets for wiring validation.
  Results and partial-failure evidence go to `benchmark-results/` and the
  GitHub step summary/artifact. Never overwrite an existing campaign directory.
  The workflow has a 45-minute job deadline and each benchmark process has a
  120-second hard-cancellation deadline. No absolute performance gates.
  `src/bench/` and this CI script must remain versionable; historical local
  benchmark tools and `docs/` retain their existing ignore rules.

- `moon run tools/concurrency-check.mbtx [scenario ...]` —
  independent-process watchdog driving the full native test suite as
  configured scenarios: per-scenario deadlines, process-tree SIGKILL and
  reaping on expiry, verbatim stdout/stderr reporting, non-zero exit on
  timeout. Build and execution phases have separate deadlines. Run it
  before claiming scheduling/lifecycle work is done.
- `moon run tools/concurrency-api-guard.mbtx` — compile-rejection
  guard for the public API contract; 4 checks (positive control, legacy
  fold `ask(cmd)` rejected, `@aqueue.Kind` config rejected, `Bounded(0)`
  fail-fast abort verified end-to-end in an independent process). Exit 0
  with "contract HOLDS" = pass.
- `moon run tools/concurrency-bench.mbtx list` /
  `[run-id=<id>] [label=<l>] <scenario...>` / `cleanup-copies` — benchmark
  orchestration over `src/bench`; writes raw output under
  `docs/benchmarks/r1/`. It never modifies the working tree (batch/diag
  variants run from isolated temp copies).
- **All agent-authored automation is `.mbtx`** (MoonBit script mode, run
  with `moon run ...`). No shell or Python orchestration.
- `src/pkg.generated.mbti` is generated: never hand-edit it. Regenerate
  with `moon info --target native` and review the diff as the
  public-API signal. `moon fmt` is a closeout-only command in
  the R1 pipeline (single authorized runner, serial); do not run it
  concurrently with other agents' edits.

## Contract documents

- `docs/concurrency-hardening-plan.md` — R1 plan (findings F1—F7,
  contracts D1—D5, test matrix T01—T15). Frozen since Gate-U; rulings are
  appended to its Work log only.
- `docs/concurrency-hardening-R1-tracking.md` — all rulings (Gate-U,
  R-W1-1, rulings 1—16, frozen signatures) and per-wave acceptance
  records. The single source of truth for what was decided and why.
- `docs/design.md` / `docs/decisions.md` — architecture arguments and the
  decision log; keep them in sync with the code.
- `docs/concurrency-benchmarks.md` + `docs/benchmarks/r1/` — measured
  data. Cite measurements, never invent numbers; label inferences as
  inferences.

## Commit messages

Write commit messages in this order:

1. **Why** — the concrete compatibility issue, bug, requirement, or maintenance need that makes the change necessary.
2. **What** — the implementation changes that address it.
3. **Result** — the observable behavior and verification outcome after the change.

Use the subject to describe the change itself. Keep the body factual and scoped to the commit.

Do not mention internal planning labels, roadmap phases, prompt names, implementation stages, or future milestones such as "M1", "M2", "phase", or "step". Those are coordination details, not repository history.

Do not include work that is unrelated to the stated reason for the commit. If unrelated cleanup is useful, put it in a separate change.

## Pull request descriptions

A pull request should answer three questions:

### Why

State the concrete problem or upstream change that requires the PR. Explain relevant compatibility or behavioral constraints.

### What changed

Describe the public API changes, runtime semantics, implementation choices, tests, and documentation changes that are actually present in the diff. Call out breaking changes explicitly.

Avoid narrating the development process. Do not describe internal phases, rejected prompts, or future work unless it is necessary to explain a deliberate compatibility boundary.

### Result

Report the behavior now guaranteed and the commands that were actually run. Distinguish passing checks from known unsupported targets or remaining limitations. Never imply a check passed when it was not run or was unsupported.

## Review quality

Prefer a small, coherent diff. Preserve unrelated documentation, benchmark evidence, ignore rules, and repository structure unless the PR's stated reason requires changing them.

For concurrency changes, document the observable semantics and test race boundaries rather than relying only on compilation. Tests should distinguish the current task's cancellation from errors reported when waiting on another task.

## Constructor naming

Use MoonBit's type-named constructor convention for public constructors:

- Declare constructors as `Type::Type(...)`.
- Call them as `Type(...)`.
- Do not introduce `Type::new(...)` when a normal public constructor is intended.

For example, declare `Supervisor::Supervisor(...)` and call it as `Supervisor(...)`.