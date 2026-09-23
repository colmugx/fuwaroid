# Contribution guidance

Keep changes and their history focused on the repository state a reviewer needs to evaluate.

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
