# Fuwaroid quickstart

An offline, standalone MoonBit executable using Fuwaroid's public API.
The local `moon.work` resolves `colmugx/fuwaroid` from this repository,
without requiring a published package during development.

From the repository root:

```sh
moon -C examples/quickstart update
moon -C examples/quickstart check --target native --output-json
moon -C examples/quickstart build --target native --output-json
moon -C examples/quickstart test --target native --output-json
moon -C examples/quickstart run --target native .
```

Expected output:

```text
count = 5
```

The counter processes two `tell` commands, then `ask` reads their
combined state. It explicitly calls `close(); join()` before the host
task group exits to confirm a graceful mailbox drain. The example test
asserts the result using only public Fuwaroid APIs.

The current async runtime is cooperative and single-threaded; the example
demonstrates message-ordering semantics, not multicore parallelism.
