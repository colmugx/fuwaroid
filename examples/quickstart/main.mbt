///|
/// A runnable single-writer counter. The state is updated only by the
/// synchronous command handler, and ask() is a FIFO processing barrier.
enum CounterCmd {
  Add(Int)
}

///|
enum CounterQuery {
  GetCount
}

///|
/// Exercise the public Fuwaroid API; no test-only helpers or external I/O.
async fn run_counter() -> Int {
  let result = Ref(0)
  @async.with_task_group(group => {
    let counter : @fuwaroid.Fuwaroid[CounterCmd, CounterQuery, Int] =
      @fuwaroid.Fuwaroid::spawn(
        group~,
        init=0,
        on_cmd=(_, state, cmd) =>
          match cmd {
            CounterCmd::Add(amount) => state + amount
          },
        on_query=(_, state, _query) => (state, state),
      )

    for amount in [2, 3] {
      match counter.tell(CounterCmd::Add(amount)) {
        Ok(_) => ()
        Err(_) => abort("quickstart: command was not admitted")
      }
    }

    match counter.ask(CounterQuery::GetCount, timeout_ms=1000) {
      Ok(count) => result.val = count
      Err(_) => abort("quickstart: query failed")
    }

    // close() stops admission; join() confirms the accepted messages drained.
    counter.close()
    match counter.join() {
      @fuwaroid.Graceful => ()
      _ => abort("quickstart: Fuwaroid did not stop gracefully")
    }
  })
  result.val
}

///|
async fn main {
  println("count = \{run_counter()}")
}
