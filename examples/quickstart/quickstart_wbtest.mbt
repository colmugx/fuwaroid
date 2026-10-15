///|
/// The public API example is also checked by CI as a standalone module.
async test "quickstart_counter_processes_commands_and_queries" {
  assert_eq(run_counter(), 5)
}
