///|
test "no successful requests must not report zero latency" {
  assert_eq(percentile_text([], 99.0), "n/a")
  assert_eq(percentile_text([0.0], 99.0), "0")
}

///|
test "request percentiles use nearest rank and retain the tail" {
  let samples : Array[Double] = Array::makei(100, i => (i + 1).to_double())
  assert_eq(percentile(samples, 50.0), Some(50.0))
  assert_eq(percentile(samples, 95.0), Some(95.0))
  assert_eq(percentile(samples, 99.0), Some(99.0))
  assert_eq(percentile([7.0], 99.0), Some(7.0))
}
