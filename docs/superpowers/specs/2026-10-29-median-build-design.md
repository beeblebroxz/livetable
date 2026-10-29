# Sort once when building percentile state

**Status:** Implemented 2026-10-29. Results: "After the percentile build fix" in `docs/VIEW_MAINTENANCE_BENCHMARK.md`.

## Why

Building an `AggregateView` with a MEDIAN or PERCENTILE aggregation inserts
each row's value into its group's sorted vector one at a time
(`ColumnAggState::add_value`), so a build is quadratic in group size. This
was measured on 2026-10-29 with a throwaway program and Float64 values
`(i * 7919) % 100_000`:

| Rows | Groups | SUM build | MEDIAN build |
|---:|---:|---:|---:|
| 100k | 8 | 21 ms | 75 ms |
| 1M | 8 | 188 ms | 5.1 s |
| 300k | 1 | 56 ms | 3.4 s |

Every rebuild pays this: construction, `refresh()`, and the rebuild fallbacks
(batches over the replay bounds, parents without history). A single
incremental insert or delete is fine, at about 0.1 ms at 1M.

## Design

- **Building:** `rebuild_index` appends values to each group's sorted vector
  without sorting, then sorts every vector once at the end with
  `f64::total_cmp`, the same order `add_value` keeps incrementally.
- **Incremental syncs** keep inserting in sorted position, unchanged.
- **The MIN/MAX multiset** is left as it is (O(log g) per insert, already
  measured within budget on 2026-10-26).

## Acceptance

- **A new contract test fails without the sort.** The lib and
  `aggregate_pipeline` tests all pass with the final sort removed. The
  randomized fuzz drivers do notice (three of six failed in review), but
  the full suite takes minutes. The new test must cover all three build
  loops: a String key, an Int32 key over one source column, and an Int32
  key over several.
- **A test pins the build's order for signed zeros.** A build that sorts
  0.0 and -0.0 as equal leaves them in input order, and a later removal of
  the -0.0 then misses (found in review).
- **Existing tests pass unchanged.**
  - Rebuilt and incremental percentile states must agree, including on
    signed zeros: `signed_zero_median_matches_a_rebuild` in
    `aggregate_pipeline.rs`.
  - The aggregate fuzz drivers check median and p90 against rebuilds.
- **Timing,** with one program built before and after the change (checked
  in as `impl/examples/aggregate_build_benchmark.rs`, so the report's
  figures trace to a harness):
  - a MEDIAN build within 2× of the SUM build at every measured size
    (1M rows and 8 groups, 300k rows and 1 group);
  - the one-insert tick unchanged.
- **Lint:** clippy is clean.
