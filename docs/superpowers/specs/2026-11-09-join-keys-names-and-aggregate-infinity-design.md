# Join key types, join output names and aggregate infinities

**Status:** Implemented 2026-11-09

## Why

A review on 2026-11-09 reproduced three silent wrong answers. Each is a view
returning a result that a reader would take as correct.

1. **Join keys of different integer widths never match.** Key parts are
   typed, so `Int32(1)` and `Int64(1)` are different keys. `JoinView::new_multi`
   checks only that the key columns exist. `Table::from_csv` and `from_json`
   pick the integer width per table from the values, so two files that share
   `id` 1 inner-join to zero rows when one of them also holds a value above
   `i32::MAX`.
2. **An infinity poisons an incremental SUM or AVG.** `extract_numeric`
   excludes NaN, but not ±inf. Removing an infinity from the running sum
   computes `inf - inf`, which is NaN, and the group stays NaN until a rebuild.
   A rebuild of the same rows gives the finite sum.
3. **Join output names break when a left column starts with `right_`.** Every
   join over a join has such columns.
   - `get_value` strips the prefix and looks only in the right parent, so the
     left column reads as "not found", although `get_row` returns it.
   - When the right parent has a column of the stripped name, both sides
     produce the same output name. `column_names()` lists it twice, and the
     right value (NULL for an unmatched row) replaces the left one.

## What

1. **Integer join keys compare by value.** The join's two key builders widen
   `Int32` to `Int64`. `GroupKey` shares the key-part type but keeps its own
   builder, so a group's `Int32` key still reads back as `Int32`.
2. **Other key type mismatches are rejected at construction.** For each key
   pair whose two column types are known, the types must be equal, or both be
   integers. The error names both columns and types. A parent that reports no
   static type for the column (computed and aggregate columns) is not checked.
3. **Infinities are counted, not summed.** `ColumnAggState` keeps a count of
   +inf and of -inf values beside the running sum of finite values.
   - SUM is NaN when both counts are non-zero, the infinity when one is, and
     the finite sum otherwise. AVG divides that by the value count.
   - These are the results a rebuild already gave. Only the incremental path
     changes.
   - MIN, MAX and percentiles already hold infinities correctly: their
     containers order by `total_cmp` and remove by exact value.
4. **A percentile that lands on a value returns that value.** The
   interpolation computed `lo * 1 + hi * 0`, which is NaN when `hi` is
   infinite: the median of `[1, 2, inf]` was NaN instead of 2. It also turned
   a `-0.0` result into `0.0`.
5. **Right output names are unique.** A right column's output name is
   `right_` plus its name. While that name is a left column or an earlier
   right output name, `right_` is applied again.
   - The names are computed once at construction, with the cached column
     names. Rows, events and `column_names()` all use them.
   - `get_value` resolves a name against the left columns first, then the
     right output names.
   - Only joins that had a collision get different names. Every other join's
     names are unchanged.

## Not included

- **Float32 joined to Float64, integers joined to floats, DATE joined to
  DATETIME.** These are rejected, not coerced. Widening a float changes which
  values are equal.
- **Drift in the running sum of finite values**, and a finite sum that
  overflows to infinity. Both need compensated or exact summation.
- **Int64 aggregates above 2^53**, which lose precision through `f64`.
- **The performance findings** from the same review: one-sided join batches
  without a size cap, per-event passes in the sorted view, the root changeset
  retained until `tick()`, and per-row allocations in aggregate builds.

## Acceptance

- **Each new test fails before its fix:**
  - (1) an `Int32` key joined to an `Int64` key matches, on build and on
    incremental inserts, and two CSV-loaded tables join on a shared `id`;
  - (2) a Float32–Float64, an integer–string and a DATE–DATETIME key pair
    are each rejected;
  - (3) a group's SUM and AVG equal a rebuild's after an infinity is deleted
    or edited away, and after one of two opposite infinities leaves;
  - (4) the median of `[1, 2, inf]` is 2;
  - (5) every column of a join over a join reads the same through
    `get_value` and `get_row`, names are unique, and replayed history
    reproduces the join.
- **Full suite:** fuzz, clippy and `run_all.sh` all pass.
