# Typed column storage

Columns store native-width values rather than one `ColumnValue` enum per cell.
`ColumnValue`, `Column`, `Table`, and the Python API keep their public read/write
interfaces; conversion happens at the boundary. Both storage hints still work.

## Layout

| Column type | Value buffer element |
|-------------|----------------------|
| INT32, DATE | `i32` (4 bytes) |
| INT64, DATETIME | `i64` (8 bytes) |
| FLOAT32 | `f32` (4 bytes) |
| FLOAT64 | `f64` (8 bytes) |
| BOOL | `bool` (1 byte; values are not bit-packed) |
| STRING, without interning | Owned `String` |
| STRING, with interning | `u32` ID (4 bytes), without a parallel placeholder buffer |

The private `ColumnData` enum selects a typed buffer once per column, with
concrete backend dispatch to avoid a per-cell virtual call.
`fast_reads` uses `ArraySequence<T>`; `fast_updates` uses
`TieredVectorSequence<T>`. There is no new storage dependency or wire format.
Dates keep their days-since-epoch representation; datetimes keep milliseconds.
Float bit patterns, NULL semantics, and runtime schema validation are preserved.

Nullable columns add packed NULL flags; non-nullable columns allocate no mask.
A set bit means NULL. NULL payload slots contain a type-appropriate default;
interned strings use `u32::MAX`, never ID zero. Ordinary strings still own their
text; this change does not introduce an Arrow-style offsets/bytes string buffer.

## NULL masks and middle edits

Array masks are `Vec<u64>` with 64 flags per word. Reads and updates are O(1);
middle insert/delete shifts O(N/64) words, consistent with array storage's
linear-time middle edits.

Tiered masks use circular bit blocks of power-of-two size B near sqrt(N).
All but the last block are full, so row-to-block addressing is O(1), with no
prefix-sum search. A middle edit shifts at most B bits in word-sized chunks and rotates
O(N/B) subsequent blocks. Ordinary middle edits therefore remain O(sqrt(N));
occasional O(N) growth/shrink rebuilds are amortized, with hysteresis to avoid
resize thrashing. Packed payload plus block metadata uses N/8 + O(sqrt(N)) space,
apart from allocation capacity and rounding. Small columns have fixed overhead.

## Mutation and ownership guarantees

Type/bounds checks and fallible interner locking occur before mutating buffers,
flags, or reference counts. Rejected writes leave the column unchanged, allowing
table-level multi-column rollback to restore earlier successful columns.
Interned updates acquire the new reference before releasing the old one;
delete, truncate, and column destruction release owned references. The shared
interner can retain reusable string slots and allocation capacity after release.
Rust callers sharing an interner must not hold its mutex while performing column
operations or destroying a column; these paths acquire that same lock.

`get_f64()` reads numeric buffers without constructing `ColumnValue`; dates,
datetimes, booleans, strings, NULL, and invalid indices still return `None`.
Views, changesets, serialization, and Python callbacks still use their existing
value/row representations. This is not a zero-copy API or a whole-process memory
reduction guarantee: retained changeset rows and view caches can dominate memory.

## Verification

- Rust tests compare all eight types against a `Vec<ColumnValue>` model on both
  backends, with/without NULL and interning, including extreme integers, exact
  float bits, signed zero, NaN, infinities, pre-epoch dates, and Unicode strings.
- Bitmap tests cover word/block boundaries, resizing, front/middle/tail edits,
  randomized operations, emptying, and reuse.
- Failure tests check invalid writes, poisoned interner locks, reference counts,
  column destruction, and multi-column table rollback without version changes.
- Python tests compare typed rows and filter → sort → aggregate pipelines with
  independent models, including mixed batches and rejected writes.

From the repository root:

```bash
cargo test --manifest-path impl/Cargo.toml --lib --features server
pytest -c tests/pytest.ini tests/python/test_typed_storage.py
```

Rebuild the Python extension first. See [the test guide](../tests/README.md)
for full propagation, WebSocket, Python, and frontend checks.

## Reproducible measurement

Harness: [column_layout_benchmark.rs](../impl/examples/column_layout_benchmark.rs).

```bash
cargo run --manifest-path impl/Cargo.toml --release --example column_layout_benchmark -- 10000 100000
```

The harness counts live allocator-requested heap bytes after constructing one
column, including capacity slack and its optional interner. It excludes the
stack-resident column header, allocator bookkeeping/fragmentation, process RSS,
tables, changesets, views, Python, and transport. Numeric scans use `get_f64()`;
string scans use `get()` and include owned-string conversion. It also times a
middle insertion followed by deletion. Five warmups precede 31 samples; p95 is
nearest-rank. Construction and destruction are not timed. Values repeat modulo
1000, strings use 16 labels, and nullable cases contain 10% NULLs.

Before/after measurements must use the same harness, sizes, compiler, and
environment. Timing improvements are workload-dependent; capacity-dependent
heap figures are not per-row layout sizes or peak-memory measurements.

## Recorded comparison

Recorded September 5, 2026 (America/Los_Angeles), Apple M2, 24 GiB RAM,
macOS 26.5.2 (25F84), arm64, rustc 1.91.1 (ed61e7d7e 2025-11-07), release
profile. Before: `38314080e4faf18fb49a997709231d96dbcc5223`, exported into a
clean temporary directory and built with the identical added harness. After:
the typed-storage implementation accompanying this report, with concrete
backend dispatch and word-sized circular bitmap shifts. Source fingerprints
below identify the measured implementation and harness.

Two alternating before/after runs used 10,000 and 100,000 rows. The first pair
primed the workload; the second pair is reported here, without concurrent
agent-started builds/tests. Heap measurements agreed exactly across both pairs.
CPU frequency, scheduling, and thermal state remain uncontrolled. The counting
allocator stays active during timing, including string-allocation overhead.

Retained heap bytes at 100,000 rows (not RSS or total table/view memory):

| Backend | Column | Before | After | Reduction |
|---------|--------|-------:|------:|----------:|
| array | INT32, non-nullable | 3,145,757 | 524,293 | 83.3% |
| array | INT32, nullable | 3,276,853 | 540,677 | 83.5% |
| array | FLOAT64, nullable | 3,276,853 | 1,064,965 | 67.5% |
| array | STRING, nullable | 4,536,853 | 4,422,117 | 2.5% |
| array | STRING, interned, nullable | 3,803,161 | 542,673 | 85.7% |
| tiered | INT32, non-nullable | 2,416,717 | 409,605 | 83.1% |
| tiered | INT32, nullable | 2,525,333 | 428,293 | 83.0% |
| tiered | FLOAT64, nullable | 2,525,333 | 829,701 | 67.1% |
| tiered | STRING, nullable | 3,785,333 | 3,695,333 | 2.4% |
| tiered | STRING, interned, nullable | 2,937,001 | 430,289 | 85.3% |

The inline column header grew from 88 to 160 bytes on this build as backend
storage moved inline; it is excluded from heap figures above. Small columns
therefore have a different tradeoff. Ordinary string value slots remain the
same width on this machine; most of their small reduction comes from NULL masks.

Operation timings at 100,000 rows, median / p95 microseconds, before → after:

| Backend | Column | Full scan | Middle insert + delete |
|---------|--------|-----------|------------------------|
| array | INT32, non-nullable | 120.166 / 135.958 → 114.167 / 114.250 | 40.125 / 43.375 → 5.584 / 5.625 |
| array | INT32, nullable | 192.084 / 195.334 → 177.125 / 180.375 | 42.583 / 42.750 → 7.000 / 7.084 |
| array | FLOAT64, nullable | 199.375 / 202.667 → 162.708 / 166.084 | 42.625 / 42.709 → 13.833 / 13.917 |
| array | STRING, nullable | 1915.375 / 2022.708 → 2002.542 / 2109.000 | 42.792 / 43.166 → 42.000 / 42.333 |
| array | STRING, interned, nullable | 2312.500 / 2389.541 → 2363.625 / 2405.792 | 50.334 / 63.667 → 7.208 / 7.333 |
| tiered | INT32, non-nullable | 179.917 / 183.208 → 168.042 / 168.167 | 0.625 / 0.667 → 0.417 / 0.459 |
| tiered | INT32, nullable | 294.250 / 306.834 → 311.625 / 314.958 | 1.083 / 1.125 → 0.750 / 0.833 |
| tiered | FLOAT64, nullable | 306.792 / 318.917 → 311.667 / 325.375 | 1.084 / 1.166 → 0.792 / 0.875 |
| tiered | STRING, nullable | 2078.167 / 2157.750 → 2305.250 / 2344.000 | 1.083 / 1.125 → 1.000 / 1.083 |
| tiered | STRING, interned, nullable | 2437.042 / 2571.791 → 2548.000 / 2578.042 | 1.625 / 1.791 → 0.875 / 0.917 |

The primary win is memory density, not universally faster scans. Numeric array
scans improved in this run; nullable tiered numeric scans were up to about 6%
slower, and string scans up to about 11% slower. Middle edits improved for the
measured numeric and interned-string cases. Sub-microsecond measurements are
sensitive to timer resolution. These results do not establish application-wide
speedups, peak-memory reductions, or performance thresholds enforced in CI.

SHA-256 source fingerprints:

```text
column.rs          a27c53fa555596ab3bdc128d3e9baf0a46d035598465e15c782466357dca9da6
column/storage.rs  6da6488351d076c66ba84b9c665eaa15f4db367ac036e5d9f649cab0d8e9376a
column/bitmap.rs   fd1948b6318fefd5816f678a0adc0d2ae7188dbbf445a966ee0d40b221de41e6
benchmark harness  33c128ab2d7d23c9547584f493e8b1a0071d05780ed8a1cac2d89221177f3411
```
