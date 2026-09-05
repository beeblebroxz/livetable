# LiveTable - Original Design Vision

*Based on design notes from February 2015*

This document captures the original design philosophy and architectural decisions that guide LiveTable's implementation.

The design sections preserve aspirations, not current API guarantees. In
particular, automatic layout tuning, parallel execution/locking, and strict mode
are not implemented. See [Implementation Status](#implementation-status) for
the current scope, the [Rust API guide](API_GUIDE.md) for supported behavior, and
the [sorted pipeline contract](INCREMENTAL_SORTED_PIPELINE.md) for recent work.

---

## Design Philosophy

The design is guided by a few core principles:

1. **Performance**: Operations should be approximately as fast as hand-coded C++
2. **Real-time**: Even very large table graphs can "tick" in real time
3. **Ease of use**: Minimize the need for "knobs" by automatically tuning behavior
4. **Abstraction**: Algorithms that operate on tabular data need not care about the physical layout chosen for that data
5. **Concurrency**: Maximize parallelism with seamless and correct locking
6. **Testability**: Users can run in a slower "strict mode" that verifies invariants

---

## Storage Layer: Sequences

A **Sequence** is the lowest-level storage implementation, invisible to the public API. It is an array-like container storing raw B-bit values, where B is either 1, 2, 4 or any multiple of 8. The interpretation of these values (integers, floating point, pointers, etc.) is up to the user of the Sequence.

### Two Sequence Implementations

#### Array (ArraySequence)
A simple contiguous array:
- **Optimal O(1)** random-access read/write
- **Poor O(N)** random-access insert and delete
- Loops over consecutive indices are simple with optimal cache locality and vectorizability
- Best for read-heavy workloads or append-only patterns

#### TieredVector (TieredVectorSequence)
An unusual data structure using circular buffers to achieve true constant-time access:
- **True O(1)** random-access via direct calculation (no binary search)
- **O(√N)** random-access insert and delete
- Loops over consecutive indices are efficient, but slightly less efficient than Array
- A TieredVector of size N is represented using at most 2×√N distinct contiguous sequences
- Requires an extra O(√N) storage overhead
- Best for insert-heavy workloads
- **Implementation**: Backed by the [tiered-vector](https://crates.io/crates/tiered-vector) crate

### Choosing Between Them

The original proposal was to dynamically choose layout strategies based on
insert/delete versus read patterns; automatic switching remains unimplemented.

**Current Implementation**: Users select the storage backend via `StorageHint` at table creation:
- `storage="fast_reads"` (default): Uses ArraySequence
- `storage="fast_updates"`: Uses TieredVectorSequence

---

## Column Layer

A **Column** is an array-like random-access data container indexed by an integer index. Each Column has a type that specifies the type of every value stored in it:

### Original Type Goals
- **B-bit integers**: Signed or unsigned fixed point values (B >= 1, with M <= B digits right of the decimal point)
- **32-bit or 64-bit floating point**
- **Strings**: Fixed-length, bounded-length, or variable-length; unicode or binary
- **Boolean**
- **Date/Time**
- **Nested Column**: A Column which contains another Column
- **Any Table**: With a Schema specifying the type

The current implementation intentionally supports a narrower concrete set:
INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL, DATE, and DATETIME. Arbitrary
bit-width/fixed-point integers, nested columns, binary strings, and table-valued
columns remain original design ideas rather than implemented APIs.

### NULL Support

Each type has a "may-be-Null" variant (e.g., "double-or-Null", "uint27-or-Null"). The special value NULL (ala SQL) may be stored. This imposes speed and space costs, so it should only be used when necessary.

Implementation options for nullable columns:
- Two Sequences: one for values, one for "is this Null?" flags
- Single Sequence with one bit reserved as the Null flag
- Sentinel value (e.g., 0xFFFFFFFF means Null) to skip storage for Nullness bits

**Important**: NULL should sort in the same order as SQL (NULLs first or last, configurable).

---

## Table Layer

A **Table** is a list of named Columns. The list of Column names and their types forms the Table's **Schema**.

### Two Types of Tables

#### Root Tables
- A standalone collection of data
- Owns its data directly
- Can be directly modified (insert, update, delete)
- Once a Root has children, its Schema can no longer change

#### View Tables
- Has no independently mutable source data (may cache derived state)
- Is just a function of the values in other Tables
- Cannot be directly modified
- Always derives from parent Tables via a DAG (Directed Acyclic Graph)

### View Materialization Strategies

Views can be implemented with different materialization strategies:

1. **Stateless View**: No changing state of its own. To fetch the Ith value, it asks each parent table for their Ith value, combines them, and returns the result. Does not need to be updated when parents change, although child Views may need to be recursively notified.

2. **Fully Materialized View**: Has its own copy of all of its state. Even if it's a View, it can satisfy read requests simply by read locking itself. Takes more memory but can answer read queries more quickly. For complex functions like filter or join, computing from scratch might be expensive, forcing the use of materialization.

3. **Hybrid View**: Tracks a little state that needs to be updated whenever the parent changes, but still doesn't keep its own copy of everything. Example: a SortedView that maintains its own row-index-permuting Sequence to track how the parent should be reordered, but without actually copying all the row data.

Today filters/joins keep row mappings, sorts also cache sort-key values and
inverse indices, and aggregates materialize group state. Reads/event payloads
clone values or affected rows. The original locking discussion does not apply
to the current single-threaded `Rc<RefCell<...>>` view graph.

---

## Change Propagation

Only Roots may be directly modified, and those modifications recursively propagate through descendant Views based on what function they are computing.

Currently this is explicit: mutate the root, then `tick()` registered views, or
manually `sync()` in parent-before-child order. Filters/sorts publish bounded
own-coordinate history; other views' children use version-checked rebuilds.
See the [filter contract](INCREMENTAL_FILTER_PIPELINE.md). Root `tick()` requires
pending mutations; refresh-only propagation requires direct child sync.

### Batched Changesets

- Some changes can be applied much more efficiently if done as a group
- For example, inserting 20 rows at scattered indexes can be done in one "slide over" pass rather than 20 separate passes
- If a batch contains changes too complex to express as an incremental update, recompute the derived table from scratch
- The right model: each Table has a queue of incoming changesets, customized for what we care about from that parent

### Incremental Updates

Each View should have a "pre-propagate" function that handles changesets from its parents:
- For a **FilterView**: if changed rows don't match the filter, do nothing; if outside the slice, do nothing; otherwise add changes to the next batch
- For a **JoinView**: if the parent deletes row 37 and the child only cares about columns X and Y, the changeset may need to record the "old" values before row 37 is physically deleted

---

## String Interning

Some Columns use the same strings over and over again, so interning is useful:
- Use an N-byte "String ID" to refer to a string, and can even change that width over time if we get too many unique strings
- String IDs are not transferrable between tables (problem for joins)
- Could use pointers to an intrusively refcounted global string table

### Optimization Ideas

- If we have bits left over in the String ID, we could steal some to assist comparisons
- First byte of the string as the high 8 bits of the ID enables bucket comparisons
- Sorting all strings in the intern table into 256 buckets allows comparing bucket numbers instead of dereferencing

---

## Group By / Aggregations

The original proposal, rather than feeding directly into a "group by" table:
1. Has the parent table feed into a group-by object that hashes, unifies, etc.
2. Then forward propagates into a normal table that can be processed efficiently

The current `AggregateView` instead exposes its own materialized group state
through `ReadableTable`; it does not populate a separate root table or publish
output changesets. SUM/COUNT/AVG maintain incremental state; requested extrema
may rescan a group, while percentiles maintain sorted values.

### Parallel Group By

Parallel group-by can be challenging because threads may fight over the hash table when inserting rows. Solutions:
- One pass to hash everything in parallel and record the hashes
- Second pass where each thread only examines entries where `hash mod num_threads == thread_rank`
- Partition hashes into groups of 64, with each thread responsible for specific partitions

---

## Serialization

The current implementation provides in-memory CSV and JSON import/export with
schema inference. The compression, chunked random-access, and parallel-formatting
items below remain original future ideas.

- Serialization must "pipe" through compression, and perhaps even to Sandra writing, to avoid taking too much contiguous memory
- Format could support random-access decompression in chunks
- Sorting the string table before serializing might compress better
- Integers can compress using delta coding or FastPFor
- Could build in CSV generation, perhaps with parallel formatting via strand-like structures

---

## Future Considerations

### Freezing Tables
"Freezing" a Table or Column (making it immutable) could enable optimizations:
- Convert TieredVector -> Array
- Realloc memory to use minimal amount
- Optimize string representation (minimum bits, renumber IDs to match sort order)

### Threading
- If read/write locking sets of tables together, each table should have a canonically sorted list of all locks it needs
- Grabbing locks left to right guarantees no deadlocks
- A low-priority "groomer" task could sweep through tables making them more cache-efficient

### Testing
- Build optional runtime bounds checking into Sequence
- Torture test: forward propagate vs. invalidate vs. from-scratch materialized vs. SQL DB comparison
- Support debug Sequence that calls into Python to trace access patterns

---

## Implementation Status

### Completed
- [x] Sequence layer (ArraySequence, TieredVectorSequence backed by tiered-vector crate)
- [x] StorageHint API for selecting storage backend (`fast_reads` / `fast_updates`)
- [x] Column layer with NULL support (INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL, DATE, DATETIME)
- [x] Table layer (Root tables with CRUD operations)
- [x] Views: FilterView, ProjectionView, ComputedView, JoinView (LEFT/INNER/RIGHT/FULL), SortedView
- [x] AggregateView with GROUP BY and incremental updates (SUM, COUNT, AVG, MIN, MAX, MEDIAN, PERCENTILE)
- [x] String interning with reference counting
- [x] Changesets and incremental view propagation
- [x] Automatic view propagation via `tick()` method
- [x] View-over-view composition (DAG): all views implement `ReadableTable`; tables, filters, and sorts emit changesets so filter-to-sort-to-aggregate chains update incrementally, while children of other views use version-checked refresh
- [x] Shared Rust/Python filter replay for bounded mixed batches, with derived-coordinate changesets, bounded history, and rebuild invalidation
- [x] Sorted-coordinate batch replay with cached keys, stable ties, row-move events, and Python SortedView.group_by(); aggregate index remapping batched for structural changes
- [x] CSV/JSON serialization with type inference
- [x] Python bindings via PyO3
- [x] WebSocket server for real-time sync (Actix-web + React frontend)
- [x] Protocol v2 server-computed view pipelines (per-connection filter/sort/group DAGs with generation-scoped snapshots)
- [x] RIGHT and FULL OUTER joins
- [x] Multi-column joins (composite key support)
- [x] Bulk/Batch operations (`append_rows`)
- [x] Python iterator protocol (`for row in table`)
- [x] Pandas DataFrame interop (`to_pandas`, `from_pandas`)

### Planned
- [ ] General-purpose fully materialized row views (beyond existing key/group caches)
- [ ] Pipeline WebSocket delta delivery and derived-row identity (currently full snapshots)
- [ ] Persistence and recovery
- [ ] Parallel view execution
- [ ] SQL/query-planning layer

---

*This document serves as a reference for the original design vision. The implementation may diverge from these notes based on practical considerations and evolving requirements.*
