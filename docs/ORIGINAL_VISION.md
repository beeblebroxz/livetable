# LiveTable - Original Design Vision

*Based on design notes from February 2015*

This document captures the original design philosophy and architectural decisions that guide LiveTable's implementation.

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
An unusual data structure using indirection and rotation to accelerate insert/delete:
- **O(1)** random-access, but with a few extra instructions to find the physical address
- **O(√N)** random-access insert and delete
- Loops over consecutive indices are efficient, but slightly less efficient than Array
- A TieredVector of size N is represented using at most 2×√N distinct contiguous sequences
- Requires an extra O(√N) storage to hold a rotation value table
- Best for insert-heavy workloads

### Choosing Between Them

The system can dynamically choose good layout strategies based on the relative ratios of insert/delete vs. loops over the entire Column, random access, etc.

---

## Column Layer

A **Column** is an array-like random-access data container indexed by an integer index. Each Column has a type that specifies the type of every value stored in it:

### Supported Types
- **B-bit integers**: Signed or unsigned fixed point values (B >= 1, with M <= B digits right of the decimal point)
- **32-bit or 64-bit floating point**
- **Strings**: Fixed-length, bounded-length, or variable-length; unicode or binary
- **Boolean**
- **Date/Time** (planned)
- **Nested Column**: A Column which contains another Column
- **Any Table**: With a Schema specifying the type

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
- Has no data of its own
- Is just a function of the values in other Tables
- Cannot be directly modified
- Always derives from parent Tables via a DAG (Directed Acyclic Graph)

### View Materialization Strategies

Views can be implemented with different materialization strategies:

1. **Stateless View**: No changing state of its own. To fetch the Ith value, it asks each parent table for their Ith value, combines them, and returns the result. Does not need to be updated when parents change, although child Views may need to be recursively notified.

2. **Fully Materialized View**: Has its own copy of all of its state. Even if it's a View, it can satisfy read requests simply by read locking itself. Takes more memory but can answer read queries more quickly. For complex functions like filter or join, computing from scratch might be expensive, forcing the use of materialization.

3. **Hybrid View**: Tracks a little state that needs to be updated whenever the parent changes, but still doesn't keep its own copy of everything. Example: a SortedView that maintains its own row-index-permuting Sequence to track how the parent should be reordered, but without actually copying all the row data.

---

## Change Propagation

Only Roots may be directly modified, and those modifications recursively propagate through descendant Views based on what function they are computing.

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

Rather than having one table feed directly into a "group by" table, the implementation:
1. Has the parent table feed into a group-by object that hashes, unifies, etc.
2. Then forward propagates into a normal table that can be processed efficiently

### Parallel Group By

Parallel group-by can be challenging because threads may fight over the hash table when inserting rows. Solutions:
- One pass to hash everything in parallel and record the hashes
- Second pass where each thread only examines entries where `hash mod num_threads == thread_rank`
- Partition hashes into groups of 64, with each thread responsible for specific partitions

---

## Serialization

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
- [x] Sequence layer (ArraySequence, TieredVectorSequence)
- [x] Column layer with NULL support (INT32, INT64, FLOAT32, FLOAT64, STRING, BOOL)
- [x] Table layer (Root tables with CRUD operations)
- [x] Views: FilterView, ProjectionView, ComputedView, JoinView (LEFT/INNER), SortedView
- [x] AggregateView with GROUP BY and incremental updates (SUM, COUNT, AVG, MIN, MAX)
- [x] String interning with reference counting
- [x] Changesets and incremental view propagation
- [x] CSV/JSON serialization with type inference
- [x] Python bindings via PyO3
- [x] WebSocket server for real-time sync (Actix-web + React frontend)

### Planned
- [ ] RIGHT and FULL OUTER joins
- [x] Multi-column joins (composite key support)
- [x] Date/Time column types (DATE and DATETIME)
- [ ] Materialized Views (cached for faster reads)
- [x] Bulk/Batch operations (`append_rows`)
- [x] Python iterator protocol (`for row in table`)
- [x] Pandas DataFrame interop (`to_pandas`, `from_pandas`)

---

*This document serves as a reference for the original design vision. The implementation may diverge from these notes based on practical considerations and evolving requirements.*
