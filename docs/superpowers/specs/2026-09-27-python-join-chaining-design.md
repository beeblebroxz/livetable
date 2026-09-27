# Python Chaining over Joins — Design

**Date:** 2026-09-27
**Status:** Approved design; not yet implemented
**Goal:** Python users can build filters, sorts, and aggregates on a join, and
`tick()` keeps the whole chain current, so the join output changesets
([spec](2026-09-27-join-output-changesets-design.md)) benefit Python.

## Problem

Rust views over a join replay its history, but Python cannot build them:

- `JoinView` has no `filter()`, `sort()`, or `group_by()`.
- The Python `FilterView` state (`PyFilterViewInner`) is bound to a root
  `Table`: it reads the table's changeset (assumed always present), schema,
  and version, and `tick()` compaction treats its cursor as a root cursor.
- Chained Python views register for `tick()` on one root table. A join has
  two, and `tick()` on either syncs the join.
- An explicit `livetable.JoinView(...)` keeps no reference to its tables, so
  it has nowhere to register children.

## Decisions

1. **Chaining methods only.** `JoinView.filter(predicate)`,
   `JoinView.sort(by, descending=None)`, and `JoinView.group_by(by, agg)`,
   with the same argument forms as the `Table` methods, on joins from
   `table.join()` and from the explicit constructor. The results chain as
   today (`FilterView.sort()/group_by()`, `SortedView.group_by()`).
   Explicit `FilterView`/`SortedView`/`AggregateView` constructors stay
   table-only.
2. **One Python filter, any parent** (approach A1 below).
3. **Chained views register on every root they depend on** (approach A2).

## Approaches considered

Filter over a join:

- **A1. Generalize `PyFilterViewInner`'s parent to
  `Rc<RefCell<dyn ReadableTable>>` (chosen).** One implementation; a table
  parent behaves exactly as today because `Table` implements `ReadableTable`.
- **B1. A second Python filter type for derived parents.** Rejected: it
  duplicates the fallible sync/refresh/retry logic.
- **C1. The Rust `FilterView` with a closure calling Python.** Rejected: Rust
  predicates are infallible, so Python exceptions would panic or be swallowed,
  losing the retryable-callback contract.

Registration:

- **A2. Chained views remember their roots (chosen).** Views downstream of a
  join register on both tables' registries, after the join.
- **B2. Register on the left table only.** Rejected: after a right-only edit,
  children stay stale, because `tick()` on the left table does nothing without
  pending left changes.
- **C2. A shared scheduler or dependency graph.** Rejected as more than needed.

## Design

### Roots and registration

`PyJoinView`, `PyFilterView`, and `PySortedView` carry the root tables their
chain depends on: `PyJoinView` keeps `left` and `right` `PyTable`s; the other
two keep `roots: Vec<PyTable>` (one table normally, the join's tables when
the chain starts at a join). `PyTable` clones share the table and its
registry, so holding them costs two `Rc`s each.

- A single helper registers a new view **once in each distinct registry**
  (registries compared by `Rc::ptr_eq`), always after its parent's entry.
  A self-join (`t.join(t)`) therefore registers each child once.
- `table.join()` already registers the join as `JoinLeft` on the left table
  and `JoinRight` on the right. An explicit `JoinView` registers itself the
  same way the first time something is chained on it, unless already present
  (checked with `Weak::ptr_eq`, like `SortedView.group_by()` does for an
  explicit sort). Until then, as today, it is not ticked.
- `FilterView.sort()/group_by()` and `SortedView.group_by()` register on all
  of the view's roots. `SortedView.group_by()` still registers its sort first
  if it is missing, now in every root registry.
- Weak registry entries stay alive while any child holds its parent `Rc`, so
  dropping the Python `JoinView` object does not stop its children updating.

`tick()` on either table then syncs the join and then its children. When both
tables tick, the second pass is a no-op sync (the join's empty-batch path
keeps its history). `registered_view_count()` counts a join child on each
table it is registered with.

### Python filter on any parent

`PyFilterViewInner.parent: Rc<RefCell<dyn ReadableTable>>` replaces
`table_inner`:

- **Sync.** If the parent exposes no changeset (a stale join), do a
  version-checked refresh: no-op when the parent's version is unchanged,
  otherwise refresh. Otherwise replay exactly as today (256-change bound,
  refresh on missing history).
- **Empty batch records the parent version.** Today an empty batch returns
  without recording it. With a join parent, an edit that produces no join
  output still advances the join's version, and the filter's `changeset()`
  would then hide its history from its own children. This mirrors the Rust
  filter.
- **Refresh.** Reads rows through the parent. Generation and cursor come from
  the parent's changeset, or `usize::MAX` without one (as the Rust filter does).
- **Schema and reads.** Column names, index, and type come from the parent.
  `FilterView.get_row/get_value/[]/iteration` read through the filter state
  instead of the `PyTable`, and the iterator's mutation guard compares the
  parent's `version()`. For a table parent that is the table's version, so
  behavior is unchanged; for a join it covers both tables. Error types are
  unchanged: an out-of-range row raises `IndexError`, an unknown column
  `KeyError`.
- **Predicate rows over a join.** The predicate receives the joined row as a
  dict, exactly as `joined[i]` returns it: left columns by name, right columns
  prefixed `right_`, and `None` for every column of an unmatched side.
- **Compaction.** `tick()` computes the filter's root cursor as
  `parent.root_changeset_cursor(last_processed_change_count)`: the cursor
  itself for a table, `usize::MAX` for a join (derived cursors never
  constrain a root).
- **Callbacks.** Unchanged: exceptions are retryable, and a predicate that
  mutates the parent raises the existing `RuntimeError`. The version check now
  covers both joined tables.
- **Names.** `PyFilterView` keeps a `name` used for chained view names
  (`<table>_filtered_sorted` as today; `<join>_filtered_sorted` over a join).

## Costs

- No change for table filters. Over a join, the Python predicate runs once per
  changed join row: a right-row edit that reaches k output rows runs it k
  times, and more than 256 join events refresh the filter (evaluating every
  row), per the existing filter contract.
- Registering on both roots means each table's `tick()` syncs the chain; the
  second, empty sync costs one version comparison per view.

## Testing

A new `tests/python/test_join_chaining.py`, each test against a fresh oracle
(a view built from scratch over the same join):

- `join.filter/sort/group_by` from `table.join()` stay correct after `tick()`
  on the left table, and after `tick()` on only the right table.
- Two-level chains: `join.filter().sort()`, `join.filter().group_by()`,
  `join.sort().group_by()`.
- An explicit `JoinView` registers itself exactly once, however many children
  chain from it (checked with `registered_view_count()`).
- A self-join child is registered once and updates.
- A failing predicate raises and succeeds on retry; a predicate that mutates
  the right table raises `RuntimeError`.
- `pending_changes_count()` is 0 on both tables after each ticks (a join-
  coordinate cursor must not hold back root compaction).
- Iterating a filter over a join raises if either table mutates mid-iteration.
- A no-output join edit (a non-key edit to an unmatched left row of an INNER
  join) keeps the filter's history: after syncing the join and the filter by
  hand, `sorted_child.sync()` returns `False`. Without the recorded version the
  child would take a version-checked refresh and return `True`; correctness
  alone cannot catch this, because the refresh gives the right answer.
- Dropping the Python `JoinView` object (and running `gc.collect()`) does not
  stop its registered children updating on `tick()`.
- Reads keep their error types: `IndexError` out of range, `KeyError` for an
  unknown column, on a filter over a join.
- Predicate rows: a LEFT-join filter's predicate sees `right_` columns, and
  `None` for an unmatched right side.

Existing Python tests (filters over tables, chaining, tick) must pass
unchanged.

## Documentation

README (Features + Example), CLAUDE.md (Python API Usage, and the Key
Patterns sentence "Python chaining supports `FilterView.sort()`, ..."),
`docs/PYTHON_BINDINGS_README.md` (JoinView methods, the predicate row shape,
and the rule: tick the table you mutated, since either table's tick updates
the join chain), `docs/ORIGINAL_VISION.md` (Implementation Status), and
`docs/GETTING_STARTED.md` if it lists the supported chains.

## Non-goals

- Joining a join (`joined.join(...)`), explicit constructors with a join
  parent, and pipeline-spec join nodes.
- Changing when `tick()` runs: a table with no pending changes still does
  nothing, so users tick the tables they mutated.

## Risks

- **Registration order.** A child registered before its parent would sync
  against a stale parent. The helper always runs after the parent is
  registered; tests tick each root separately.
- **Compaction.** A wrong root cursor either holds back compaction (memory)
  or compacts too far (forced refreshes). The `pending_changes_count()` test
  covers the first; the second cannot happen because derived parents report
  `usize::MAX`.
- **Behavior drift for table filters.** Mitigated by running the existing
  suite unchanged.
