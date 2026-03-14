# Design: RIGHT and FULL OUTER Joins + tick() Integration

**Date:** 2026-03-14
**Status:** Approved
**Scope:** Add RIGHT and FULL OUTER join types to JoinView, integrate JoinView with tick() auto-propagation

---

## Context

LiveTable's JoinView currently supports LEFT and INNER joins. The `join_index` stores `Vec<(usize, Option<usize>)>` — each entry maps a left row to an optional right row. RIGHT and FULL OUTER joins are listed as planned in the original vision document.

Additionally, JoinView is excluded from `tick()` auto-propagation (unlike FilterView, SortedView, AggregateView). Users must manually call `.refresh()` to update join results after parent table mutations. This inconsistency is addressed as part of this work.

## Design

### 1. Data Model

Change `join_index` from `Vec<(usize, Option<usize>)>` to `Vec<(Option<usize>, Option<usize>)>`.

Semantics per join type:
- **INNER:** always `(Some(left), Some(right))`
- **LEFT:** `(Some(left), Some(right))` or `(Some(left), None)`
- **RIGHT:** `(Some(left), Some(right))` or `(None, Some(right))`
- **FULL:** all three patterns

**Index ordering invariant:** All entries with `Some(left_idx)` appear first, ordered by ascending `left_idx` (with multiple right matches adjacent in right-table order). All entries with `None` left index (unmatched right rows) appear after, in ascending `right_idx` order. This is a deterministic convention, not a SQL guarantee (SQL specifies no output order without ORDER BY).

### 2. JoinType Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Left,
    Inner,
    Right,
    Full,
}
```

### 3. Rebuild Algorithm

Unified 4-phase algorithm for all join types:

```
Phase 1: Pre-compute column indices for join keys (existing)
Phase 2: Scan right table → build HashMap<composite_key, Vec<right_idx>>
         (rows where build_key_from_indices returns None are excluded from the map)
Phase 3: Scan left rows:
  - If build_key_from_indices returns None (NULL key):
    - LEFT or FULL → push (Some(left), None) — row exists but matches nothing
    - INNER or RIGHT → skip
  - If key is non-NULL and match found:
    - push (Some(left), Some(right)) for each match
    - add each right_idx to matched_right: HashSet<usize>
  - If key is non-NULL and no match:
    - LEFT or FULL → push (Some(left), None)
    - INNER or RIGHT → skip
Phase 4: (RIGHT or FULL only) Scan right rows 0..right_len:
  - If right_idx NOT in matched_right → push (None, Some(right))
    (This naturally includes right rows with NULL keys, since they
     were never added to the HashMap and thus never matched)
```

**Note on NULL-key left rows:** The existing LEFT join implementation silently drops left rows with NULL keys (they are never evaluated in the rebuild loop). This is a pre-existing bug. This design fixes it for all join types by explicitly handling the `None` return from `build_key_from_indices` in Phase 3.

### 4. get_row() / get_value() Changes

Both functions need symmetric None-handling for left and right indices:

**`get_row(index)`:**
- `Some(left_idx)` → fetch all left columns from left table
- `None` left_idx → all left columns are `ColumnValue::Null`
- `Some(right_idx)` → fetch all right columns from right table (with `right_` prefix)
- `None` right_idx → all right columns are `ColumnValue::Null` (existing behavior)

**`get_value(row, column)`:**
- If column starts with `right_` → route to right table logic:
  - `Some(right_idx)` → fetch from right table
  - `None` → return `ColumnValue::Null`
- Otherwise → route to left table logic:
  - `Some(left_idx)` → fetch from left table (existing)
  - `None` → return `ColumnValue::Null` **(new branch, mirrors existing right-side handling)**

### 5. tick() Integration

**RegisteredView enum extension:**
```rust
enum RegisteredView {
    Filter(Rc<RefCell<FilterView>>),
    Sorted(Rc<RefCell<SortedView>>),
    Aggregate(Rc<RefCell<AggregateView>>),
    JoinLeft(Rc<RefCell<JoinView>>),    // registered on the LEFT parent
    JoinRight(Rc<RefCell<JoinView>>),   // registered on the RIGHT parent
}
```

Two variants are needed because each parent's `tick()` must report the correct cursor for changeset compaction. `JoinLeft` reports `left_last_processed_change_count` and `JoinRight` reports `right_last_processed_change_count`. Both call the same `sync()` on the shared `JoinView`.

**Dual-parent registration:** When a JoinView is created via the simplified `.join()` API, register `JoinLeft(view.clone())` with the left table and `JoinRight(view.clone())` with the right table. The `Rc<RefCell<JoinView>>` clone bumps the reference count — same underlying view.

**PyTable Clone semantics:** This depends on `PyTable::Clone` sharing the `Rc<RefCell<Vec<RegisteredView>>>` (not creating a new empty vector). The existing clone implementation does share the Rc, so registering on `other` from within `.join()` correctly mutates the other table's view list.

**Idempotent sync:** When `left_table.tick()` fires, it calls `sync()` on the join, which processes changes from both parents and advances both cursors. If `right_table.tick()` also fires, `sync()` checks its cursors and returns `false` (no new changes). Already idempotent by existing design.

**Expose `.sync()` to Python:** Currently only `.refresh()` (full rebuild) is exposed. Add `.sync()` to `PyJoinView` to match the pattern of `AggregateView`.

**Changeset compaction:** Each parent independently compacts based on its own registered views' cursors. `JoinLeft` reports the left cursor to the left parent; `JoinRight` reports the right cursor to the right parent. Each parent compacts only up to the minimum cursor across its own registered views.

### 6. Incremental Sync Extensions

Existing behavior preserved: deletes and key-column updates trigger full `rebuild_index()` for all join types. Only row inserts are handled incrementally.

**Index position helpers:** `find_left_insert_position()` and `find_right_insert_position()` currently compare raw `usize` left indices. With `Option<usize>`, these must be updated:
- `None` left indices are always ordered *after* all `Some(left_idx)` entries
- When comparing `Some(a)` vs `Some(b)`, use the existing `a > b` logic
- When looking for insert position among `None`-left entries (unmatched right rows in tail section), use `right_idx` ordering

**Left-table insert (extending existing logic):**
- Shift all existing `Some(left_idx)` values >= insert position up by 1
- If match found → insert `(Some(new_left), Some(right))` entries at correct position
- If no match + LEFT or FULL → insert `(Some(new_left), None)` at correct position
- If no match + INNER or RIGHT → skip
- **New for RIGHT/FULL:** If this left row matches a right row that exists as `(None, Some(right))` in the tail section, **remove** that entry from the tail and **insert** `(Some(new_left), Some(right))` at the correct position in the left-ordered section. Simply modifying in-place would violate the ordering invariant.

**Right-table insert (extending existing logic):**
- Shift all existing `Some(right_idx)` values >= insert position up by 1
- If match found → insert `(Some(left), Some(new_right))` entries at correct position
- If no match + RIGHT or FULL → append `(None, Some(new_right))` at end of tail section
- If no match + LEFT or INNER → skip
- **Existing LEFT behavior:** Replace `(Some(left), None)` with `(Some(left), Some(new_right))` when a new right row matches a previously-unmatched left row. This can be done in-place since the left index doesn't change and the entry stays in the same position.

**Finding unmatched entries during incremental sync:** When a new left row matches a previously-unmatched right row (or vice versa), scan the join_index for the target `(None, Some(matching_right_idx))` entry. This is O(N) but only occurs for RIGHT/FULL joins on match, and the existing incremental sync is already O(N) due to the index shifting.

**Ordering invariant:** Incremental sync must produce the same result as a full `rebuild_index()`. Verified by tests.

### 7. Python API

**JoinType class — new attributes:**
- `JoinType.RIGHT`
- `JoinType.FULL`

**Simplified `.join()` API — `how=` parameter:**
```python
joined = table.join(other, on="id", how="right")
joined = table.join(other, on="id", how="full")
joined = table.join(other, on="id", how="outer")      # alias for "full"
joined = table.join(other, on="id", how="full_outer")  # alias for "full"
```

Both `"outer"` and `"full_outer"` are accepted as aliases for `"full"`, since pandas uses `"outer"` and SQL uses `FULL OUTER JOIN`.

**PyJoinView — new method:**
```python
joined.sync()     # Incremental update, returns bool
joined.refresh()  # Full rebuild (existing)
```

### 8. Testing

**Rust unit tests (view.rs):**

| Test | Coverage |
|------|----------|
| `test_right_join` | Basic RIGHT join: all right rows, NULL left columns for unmatched |
| `test_full_join` | FULL join: all rows from both tables, NULLs on both sides |
| `test_right_join_multiple_matches` | One-to-many: one right row matching multiple left |
| `test_full_join_no_matches` | Disjoint tables: all rows, all cross-columns NULL |
| `test_full_join_null_key_rows` | NULL keys on both sides: included in result, match nothing |
| `test_right_join_sync_left_insert` | Incremental left insert, verify matches rebuild |
| `test_right_join_sync_right_insert` | Incremental right insert, verify matches rebuild |
| `test_full_join_sync_left_insert` | Incremental left insert, verify matches rebuild |
| `test_full_join_sync_right_insert` | Incremental right insert, verify matches rebuild |
| `test_full_join_unmatched_becomes_matched` | Insert matches previously-unmatched row, verify relocation |
| `test_right_join_rebuild_after_delete` | Delete triggers full rebuild, correct result for RIGHT |
| `test_full_join_rebuild_after_delete` | Delete triggers full rebuild, correct result for FULL |

**Python tests (tests/python/test_right_full_joins.py):**

| Test | Coverage |
|------|----------|
| `test_right_join_basic` | `.join(how="right")` correct rows |
| `test_full_join_basic` | `.join(how="full")` and `how="outer"` alias |
| `test_full_join_full_outer_alias` | `.join(how="full_outer")` alias |
| `test_right_join_constructor` | `JoinView(..., JoinType.RIGHT)` |
| `test_full_join_constructor` | `JoinView(..., JoinType.FULL)` |
| `test_right_join_multi_column` | Composite keys with RIGHT join |
| `test_full_join_null_keys` | NULL in join key columns — present but unmatched |
| `test_join_tick_propagation` | `table.tick()` updates JoinView on both parents |
| `test_join_sync_exposed` | `.sync()` callable from Python |
| `test_right_full_iteration` | `for row in joined` works |
| `test_right_full_slicing` | Indexing and slicing |

### 9. Documentation Updates

1. **README.md** — Add RIGHT/FULL to features, add example
2. **CLAUDE.md** — Update Python API Usage and Architecture sections
3. **docs/PYTHON_BINDINGS_README.md** — Full API reference for new join types and `.sync()`
4. **docs/ORIGINAL_VISION.md** — Check off "RIGHT and FULL OUTER joins"

## Non-Goals

- Changing the `right_` column prefix convention
- Supporting JoinView-to-JoinView chaining (joining views, not just tables)
- CROSS joins
- Epsilon-based float key comparison
