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
Phase 3: Scan left rows:
  - Match found → push (Some(left), Some(right)) for each match
                   add right_idx to matched_right: HashSet<usize>
  - No match + LEFT or FULL → push (Some(left), None)
  - No match + INNER or RIGHT → skip
Phase 4: (RIGHT or FULL only) Scan right rows 0..right_len:
  - If right_idx NOT in matched_right → push (None, Some(right))
```

Row ordering: left-matched rows first in left-table order, unmatched right rows appended in right-table order. This follows standard SQL semantics.

### 4. get_row() / get_value() Changes

- `Some(left_idx)` → fetch from left table (existing behavior)
- `None` left_idx → all left columns return `ColumnValue::Null` (new)
- `Some(right_idx)` → fetch from right table (existing behavior)
- `None` right_idx → all right columns return `ColumnValue::Null` (existing behavior)

### 5. tick() Integration

**RegisteredView enum extension:**
```rust
enum RegisteredView {
    Filter(Rc<RefCell<FilterView>>),
    Sorted(Rc<RefCell<SortedView>>),
    Aggregate(Rc<RefCell<AggregateView>>),
    Join(Rc<RefCell<JoinView>>),    // new
}
```

**Dual-parent registration:** When a JoinView is created via the simplified `.join()` API, register it with both left and right parent tables. The `Rc<RefCell<JoinView>>` is cloned (reference count bumped, same underlying view).

**Idempotent sync:** When `left_table.tick()` fires, it calls `sync()` on the join. If `right_table.tick()` also fires, `sync()` checks its cursors and returns `false` (no new changes). Already idempotent by existing design.

**Expose `.sync()` to Python:** Currently only `.refresh()` (full rebuild) is exposed. Add `.sync()` to `PyJoinView` to match the pattern of `AggregateView`.

**Changeset compaction:** Each parent independently compacts based on its own registered views' cursors. No changes needed — existing logic handles this correctly.

### 6. Incremental Sync Extensions

Existing behavior preserved: deletes and key-column updates trigger full `rebuild_index()` for all join types. Only row inserts are handled incrementally.

**Left-table insert (extending existing logic):**
- Shift all existing `Some(left_idx)` values >= insert position up by 1
- If match found → insert `(Some(new_left), Some(right))` entries
- If no match + LEFT or FULL → insert `(Some(new_left), None)`
- If no match + INNER or RIGHT → skip
- **New:** If this left row matches a previously-unmatched right row `(None, Some(right))`, replace with `(Some(new_left), Some(right))`

**Right-table insert (extending existing logic):**
- Shift all existing `Some(right_idx)` values >= insert position up by 1
- If match found → insert `(Some(left), Some(new_right))` entries
- If no match + RIGHT or FULL → append `(None, Some(new_right))` at end
- If no match + LEFT or INNER → skip
- **Existing:** Replace `(Some(left), None)` with `(Some(left), Some(new_right))` when a new right row matches a previously-unmatched left row (already implemented for LEFT joins)

**Ordering invariant:** Incremental sync must produce the same result as a full `rebuild_index()`. Verified by tests.

### 7. Python API

**JoinType class — new attributes:**
- `JoinType.RIGHT`
- `JoinType.FULL`

**Simplified `.join()` API — `how=` parameter:**
```python
joined = table.join(other, on="id", how="right")
joined = table.join(other, on="id", how="full")
joined = table.join(other, on="id", how="outer")   # alias for "full"
```

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
| `test_right_join_sync_left_insert` | Incremental left insert, verify matches rebuild |
| `test_right_join_sync_right_insert` | Incremental right insert, verify matches rebuild |
| `test_full_join_sync_left_insert` | Incremental left insert, verify matches rebuild |
| `test_full_join_sync_right_insert` | Incremental right insert, verify matches rebuild |
| `test_full_join_unmatched_becomes_matched` | Insert matches previously-unmatched row, verify replacement |

**Python tests (tests/python/test_right_full_joins.py):**

| Test | Coverage |
|------|----------|
| `test_right_join_basic` | `.join(how="right")` correct rows |
| `test_full_join_basic` | `.join(how="full")` and `how="outer"` alias |
| `test_right_join_constructor` | `JoinView(..., JoinType.RIGHT)` |
| `test_full_join_constructor` | `JoinView(..., JoinType.FULL)` |
| `test_right_join_multi_column` | Composite keys with RIGHT join |
| `test_full_join_null_keys` | NULL in join key columns |
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
