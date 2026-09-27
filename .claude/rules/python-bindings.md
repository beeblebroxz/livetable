---
paths:
  - "impl/src/python_bindings.rs"
  - "impl/src/python_bindings/**"
  - "tests/python/**"
  - "docs/PYTHON_BINDINGS_README.md"
---

# Python bindings

- Python filter/computed callbacks still execute Python; they are not compiled into native predicates. Filter callbacks are fallible and must return a bool.
- Python chaining supports `FilterView.sort()/group_by()`, `SortedView.group_by()`, and `JoinView.filter()/sort()/group_by()`, not every Rust DAG. Simplified stateful methods auto-register. Views chained on a join register on both joined tables (after the join, once per distinct registry via `register_on_roots`), so either table's `tick()` updates them. Explicit constructors normally do not register; `SortedView.group_by()` registers its sort parent once and an explicit `JoinView` registers itself on first chaining, but children of an explicit filter still require that filter to be synced manually first. The Python filter (`PyFilterViewInner`) accepts any `ReadableTable` parent and translates its cursor with `root_changeset_cursor()` for compaction; tick registries hold weak references, so keep chained views alive. Root `tick()` does nothing without pending root changes: after a manual parent refresh, sync children directly.
- `Table.__new__` Python API: `storage=` accepts "fast_reads"|"fast_updates", `use_string_interning=` is bool
