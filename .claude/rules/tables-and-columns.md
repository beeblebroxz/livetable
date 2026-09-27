---
paths:
  - "impl/src/column.rs"
  - "impl/src/column/**"
  - "impl/src/sequence.rs"
  - "impl/src/table.rs"
  - "impl/src/interner.rs"
  - "impl/src/expr.rs"
  - "impl/src/python_bindings.rs"
---

# Tables, columns and expressions

- `ColumnData` selects `Sequence<T>` once per column; never reintroduce a per-cell enum or an interned-string placeholder buffer. `ColumnValue` stays at API/event boundaries. Numeric `get_f64()` must preserve NULL/type semantics and float bits.
- `NullBitmap` packs array flags into u64 words; tiered flags use circular bit blocks with O(1) addressing and amortized O(sqrt(N)) middle edits. A flat shifting bitmap would regress the tiered backend. Keep masks/value buffers aligned through growth, shrink, and rollback.
- Column mutations prevalidate type/bounds and acquire fallible interner locks before changing any state. Updates acquire a new string reference before releasing the old; delete/truncate/drop release owned IDs. Drop also locks the shared interner: callers must not hold its mutex while destroying a column. See `docs/TYPED_COLUMN_STORAGE.md`.
- `Table::from_json`/`from_csv` infer each column's type by scanning all rows and unifying (INT32 → INT64 → FLOAT64, DATE → DATETIME, date-ish ⊔ plain string → STRING; all-null/empty → STRING); values are then converted against the inferred schema, not in isolation. JSON rejects incompatible mixes (number + string) at inference with a clear error; CSV falls back to STRING since every CSV value is a string at heart.
- Interned string buffers (`ColumnData::StringIds`) use `NULL_STRING_ID` (u32::MAX) as null sentinel — never use 0
- `Column::check_value_type(&value)` validates without consuming — use before batch mutations
- Expression parser (`expr.rs`): Lexer has `input: Vec<char>` + `pos: usize`, supports negative literals
- `filter_expr` uses SQL NULL semantics: all comparisons with NULL return false, use IS NULL/IS NOT NULL
