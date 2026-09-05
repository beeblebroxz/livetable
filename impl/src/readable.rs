//! ReadableTable — the read surface shared by root tables and views.
//!
//! Implemented by `Table` and every view type, so views can derive from
//! views (a DAG over root tables, per the original design vision). Parents
//! are held as `Rc<RefCell<dyn ReadableTable>>`; an `Rc<RefCell<Table>>`
//! coerces implicitly at call sites.
//!
//! Root tables and synchronized filters/sorts expose changesets in their own
//! row coordinates. Filters/sorts retain one bounded batch; a rebuild invalidates
//! the previous history. Consumers refresh if their cursor is outside the retained
//! window. Other view types expose no output history and their children use
//! version-checked rebuilds. Version still includes ancestors for stale-read
//! and iterator guards; an unchanged output stream can skip downstream sync
//! work even when an ancestor's version advanced.
//!
//! Sync ordering: a child reflects its parent *as currently visible*. To
//! bring a whole chain up to date, sync parents before children — which is
//! exactly the order `tick()` registration produces (creation order is
//! topological).

use crate::changeset::Changeset;
use crate::column::{ColumnType, ColumnValue};
use crate::table::Table;
use std::collections::HashMap;

pub trait ReadableTable {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn column_names(&self) -> Vec<String>;

    fn column_index(&self, name: &str) -> Option<usize> {
        self.column_names().iter().position(|c| c == name)
    }

    /// Static type of the column at `col_idx`, when known. Views with
    /// dynamically-typed columns (computed, aggregate results) return None;
    /// callers must treat None as "use the general value-based path".
    fn column_type(&self, col_idx: usize) -> Option<ColumnType> {
        let _ = col_idx;
        None
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String>;

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String>;

    fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        let names = self.column_names();
        let name = names
            .get(col_idx)
            .ok_or_else(|| format!("Column index {} out of range", col_idx))?;
        self.get_value(row, name)
    }

    /// Monotonic content version. Bumped on every mutation (tables) or
    /// whenever a sync/refresh may have changed visible content (views).
    /// Includes the parent's version for views, so a stale ancestor makes
    /// every descendant register as stale.
    fn version(&self) -> u64;

    /// Incremental history in this table/view's own row coordinates. None
    /// means unavailable (including a filter/sort that has not synced its parent).
    /// Consumers must refresh when history does not cover their cursor.
    fn changeset(&self) -> Option<&Changeset> {
        None
    }

    /// Translate a consumer cursor for root-table compaction. A derived view's
    /// sequence is independent of the root's, so it must never constrain it.
    fn root_changeset_cursor(&self, _cursor: usize) -> usize {
        usize::MAX
    }
}

impl ReadableTable for Table {
    fn len(&self) -> usize {
        Table::len(self)
    }

    fn column_names(&self) -> Vec<String> {
        self.schema()
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn column_index(&self, name: &str) -> Option<usize> {
        self.schema().get_column_index(name)
    }

    fn column_type(&self, col_idx: usize) -> Option<ColumnType> {
        self.schema().get_column_info(col_idx).map(|(_, t, _)| t)
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        Table::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        Table::get_value(self, row, column)
    }

    fn get_value_by_index(&self, row: usize, col_idx: usize) -> Result<ColumnValue, String> {
        Table::get_value_by_index(self, row, col_idx)
    }

    fn version(&self) -> u64 {
        Table::version(self)
    }

    fn changeset(&self) -> Option<&Changeset> {
        Some(Table::changeset(self))
    }

    fn root_changeset_cursor(&self, cursor: usize) -> usize {
        cursor
    }
}
