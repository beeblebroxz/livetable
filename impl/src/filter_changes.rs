//! Shared filter replay for native Rust predicates and fallible Python callbacks.
//!
//! Changes use indices in the layout immediately before/after each event, not
//! indices in the final parent. Reconstruct only updated rows by following them
//! through later events; never copy the whole source table for incremental sync.

use crate::changeset::{
    apply_filter_row_deleted, apply_filter_row_inserted, Changeset, TableChange,
};
use crate::column::ColumnValue;
use std::collections::HashMap;

type Row = HashMap<String, ColumnValue>;

/// Replay does O(B²) index reconciliation in the worst case. Bound that work
/// and the retained output history; larger batches use a full refresh.
pub(crate) const MAX_FILTER_REPLAY_CHANGES: usize = 256;

/// Recover the row immediately after one CellUpdated event. A later deletion
/// supplies a complete row even if it no longer exists in the live parent.
pub(crate) fn row_after_update<E>(
    changes: &[TableChange],
    change_index: usize,
    mut get_row: impl FnMut(usize) -> Result<Row, E>,
) -> Result<Row, E> {
    let mut index = changes[change_index].row_index();
    let mut undo = Vec::new();
    let mut deleted = None;
    for change in &changes[change_index + 1..] {
        match change {
            TableChange::RowInserted {
                index: inserted, ..
            } if *inserted <= index => index += 1,
            TableChange::RowDeleted {
                index: removed,
                data,
            } if *removed == index => {
                deleted = Some(data.clone());
                break;
            }
            TableChange::RowDeleted { index: removed, .. } if *removed < index => index -= 1,
            TableChange::CellUpdated {
                row,
                column,
                old_value,
                ..
            } if *row == index => {
                undo.push((column, old_value));
            }
            _ => {}
        }
    }
    let mut row = match deleted {
        Some(row) => row,
        None => get_row(index)?,
    };
    for (column, value) in undo.into_iter().rev() {
        row.insert(column.clone(), value.clone());
    }
    Ok(row)
}

pub(crate) struct PreparedFilterChange {
    change: TableChange,
    matched: bool,
    updated_row: Option<Row>,
}

/// Evaluate every predicate before changing indices or publishing history.
/// A failed Python callback therefore leaves the previous sync state intact.
pub(crate) fn prepare_filter_changes<E>(
    changes: &[TableChange],
    mut get_row: impl FnMut(usize) -> Result<Row, E>,
    mut predicate: impl FnMut(&Row) -> Result<bool, E>,
) -> Result<Vec<PreparedFilterChange>, E> {
    let mut prepared = Vec::with_capacity(changes.len());
    for (index, change) in changes.iter().enumerate() {
        let (matched, updated_row) = match change {
            TableChange::RowInserted { data, .. } => (predicate(data)?, None),
            TableChange::RowDeleted { .. } => (false, None),
            TableChange::CellUpdated { .. } => {
                let row = row_after_update(changes, index, &mut get_row)?;
                (predicate(&row)?, Some(row))
            }
        };
        prepared.push(PreparedFilterChange {
            change: change.clone(),
            matched,
            updated_row,
        });
    }
    Ok(prepared)
}

/// Commit an already-evaluated batch and emit changes in FILTER coordinates.
/// Retain one successful batch. Consumers behind its base cursor rebuild.
pub(crate) fn apply_filter_changes(
    indices: &mut Vec<usize>,
    output: &mut Changeset,
    prepared: Vec<PreparedFilterChange>,
) -> bool {
    output.clear();
    for prepared in prepared {
        match prepared.change {
            TableChange::RowInserted { index, data } => {
                let view_index = indices.partition_point(|&p| p < index);
                apply_filter_row_inserted(indices, index, prepared.matched);
                if prepared.matched {
                    output.push(TableChange::RowInserted {
                        index: view_index,
                        data,
                    });
                }
            }
            TableChange::RowDeleted { index, data } => {
                let view_index = indices.binary_search(&index).ok();
                apply_filter_row_deleted(indices, index);
                if let Some(index) = view_index {
                    output.push(TableChange::RowDeleted { index, data });
                }
            }
            TableChange::CellUpdated {
                row,
                column,
                old_value,
                new_value,
            } => match (indices.binary_search(&row), prepared.matched) {
                (Ok(view_index), true) => output.push(TableChange::CellUpdated {
                    row: view_index,
                    column,
                    old_value,
                    new_value,
                }),
                (Ok(view_index), false) => {
                    indices.remove(view_index);
                    let mut data = prepared.updated_row.expect("prepared update row");
                    data.insert(column, old_value);
                    output.push(TableChange::RowDeleted {
                        index: view_index,
                        data,
                    });
                }
                (Err(view_index), true) => {
                    indices.insert(view_index, row);
                    output.push(TableChange::RowInserted {
                        index: view_index,
                        data: prepared.updated_row.expect("prepared update row"),
                    });
                }
                (Err(_), false) => {}
            },
        }
    }
    !output.is_empty()
}
