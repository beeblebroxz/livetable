//! One-pass batch replay for `SortedView`.
//!
//! Applying a batch one event at a time shifts the view's three vectors for
//! every structural event, O(batch × rows). Here a batch is applied in two
//! phases instead:
//!
//! 1. `resolve_batch` walks the events against the vectors as they were
//!    before the batch, which it never changes, and works out each event's
//!    sorted position. It emits the same events the per-event path emits.
//! 2. `commit_batch` rewrites each vector once.
//!
//! During phase 1 a live row is in one of two places. An *anchored* row is a
//! pre-batch row still in its slot of the frozen `sorted_index`. A *floating*
//! row was inserted by the batch, or is a pre-batch row whose sort key the
//! batch changed; it is kept in a small list in view order. A pre-batch row
//! that was deleted or re-keyed leaves a *tombstone* at its frozen slot.
//!
//! A row's view position is then a sum of two ranks: the frozen entries
//! ordered before it, less the tombstones among them, plus the floating rows
//! ordered before it.
//!
//! See docs/superpowers/specs/2026-11-09-batch-replay-costs-design.md.

use super::{SortKey, SortedView};
use crate::changeset::TableChange;
use crate::column::ColumnValue;
use std::cmp::Ordering;
use std::collections::HashMap;

type Row = HashMap<String, ColumnValue>;

/// A row's identity through a batch.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Id {
    /// A pre-batch row, by its pre-batch parent index.
    Old(usize),
    /// A row the batch inserted, numbered in insertion order.
    New(usize),
}

/// The parent's rows in their current order: runs of pre-batch rows that are
/// still consecutive, and rows the batch inserted.
#[derive(Clone, Copy)]
enum Segment {
    Run { start: usize, len: usize },
    New(usize),
}

struct Floating {
    id: Id,
    /// One value per sort key.
    keys: Vec<ColumnValue>,
    /// Where the row stands in parent order, which breaks ties between equal
    /// keys. A pre-batch row's anchor is its own pre-batch index. An inserted
    /// row's anchor is the pre-batch row it was inserted before (the row
    /// count if none follows): it comes after every pre-batch row below the
    /// anchor and before the rest. Neither changes during the batch, because
    /// inserts and deletes never reorder the other rows.
    anchor: usize,
    /// How many frozen `sorted_index` entries are ordered before this row.
    boundary: usize,
}

/// What `resolve_batch` leaves for `commit_batch`.
pub(super) struct Resolved {
    segments: Vec<Segment>,
    /// Frozen view positions of deleted and re-keyed pre-batch rows, sorted.
    tombstones: Vec<usize>,
    /// In view order.
    floating: Vec<Floating>,
    /// How many rows the batch inserted, including ones it deleted again.
    inserted: usize,
    pub(super) output: Vec<TableChange>,
}

fn compare_keys(sort_keys: &[SortKey], a: &[ColumnValue], b: &[ColumnValue]) -> Ordering {
    for (index, key) in sort_keys.iter().enumerate() {
        let cmp = SortedView::compare_values(&a[index], &b[index], key);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

struct Resolver<'a> {
    view: &'a SortedView,
    segments: Vec<Segment>,
    /// The parent's current row count.
    rows: usize,
    tombstones: Vec<usize>,
    floating: Vec<Floating>,
    /// Each inserted row's anchor, by `Id::New` number.
    new_anchors: Vec<usize>,
}

impl Resolver<'_> {
    /// The segment holding current parent row `index`, the row's offset in
    /// it, and the row. None if the index is out of range.
    fn locate(&self, index: usize) -> Option<(usize, usize, Id)> {
        let mut at = 0;
        for (segment, entry) in self.segments.iter().enumerate() {
            match *entry {
                Segment::Run { start, len } => {
                    if index < at + len {
                        return Some((segment, index - at, Id::Old(start + index - at)));
                    }
                    at += len;
                }
                Segment::New(id) => {
                    if index == at {
                        return Some((segment, 0, Id::New(id)));
                    }
                    at += 1;
                }
            }
        }
        None
    }

    /// Record a new row at current parent row `index` and return its `Id`
    /// number and anchor.
    fn insert_row(&mut self, index: usize) -> Option<(usize, usize)> {
        let id = self.new_anchors.len();
        let anchor = if index == self.rows {
            self.segments.push(Segment::New(id));
            self.view.sorted_index.len()
        } else {
            let (segment, offset, next) = self.locate(index)?;
            match self.segments[segment] {
                Segment::Run { start, len } if offset > 0 => {
                    self.segments[segment] = Segment::Run { start, len: offset };
                    self.segments.insert(segment + 1, Segment::New(id));
                    self.segments.insert(
                        segment + 2,
                        Segment::Run {
                            start: start + offset,
                            len: len - offset,
                        },
                    );
                }
                _ => self.segments.insert(segment, Segment::New(id)),
            }
            match next {
                Id::Old(row) => row,
                Id::New(next) => self.new_anchors[next],
            }
        };
        self.new_anchors.push(anchor);
        self.rows += 1;
        Some((id, anchor))
    }

    fn remove_row(&mut self, segment: usize, offset: usize) {
        match self.segments[segment] {
            Segment::New(_) | Segment::Run { len: 1, .. } => {
                self.segments.remove(segment);
            }
            Segment::Run { start, len } if offset == 0 => {
                self.segments[segment] = Segment::Run {
                    start: start + 1,
                    len: len - 1,
                };
            }
            Segment::Run { start, len } if offset == len - 1 => {
                self.segments[segment] = Segment::Run {
                    start,
                    len: len - 1,
                };
            }
            Segment::Run { start, len } => {
                self.segments[segment] = Segment::Run { start, len: offset };
                self.segments.insert(
                    segment + 1,
                    Segment::Run {
                        start: start + offset + 1,
                        len: len - offset - 1,
                    },
                );
            }
        }
        self.rows -= 1;
    }

    fn tombstones_before(&self, frozen_position: usize) -> usize {
        self.tombstones.partition_point(|&t| t < frozen_position)
    }

    /// A live row's current view position.
    fn position(&self, id: Id) -> usize {
        if let Some(index) = self.floating.iter().position(|f| f.id == id) {
            let boundary = self.floating[index].boundary;
            return boundary - self.tombstones_before(boundary) + index;
        }
        let Id::Old(row) = id else {
            unreachable!("an inserted row floats until it is deleted")
        };
        let frozen = self.view.parent_to_view[row];
        frozen - self.tombstones_before(frozen)
            + self.floating.partition_point(|f| f.boundary <= frozen)
    }

    /// Take a live row out of the view order, returning its keys and anchor:
    /// a floating row leaves the list, an anchored row leaves a tombstone.
    fn lift(&mut self, id: Id) -> (Vec<ColumnValue>, usize) {
        if let Some(index) = self.floating.iter().position(|f| f.id == id) {
            let floating = self.floating.remove(index);
            return (floating.keys, floating.anchor);
        }
        let Id::Old(row) = id else {
            unreachable!("an inserted row floats until it is deleted")
        };
        let frozen = self.view.parent_to_view[row];
        let at = self.tombstones_before(frozen);
        self.tombstones.insert(at, frozen);
        let keys = self
            .view
            .sort_values
            .iter()
            .map(|column| column[row].clone())
            .collect();
        (keys, row)
    }

    /// Whether floating row `other` is ordered before a row with these keys
    /// and this place in parent order.
    fn precedes(&self, other: &Floating, id: Id, keys: &[ColumnValue], anchor: usize) -> bool {
        match compare_keys(&self.view.sort_keys, &other.keys, keys) {
            Ordering::Less => true,
            Ordering::Greater => false,
            // Equal keys follow parent order. An inserted row comes before
            // the pre-batch row at its anchor.
            Ordering::Equal => {
                let old = |id| matches!(id, Id::Old(_));
                match (other.anchor, old(other.id)).cmp(&(anchor, old(id))) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    // Two inserted rows before the same pre-batch row: the
                    // segment list has them in parent order.
                    Ordering::Equal => self
                        .segments
                        .iter()
                        .find_map(|segment| match *segment {
                            Segment::New(new) if Id::New(new) == other.id => Some(true),
                            Segment::New(new) if Id::New(new) == id => Some(false),
                            _ => None,
                        })
                        .unwrap_or(false),
                }
            }
        }
    }

    /// Put a row into the view order as a floating row and return its view
    /// position.
    fn place(&mut self, id: Id, keys: Vec<ColumnValue>, anchor: usize) -> usize {
        let view = self.view;
        // The frozen index is sorted by key, then parent row, and stays
        // sorted with tombstones left in it. A pre-batch row with equal keys
        // comes first exactly when it is below the anchor.
        let boundary = view.sorted_index.partition_point(|&row| {
            for (index, key) in view.sort_keys.iter().enumerate() {
                let cmp =
                    SortedView::compare_values(&view.sort_values[index][row], &keys[index], key);
                if cmp != Ordering::Equal {
                    return cmp == Ordering::Less;
                }
            }
            row < anchor
        });
        let index = self
            .floating
            .partition_point(|other| self.precedes(other, id, &keys, anchor));
        self.floating.insert(
            index,
            Floating {
                id,
                keys,
                anchor,
                boundary,
            },
        );
        boundary - self.tombstones_before(boundary) + index
    }
}

impl SortedView {
    /// Phase 1: every event's output, and the bookkeeping `commit_batch`
    /// needs, without changing the view. `updated_rows` holds the row after
    /// each sort-key update, by change index. None if an event's index is
    /// out of range; the caller rebuilds.
    pub(super) fn resolve_batch(
        &self,
        changes: &[TableChange],
        mut updated_rows: HashMap<usize, Row>,
    ) -> Option<Resolved> {
        let rows = self.sorted_index.len();
        let mut resolver = Resolver {
            view: self,
            segments: if rows == 0 {
                Vec::new()
            } else {
                vec![Segment::Run {
                    start: 0,
                    len: rows,
                }]
            },
            rows,
            tombstones: Vec::new(),
            floating: Vec::new(),
            new_anchors: Vec::new(),
        };
        let mut output = Vec::with_capacity(changes.len());
        for (change_index, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowInserted { index, data } => {
                    if *index > resolver.rows {
                        return None;
                    }
                    let (id, anchor) = resolver.insert_row(*index)?;
                    let keys = self
                        .sort_keys
                        .iter()
                        .map(|key| data.get(&key.column).cloned().unwrap_or(ColumnValue::Null))
                        .collect();
                    let position = resolver.place(Id::New(id), keys, anchor);
                    output.push(TableChange::RowInserted {
                        index: position,
                        data: data.clone(),
                    });
                }
                TableChange::RowDeleted { index, data } => {
                    let (segment, offset, id) = resolver.locate(*index)?;
                    let position = resolver.position(id);
                    resolver.lift(id);
                    resolver.remove_row(segment, offset);
                    output.push(TableChange::RowDeleted {
                        index: position,
                        data: data.clone(),
                    });
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    let (_, _, id) = resolver.locate(*row)?;
                    let position = resolver.position(id);
                    let Some(data) = updated_rows.remove(&change_index) else {
                        // Not a sort key: the row stays where it is.
                        output.push(TableChange::CellUpdated {
                            row: position,
                            column: column.clone(),
                            old_value: old_value.clone(),
                            new_value: new_value.clone(),
                        });
                        continue;
                    };
                    let (mut keys, anchor) = resolver.lift(id);
                    // A column can appear in more than one SortKey.
                    for (key, value) in self.sort_keys.iter().zip(&mut keys) {
                        if key.column == *column {
                            *value = new_value.clone();
                        }
                    }
                    let new_position = resolver.place(id, keys, anchor);
                    if new_position == position {
                        output.push(TableChange::CellUpdated {
                            row: position,
                            column: column.clone(),
                            old_value: old_value.clone(),
                            new_value: new_value.clone(),
                        });
                    } else {
                        let mut old_data = data.clone();
                        old_data.insert(column.clone(), old_value.clone());
                        output.push(TableChange::RowDeleted {
                            index: position,
                            data: old_data,
                        });
                        output.push(TableChange::RowInserted {
                            index: new_position,
                            data,
                        });
                    }
                }
            }
        }
        Some(Resolved {
            segments: resolver.segments,
            tombstones: resolver.tombstones,
            floating: resolver.floating,
            inserted: resolver.new_anchors.len(),
            output,
        })
    }

    /// Phase 2: bring the three vectors to the state after the batch, each
    /// in one pass, and return the batch's output events.
    pub(super) fn commit_batch(&mut self, resolved: Resolved) -> Vec<TableChange> {
        let Resolved {
            segments,
            tombstones,
            floating,
            inserted,
            output,
        } = resolved;
        let rows = self.sorted_index.len();

        // Final parent rows. `unmoved` pre-batch rows at the front keep
        // their index; `remap` holds the rest (usize::MAX: deleted).
        let mut new_rows = vec![0usize; inserted];
        let mut live_new_rows = Vec::new();
        let mut unmoved = 0;
        let mut identical = true;
        let mut at = 0;
        for segment in &segments {
            match *segment {
                Segment::Run { start, len } => {
                    if identical && start == at {
                        unmoved = at + len;
                    } else {
                        identical = false;
                    }
                    at += len;
                }
                Segment::New(id) => {
                    identical = false;
                    new_rows[id] = at;
                    live_new_rows.push(at);
                    at += 1;
                }
            }
        }
        let final_rows = at;
        if tombstones.is_empty() && floating.is_empty() && unmoved == rows {
            // Only edits to columns that are not sort keys.
            return output;
        }
        let mut remap = vec![usize::MAX; rows - unmoved];
        let mut at = 0;
        for segment in &segments {
            match *segment {
                Segment::Run { start, len } => {
                    for offset in 0..len {
                        if start + offset >= unmoved {
                            remap[start + offset - unmoved] = at + offset;
                        }
                    }
                    at += len;
                }
                Segment::New(_) => at += 1,
            }
        }
        let final_row = |row: usize| {
            if row < unmoved {
                row
            } else {
                remap[row - unmoved]
            }
        };
        let floating_row = |floating: &Floating| match floating.id {
            Id::Old(row) => final_row(row),
            Id::New(id) => new_rows[id],
        };

        // Sort keys, in parent order: drop deleted rows, open a gap for each
        // inserted row from the end backwards, then write every floating
        // row's keys (inserted rows into their gaps, re-keyed rows in place).
        let deleted = rows + live_new_rows.len() - final_rows;
        for (index, column) in self.sort_values.iter_mut().enumerate() {
            if deleted > 0 {
                let mut row = 0;
                column.retain(|_| {
                    let keep = row < unmoved || remap[row - unmoved] != usize::MAX;
                    row += 1;
                    keep
                });
            }
            let mut source = column.len();
            let mut target = final_rows;
            column.resize(final_rows, ColumnValue::Null);
            for &new_row in live_new_rows.iter().rev() {
                // The rows after this gap, up to the previous one, move
                // right past the gaps still to open; Nulls take their place.
                let moved = target - 1 - new_row;
                column[source - moved..target].rotate_right(target - source);
                source -= moved;
                target = new_row;
            }
            for row in &floating {
                column[floating_row(row)] = row.keys[index].clone();
            }
        }

        // The sorted index, from the first position the batch touched: merge
        // the surviving frozen entries with the floating rows.
        let first = tombstones
            .first()
            .copied()
            .into_iter()
            .chain(floating.first().map(|row| row.boundary))
            .min()
            .unwrap_or(rows);
        if unmoved < rows {
            for row in &mut self.sorted_index[..first] {
                *row = final_row(*row);
            }
        }
        let tail = self.sorted_index.split_off(first);
        self.sorted_index.reserve(final_rows - first);
        // Copy the stretches of frozen entries between the positions where
        // a floating row goes in or a tombstone comes out.
        let mut floating_rows = floating.iter().peekable();
        let mut dead = tombstones.iter().copied().peekable();
        let mut frozen = first;
        while frozen < rows {
            while let Some(floating) = floating_rows.next_if(|row| row.boundary == frozen) {
                self.sorted_index.push(floating_row(floating));
            }
            if dead.next_if_eq(&frozen).is_some() {
                frozen += 1;
                continue;
            }
            let next_floating = floating_rows.peek().map_or(rows, |row| row.boundary);
            let next_dead = dead.peek().copied().unwrap_or(rows);
            let end = next_floating.min(next_dead);
            let stretch = &tail[frozen - first..end - first];
            if unmoved < rows {
                self.sorted_index
                    .extend(stretch.iter().map(|&row| final_row(row)));
            } else {
                self.sorted_index.extend_from_slice(stretch);
            }
            frozen = end;
        }
        for floating in floating_rows {
            self.sorted_index.push(floating_row(floating));
        }
        debug_assert_eq!(self.sorted_index.len(), final_rows);

        // The inverse: every row whose view position or parent row changed.
        self.parent_to_view.resize(final_rows, 0);
        let from = if unmoved < rows { 0 } else { first };
        for position in from..final_rows {
            self.parent_to_view[self.sorted_index[position]] = position;
        }
        output
    }
}
