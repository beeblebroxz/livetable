//! JoinView — LEFT/INNER/RIGHT/FULL joins with incremental sync.

use crate::changeset::{Changeset, TableChange};
use crate::column::ColumnValue;
use crate::filter_changes::{row_after_update, row_before_batch, MAX_FILTER_REPLAY_CHANGES};
use crate::readable::ReadableTable;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};
use std::ops::Range;
use std::rc::Rc;

use super::{column_value_into_join_key_part, column_value_to_join_key_part, JoinKey, JoinKeyPart};

/// Join type specification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Left join: All rows from left table, matched rows from right (nulls if no match)
    Left,
    /// Inner join: Only rows that match in both tables
    Inner,
    /// Right join: All rows from right table, matched rows from left (nulls if no match)
    Right,
    /// Full outer join: All rows from both tables (nulls where no match on either side)
    Full,
}

type Row = HashMap<String, ColumnValue>;
/// (left row, right row); `None` is the unmatched side.
type Entry = (Option<usize>, Option<usize>);

/// Key hash of a row whose join key is NULL or NaN: it never matches.
const NO_KEY: u64 = u64::MAX;

/// Hash of a join key for the per-side key-hash vectors. Unit tests keep only
/// two bits, so distinct keys share hashes and every match must be verified.
fn key_hash(key: Option<&JoinKey>) -> u64 {
    let Some(key) = key else {
        return NO_KEY;
    };
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = if cfg!(test) { hasher.finish() & 0b11 } else { hasher.finish() };
    if hash == NO_KEY {
        NO_KEY - 1
    } else {
        hash
    }
}

/// Hasher for keys that are already hashes.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("IdentityHasher only hashes u64 keys")
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

type HashedMap<V> = HashMap<u64, V, BuildHasherDefault<IdentityHasher>>;

/// A batch whose output would exceed this invalidates its history instead:
/// consumers rebuild above their own replay bounds anyway.
const MAX_JOIN_OUTPUT_CHANGES: usize = MAX_FILTER_REPLAY_CHANGES * 2;

/// One incremental batch's output events. Past the bound it stops recording,
/// and building payloads; the batch then invalidates history.
#[derive(Default)]
struct JoinOutput {
    changes: Vec<TableChange>,
    overflowed: bool,
}

impl JoinOutput {
    fn record(
        &mut self,
        change: impl FnOnce() -> Result<TableChange, String>,
    ) -> Result<(), String> {
        if self.overflowed {
            return Ok(());
        }
        if self.changes.len() == MAX_JOIN_OUTPUT_CHANGES {
            self.overflowed = true;
            self.changes = Vec::new();
            return Ok(());
        }
        self.changes.push(change()?);
        Ok(())
    }

    fn emitted(&self) -> bool {
        self.overflowed || !self.changes.is_empty()
    }
}

/// Where one half of an emitted output row comes from.
#[derive(Clone, Copy)]
enum Half<'a> {
    /// The parent row as of the change being applied, from its changeset.
    Given(&'a Row),
    /// Read live from the parent; `None` is all Nulls.
    Live(Option<usize>),
}

/// A right-parent row with its columns named as the join outputs them.
fn prefix_right(row: Row) -> Row {
    row.into_iter()
        .map(|(column, value)| (format!("right_{column}"), value))
        .collect()
}

/// A JoinView combines two tables based on matching column values.
///
/// Supports:
/// - Left Join: All rows from left table, matched rows from right (nulls if no match)
/// - Inner Join: Only rows that match in both tables
/// - Right Join: All rows from right table, matched rows from left (nulls if no match)
/// - Full Outer Join: All rows from both tables (nulls where no match)
///
/// # Examples
///
/// ```
/// use livetable::{Table, Schema, ColumnType, ColumnValue, JoinView, JoinType};
/// use std::rc::Rc;
/// use std::cell::RefCell;
/// use std::collections::HashMap;
///
/// // Create users table
/// let users_schema = Schema::new(vec![
///     ("user_id".to_string(), ColumnType::Int32, false),
///     ("name".to_string(), ColumnType::String, false),
/// ]);
/// let users = Rc::new(RefCell::new(Table::new("users".to_string(), users_schema)));
///
/// // Create orders table
/// let orders_schema = Schema::new(vec![
///     ("order_id".to_string(), ColumnType::Int32, false),
///     ("user_id".to_string(), ColumnType::Int32, false),
///     ("amount".to_string(), ColumnType::Float64, false),
/// ]);
/// let orders = Rc::new(RefCell::new(Table::new("orders".to_string(), orders_schema)));
///
/// // Left join users with orders on user_id
/// let joined = JoinView::new(
///     "user_orders".to_string(),
///     users.clone(),
///     orders.clone(),
///     "user_id".to_string(),
///     "user_id".to_string(),
///     JoinType::Left,
/// ).unwrap();
/// ```
pub struct JoinView {
    name: String,
    left_table: Rc<RefCell<dyn ReadableTable>>,
    right_table: Rc<RefCell<dyn ReadableTable>>,
    /// Column names in left table to join on (supports multi-column joins)
    left_keys: Vec<String>,
    /// Column names in right table to join on (supports multi-column joins)
    right_keys: Vec<String>,
    join_type: JoinType,
    /// Cached joined rows: (optional_left_row_index, optional_right_row_index)
    /// - INNER: always (Some(left), Some(right))
    /// - LEFT: (Some(left), Some(right)) or (Some(left), None)
    /// - RIGHT: (Some(left), Some(right)) or (None, Some(right))
    /// - FULL: all three patterns
    join_index: Vec<(Option<usize>, Option<usize>)>,
    /// Cached column names from parent schemas — captured at construction so
    /// `get_row` does not clone schemas on every call (schemas are immutable
    /// after Table construction in this crate).
    left_column_names: Vec<String>,
    right_column_names: Vec<String>,
    /// Number of changes already processed from left table (absolute index).
    /// usize::MAX without a coherent baseline. root_changeset_cursors()
    /// translates both parent cursors for root compaction.
    left_last_processed_change_count: usize,
    /// Number of changes already processed from right table (absolute index)
    right_last_processed_change_count: usize,
    /// Own sync counter; the visible version() adds both parents' versions.
    sync_count: u64,
    /// Parent versions observed at the last sync/rebuild.
    last_left_parent_version: u64,
    last_right_parent_version: u64,
    /// The latest incremental batch in JOIN coordinates (join_index
    /// positions). Rebuilds and oversized batches invalidate it.
    output_changes: Changeset,
    /// Each parent row's `key_hash`, by position, once first needed, in the
    /// frame the join has applied for that side (see `matching_rows`). Read
    /// only to find matches. Maintained by that side's structural events;
    /// rebuilds drop it.
    left_key_hashes: Option<Vec<u64>>,
    right_key_hashes: Option<Vec<u64>>,
    /// Parent row counts as of the last applied event.
    left_rows: usize,
    right_rows: usize,
    /// During phase 1 of a two-sided sync, the right batch not yet applied:
    /// every right read then reconstructs the pre-batch row consumers saw.
    right_before: Option<Vec<TableChange>>,
}

impl JoinView {
    /// Creates a new join view with single-column join keys.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for this view
    /// * `left_table` - Left table (all rows included in left join)
    /// * `right_table` - Right table (matched rows included)
    /// * `left_key` - Column name in left table to join on
    /// * `right_key` - Column name in right table to join on
    /// * `join_type` - Type of join (Left, Inner, Right, or Full)
    ///
    /// # Returns
    ///
    /// Result containing the JoinView or an error if columns don't exist
    pub fn new(
        name: String,
        left_table: Rc<RefCell<dyn ReadableTable>>,
        right_table: Rc<RefCell<dyn ReadableTable>>,
        left_key: String,
        right_key: String,
        join_type: JoinType,
    ) -> Result<Self, String> {
        Self::new_multi(
            name,
            left_table,
            right_table,
            vec![left_key],
            vec![right_key],
            join_type,
        )
    }

    /// Creates a new join view with multi-column join keys.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for this view
    /// * `left_table` - Left table (all rows included in left join)
    /// * `right_table` - Right table (matched rows included)
    /// * `left_keys` - Column names in left table to join on
    /// * `right_keys` - Column names in right table to join on
    /// * `join_type` - Type of join (Left, Inner, Right, or Full)
    ///
    /// # Returns
    ///
    /// Result containing the JoinView or an error if columns don't exist
    /// or key counts don't match
    pub fn new_multi(
        name: String,
        left_table: Rc<RefCell<dyn ReadableTable>>,
        right_table: Rc<RefCell<dyn ReadableTable>>,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        join_type: JoinType,
    ) -> Result<Self, String> {
        // Validate key counts match
        if left_keys.len() != right_keys.len() {
            return Err(format!(
                "Join key count mismatch: left has {} keys, right has {} keys",
                left_keys.len(),
                right_keys.len()
            ));
        }

        if left_keys.is_empty() {
            return Err("At least one join key is required".to_string());
        }

        // Validate all left keys exist
        {
            let left = left_table.borrow();
            for key in &left_keys {
                if left.column_index(key).is_none() {
                    return Err(format!("Left table missing column '{}'", key));
                }
            }
        }

        // Validate all right keys exist
        {
            let right = right_table.borrow();
            for key in &right_keys {
                if right.column_index(key).is_none() {
                    return Err(format!("Right table missing column '{}'", key));
                }
            }
        }

        let left_change_count = left_table
            .borrow()
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        let right_change_count = right_table
            .borrow()
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        let left_version = left_table.borrow().version();
        let right_version = right_table.borrow().version();

        // Snapshot column names once — schemas are immutable post-construction,
        // so we never need to re-read them on each get_row call.
        let left_column_names: Vec<String> = left_table.borrow().column_names();
        let right_column_names: Vec<String> = right_table.borrow().column_names();

        let mut view = JoinView {
            name,
            left_table,
            right_table,
            left_keys,
            right_keys,
            join_type,
            join_index: Vec::new(),
            left_column_names,
            right_column_names,
            left_last_processed_change_count: left_change_count,
            right_last_processed_change_count: right_change_count,
            sync_count: 0,
            last_left_parent_version: left_version,
            last_right_parent_version: right_version,
            output_changes: Changeset::new(),
            left_key_hashes: None,
            right_key_hashes: None,
            left_rows: 0,
            right_rows: 0,
            right_before: None,
        };

        view.rebuild_index();
        Ok(view)
    }

    /// Build a typed composite key from a HashMap row (used in incremental sync).
    /// Returns None if any key column is missing, NULL, or contains a NaN float
    /// (SQL semantics: NaN never equals anything, so NaN keys can't participate).
    /// IMPORTANT: Output must match build_key_from_indices structurally.
    fn build_composite_key(row: &HashMap<String, ColumnValue>, keys: &[String]) -> Option<JoinKey> {
        let mut parts: Vec<JoinKeyPart> = Vec::with_capacity(keys.len());
        for key in keys {
            let value = row.get(key)?;
            parts.push(column_value_to_join_key_part(value)?);
        }
        Some(parts)
    }

    /// Build a typed composite key from column values at given indices.
    /// Returns None if any key column is NULL or NaN — these rows are excluded
    /// from joins per SQL semantics.
    fn build_key_from_indices(
        table: &dyn ReadableTable,
        row: usize,
        col_indices: &[usize],
    ) -> Option<JoinKey> {
        let mut parts: Vec<JoinKeyPart> = Vec::with_capacity(col_indices.len());
        for &col_idx in col_indices {
            let value = table.get_value_by_index(row, col_idx).ok()?;
            parts.push(column_value_into_join_key_part(value)?);
        }
        Some(parts)
    }

    /// Rebuilds the join index by scanning both tables.
    /// Unified 4-phase algorithm for all join types (INNER, LEFT, RIGHT, FULL).
    fn rebuild_index(&mut self) {
        self.join_index.clear();

        let left = self.left_table.borrow();
        let right = self.right_table.borrow();

        // Phase 1: Pre-compute column indices for join keys (done once, not per row)
        let left_col_indices: Vec<usize> = self
            .left_keys
            .iter()
            .filter_map(|k| left.column_index(k))
            .collect();
        let right_col_indices: Vec<usize> = self
            .right_keys
            .iter()
            .filter_map(|k| right.column_index(k))
            .collect();

        // Phase 2: Build a hashmap of right table values for efficient lookup
        let mut right_index: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        for i in 0..right.len() {
            if let Some(key) = Self::build_key_from_indices(&*right, i, &right_col_indices) {
                right_index.entry(key).or_default().push(i);
            }
        }

        // Phase 3: Scan left rows — find matching right rows
        let mut matched_right: HashSet<usize> = HashSet::new();

        for i in 0..left.len() {
            if let Some(key) = Self::build_key_from_indices(&*left, i, &left_col_indices) {
                if let Some(matching_indices) = right_index.get(&key) {
                    // Found matches - add each combination
                    for &right_idx in matching_indices {
                        self.join_index.push((Some(i), Some(right_idx)));
                        matched_right.insert(right_idx);
                    }
                } else {
                    // Non-NULL key, no match
                    match self.join_type {
                        JoinType::Left | JoinType::Full => {
                            self.join_index.push((Some(i), None));
                        }
                        JoinType::Inner | JoinType::Right => {
                            // Skip - no match means not included
                        }
                    }
                }
            } else {
                // NULL key — row exists but can never match anything
                match self.join_type {
                    JoinType::Left | JoinType::Full => {
                        self.join_index.push((Some(i), None));
                    }
                    JoinType::Inner | JoinType::Right => {
                        // Skip
                    }
                }
            }
        }

        // Phase 4: (RIGHT/FULL only) Scan right rows for unmatched entries
        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
            for i in 0..right.len() {
                if !matched_right.contains(&i) {
                    self.join_index.push((None, Some(i)));
                }
            }
        }

        // Update cursor trackers (one per parent); MAX = no coherent history.
        self.left_last_processed_change_count =
            left.changeset().map_or(usize::MAX, |cs| cs.total_len());
        self.right_last_processed_change_count =
            right.changeset().map_or(usize::MAX, |cs| cs.total_len());
        self.last_left_parent_version = left.version();
        self.last_right_parent_version = right.version();
        self.left_rows = left.len();
        self.right_rows = right.len();
        drop(left);
        drop(right);
        self.left_key_hashes = None;
        self.right_key_hashes = None;
        self.output_changes.invalidate();
        self.sync_count += 1;
    }

    /// Rows of one parent whose key equals each target key, ascending, in the
    /// frame the join has applied for that parent: `rows` rows, plus
    /// `before`, its batch not yet applied (phase 1 of a two-sided sync).
    /// One scan of the parent's key-hash vector finds candidates; each is
    /// verified against its key in that frame, as hashes can collide. A vector
    /// not built yet is built from the live parent, then taken back through
    /// `before` in reverse. The early return keeps the value-only pass that
    /// runs first from ever building one. A parent whose live length
    /// disagrees with its changeset is an error, which rebuilds, so a vector
    /// can never fall out of step.
    fn matching_rows(
        hashes: &mut Option<Vec<u64>>,
        table: &RefCell<dyn ReadableTable>,
        keys: &[String],
        rows: usize,
        before: Option<&[TableChange]>,
        targets: Vec<JoinKey>,
    ) -> Result<HashMap<JoinKey, Vec<usize>>, String> {
        let mut found: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        if targets.is_empty() {
            return Ok(found);
        }
        let table = table.borrow();
        let batch = before.unwrap_or(&[]);
        let live_rows = batch.iter().fold(rows, |rows, change| match change {
            TableChange::RowInserted { .. } => rows + 1,
            TableChange::RowDeleted { .. } => rows - 1,
            TableChange::CellUpdated { .. } => rows,
        });
        if table.len() != live_rows {
            return Err("parent rows disagree with its changeset".to_string());
        }
        let cols: Vec<usize> = keys.iter().filter_map(|k| table.column_index(k)).collect();
        if hashes.is_none() {
            let mut built: Vec<u64> = (0..table.len())
                .map(|row| key_hash(Self::build_key_from_indices(&*table, row, &cols).as_ref()))
                .collect();
            for (t, change) in batch.iter().enumerate().rev() {
                match change {
                    TableChange::RowInserted { index, .. } => {
                        built.remove(*index);
                    }
                    TableChange::RowDeleted { index, data } => {
                        built.insert(*index, key_hash(Self::build_composite_key(data, keys).as_ref()));
                    }
                    TableChange::CellUpdated {
                        row,
                        column,
                        old_value,
                        ..
                    } if keys.contains(column) => {
                        let mut prior = row_after_update(batch, t, |i| table.get_row(i))?;
                        prior.insert(column.clone(), old_value.clone());
                        built[*row] = key_hash(Self::build_composite_key(&prior, keys).as_ref());
                    }
                    TableChange::CellUpdated { .. } => {}
                }
            }
            *hashes = Some(built);
        }
        let hashes = hashes.as_ref().expect("built above");
        let key_at = |row: usize| -> Result<Option<JoinKey>, String> {
            match before {
                Some(batch) => {
                    let row = row_before_batch(batch, row, |i| table.get_row(i))?;
                    Ok(Self::build_composite_key(&row, keys))
                }
                None => Ok(Self::build_key_from_indices(&*table, row, &cols)),
            }
        };
        let mut by_hash: HashedMap<Vec<JoinKey>> = HashedMap::default();
        for key in targets {
            let keys = by_hash.entry(key_hash(Some(&key))).or_default();
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        // A handful of target hashes compare faster than a map lookup per row.
        let few: Vec<u64> = if by_hash.len() <= 4 {
            by_hash.keys().copied().collect()
        } else {
            Vec::new()
        };
        for (row, hash) in hashes.iter().enumerate() {
            let hit = if few.is_empty() {
                by_hash.contains_key(hash)
            } else {
                few.contains(hash)
            };
            if !hit {
                continue;
            }
            if let Some(key) = key_at(row)? {
                if by_hash[hash].contains(&key) {
                    found.entry(key).or_default().push(row);
                }
            }
        }
        Ok(found)
    }

    /// Whether every event's index is in range for the rows before it:
    /// inserts at or before the end, deletes and updates before it.
    fn indices_in_range(mut rows: usize, changes: &[TableChange]) -> bool {
        for change in changes {
            match change {
                TableChange::RowInserted { index, .. } if *index <= rows => rows += 1,
                TableChange::RowDeleted { index, .. } if *index < rows => rows -= 1,
                TableChange::CellUpdated { row, .. } if *row < rows => {}
                _ => return false,
            }
        }
        true
    }

    /// Binary search for the insertion position of a new (Some(left_idx), _) entry.
    /// `join_index` is partitioned: entries before — Some(el) with el ≤ left_idx;
    /// entries at-or-after — Some(el) with el > left_idx, or None-left.
    /// Was a linear `.iter().position(...)` — O(N); now O(log N).
    fn find_left_insert_position(&self, left_idx: usize) -> usize {
        self.join_index
            .partition_point(|(existing_left, _)| match existing_left {
                Some(el) => *el <= left_idx,
                None => false, // None-left entries are after; not "before our insert"
            })
    }

    /// Binary search for the insertion position of a new (None, Some(right_idx))
    /// orphan entry within the None-left tail. Tail invariant: orphans are
    /// sorted by right_idx ASC (matching rebuild_index's iteration order).
    fn find_orphan_insert_position(&self, right_idx: usize) -> usize {
        self.join_index.partition_point(|(l, r)| match (l, r) {
            (Some(_), _) => true, // All Some(l) entries precede the None-left tail
            (None, Some(r_existing)) => *r_existing < right_idx,
            (None, None) => true, // Defensive; not produced by current code
        })
    }

    /// Binary search for the insertion position of a new (Some(left_idx), Some(right_idx))
    /// entry. Ordering invariant within same left_idx: matched entries (Some right)
    /// sorted by right_idx ASC, then the unmatched (None right) entry if any.
    /// Was a linear scan — O(N); now O(log N).
    fn find_right_insert_position(&self, left_idx: usize, right_idx: usize) -> usize {
        self.join_index
            .partition_point(|(existing_left, existing_right)| match existing_left {
                Some(el) if *el < left_idx => true,
                Some(el) if *el > left_idx => false,
                Some(_) => {
                    // existing_left == left_idx — order by right_idx; None-right is after.
                    match existing_right {
                        Some(existing_right_idx) => *existing_right_idx <= right_idx,
                        None => false,
                    }
                }
                None => false, // None-left entries are after
            })
    }

    /// An output row: left columns plus `right_`-prefixed right columns.
    fn output_row(&self, left: Half, right: Half) -> Result<Row, String> {
        let mut row = match left {
            Half::Given(row) => row.clone(),
            Half::Live(Some(l)) => self.left_table.borrow().get_row(l)?,
            Half::Live(None) => self
                .left_column_names
                .iter()
                .map(|column| (column.clone(), ColumnValue::Null))
                .collect(),
        };
        match right {
            Half::Given(right) => row.extend(prefix_right(right.clone())),
            Half::Live(Some(r)) => row.extend(prefix_right(self.right_row(r)?)),
            Half::Live(None) => row.extend(
                self.right_column_names
                    .iter()
                    .map(|column| (format!("right_{column}"), ColumnValue::Null)),
            ),
        }
        Ok(row)
    }

    /// Right row `r` in the frame the join has applied: during phase 1 of a
    /// two-sided sync, as it was before the right batch.
    fn right_row(&self, r: usize) -> Result<Row, String> {
        let right = self.right_table.borrow();
        match &self.right_before {
            Some(changes) => row_before_batch(changes, r, |i| right.get_row(i)),
            None => right.get_row(r),
        }
    }

    /// Positions of left row `l`'s entries: contiguous, since `join_index` is
    /// sorted by left row.
    fn left_range(&self, l: usize) -> Range<usize> {
        let start = self
            .join_index
            .partition_point(|(el, _)| el.is_some_and(|el| el < l));
        start..self.find_left_insert_position(l)
    }

    /// Ascending positions of right row `r`'s entries (scattered).
    fn right_positions(&self, r: usize) -> Vec<usize> {
        (0..self.join_index.len())
            .filter(|&p| self.join_index[p].1 == Some(r))
            .collect()
    }

    /// Ascending positions of each target right row, found in one pass over
    /// `join_index`. Valid for a right batch that moves no rows, in any
    /// ordering: it reads only `join_index`, which is in the events' frame.
    fn right_targets_positions(&self, rows: &[usize]) -> HashMap<usize, Vec<usize>> {
        let mut positions: HashMap<usize, Vec<usize>> = HashMap::new();
        let Some(&max) = rows.iter().max() else {
            return positions;
        };
        let mut wanted = vec![false; max + 1];
        for &r in rows {
            wanted[r] = true;
        }
        for (p, &(_, r)) in self.join_index.iter().enumerate() {
            if let Some(r) = r.filter(|&r| r <= max && wanted[r]) {
                positions.entry(r).or_default().push(p);
            }
        }
        positions
    }

    fn insert_entry(
        &mut self,
        pos: usize,
        entry: Entry,
        left: Half,
        right: Half,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        self.join_index.insert(pos, entry);
        out.record(|| {
            Ok(TableChange::RowInserted {
                index: pos,
                data: self.output_row(left, right)?,
            })
        })
    }

    /// Remove left row `l`'s entries, recorded with `left` as the left half.
    /// Returns the right rows they matched.
    fn remove_left_entries(
        &mut self,
        l: usize,
        left: &Row,
        out: &mut JoinOutput,
    ) -> Result<Vec<usize>, String> {
        let range = self.left_range(l);
        let removed: Vec<Entry> = self.join_index.drain(range.clone()).collect();
        for &(_, r) in &removed {
            out.record(|| {
                Ok(TableChange::RowDeleted {
                    index: range.start,
                    data: self.output_row(Half::Given(left), Half::Live(r))?,
                })
            })?;
        }
        Ok(removed.into_iter().filter_map(|(_, r)| r).collect())
    }

    /// Remove right row `r`'s entries in ascending order, each recorded at its
    /// index after the removals before it. Returns the left rows they matched.
    fn remove_right_entries(
        &mut self,
        r: usize,
        right: &Row,
        out: &mut JoinOutput,
    ) -> Result<Vec<usize>, String> {
        let positions = self.right_positions(r);
        for (removed, &p) in positions.iter().enumerate() {
            let left = self.join_index[p].0;
            out.record(|| {
                Ok(TableChange::RowDeleted {
                    index: p - removed,
                    data: self.output_row(Half::Live(left), Half::Given(right))?,
                })
            })?;
        }
        let lefts = positions
            .iter()
            .filter_map(|&p| self.join_index[p].0)
            .collect();
        self.join_index.retain(|&(_, er)| er != Some(r));
        Ok(lefts)
    }

    /// RIGHT/FULL: right rows that lost their last match become unmatched
    /// entries in the tail, which is sorted by right row.
    fn orphan_unmatched(&mut self, rights: Vec<usize>, out: &mut JoinOutput) -> Result<(), String> {
        if !matches!(self.join_type, JoinType::Right | JoinType::Full) {
            return Ok(());
        }
        for r in rights {
            let still_matched = self
                .join_index
                .iter()
                .any(|&(l, er)| l.is_some() && er == Some(r));
            if !still_matched {
                let pos = self.find_orphan_insert_position(r);
                self.insert_entry(
                    pos,
                    (None, Some(r)),
                    Half::Live(None),
                    Half::Live(Some(r)),
                    out,
                )?;
            }
        }
        Ok(())
    }

    /// LEFT/FULL: left rows that lost their last match get a placeholder.
    fn placehold_unmatched(
        &mut self,
        lefts: Vec<usize>,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        if !matches!(self.join_type, JoinType::Left | JoinType::Full) {
            return Ok(());
        }
        for l in lefts {
            let range = self.left_range(l);
            if range.is_empty() {
                let pos = range.start;
                self.insert_entry(
                    pos,
                    (Some(l), None),
                    Half::Live(Some(l)),
                    Half::Live(None),
                    out,
                )?;
            }
        }
        Ok(())
    }

    /// Insert left row `l`'s entries: one per matching right row (claiming
    /// any RIGHT/FULL orphan first), or the LEFT/FULL placeholder.
    fn insert_left_matches(
        &mut self,
        l: usize,
        matching: Option<&Vec<usize>>,
        left: &Row,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        match matching {
            Some(rights) => {
                if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                    for &r in rights {
                        // The orphan tail is sorted by right row.
                        let pos = self.find_orphan_insert_position(r);
                        let orphan = (self.join_index.get(pos) == Some(&(None, Some(r)))).then_some(pos);
                        if let Some(pos) = orphan {
                            out.record(|| {
                                Ok(TableChange::RowDeleted {
                                    index: pos,
                                    data: self.output_row(Half::Live(None), Half::Live(Some(r)))?,
                                })
                            })?;
                            self.join_index.remove(pos);
                        }
                    }
                }
                let pos = self.find_left_insert_position(l);
                for (offset, &r) in rights.iter().enumerate() {
                    self.insert_entry(
                        pos + offset,
                        (Some(l), Some(r)),
                        Half::Given(left),
                        Half::Live(Some(r)),
                        out,
                    )?;
                }
            }
            None if matches!(self.join_type, JoinType::Left | JoinType::Full) => {
                let pos = self.find_left_insert_position(l);
                self.insert_entry(
                    pos,
                    (Some(l), None),
                    Half::Given(left),
                    Half::Live(None),
                    out,
                )?;
            }
            None => {}
        }
        Ok(())
    }

    /// Insert right row `r`'s entries: fill LEFT/FULL placeholders in place,
    /// add matched entries, or the RIGHT/FULL unmatched entry.
    fn insert_right_matches(
        &mut self,
        r: usize,
        matching: Option<&Vec<usize>>,
        right: &Row,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        match matching {
            Some(lefts) => {
                for &l in lefts {
                    let placeholder = self.left_range(l).find(|&p| self.join_index[p].1.is_none());
                    if let Some(pos) = placeholder {
                        self.join_index[pos] = (Some(l), Some(r));
                        for column in &self.right_column_names {
                            let value = right.get(column).cloned().unwrap_or(ColumnValue::Null);
                            if value != ColumnValue::Null {
                                out.record(|| {
                                    Ok(TableChange::CellUpdated {
                                        row: pos,
                                        column: format!("right_{column}"),
                                        old_value: ColumnValue::Null,
                                        new_value: value,
                                    })
                                })?;
                            }
                        }
                    } else {
                        let pos = self.find_right_insert_position(l, r);
                        self.insert_entry(
                            pos,
                            (Some(l), Some(r)),
                            Half::Live(Some(l)),
                            Half::Given(right),
                            out,
                        )?;
                    }
                }
            }
            None if matches!(self.join_type, JoinType::Right | JoinType::Full) => {
                let pos = self.find_orphan_insert_position(r);
                self.insert_entry(
                    pos,
                    (None, Some(r)),
                    Half::Live(None),
                    Half::Given(right),
                    out,
                )?;
            }
            None => {}
        }
        Ok(())
    }

    /// Record one value update at each output position.
    fn update_cells(
        positions: impl IntoIterator<Item = usize>,
        column: String,
        old_value: &ColumnValue,
        new_value: &ColumnValue,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        for pos in positions {
            out.record(|| {
                Ok(TableChange::CellUpdated {
                    row: pos,
                    column: column.clone(),
                    old_value: old_value.clone(),
                    new_value: new_value.clone(),
                })
            })?;
        }
        Ok(())
    }

    /// Apply the left parent's batch in order, recording output events. Right
    /// reads go through `right_row` and `matching_rows`: live when the right
    /// has no structural changes, pre-batch during phase 1 of a two-sided
    /// sync.
    fn apply_left_changes(
        &mut self,
        changes: &[TableChange],
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        // Pre-pass: each key update's row after it, and every key this batch
        // must match on the right, for one scan of the right's key hashes.
        let mut after_rows: HashMap<usize, Row> = HashMap::new();
        let mut targets: Vec<JoinKey> = Vec::new();
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowInserted { data, .. } => {
                    targets.extend(Self::build_composite_key(data, &self.left_keys));
                }
                TableChange::CellUpdated { column, .. } if self.left_keys.contains(column) => {
                    let after =
                        row_after_update(changes, t, |i| self.left_table.borrow().get_row(i))?;
                    targets.extend(Self::build_composite_key(&after, &self.left_keys));
                    after_rows.insert(t, after);
                }
                _ => {}
            }
        }
        let right_matches = Self::matching_rows(
            &mut self.right_key_hashes,
            &self.right_table,
            &self.right_keys,
            self.right_rows,
            self.right_before.as_deref(),
            targets,
        )?;
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowDeleted { index, data } => {
                    let rights = self.remove_left_entries(*index, data, out)?;
                    for (l, _) in self.join_index.iter_mut() {
                        if let Some(l) = l {
                            if *l > *index {
                                *l -= 1;
                            }
                        }
                    }
                    if let Some(hashes) = &mut self.left_key_hashes {
                        hashes.remove(*index);
                    }
                    self.left_rows -= 1;
                    self.orphan_unmatched(rights, out)?;
                }
                TableChange::RowInserted { index, data } => {
                    // Tail-insert fast path: join_index is sorted by left row
                    // with None-left entries last, so the max existing left row
                    // is the last Some(l) — usually found in O(1) from the end.
                    let max_existing_left = self.join_index.iter().rev().find_map(|(l, _)| *l);
                    if max_existing_left.is_some_and(|max_l| max_l >= *index) {
                        for (l, _) in self.join_index.iter_mut() {
                            if let Some(l) = l {
                                if *l >= *index {
                                    *l += 1;
                                }
                            }
                        }
                    }
                    let key = Self::build_composite_key(data, &self.left_keys);
                    if let Some(hashes) = &mut self.left_key_hashes {
                        hashes.insert(*index, key_hash(key.as_ref()));
                    }
                    self.left_rows += 1;
                    let matching = key.as_ref().and_then(|k| right_matches.get(k));
                    self.insert_left_matches(*index, matching, data, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } if self.left_keys.contains(column) => {
                    let after = after_rows.remove(&t).expect("pre-pass row for key update");
                    let mut before = after.clone();
                    before.insert(column.clone(), old_value.clone());
                    let new_key = Self::build_composite_key(&after, &self.left_keys);
                    if Self::build_composite_key(&before, &self.left_keys) == new_key {
                        let at = self.left_range(*row);
                        Self::update_cells(at, column.clone(), old_value, new_value, out)?;
                        continue;
                    }
                    if let Some(hashes) = &mut self.left_key_hashes {
                        hashes[*row] = key_hash(new_key.as_ref());
                    }
                    let rights = self.remove_left_entries(*row, &before, out)?;
                    self.orphan_unmatched(rights, out)?;
                    let matching = new_key.as_ref().and_then(|k| right_matches.get(k));
                    self.insert_left_matches(*row, matching, &after, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    let at = self.left_range(*row);
                    Self::update_cells(at, column.clone(), old_value, new_value, out)?;
                }
            }
        }
        Ok(())
    }

    /// Apply the right parent's batch in order, recording output events. The
    /// left side has no structural changes, so it is read live. A value-only
    /// batch (`structural == false`) locates its rows in one targeted pass.
    fn apply_right_changes(
        &mut self,
        changes: &[TableChange],
        structural: bool,
        out: &mut JoinOutput,
    ) -> Result<(), String> {
        // Pre-pass: each key update's row after it, and every key this batch
        // must match on the left, for one scan of the left's key hashes.
        let mut after_rows: HashMap<usize, Row> = HashMap::new();
        let mut targets: Vec<JoinKey> = Vec::new();
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowInserted { data, .. } => {
                    targets.extend(Self::build_composite_key(data, &self.right_keys));
                }
                TableChange::CellUpdated { column, .. } if self.right_keys.contains(column) => {
                    let after =
                        row_after_update(changes, t, |i| self.right_table.borrow().get_row(i))?;
                    targets.extend(Self::build_composite_key(&after, &self.right_keys));
                    after_rows.insert(t, after);
                }
                _ => {}
            }
        }
        let left_matches = Self::matching_rows(
            &mut self.left_key_hashes,
            &self.left_table,
            &self.left_keys,
            self.left_rows,
            None,
            targets,
        )?;
        // A value-only batch moves no rows: one pass locates every target.
        let positions = if structural {
            HashMap::new()
        } else {
            let rows: Vec<usize> = changes
                .iter()
                .filter_map(|change| match change {
                    TableChange::CellUpdated { row, .. } => Some(*row),
                    _ => None,
                })
                .collect();
            self.right_targets_positions(&rows)
        };
        for (t, change) in changes.iter().enumerate() {
            match change {
                TableChange::RowDeleted { index, data } => {
                    let lefts = self.remove_right_entries(*index, data, out)?;
                    for (_, r) in self.join_index.iter_mut() {
                        if let Some(r) = r {
                            if *r > *index {
                                *r -= 1;
                            }
                        }
                    }
                    if let Some(hashes) = &mut self.right_key_hashes {
                        hashes.remove(*index);
                    }
                    self.right_rows -= 1;
                    self.placehold_unmatched(lefts, out)?;
                }
                TableChange::RowInserted { index, data } => {
                    // Right rows are not monotonic in join_index (sorted by
                    // left), so the shift is a single conditional pass. A
                    // tail insert has no row at or after it to shift.
                    if *index < self.right_rows {
                        for (_, r) in self.join_index.iter_mut() {
                            if let Some(r) = r {
                                if *r >= *index {
                                    *r += 1;
                                }
                            }
                        }
                    }
                    let key = Self::build_composite_key(data, &self.right_keys);
                    if let Some(hashes) = &mut self.right_key_hashes {
                        hashes.insert(*index, key_hash(key.as_ref()));
                    }
                    self.right_rows += 1;
                    let matching = key.as_ref().and_then(|k| left_matches.get(k));
                    self.insert_right_matches(*index, matching, data, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } if self.right_keys.contains(column) => {
                    let after = after_rows.remove(&t).expect("pre-pass row for key update");
                    let mut before = after.clone();
                    before.insert(column.clone(), old_value.clone());
                    let new_key = Self::build_composite_key(&after, &self.right_keys);
                    if Self::build_composite_key(&before, &self.right_keys) == new_key {
                        let at = self.right_positions(*row);
                        Self::update_cells(
                            at,
                            format!("right_{column}"),
                            old_value,
                            new_value,
                            out,
                        )?;
                        continue;
                    }
                    if let Some(hashes) = &mut self.right_key_hashes {
                        hashes[*row] = key_hash(new_key.as_ref());
                    }
                    let lefts = self.remove_right_entries(*row, &before, out)?;
                    self.placehold_unmatched(lefts, out)?;
                    let matching = new_key.as_ref().and_then(|k| left_matches.get(k));
                    self.insert_right_matches(*row, matching, &after, out)?;
                }
                TableChange::CellUpdated {
                    row,
                    column,
                    old_value,
                    new_value,
                } => {
                    let at = if structural {
                        self.right_positions(*row)
                    } else {
                        positions.get(row).cloned().unwrap_or_default()
                    };
                    Self::update_cells(at, format!("right_{column}"), old_value, new_value, out)?;
                }
            }
        }
        Ok(())
    }

    /// Returns the number of rows in the joined result
    pub fn len(&self) -> usize {
        self.join_index.len()
    }

    /// Returns true if the join has no rows
    pub fn is_empty(&self) -> bool {
        self.join_index.is_empty()
    }

    /// Gets a row from the joined view
    ///
    /// The returned row contains all columns from both tables.
    /// For left joins where no right match exists, right columns will be Null.
    pub fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        let &(left, right) = self
            .join_index
            .get(index)
            .ok_or_else(|| format!("Index {} out of range [0, {})", index, self.len()))?;
        self.output_row(Half::Live(left), Half::Live(right))
    }

    /// Gets a specific value from the joined view
    pub fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        if row >= self.join_index.len() {
            return Err(format!("Row {} out of range [0, {})", row, self.len()));
        }

        let (left_idx_opt, right_idx_opt) = self.join_index[row];
        if let Some(right_column) = column.strip_prefix("right_") {
            if self
                .right_table
                .borrow()
                .column_index(right_column)
                .is_none()
            {
                return Err(format!("Column '{}' not found in joined view", column));
            }

            return match right_idx_opt {
                Some(right_idx) => self.right_table.borrow().get_value(right_idx, right_column),
                None => Ok(ColumnValue::Null),
            };
        }

        // Left column
        match left_idx_opt {
            Some(left_idx) => self.left_table.borrow().get_value(left_idx, column),
            None => {
                // Verify the column exists in left schema
                if self.left_table.borrow().column_index(column).is_none() {
                    return Err(format!("Column '{}' not found in joined view", column));
                }
                Ok(ColumnValue::Null)
            }
        }
    }

    /// Refreshes the join index after tables have changed
    pub fn refresh(&mut self) {
        self.rebuild_index();
    }

    /// Returns the name of the view
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn last_processed_change_count(&self) -> (usize, usize) {
        (
            self.left_last_processed_change_count,
            self.right_last_processed_change_count,
        )
    }

    pub(crate) fn root_changeset_cursors(&self) -> (usize, usize) {
        (
            self.left_table.borrow().root_changeset_cursor(self.left_last_processed_change_count),
            self.right_table.borrow().root_changeset_cursor(self.right_last_processed_change_count),
        )
    }

    /// True if the batch contains changes that can alter join_index
    /// membership or row positions: inserts, deletes, or updates to a
    /// join-key column. Non-key cell updates are positionally inert.
    fn has_structural_changes(changes: &[TableChange], keys: &[String]) -> bool {
        changes.iter().any(|c| match c {
            TableChange::RowInserted { .. } | TableChange::RowDeleted { .. } => true,
            TableChange::CellUpdated { column, .. } => keys.contains(column),
        })
    }

    /// Incrementally sync with both parents' changes, recording output
    /// history. Returns true if the output changed (rows moved or any value
    /// changed).
    ///
    /// Batches with structural changes on one side apply incrementally, the
    /// value-only side first. Batches with structural changes on both sides
    /// apply as a delta join in two phases, and rebuild only above 256 events
    /// on a side. Unreadable rows and out-of-range indices also rebuild.
    pub fn sync(&mut self) -> bool {
        let left_table = self.left_table.borrow();
        let right_table = self.right_table.borrow();

        // View parent(s): no changeset to consume — fall back to a
        // version-checked rebuild covering both sides at once.
        let (Some(left_changeset), Some(right_changeset)) =
            (left_table.changeset(), right_table.changeset())
        else {
            let stale = left_table.version() != self.last_left_parent_version
                || right_table.version() != self.last_right_parent_version;
            drop(left_table);
            drop(right_table);
            if !stale {
                return false;
            }
            self.rebuild_index();
            return true;
        };

        let left_changes = match left_changeset.changes_from(self.left_last_processed_change_count)
        {
            Some(changes) => changes,
            None => {
                drop(left_table);
                drop(right_table);
                self.rebuild_index();
                return true;
            }
        };
        let right_changes =
            match right_changeset.changes_from(self.right_last_processed_change_count) {
                Some(changes) => changes,
                None => {
                    drop(left_table);
                    drop(right_table);
                    self.rebuild_index();
                    return true;
                }
            };

        let left_changes: Vec<TableChange> = left_changes.to_vec();
        let right_changes: Vec<TableChange> = right_changes.to_vec();
        let parent_versions = (left_table.version(), right_table.version());
        drop(left_table);
        drop(right_table);

        if left_changes.is_empty() && right_changes.is_empty() {
            // An upstream view can advance a parent's version without emitting
            // rows (a filter's excluded edit). Keep history, but record the
            // versions so children still see a coherent baseline.
            (
                self.last_left_parent_version,
                self.last_right_parent_version,
            ) = parent_versions;
            return false;
        }

        // When both sides change structure, the sync is a delta join in two
        // phases (see below). Phase 1 reconstructs each right row it reads
        // from the right batch, O(batch) per read, so larger batches rebuild.
        let left_structural = Self::has_structural_changes(&left_changes, &self.left_keys);
        let right_structural = Self::has_structural_changes(&right_changes, &self.right_keys);
        let two_sided = left_structural && right_structural;
        if two_sided
            && (left_changes.len() > MAX_FILTER_REPLAY_CHANGES
                || right_changes.len() > MAX_FILTER_REPLAY_CHANGES)
        {
            self.rebuild_index();
            return true;
        }

        if !Self::indices_in_range(self.left_rows, &left_changes)
            || !Self::indices_in_range(self.right_rows, &right_changes)
        {
            self.rebuild_index();
            return true;
        }

        // Past every rebuild fallback: this batch replaces retained history.
        self.output_changes.clear();
        let mut out = JoinOutput::default();
        // The value-only side goes first, so the structural side reads the
        // other parent live in the state consumers have already applied.
        // Both structural: phase 1 applies the left batch against the right
        // as consumers last saw it (every right read reconstructed from the
        // right batch); phase 2 applies the right batch against the left,
        // which is now the live left parent.
        let applied = if two_sided {
            self.right_before = Some(right_changes.clone());
            let phase_one = self.apply_left_changes(&left_changes, &mut out);
            self.right_before = None;
            phase_one.and_then(|()| self.apply_right_changes(&right_changes, true, &mut out))
        } else if left_structural {
            self.apply_right_changes(&right_changes, false, &mut out)
                .and_then(|()| self.apply_left_changes(&left_changes, &mut out))
        } else {
            self.apply_left_changes(&left_changes, &mut out)
                .and_then(|()| self.apply_right_changes(&right_changes, right_structural, &mut out))
        };
        if applied.is_err() {
            // An unreadable parent row: rebuild rather than guess.
            self.rebuild_index();
            return true;
        }
        let emitted = out.emitted();
        if out.overflowed {
            self.output_changes.invalidate();
        } else {
            for change in out.changes {
                self.output_changes.push(change);
            }
        }

        let left_table = self.left_table.borrow();
        let right_table = self.right_table.borrow();
        self.left_last_processed_change_count = left_table
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        self.right_last_processed_change_count = right_table
            .changeset()
            .map_or(usize::MAX, |cs| cs.total_len());
        self.last_left_parent_version = left_table.version();
        self.last_right_parent_version = right_table.version();
        drop(left_table);
        drop(right_table);
        self.sync_count += 1;

        emitted
    }
}

impl ReadableTable for JoinView {
    fn len(&self) -> usize {
        self.join_index.len()
    }

    fn column_names(&self) -> Vec<String> {
        let mut names = self.left_column_names.clone();
        for col in &self.right_column_names {
            names.push(format!("right_{}", col));
        }
        names
    }

    fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
        JoinView::get_row(self, index)
    }

    fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
        JoinView::get_value(self, row, column)
    }

    fn version(&self) -> u64 {
        self.sync_count
            .wrapping_add(self.left_table.borrow().version())
            .wrapping_add(self.right_table.borrow().version())
    }

    fn changeset(&self) -> Option<&Changeset> {
        // A child built while this join is stale has no coherent delta
        // baseline. It must refresh once we synchronize the parents.
        (self.left_table.borrow().version() == self.last_left_parent_version
            && self.right_table.borrow().version() == self.last_right_parent_version)
            .then_some(&self.output_changes)
    }
}
