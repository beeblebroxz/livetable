//! TickableTable — pairs a table with a view registry for auto-tick propagation.
//!
//! `TickableTable` adds the registry WITHOUT adding state to `Table` itself.
//! The WebSocket server owns its `Rc`-based tables and views inside a
//! single-threaded actor, while other core users can still use `Table`
//! independently. Views are registered in creation order, which is topological
//! for view-over-view chains: a
//! chained view registered after its parent always syncs after it.
//!
//! A consumer of a derived stream reports usize::MAX for root compaction,
//! independently of its cursor within the derived stream. Sequence numbers
//! from different sources must never be compared in the min-cursor fold.

use crate::table::Table;
use std::cell::RefCell;
use std::rc::Rc;

use super::{AggregateView, FilterView, JoinView, SortedView};

/// Per-view "syncer" closure stored in the registry.
///
/// When invoked: upgrades the captured `Weak<RefCell<View>>`, calls
/// `view.sync()`, and returns the consumed root-table cursor (absolute change
/// index), or `usize::MAX` when the view consumes a derived stream. This is not
/// the view's immediate-parent cursor in that case. Returns `None` if
/// the view has been dropped, letting `TickableTable::tick` prune dead
/// entries in one pass.
type SyncerFn = Box<dyn FnMut() -> Option<usize>>;

/// Wrapper that pairs a table with a view registry for auto-tick propagation.
///
/// Construct with [`TickableTable::new`] from an existing
/// `Rc<RefCell<Table>>`. Register views via `register_filter`,
/// `register_sorted`, `register_aggregate`, `register_join_as_left`, or
/// `register_join_as_right`. Call [`tick`](TickableTable::tick) after
/// mutations to sync all registered views and compact the changeset.
///
/// Views are held as `Weak` references — dropping the strong `Rc` to a
/// view de-registers it automatically (entry pruned on next `tick()`).
pub struct TickableTable {
    table: Rc<RefCell<Table>>,
    syncers: RefCell<Vec<SyncerFn>>,
}

impl TickableTable {
    /// Wrap an existing table for tick-based view propagation.
    pub fn new(table: Rc<RefCell<Table>>) -> Self {
        TickableTable {
            table,
            syncers: RefCell::new(Vec::new()),
        }
    }

    /// Borrow the underlying table handle — useful for mutations or for
    /// constructing views (the handle coerces to `Rc<RefCell<dyn ReadableTable>>`).
    pub fn table(&self) -> &Rc<RefCell<Table>> {
        &self.table
    }

    /// Register a `FilterView` for auto-sync on `tick()`.
    pub fn register_filter(&self, view: &Rc<RefCell<FilterView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().root_changeset_cursor();
            Some(cursor)
        }));
    }

    /// Register a `SortedView` for auto-sync on `tick()`.
    pub fn register_sorted(&self, view: &Rc<RefCell<SortedView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().root_changeset_cursor();
            Some(cursor)
        }));
    }

    /// Register an `AggregateView` for auto-sync on `tick()`.
    pub fn register_aggregate(&self, view: &Rc<RefCell<AggregateView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let cursor = v.borrow().root_changeset_cursor();
            Some(cursor)
        }));
    }

    /// Register a `JoinView` on its LEFT parent table. Both this AND
    /// `register_join_as_right` on the right parent's TickableTable must
    /// be called for the join to be fully wired up.
    pub fn register_join_as_left(&self, view: &Rc<RefCell<JoinView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let (left_cursor, _) = v.borrow().root_changeset_cursors();
            let cursor = left_cursor;
            Some(cursor)
        }));
    }

    /// Register a `JoinView` on its RIGHT parent table. See `register_join_as_left`.
    pub fn register_join_as_right(&self, view: &Rc<RefCell<JoinView>>) {
        let weak = Rc::downgrade(view);
        self.syncers.borrow_mut().push(Box::new(move || {
            let v = weak.upgrade()?;
            v.borrow_mut().sync();
            let (_, right_cursor) = v.borrow().root_changeset_cursors();
            let cursor = right_cursor;
            Some(cursor)
        }));
    }

    /// Returns the number of registered syncers. Dead `Weak` references
    /// are only pruned by `tick()`, so this can include entries whose
    /// underlying views have been dropped.
    pub fn registered_view_count(&self) -> usize {
        self.syncers.borrow().len()
    }

    /// Synchronize all registered views with the table's pending changes.
    ///
    /// For each live view: invokes its sync closure (which calls
    /// `view.sync()` internally and reports the view's post-sync cursor).
    /// Dead `Weak` references (views that have been dropped) are pruned
    /// in the same pass. After all syncs, compacts the changeset up to
    /// `min(all reported cursors)` so memory doesn't grow unbounded.
    ///
    /// Returns the number of live views synced.
    pub fn tick(&self) -> usize {
        if !self.table.borrow().has_pending_changes() {
            return 0;
        }

        // Move syncers OUT so calling them does not hold a borrow of
        // `self.syncers`. The closure bodies call `view.borrow_mut().sync()`,
        // which in turn does `self.parent.borrow()` on the view — and
        // `self.parent` is the SAME `Rc<RefCell<Table>>` as `self.table`,
        // so the only borrow held during the loop is whatever the view's
        // sync acquires internally (immutable).
        let mut syncers: Vec<SyncerFn> = std::mem::take(&mut *self.syncers.borrow_mut());
        let mut min_cursor = self.table.borrow().changeset().total_len();
        let mut synced = 0usize;
        let mut alive: Vec<SyncerFn> = Vec::with_capacity(syncers.len());

        for mut syncer in syncers.drain(..) {
            match syncer() {
                Some(cursor) => {
                    min_cursor = min_cursor.min(cursor);
                    alive.push(syncer);
                    synced += 1;
                }
                None => {
                    // Weak upgrade failed — view dropped; do not re-add.
                }
            }
        }

        *self.syncers.borrow_mut() = alive;
        // Compaction in its own borrow_mut scope, AFTER the sync pass —
        // syncs only need shared borrows of the parent table.
        self.table.borrow_mut().compact_changeset(min_cursor);

        synced
    }
}
