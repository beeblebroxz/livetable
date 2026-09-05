//! Bounded pipeline delivery, independent of view versions and input cursors.
use super::*;
use crate::changeset::TableChange;

pub(super) struct DeliveryState {
    pub seq: u64,
    version: u64,
    cursor: Option<usize>,
}

impl DeliveryState {
    pub(super) fn new(view: &dyn ReadableTable) -> Self {
        Self {
            seq: 0,
            version: view.version(),
            cursor: view.changeset().map(|changes| changes.total_len()),
        }
    }
}

impl NodeSnapshot {
    pub fn into_message(self, table_name: &str) -> ServerMessage {
        ServerMessage::ViewData {
            table_name: table_name.to_string(),
            pipeline_generation: self.pipeline_generation,
            node_id: self.node_id,
            source_id: self.source_id,
            kind: self.kind,
            seq: self.seq,
            columns: self.columns,
            rows: self.rows,
        }
    }
}

fn view_change(change: &TableChange) -> ViewChange {
    match change {
        TableChange::RowInserted { index, data } => ViewChange::RowInserted {
            index: *index,
            row: WireViewRow {
                row_id: None,
                row: row_to_json(data),
            },
        },
        TableChange::RowDeleted { index, .. } => ViewChange::RowDeleted { index: *index },
        TableChange::CellUpdated {
            row,
            column,
            new_value,
            ..
        } => ViewChange::CellUpdated {
            index: *row,
            column: column.clone(),
            value: column_value_to_json(new_value),
        },
    }
}

fn view_error(table: &str, generation: u32, node: &str, message: String) -> ServerMessage {
    ServerMessage::ViewError {
        table_name: table.to_string(),
        pipeline_generation: generation,
        node_id: node.to_string(),
        message,
    }
}

impl ViewNode {
    fn snapshot(&mut self, table: &str, generation: u32) -> Result<ServerMessage, String> {
        let seq = self.delivery.seq + 1;
        let snapshot = snapshot_view(
            generation,
            &self.id,
            &self.source_id,
            &self.kind,
            seq,
            &self.view,
        )?;
        self.delivery = DeliveryState::new(&*self.view.borrow());
        self.delivery.seq = seq;
        Ok(snapshot.into_message(table))
    }

    fn collect(&mut self, table: &str, generation: u32) -> Result<Option<ServerMessage>, String> {
        let view = self.view.borrow();
        let version = view.version();
        if version == self.delivery.version {
            return Ok(None);
        }
        // No snapshot diffing or full-row reads on this path. A missing cursor,
        // rebuild invalidation, or oversized batch explicitly falls back.
        if let Some((history, changes)) = view.changeset().and_then(|history| {
            self.delivery
                .cursor
                .and_then(|cursor| history.changes_from(cursor))
                .filter(|changes| changes.len() <= MAX_VIEW_DELTA_CHANGES)
                .map(|changes| (history, changes))
        }) {
            let changes: Vec<_> = changes.iter().map(view_change).collect();
            self.delivery.cursor = Some(history.total_len());
            self.delivery.version = version;
            if changes.is_empty() {
                return Ok(None);
            }
            let from_seq = self.delivery.seq;
            self.delivery.seq += 1;
            return Ok(Some(ServerMessage::ViewDelta {
                table_name: table.to_string(),
                pipeline_generation: generation,
                node_id: self.id.clone(),
                from_seq,
                seq: self.delivery.seq,
                changes,
            }));
        }
        drop(view);
        self.snapshot(table, generation).map(Some)
    }
}

impl BaseState {
    pub(super) fn record_change(&mut self, change: ViewChange) {
        if self.pending_base.len() == MAX_VIEW_DELTA_CHANGES {
            self.pending_base.pop_front();
        }
        self.pending_base.push_back((self.base_seq(), change));
    }
}

impl TableEngine {
    /// Sync views, then collect ordered deltas or fallback snapshots. Each
    /// connection has its own delivery baseline. Safe for batched mutations;
    /// the actor normally invokes it once per successful mutation.
    pub fn tick_and_collect(&mut self, table_name: &str) -> HashMap<ConnId, Vec<ServerMessage>> {
        let Some(base) = self.bases.get_mut(table_name) else {
            return HashMap::new();
        };
        base.tickable.tick();
        let base_seq = base.base_seq();
        let mut collected = HashMap::new();
        for (conn, pipeline) in &mut base.pipelines {
            let mut messages = Vec::new();
            if base_seq != pipeline.base_last_seq {
                let pending: Vec<_> = base
                    .pending_base
                    .iter()
                    .filter(|(seq, _)| *seq > pipeline.base_last_seq)
                    .collect();
                let covered = pending.len() as u64 == base_seq - pipeline.base_last_seq
                    && pending
                        .iter()
                        .enumerate()
                        .all(|(index, (seq, _))| *seq == pipeline.base_last_seq + index as u64 + 1);
                let seq = pipeline.base_delivery_seq + 1;
                let result = if covered {
                    Ok(ServerMessage::ViewDelta {
                        table_name: table_name.to_string(),
                        pipeline_generation: pipeline.generation,
                        node_id: "base".to_string(),
                        from_seq: pipeline.base_delivery_seq,
                        seq,
                        changes: pending
                            .into_iter()
                            .map(|(_, change)| change.clone())
                            .collect(),
                    })
                } else {
                    snapshot_base(pipeline.generation, seq, &base.table, &base.row_ids)
                        .map(|snapshot| snapshot.into_message(table_name))
                };
                match result {
                    Ok(message) => {
                        messages.push(message);
                        pipeline.base_last_seq = base_seq;
                        pipeline.base_delivery_seq = seq;
                    }
                    Err(error) => {
                        messages.push(view_error(table_name, pipeline.generation, "base", error))
                    }
                }
            }
            for node in &mut pipeline.nodes {
                match node.collect(table_name, pipeline.generation) {
                    Ok(Some(message)) => messages.push(message),
                    Ok(None) => {}
                    Err(error) => {
                        messages.push(view_error(table_name, pipeline.generation, &node.id, error))
                    }
                }
            }
            if !messages.is_empty() {
                collected.insert(*conn, messages);
            }
        }
        // Bounded delivery journal only; consumers missing it get a snapshot.
        base.pending_base.clear();
        collected
    }

    /// Return a fresh baseline for one node. Advances only that connection's
    /// node delivery sequence, even if the data hasn't changed. This makes a
    /// repair unambiguous against in-flight/duplicate snapshots and deltas.
    pub fn query_view(
        &mut self,
        conn: ConnId,
        table_name: &str,
        generation: u32,
        node_id: &str,
    ) -> Result<ServerMessage, String> {
        let base = self
            .bases
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{table_name}' not found"))?;
        let pipeline = base
            .pipelines
            .get_mut(&conn)
            .ok_or("No pipeline installed for this connection")?;
        if pipeline.generation != generation {
            return Err("Pipeline generation is no longer current".into());
        }
        // Sync without consuming other connections' delivery state. Any
        // overwritten derived history is detected by their cursor fallback.
        base.tickable.tick();
        if node_id == "base" {
            let seq = pipeline.base_delivery_seq + 1;
            let snapshot = snapshot_base(generation, seq, &base.table, &base.row_ids)?;
            pipeline.base_delivery_seq = seq;
            pipeline.base_last_seq = base.table.borrow().changeset().total_len() as u64;
            Ok(snapshot.into_message(table_name))
        } else {
            pipeline
                .nodes
                .iter_mut()
                .find(|node| node.id == node_id)
                .ok_or_else(|| format!("Pipeline node '{node_id}' not found"))?
                .snapshot(table_name, generation)
        }
    }

    /// Cheap periodic watermarks; no table scans. A client behind a watermark
    /// requests a snapshot, including when the *last* delivery was dropped.
    pub fn pipeline_statuses(&self) -> Vec<(ConnId, ServerMessage)> {
        self.bases
            .iter()
            .flat_map(|(table, base)| {
                base.pipelines.iter().map(move |(conn, pipeline)| {
                    let mut sequences =
                        HashMap::from([("base".to_string(), pipeline.base_delivery_seq)]);
                    sequences.extend(
                        pipeline
                            .nodes
                            .iter()
                            .map(|node| (node.id.clone(), node.delivery.seq)),
                    );
                    (
                        *conn,
                        ServerMessage::PipelineStatus {
                            table_name: table.clone(),
                            pipeline_generation: pipeline.generation,
                            sequences,
                        },
                    )
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct ReadProbe {
        table: Table,
        reads: Cell<usize>,
        fail_reads: bool,
    }

    impl ReadableTable for ReadProbe {
        fn len(&self) -> usize {
            self.table.len()
        }
        fn column_names(&self) -> Vec<String> {
            self.table.column_names()
        }
        fn get_row(&self, index: usize) -> Result<HashMap<String, ColumnValue>, String> {
            self.reads.set(self.reads.get() + 1);
            if self.fail_reads {
                Err("injected snapshot failure".into())
            } else {
                self.table.get_row(index)
            }
        }
        fn get_value(&self, row: usize, column: &str) -> Result<ColumnValue, String> {
            self.table.get_value(row, column)
        }
        fn version(&self) -> u64 {
            self.table.version()
        }
        fn changeset(&self) -> Option<&crate::changeset::Changeset> {
            Some(self.table.changeset())
        }
    }

    #[test]
    fn delta_does_not_read_rows_and_failed_snapshot_does_not_advance_baseline() {
        let mut table = Table::new(
            "probe".into(),
            Schema::new(vec![("x".into(), ColumnType::Int32, false)]),
        );
        table
            .append_row(HashMap::from([("x".into(), ColumnValue::Int32(1))]))
            .unwrap();
        table.clear_changeset();
        let probe = Rc::new(RefCell::new(ReadProbe {
            table,
            reads: Cell::new(0),
            fail_reads: false,
        }));
        let delivery = DeliveryState::new(&*probe.borrow());
        let mut node = ViewNode {
            id: "probe".into(),
            source_id: "base".into(),
            kind: "filter".into(),
            view: probe.clone(),
            delivery,
        };
        probe
            .borrow_mut()
            .table
            .set_value(0, "x", ColumnValue::Int32(2))
            .unwrap();
        assert!(matches!(
            node.collect("demo", 1).unwrap(),
            Some(ServerMessage::ViewDelta { seq: 1, .. })
        ));
        assert_eq!(probe.borrow().reads.get(), 0);
        let baseline = (
            node.delivery.seq,
            node.delivery.cursor,
            node.delivery.version,
        );
        {
            let mut probe = probe.borrow_mut();
            probe
                .table
                .set_value(0, "x", ColumnValue::Int32(3))
                .unwrap();
            probe.table.clear_changeset(); // input is gone, so a snapshot is required
            probe.fail_reads = true;
        }
        assert!(node.collect("demo", 1).is_err());
        assert_eq!(
            (
                node.delivery.seq,
                node.delivery.cursor,
                node.delivery.version
            ),
            baseline
        );
        probe.borrow_mut().fail_reads = false;
        assert!(matches!(
            node.collect("demo", 1).unwrap(),
            Some(ServerMessage::ViewData { seq: 2, .. })
        ));
        assert_eq!(probe.borrow().reads.get(), 2);
        assert!(node.collect("demo", 1).unwrap().is_none());
    }
}
