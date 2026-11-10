//! A bounded, deterministic demo workload. Never operates on user tables.
use super::*;
use crate::messages::{LabAction, LabTiming, ViewTiming};
use std::time::Instant;

pub const LAB_TABLE: &str = "lab";
/// 256 is the filter's replay bound, so a burst keeps every node incremental
/// and each node's delivery fits one `ViewDelta`.
const BURST_SIZES: [usize; 3] = [16, 64, 256];
const REGIONS: [&str; 4] = ["Americas", "Europe", "Asia Pacific", "Middle East"];
const PRODUCTS: [&str; 6] = [
    "Studio Display",
    "Mechanical Keyboard",
    "Dock Pro",
    "Portable SSD",
    "Monitor Arm",
    "Webcam 4K",
];

fn order(index: u64) -> JsonRow {
    let amount = match index {
        0 => 240.0,
        1 => 1480.0,
        2 => 760.0,
        _ => (100 + (index * 137 % 4900)) as f64,
    };
    HashMap::from([
        ("order".into(), JsonValue::from(index + 10001)),
        (
            "region".into(),
            JsonValue::from(REGIONS[index as usize % REGIONS.len()]),
        ),
        (
            "product".into(),
            JsonValue::from(PRODUCTS[index as usize % PRODUCTS.len()]),
        ),
        ("quantity".into(), JsonValue::from(1 + index % 12)),
        ("amount".into(), JsonValue::from(amount)),
    ])
}

impl TableEngine {
    /// Enable a separate synthetic table. Calling this twice preserves its data.
    pub fn enable_lab(&mut self) -> Result<(), String> {
        if self.has_table(LAB_TABLE) {
            return Ok(());
        }
        let schema = Schema::new(vec![
            ("order".into(), ColumnType::Int32, false),
            ("region".into(), ColumnType::String, false),
            ("product".into(), ColumnType::String, false),
            ("quantity".into(), ColumnType::Int32, false),
            ("amount".into(), ColumnType::Float64, false),
        ]);
        let table = Table::with_hint_and_interning(
            LAB_TABLE.into(),
            schema,
            crate::table::StorageHint::FastReads,
            true,
        );
        self.bases
            .insert(LAB_TABLE.into(), BaseState::with_seed_data(table));
        self.lab_command(&LabAction::Reset { rows: 1000 })?;
        self.tick_and_collect(LAB_TABLE);
        Ok(())
    }

    /// Returns flat deliveries (empty on reset), mutation count, row count, step.
    /// The actor ticks once per command, so Step is a genuine mixed batch.
    pub fn lab_command(
        &mut self,
        action: &LabAction,
    ) -> Result<(Vec<ServerMessage>, usize, usize, u64), String> {
        if !self.has_table(LAB_TABLE) {
            return Err("Orders lab is disabled. Start livetable-server with --lab.".into());
        }
        let mut messages = Vec::new();
        let mutations = match action {
            LabAction::Reset { rows } => {
                if ![1000, 10000, 100000].contains(rows) {
                    return Err("Lab size must be 1000, 10000, or 100000 rows".into());
                }
                let base = self.bases.get_mut(LAB_TABLE).unwrap();
                let old_len = base.row_ids.len();
                // Tail deletion is O(1) for these buffers; front deletion would
                // shift O(N) per row. Keep the same table, monotonic IDs and change clock;
                // existing clients recover through ordinary fallback snapshots.
                for index in (0..old_len).rev() {
                    base.table.borrow_mut().delete_row(index)?;
                    base.row_ids.pop();
                    base.record_change(ViewChange::RowDeleted { index });
                }
                for index in 0..*rows {
                    self.insert_row(LAB_TABLE, order(index as u64))?;
                }
                self.lab_step = 0;
                old_len + rows
            }
            LabAction::Update { row_id, amount } => {
                if !amount.is_finite() || !(0.0..=1_000_000.0).contains(amount) {
                    return Err("Amount must be finite and between 0 and 1000000".into());
                }
                messages.push(self.update_cell(
                    LAB_TABLE,
                    *row_id,
                    "amount",
                    &JsonValue::from(*amount),
                )?);
                1
            }
            LabAction::Step => {
                let base = self.bases.get(LAB_TABLE).unwrap();
                let len = base.row_ids.len();
                if len < 3 {
                    return Err("Reset the lab before streaming".into());
                }
                // Bound the run as well as row count; order uses an INT32 buffer.
                if self.lab_step >= 1_000_000 {
                    return Err("Reset the lab to start a new run".into());
                }
                let row_id = base.row_ids[self.lab_step as usize % len.min(512)];
                let tail_id = *base.row_ids.last().unwrap();
                let amount = 100 + (self.lab_step * 193 % 4900);
                messages.push(self.update_cell(
                    LAB_TABLE,
                    row_id,
                    "amount",
                    &JsonValue::from(amount),
                )?);
                messages.push(self.delete_row(LAB_TABLE, tail_id)?);
                messages.push(self.insert_row(LAB_TABLE, order(len as u64 + self.lab_step))?);
                self.lab_step += 1;
                3
            }
            LabAction::Burst { size } => {
                if !BURST_SIZES.contains(size) {
                    return Err("Burst size must be 16, 64, or 256".into());
                }
                if self.bases[LAB_TABLE].row_ids.len() < 3 {
                    return Err("Reset the lab before streaming".into());
                }
                if self.lab_step >= 1_000_000 {
                    return Err("Reset the lab to start a new run".into());
                }
                // Two edits, a delete and an append, repeated: the row count
                // stays steady. Edited and deleted rows are spread over the
                // whole table, so an edit that crosses a client's threshold
                // reaches its filter, and the views over it, as an insert or
                // delete in the middle. Base inserts only append: base row
                // IDs must stay ascending.
                for i in 0..*size as u64 {
                    let base = &self.bases[LAB_TABLE];
                    let len = base.row_ids.len() as u64;
                    let scattered = (self.lab_step * 7919 + i * 104_729) % len;
                    let row_id = base.row_ids[scattered as usize];
                    let new_order = base.next_row_id;
                    messages.push(match i % 4 {
                        0 | 1 => {
                            let amount = 100 + ((self.lab_step * 193 + i * 389) % 4900);
                            self.update_cell(LAB_TABLE, row_id, "amount", &JsonValue::from(amount))?
                        }
                        2 => self.delete_row(LAB_TABLE, row_id)?,
                        _ => self.insert_row(LAB_TABLE, order(new_order))?,
                    });
                }
                self.lab_step += 1;
                *size
            }
        };
        Ok((
            messages,
            mutations,
            self.bases[LAB_TABLE].row_ids.len(),
            self.lab_step,
        ))
    }

    /// Sync the requester's lab pipeline, one node at a time in pipeline
    /// order, and report what each `sync()` cost. Call it after
    /// `lab_command` and before `tick_and_collect`, whose tick then finds
    /// these nodes already synced. A burst also times a fresh build of each
    /// view over its parent: what a rebuild would cost, measured without
    /// disturbing the live view. None after a reset, and for a connection
    /// with no pipeline on the lab table.
    pub fn lab_timing(&mut self, conn: ConnId, action: &LabAction) -> Option<LabTiming> {
        if matches!(action, LabAction::Reset { .. }) {
            return None;
        }
        let pipeline = self.bases.get(LAB_TABLE)?.pipelines.get(&conn)?;
        let micros = |start: Instant| start.elapsed().as_secs_f64() * 1e6;
        let mut views: Vec<ViewTiming> = pipeline
            .nodes
            .iter()
            .map(|node| {
                let start = Instant::now();
                (node.sync)();
                ViewTiming {
                    node_id: node.id.clone(),
                    sync_us: micros(start),
                    rebuild_us: None,
                }
            })
            .collect();
        if matches!(action, LabAction::Burst { .. }) {
            for (node, timing) in pipeline.nodes.iter().zip(&mut views) {
                let start = Instant::now();
                // A view that built once builds again; without a time the
                // client shows the sync alone.
                if (node.rebuild)().is_ok() {
                    timing.rebuild_us = Some(micros(start));
                }
            }
        }
        Some(LabTiming { views })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lab_is_opt_in_bounded_and_isolated() {
        let mut engine = TableEngine::new();
        assert!(engine.lab_command(&LabAction::Step).is_err());
        engine.enable_lab().unwrap();
        assert!(engine.lab_command(&LabAction::Reset { rows: 999 }).is_err());
        for _ in 0..5 {
            let (_, changes, rows, _) = engine.lab_command(&LabAction::Step).unwrap();
            assert_eq!((changes, rows), (3, 1000));
            engine.tick_and_collect(LAB_TABLE);
        }
        let demo = engine.query_table("demo").unwrap();
        if let ServerMessage::TableData { rows, .. } = demo {
            assert_eq!(rows.len(), 2);
        } else {
            panic!();
        }
    }

    #[test]
    fn reset_preserves_ids_clocks_and_existing_pipeline_baselines() {
        let mut engine = TableEngine::new();
        engine.enable_lab().unwrap();
        let specs: Vec<ViewNodeSpec> = serde_json::from_value(serde_json::json!([
            {"id":"high-value", "source_id":"base", "kind":"filter", "predicate":"amount >= 1000"},
            {"id":"ranked", "source_id":"high-value", "kind":"sort", "keys":[{"column":"amount", "descending":true}]}
        ])).unwrap();
        for conn in [1, 2] {
            assert!(engine
                .set_pipeline(conn, LAB_TABLE, 1, &specs)
                .iter()
                .all(Result::is_ok));
        }
        let old_id = engine.bases[LAB_TABLE].row_ids[0];
        let old_clock = engine.bases[LAB_TABLE].base_seq();
        engine
            .lab_command(&LabAction::Reset { rows: 10000 })
            .unwrap();
        let deliveries = engine.tick_and_collect(LAB_TABLE);
        for conn in [1, 2] {
            assert_eq!(deliveries[&conn].len(), 3);
            assert!(deliveries[&conn].iter().all(|message| matches!(
                message,
                ServerMessage::ViewData {
                    seq: 1,
                    pipeline_generation: 1,
                    ..
                }
            )));
        }
        assert!(engine.bases[LAB_TABLE].row_ids[0] > old_id);
        assert!(engine.bases[LAB_TABLE].base_seq() > old_clock);
        assert!(engine
            .lab_command(&LabAction::Update {
                row_id: old_id,
                amount: 999.0
            })
            .is_err());
        engine.lab_command(&LabAction::Step).unwrap();
        assert!(engine.tick_and_collect(LAB_TABLE)[&1]
            .iter()
            .any(|m| matches!(
                m,
                ServerMessage::ViewDelta {
                    from_seq: 1,
                    seq: 2,
                    ..
                }
            )));
    }

    /// The Lab page's pipeline: a filter, with a sort and a group over it.
    fn lab_pipeline(threshold: u32) -> Vec<ViewNodeSpec> {
        serde_json::from_value(serde_json::json!([
            {"id":"high-value", "source_id":"base", "kind":"filter", "predicate":format!("amount >= {threshold}")},
            {"id":"ranked", "source_id":"high-value", "kind":"sort", "keys":[{"column":"amount", "descending":true}]},
            {"id":"regions", "source_id":"high-value", "kind":"group", "group_by":["region"], "aggs":[
                {"alias":"total", "op":"sum", "column":"amount"},
                {"alias":"orders", "op":"count", "column":"amount"},
                {"alias":"average", "op":"avg", "column":"amount"}
            ]}
        ]))
        .unwrap()
    }

    /// A node's rows as JSON. Groups are compared in region order: a group
    /// emptied and refilled moves to the end, where a fresh build has it in
    /// first-seen order.
    fn rows_json(node_id: &str, rows: &[WireViewRow]) -> Vec<String> {
        let mut rows: Vec<String> = rows
            .iter()
            .map(|row| serde_json::to_value(&row.row).unwrap().to_string())
            .collect();
        if node_id == "regions" {
            rows.sort();
        }
        rows
    }

    #[test]
    fn a_burst_stays_incremental_and_reports_per_view_engine_time() {
        let mut engine = TableEngine::new();
        engine.enable_lab().unwrap();
        engine
            .lab_command(&LabAction::Reset { rows: 10000 })
            .unwrap();
        engine.tick_and_collect(LAB_TABLE);
        for (conn, threshold) in [(1, 1000), (2, 2500)] {
            assert!(engine
                .set_pipeline(conn, LAB_TABLE, 1, &lab_pipeline(threshold))
                .iter()
                .all(Result::is_ok));
        }
        let burst = LabAction::Burst { size: 256 };
        let nodes = ["high-value", "ranked", "regions"];

        for round in 0..6 {
            let (_, mutations, rows, _) = engine.lab_command(&burst).unwrap();
            assert_eq!((mutations, rows), (256, 10000), "round {round}");

            let timing = engine.lab_timing(1, &burst).expect("conn 1 has a pipeline");
            let timed: Vec<&str> = timing.views.iter().map(|v| v.node_id.as_str()).collect();
            assert_eq!(timed, nodes);
            for view in &timing.views {
                assert!(view.sync_us.is_finite() && view.sync_us >= 0.0);
                assert!(view.rebuild_us.is_some_and(|us| us.is_finite() && us > 0.0));
            }

            // Both connections, including the one that was not timed, get a
            // delta for every node and no snapshot.
            let deliveries = engine.tick_and_collect(LAB_TABLE);
            for conn in [1, 2] {
                for node in ["base"].iter().chain(&nodes) {
                    assert!(
                        deliveries[&conn].iter().any(|message| matches!(message,
                            ServerMessage::ViewDelta { node_id, .. } if node_id == node)),
                        "round {round} conn {conn}: no delta for {node}"
                    );
                }
                assert!(
                    !deliveries[&conn]
                        .iter()
                        .any(|message| matches!(message, ServerMessage::ViewData { .. })),
                    "round {round} conn {conn}: a node fell back to a snapshot"
                );
            }
        }

        // After the bursts every node holds what a fresh pipeline builds.
        for (conn, threshold) in [(1, 1000), (2, 2500)] {
            let fresh = engine.set_pipeline(100 + conn, LAB_TABLE, 1, &lab_pipeline(threshold));
            for snapshot in fresh.into_iter().map(Result::unwrap) {
                let ServerMessage::ViewData { rows, .. } = engine
                    .query_view(conn, LAB_TABLE, 1, &snapshot.node_id)
                    .unwrap()
                else {
                    panic!("QueryView returns a snapshot");
                };
                assert_eq!(
                    rows_json(&snapshot.node_id, &rows),
                    rows_json(&snapshot.node_id, &snapshot.rows),
                    "conn {conn} node {}",
                    snapshot.node_id
                );
            }
        }
    }

    #[test]
    fn timing_covers_the_requesters_views_and_rebuilds_only_for_a_burst() {
        let mut engine = TableEngine::new();
        engine.enable_lab().unwrap();
        assert!(engine
            .set_pipeline(1, LAB_TABLE, 1, &lab_pipeline(1000))
            .iter()
            .all(Result::is_ok));

        engine.lab_command(&LabAction::Step).unwrap();
        let step = engine.lab_timing(1, &LabAction::Step).unwrap();
        assert_eq!(step.views.len(), 3);
        assert!(step.views.iter().all(|view| view.rebuild_us.is_none()));
        // The timed sync leaves nothing for the tick to deliver late.
        assert!(engine.tick_and_collect(LAB_TABLE)[&1]
            .iter()
            .all(|message| matches!(message, ServerMessage::ViewDelta { .. })));

        assert!(
            engine.lab_timing(2, &LabAction::Step).is_none(),
            "no pipeline"
        );
        let reset = LabAction::Reset { rows: 1000 };
        engine.lab_command(&reset).unwrap();
        assert!(
            engine.lab_timing(1, &reset).is_none(),
            "a reset is not timed"
        );

        for size in [0, 3, 100, 257, 1024] {
            assert!(
                engine.lab_command(&LabAction::Burst { size }).is_err(),
                "burst of {size}"
            );
        }
    }

    #[test]
    fn lab_complete_serializes_timing_only_when_present() {
        let complete = |timing| {
            serde_json::to_value(ServerMessage::LabComplete {
                request_id: 7,
                rows: 1000,
                step: 2,
                mutations: 256,
                timing,
            })
            .unwrap()
        };
        assert!(complete(None).get("timing").is_none());
        let timed = complete(Some(LabTiming {
            views: vec![
                ViewTiming {
                    node_id: "ranked".into(),
                    sync_us: 12.5,
                    rebuild_us: Some(800.0),
                },
                ViewTiming {
                    node_id: "regions".into(),
                    sync_us: 3.0,
                    rebuild_us: None,
                },
            ],
        }));
        assert_eq!(
            timed["timing"],
            serde_json::json!({"views": [
                {"node_id": "ranked", "sync_us": 12.5, "rebuild_us": 800.0},
                {"node_id": "regions", "sync_us": 3.0}
            ]})
        );
        let burst: LabAction =
            serde_json::from_value(serde_json::json!({"kind": "burst", "size": 64})).unwrap();
        assert!(matches!(burst, LabAction::Burst { size: 64 }));
    }
}
