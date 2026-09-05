//! A bounded, deterministic demo workload. Never operates on user tables.
use super::*;
use crate::messages::LabAction;

pub const LAB_TABLE: &str = "lab";
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
                // Tail deletion is O(1) for these buffers and avoids O(N²) ID
                // lookups. Keep the same table, monotonic IDs and change clock;
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
        };
        Ok((
            messages,
            mutations,
            self.bases[LAB_TABLE].row_ids.len(),
            self.lab_step,
        ))
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
}
