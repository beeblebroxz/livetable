//! `apply_batch` against `apply_each`: the same events and the same state,
//! for random batches of every size the sorted view replays.
use super::*;
use crate::table::{Schema, Table};

/// Deterministic choices.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, limit: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % limit
    }
}

/// Few distinct values, so ties are common: NULLs in `a`, and NaN and both
/// zeros in `b`.
fn random_row(rng: &mut Lcg) -> Row {
    let a = match rng.below(5) {
        0 => ColumnValue::Null,
        value => ColumnValue::Int32(value as i32),
    };
    HashMap::from([
        ("a".to_string(), a),
        ("b".to_string(), random_float(rng)),
        (
            "payload".to_string(),
            ColumnValue::Int32(rng.below(1000) as i32),
        ),
    ])
}

fn random_float(rng: &mut Lcg) -> ColumnValue {
    ColumnValue::Float64(match rng.below(6) {
        0 => f64::NAN,
        1 => -0.0,
        2 => 0.0,
        value => value as f64,
    })
}

fn key_sets() -> Vec<Vec<SortKey>> {
    vec![
        vec![SortKey::ascending("a"), SortKey::descending("b")],
        vec![SortKey::new("a", SortOrder::Descending, true)],
        vec![
            SortKey::ascending("b"),
            SortKey::new("a", SortOrder::Ascending, true),
        ],
        // One column in two keys.
        vec![
            SortKey::descending("b"),
            SortKey::ascending("a"),
            SortKey::ascending("b"),
        ],
    ]
}

fn mutate(table: &RefCell<Table>, rng: &mut Lcg) {
    let mut table = table.borrow_mut();
    let len = table.len();
    match rng.below(if len == 0 { 2 } else { 8 }) {
        0 => table.append_row(random_row(rng)).unwrap(),
        1 => table
            .insert_row(rng.below(len + 1), random_row(rng))
            .unwrap(),
        2 | 3 => {
            table.delete_row(rng.below(len)).unwrap();
        }
        4 => {
            let value = match rng.below(5) {
                0 => ColumnValue::Null,
                value => ColumnValue::Int32(value as i32),
            };
            table.set_value(rng.below(len), "a", value).unwrap();
        }
        5 | 6 => table
            .set_value(rng.below(len), "b", random_float(rng))
            .unwrap(),
        _ => table
            .set_value(rng.below(len), "payload", ColumnValue::Int32(7))
            .unwrap(),
    }
}

/// A row's cells in column order: a HashMap's own order differs per map.
fn cells(row: &Row) -> String {
    let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
    cells.sort();
    cells.join(",")
}

/// Everything the two paths must agree on. Debug text, because NaN keys and
/// payloads are not equal to themselves.
fn state(view: &SortedView) -> String {
    let events: Vec<String> = view
        .output_changes
        .changes()
        .iter()
        .map(|change| match change {
            TableChange::RowInserted { index, data } => format!("insert {index} {}", cells(data)),
            TableChange::RowDeleted { index, data } => format!("delete {index} {}", cells(data)),
            other => format!("{other:?}"),
        })
        .collect();
    format!("{events:#?}\n{}", index_state(view))
}

fn index_state(view: &SortedView) -> String {
    format!(
        "{:?}\n{:?}\n{:?}",
        view.sorted_index, view.parent_to_view, view.sort_values
    )
}

#[test]
fn batch_replay_matches_event_by_event_replay() {
    let mut rng = Lcg(2026);
    for trial in 0..400 {
        let keys = key_sets().swap_remove(trial % 4);
        let schema = Schema::new(vec![
            ("a".to_string(), ColumnType::Int32, true),
            ("b".to_string(), ColumnType::Float64, false),
            ("payload".to_string(), ColumnType::Int32, false),
        ]);
        let mut table = Table::new("t".to_string(), schema);
        for _ in 0..rng.below(40) {
            table.append_row(random_row(&mut rng)).unwrap();
        }
        let table = Rc::new(RefCell::new(table));
        let sorted = || SortedView::new("s".to_string(), table.clone(), keys.clone()).unwrap();
        let mut each = sorted();
        let mut batch = sorted();
        for step in 0..8 {
            let cursor = table.borrow().changeset().total_len();
            // Mostly short batches; every tenth trial runs to the replay bound.
            let events = match trial % 10 {
                0 => 1 + rng.below(MAX_FILTER_REPLAY_CHANGES),
                1 => 1,
                _ => 1 + rng.below(12),
            };
            for _ in 0..events {
                mutate(&table, &mut rng);
            }
            let changes = table
                .borrow()
                .changeset()
                .changes_from(cursor)
                .unwrap()
                .to_vec();
            let label = format!("trial {trial} step {step}, {} events", changes.len());

            let updated = each.updated_rows(&changes).expect("readable rows");
            each.apply_each(&changes, updated);
            let updated = batch.updated_rows(&changes).expect("readable rows");
            batch.apply_batch(&changes, updated);
            assert_eq!(state(&each), state(&batch), "{label}");
            assert_eq!(
                index_state(&batch),
                index_state(&sorted()),
                "{label}: rebuild"
            );
        }
    }
}
