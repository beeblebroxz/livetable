//! Differential fuzz for the in-memory forward-propagation engine.
//!
//! The strongest possible correctness check for incremental view maintenance:
//! after random mutations on the root table + a `tick()`, each view's
//! incrementally-maintained state must EXACTLY equal a view built from scratch
//! on the current root state. Both sides run the SAME engine code, so semantics
//! (NULL/NaN handling, sort/tie rules, aggregate math) are identical by
//! construction — any divergence is a *maintenance* bug in the incremental sync
//! path, which is exactly what we want to catch.
//!
//! Coverage:
//!   * FilterView directly on root           (incremental changeset path)
//!   * SortedView directly on root           (incremental changeset path)
//!   * AggregateView directly on root         (incremental running aggregates,
//!                                             incl. MIN/MAX recalc + MEDIAN/p90
//!                                             sorted_values maintenance)
//!   * SortedView over a FilterView           (chained, version-checked refresh)
//!   * AggregateView over the chained sort     (chained-of-chained)
//! plus multi-consumer min-cursor changeset compaction (filter + the two direct
//! views all consume the root changeset on the same TickableTable).
//!
//! Two drivers:
//!   * `differential_chained_forward_prop_fuzz` ticks after EVERY mutation
//!     (single-change ticks).
//!   * `differential_batched_forward_prop_fuzz` applies a RANDOM BATCH of
//!     mutations between ticks (multi-change ticks) — this exercises the
//!     end-of-batch MIN/MAX recalc: a group's row indices line up with the
//!     parent only AFTER every insert/delete in the batch has shifted them.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use livetable::{
    AggregateFunction, AggregateView, ColumnType, ColumnValue, FilterView, ReadableTable, Schema,
    SortKey, SortedView, Table, TickableTable,
};

type Row = HashMap<String, ColumnValue>;

/// Deterministic LCG so any failure reproduces from (trial, step).
struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        ((self.next_u64() >> 33) as usize) % n
    }
    fn pct(&mut self) -> usize {
        self.below(100)
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        ("id".to_string(), ColumnType::Int64, false),
        ("region".to_string(), ColumnType::String, false),
        ("product".to_string(), ColumnType::String, false),
        ("amount".to_string(), ColumnType::Float64, true),
    ])
}

fn passes(row: &Row) -> bool {
    matches!(row.get("amount"), Some(ColumnValue::Float64(v)) if *v >= 500.0)
}

fn sort_keys() -> Vec<SortKey> {
    // Unique secondary key (id) makes the order total, so live and oracle must
    // match exactly (no tie ambiguity).
    vec![SortKey::descending("amount"), SortKey::ascending("id")]
}

/// Aggregate output columns compared between live and oracle, in order.
const AGG_COLS: [&str; 7] = ["total", "avg", "lo", "hi", "cnt", "med", "p90"];

fn aggs() -> Vec<(String, String, AggregateFunction)> {
    vec![
        ("total".to_string(), "amount".to_string(), AggregateFunction::Sum),
        ("avg".to_string(), "amount".to_string(), AggregateFunction::Avg),
        ("lo".to_string(), "amount".to_string(), AggregateFunction::Min),
        ("hi".to_string(), "amount".to_string(), AggregateFunction::Max),
        ("cnt".to_string(), "amount".to_string(), AggregateFunction::Count),
        ("med".to_string(), "amount".to_string(), AggregateFunction::Median),
        ("p90".to_string(), "amount".to_string(), AggregateFunction::Percentile(0.9)),
    ]
}

fn snapshot(v: &dyn ReadableTable) -> Vec<Row> {
    (0..v.len()).map(|i| v.get_row(i).unwrap()).collect()
}

/// A detached, independent table holding a copy of `base`'s current rows.
fn clone_base(base: &Rc<RefCell<Table>>) -> Rc<RefCell<Table>> {
    let fresh = Rc::new(RefCell::new(Table::new("oracle".to_string(), schema())));
    let src = base.borrow();
    for i in 0..src.len() {
        fresh.borrow_mut().append_row(src.get_row(i).unwrap()).unwrap();
    }
    fresh
}

fn num(row: &Row, k: &str) -> Option<f64> {
    match row.get(k) {
        Some(ColumnValue::Float64(v)) => Some(*v),
        Some(ColumnValue::Int64(v)) => Some(*v as f64),
        Some(ColumnValue::Int32(v)) => Some(*v as f64),
        _ => None,
    }
}

fn id_of(row: &Row) -> i64 {
    match row.get("id") {
        Some(ColumnValue::Int64(v)) => *v,
        other => panic!("bad id {:?}", other),
    }
}

fn close(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * (1.0 + x.abs().max(y.abs())),
        _ => false,
    }
}

fn assert_ordered_eq(label: &str, trial: u64, step: usize, live: &[Row], oracle: &[Row]) {
    assert_eq!(
        live.len(),
        oracle.len(),
        "[{label}] trial {trial} step {step}: length live={} oracle={}\nlive={live:?}\noracle={oracle:?}",
        live.len(),
        oracle.len()
    );
    for (i, (l, o)) in live.iter().zip(oracle.iter()).enumerate() {
        assert_eq!(
            id_of(l),
            id_of(o),
            "[{label}] trial {trial} step {step} pos {i}: id live={} oracle={}\nlive={live:?}\noracle={oracle:?}",
            id_of(l),
            id_of(o)
        );
        assert!(
            close(num(l, "amount"), num(o, "amount")),
            "[{label}] trial {trial} step {step} pos {i}: amount live={:?} oracle={:?}",
            num(l, "amount"),
            num(o, "amount")
        );
    }
}

fn agg_map(rows: &[Row]) -> HashMap<String, Vec<Option<f64>>> {
    let mut m = HashMap::new();
    for r in rows {
        let region = match r.get("region") {
            Some(ColumnValue::String(s)) => s.clone(),
            other => format!("{other:?}"),
        };
        m.insert(region, AGG_COLS.iter().map(|c| num(r, c)).collect());
    }
    m
}

fn assert_agg_eq(label: &str, trial: u64, step: usize, live: &[Row], oracle: &[Row]) {
    let lm = agg_map(live);
    let om = agg_map(oracle);
    assert_eq!(
        lm.len(),
        om.len(),
        "[{label}] trial {trial} step {step}: group count live={lm:?} oracle={om:?}"
    );
    for (k, ov) in &om {
        let lv = lm
            .get(k)
            .unwrap_or_else(|| panic!("[{label}] trial {trial} step {step}: missing group {k}\nlive={lm:?}\noracle={om:?}"));
        for (i, col) in AGG_COLS.iter().enumerate() {
            assert!(
                close(lv[i], ov[i]),
                "[{label}] trial {trial} step {step} group {k} {col}: live={:?} oracle={:?}\nlive_row={lv:?}\noracle_row={ov:?}",
                lv[i],
                ov[i]
            );
        }
    }
}

/// Apply one random mutation (insert / update-amount / update-region / delete)
/// to the root table. Amounts are biased around the 500 filter boundary and
/// regions are mutated, so rows cross the filter, reorder, and migrate groups.
fn apply_random_op(rng: &mut Lcg, base: &Rc<RefCell<Table>>, next_id: &mut i64) {
    const REGIONS: [&str; 4] = ["West", "East", "North", "South"];
    const PRODUCTS: [&str; 3] = ["Widget", "Gadget", "Premium"];

    let amount = |rng: &mut Lcg| {
        if rng.pct() < 12 {
            ColumnValue::Null
        } else {
            ColumnValue::Float64((300 + rng.below(401)) as f64) // 300..=700
        }
    };

    let len = base.borrow().len();
    let roll = rng.pct();

    if len == 0 || roll < 45 {
        let id = *next_id;
        *next_id += 1;
        let mut row: Row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int64(id));
        row.insert("region".to_string(), ColumnValue::String(REGIONS[rng.below(REGIONS.len())].to_string()));
        row.insert("product".to_string(), ColumnValue::String(PRODUCTS[rng.below(PRODUCTS.len())].to_string()));
        let a = amount(rng);
        row.insert("amount".to_string(), a);
        base.borrow_mut().append_row(row).unwrap();
    } else if roll < 70 {
        let idx = rng.below(len);
        let a = amount(rng);
        base.borrow_mut().set_value(idx, "amount", a).unwrap();
    } else if roll < 85 {
        let idx = rng.below(len);
        base.borrow_mut()
            .set_value(idx, "region", ColumnValue::String(REGIONS[rng.below(REGIONS.len())].to_string()))
            .unwrap();
    } else {
        let idx = rng.below(len);
        base.borrow_mut().delete_row(idx).unwrap();
    }
}

/// The live pipeline under test: chained (filter -> sort -> group) AND
/// direct-on-root views, all auto-synced by one TickableTable.
struct Pipeline {
    filter: Rc<RefCell<FilterView>>,
    sorted_chain: Rc<RefCell<SortedView>>,
    agg_chain: Rc<RefCell<AggregateView>>,
    sorted_direct: Rc<RefCell<SortedView>>,
    agg_direct: Rc<RefCell<AggregateView>>,
    tick: TickableTable,
}

fn build_pipeline(base: &Rc<RefCell<Table>>) -> Pipeline {
    let filter = Rc::new(RefCell::new(FilterView::new("f".to_string(), base.clone(), passes)));
    let sorted_chain = Rc::new(RefCell::new(
        SortedView::new("sc".to_string(), filter.clone(), sort_keys()).unwrap(),
    ));
    let agg_chain = Rc::new(RefCell::new(
        AggregateView::new("gc".to_string(), sorted_chain.clone(), vec!["region".to_string()], aggs()).unwrap(),
    ));
    let sorted_direct = Rc::new(RefCell::new(
        SortedView::new("sd".to_string(), base.clone(), sort_keys()).unwrap(),
    ));
    let agg_direct = Rc::new(RefCell::new(
        AggregateView::new("gd".to_string(), base.clone(), vec!["region".to_string()], aggs()).unwrap(),
    ));

    // Registration order = topological order (parents before children).
    let tick = TickableTable::new(base.clone());
    tick.register_filter(&filter);
    tick.register_sorted(&sorted_direct);
    tick.register_aggregate(&agg_direct);
    tick.register_sorted(&sorted_chain);
    tick.register_aggregate(&agg_chain);

    Pipeline { filter, sorted_chain, agg_chain, sorted_direct, agg_direct, tick }
}

/// Assert every live view equals a from-scratch rebuild on the current root.
fn assert_pipeline_matches(trial: u64, step: usize, base: &Rc<RefCell<Table>>, p: &Pipeline) {
    let ob = clone_base(base);
    let of = Rc::new(RefCell::new(FilterView::new("of".to_string(), ob.clone(), passes)));
    let osc = Rc::new(RefCell::new(SortedView::new("osc".to_string(), of.clone(), sort_keys()).unwrap()));
    let ogc = Rc::new(RefCell::new(
        AggregateView::new("ogc".to_string(), osc.clone(), vec!["region".to_string()], aggs()).unwrap(),
    ));
    let osd = Rc::new(RefCell::new(SortedView::new("osd".to_string(), ob.clone(), sort_keys()).unwrap()));
    let ogd = Rc::new(RefCell::new(
        AggregateView::new("ogd".to_string(), ob.clone(), vec!["region".to_string()], aggs()).unwrap(),
    ));

    assert_ordered_eq("filter", trial, step, &snapshot(&*p.filter.borrow()), &snapshot(&*of.borrow()));
    assert_ordered_eq("sorted_direct", trial, step, &snapshot(&*p.sorted_direct.borrow()), &snapshot(&*osd.borrow()));
    assert_ordered_eq("sorted_chain", trial, step, &snapshot(&*p.sorted_chain.borrow()), &snapshot(&*osc.borrow()));
    assert_agg_eq("agg_direct", trial, step, &snapshot(&*p.agg_direct.borrow()), &snapshot(&*ogd.borrow()));
    assert_agg_eq("agg_chain", trial, step, &snapshot(&*p.agg_chain.borrow()), &snapshot(&*ogc.borrow()));
}

#[test]
fn differential_chained_forward_prop_fuzz() {
    for trial in 0..60u64 {
        let mut rng = Lcg(0x9E37_79B9_7F4A_7C15 ^ trial.wrapping_mul(0xD1B5_4A32_D192_ED03));
        let base = Rc::new(RefCell::new(Table::new("base".to_string(), schema())));
        let p = build_pipeline(&base);
        let mut next_id: i64 = 0;

        for step in 0..200usize {
            apply_random_op(&mut rng, &base, &mut next_id);
            p.tick.tick();
            assert_pipeline_matches(trial, step, &base, &p);
        }
    }
}

#[test]
fn differential_batched_forward_prop_fuzz() {
    for trial in 0..60u64 {
        let mut rng = Lcg(0x2545_F491_4F6C_DD1D ^ trial.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let base = Rc::new(RefCell::new(Table::new("base".to_string(), schema())));
        let p = build_pipeline(&base);
        let mut next_id: i64 = 0;

        for step in 0..150usize {
            // 1..=6 mutations applied before a single tick() — a multi-change
            // batch. The deferred end-of-batch MIN/MAX recalc must read row
            // indices that match the parent only after all batch shifts apply.
            let batch = 1 + rng.below(6);
            for _ in 0..batch {
                apply_random_op(&mut rng, &base, &mut next_id);
            }
            p.tick.tick();
            assert_pipeline_matches(trial, step, &base, &p);
        }
    }
}
