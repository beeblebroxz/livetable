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
    AggregateFunction, AggregateView, ColumnType, ColumnValue, ComputedView, FilterView, JoinType,
    JoinView, ReadableTable, Schema, SortKey, SortedView, Table, TickableTable,
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

fn agg_map(rows: &[Row], key_col: &str) -> HashMap<String, Vec<Option<f64>>> {
    let mut m = HashMap::new();
    for r in rows {
        // Debug-format the group key so String and Int32 keys both work and
        // live/oracle stringify identically.
        let key = format!("{:?}", r.get(key_col));
        m.insert(key, AGG_COLS.iter().map(|c| num(r, c)).collect());
    }
    m
}

fn assert_agg_eq(label: &str, trial: u64, step: usize, key_col: &str, live: &[Row], oracle: &[Row]) {
    let lm = agg_map(live, key_col);
    let om = agg_map(oracle, key_col);
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
    assert_agg_eq("agg_direct", trial, step, "region", &snapshot(&*p.agg_direct.borrow()), &snapshot(&*ogd.borrow()));
    assert_agg_eq("agg_chain", trial, step, "region", &snapshot(&*p.agg_chain.borrow()), &snapshot(&*ogc.borrow()));
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

/// GROUP BY a single Int32 column exercises a distinct fast path in
/// `rebuild_index` (`is_single_int_group`) that the string-keyed fuzz never
/// touches — including its deferred `row_to_group` population. Views are built
/// AFTER seeding rows so the first incremental sync runs against a fast-path
/// rebuild that already holds data.
#[test]
fn differential_int_group_by_fuzz() {
    fn int_schema() -> Schema {
        Schema::new(vec![
            ("id".to_string(), ColumnType::Int64, false),
            ("bucket".to_string(), ColumnType::Int32, false),
            ("amount".to_string(), ColumnType::Float64, true),
        ])
    }
    fn clone_int_base(base: &Rc<RefCell<Table>>) -> Rc<RefCell<Table>> {
        let fresh = Rc::new(RefCell::new(Table::new("oracle".to_string(), int_schema())));
        let src = base.borrow();
        for i in 0..src.len() {
            fresh.borrow_mut().append_row(src.get_row(i).unwrap()).unwrap();
        }
        fresh
    }
    let amount = |rng: &mut Lcg| {
        if rng.pct() < 12 {
            ColumnValue::Null
        } else {
            ColumnValue::Float64((300 + rng.below(401)) as f64)
        }
    };
    let insert = |rng: &mut Lcg, base: &Rc<RefCell<Table>>, next_id: &mut i64| {
        let id = *next_id;
        *next_id += 1;
        let mut row: Row = HashMap::new();
        row.insert("id".to_string(), ColumnValue::Int64(id));
        row.insert("bucket".to_string(), ColumnValue::Int32(rng.below(4) as i32));
        let a = amount(rng);
        row.insert("amount".to_string(), a);
        base.borrow_mut().append_row(row).unwrap();
    };

    for trial in 0..40u64 {
        let mut rng = Lcg(0xABCD_EF01_2345_6789 ^ trial.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let base = Rc::new(RefCell::new(Table::new("base".to_string(), int_schema())));
        let mut next_id: i64 = 0;

        // Seed rows BEFORE building the view so construction rebuilds via the
        // Int32 fast path with data already present.
        for _ in 0..12 {
            insert(&mut rng, &base, &mut next_id);
        }
        let agg = Rc::new(RefCell::new(
            AggregateView::new("g".to_string(), base.clone(), vec!["bucket".to_string()], aggs()).unwrap(),
        ));
        let tick = TickableTable::new(base.clone());
        tick.register_aggregate(&agg);

        for step in 0..200usize {
            let len = base.borrow().len();
            let roll = rng.pct();
            if len == 0 || roll < 45 {
                insert(&mut rng, &base, &mut next_id);
            } else if roll < 70 {
                let idx = rng.below(len);
                let a = amount(&mut rng);
                base.borrow_mut().set_value(idx, "amount", a).unwrap();
            } else if roll < 85 {
                let idx = rng.below(len);
                base.borrow_mut()
                    .set_value(idx, "bucket", ColumnValue::Int32(rng.below(4) as i32))
                    .unwrap();
            } else {
                let idx = rng.below(len);
                base.borrow_mut().delete_row(idx).unwrap();
            }
            tick.tick();

            let ob = clone_int_base(&base);
            let og = Rc::new(RefCell::new(
                AggregateView::new("og".to_string(), ob.clone(), vec!["bucket".to_string()], aggs()).unwrap(),
            ));
            assert_agg_eq("int_agg", trial, step, "bucket", &snapshot(&*agg.borrow()), &snapshot(&*og.borrow()));
        }
    }
}

// ===========================================================================
// JoinView differential fuzz
// ===========================================================================

fn left_schema() -> Schema {
    Schema::new(vec![
        ("lid".to_string(), ColumnType::Int64, false),
        ("k".to_string(), ColumnType::Int32, false),
        ("lval".to_string(), ColumnType::Int32, false),
    ])
}

fn right_schema() -> Schema {
    Schema::new(vec![
        ("rid".to_string(), ColumnType::Int64, false),
        ("k".to_string(), ColumnType::Int32, false),
        ("rval".to_string(), ColumnType::Int32, false),
    ])
}

fn clone_table(src: &Rc<RefCell<Table>>, schema: Schema) -> Rc<RefCell<Table>> {
    let fresh = Rc::new(RefCell::new(Table::new("oracle".to_string(), schema)));
    let s = src.borrow();
    for i in 0..s.len() {
        fresh.borrow_mut().append_row(s.get_row(i).unwrap()).unwrap();
    }
    fresh
}

/// Canonical, order-independent serialization of a row.
fn canon_row(r: &Row) -> String {
    let mut kv: Vec<String> = r.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
    kv.sort();
    kv.join("|")
}

/// Rows as a sorted multiset (join output order may differ between incremental
/// and rebuild, but the SET of rows — with duplicates — must match exactly).
fn multiset(rows: &[Row]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(canon_row).collect();
    v.sort();
    v
}

#[test]
fn differential_join_fuzz() {
    let join_types = [
        ("left", JoinType::Left),
        ("inner", JoinType::Inner),
        ("right", JoinType::Right),
        ("full", JoinType::Full),
    ];

    for (jt_name, jt) in join_types {
        for trial in 0..30u64 {
            let mut rng = Lcg(0x1357_9BDF_0246_8ACE ^ trial.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema())));
            let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema())));
            let mut joined = JoinView::new(
                "j".to_string(),
                left.clone(),
                right.clone(),
                "k".to_string(),
                "k".to_string(),
                jt,
            )
            .unwrap();
            let mut next_lid: i64 = 0;
            let mut next_rid: i64 = 0;

            for step in 0..160usize {
                let llen = left.borrow().len();
                let rlen = right.borrow().len();
                let roll = rng.pct();
                // Small key space (0..4) so rows match 1:1, 1:many, many:many,
                // and go unmatched. Mutate BOTH parents incl. join-key updates.
                if (llen == 0 && rlen == 0) || roll < 28 {
                    let id = next_lid;
                    next_lid += 1;
                    let mut row: Row = HashMap::new();
                    row.insert("lid".to_string(), ColumnValue::Int64(id));
                    row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
                    row.insert("lval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
                    left.borrow_mut().append_row(row).unwrap();
                } else if roll < 54 {
                    let id = next_rid;
                    next_rid += 1;
                    let mut row: Row = HashMap::new();
                    row.insert("rid".to_string(), ColumnValue::Int64(id));
                    row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
                    row.insert("rval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
                    right.borrow_mut().append_row(row).unwrap();
                } else if roll < 66 && llen > 0 {
                    let idx = rng.below(llen);
                    left.borrow_mut().set_value(idx, "k", ColumnValue::Int32(rng.below(4) as i32)).unwrap();
                } else if roll < 78 && rlen > 0 {
                    let idx = rng.below(rlen);
                    right.borrow_mut().set_value(idx, "k", ColumnValue::Int32(rng.below(4) as i32)).unwrap();
                } else if roll < 84 && llen > 0 {
                    let idx = rng.below(llen);
                    left.borrow_mut().set_value(idx, "lval", ColumnValue::Int32(rng.below(1000) as i32)).unwrap();
                } else if roll < 90 && rlen > 0 {
                    let idx = rng.below(rlen);
                    right.borrow_mut().set_value(idx, "rval", ColumnValue::Int32(rng.below(1000) as i32)).unwrap();
                } else if roll < 95 && llen > 0 {
                    let idx = rng.below(llen);
                    left.borrow_mut().delete_row(idx).unwrap();
                } else if rlen > 0 {
                    let idx = rng.below(rlen);
                    right.borrow_mut().delete_row(idx).unwrap();
                } else {
                    // Nothing applicable (e.g. right empty): append left.
                    let id = next_lid;
                    next_lid += 1;
                    let mut row: Row = HashMap::new();
                    row.insert("lid".to_string(), ColumnValue::Int64(id));
                    row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
                    row.insert("lval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
                    left.borrow_mut().append_row(row).unwrap();
                }

                joined.sync();

                // Oracle: a from-scratch join on the current parent states.
                let ol = clone_table(&left, left_schema());
                let or = clone_table(&right, right_schema());
                let ojoined = JoinView::new(
                    "oj".to_string(),
                    ol.clone(),
                    or.clone(),
                    "k".to_string(),
                    "k".to_string(),
                    jt,
                )
                .unwrap();

                let live = multiset(&snapshot(&joined));
                let oracle = multiset(&snapshot(&ojoined));
                assert_eq!(
                    live, oracle,
                    "[join:{jt_name}] trial {trial} step {step}: incremental != rebuild\nlive={live:#?}\noracle={oracle:#?}"
                );
            }
        }
    }
}

// ===========================================================================
// ComputedView differential check
// ===========================================================================

fn tier(row: &Row) -> ColumnValue {
    match row.get("amount") {
        Some(ColumnValue::Float64(v)) if *v >= 500.0 => ColumnValue::String("hi".to_string()),
        _ => ColumnValue::String("lo".to_string()),
    }
}

#[test]
fn differential_computed_view_fuzz() {
    // ComputedView is lazy/pass-through. Verify it always reflects its parent —
    // both directly on the root and over a synced FilterView in a chain — and
    // that the computed column matches a from-scratch ComputedView.
    for trial in 0..30u64 {
        let mut rng = Lcg(0xFACE_B00C_1234_5678 ^ trial.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let base = Rc::new(RefCell::new(Table::new("base".to_string(), schema())));

        let computed_direct = Rc::new(RefCell::new(ComputedView::new(
            "cd".to_string(),
            base.clone(),
            "tier".to_string(),
            tier,
        )));
        let filter = Rc::new(RefCell::new(FilterView::new("f".to_string(), base.clone(), passes)));
        let computed_chain = Rc::new(RefCell::new(ComputedView::new(
            "cc".to_string(),
            filter.clone(),
            "tier".to_string(),
            tier,
        )));
        let tick = TickableTable::new(base.clone());
        tick.register_filter(&filter);

        let mut next_id: i64 = 0;
        for step in 0..120usize {
            apply_random_op(&mut rng, &base, &mut next_id);
            tick.tick();

            let ob = clone_base(&base);
            let ocd = Rc::new(RefCell::new(ComputedView::new(
                "ocd".to_string(),
                ob.clone(),
                "tier".to_string(),
                tier,
            )));
            let ofilt = Rc::new(RefCell::new(FilterView::new("of".to_string(), ob.clone(), passes)));
            let occ = Rc::new(RefCell::new(ComputedView::new(
                "occ".to_string(),
                ofilt.clone(),
                "tier".to_string(),
                tier,
            )));

            assert_eq!(
                multiset(&snapshot(&*computed_direct.borrow())),
                multiset(&snapshot(&*ocd.borrow())),
                "[computed_direct] trial {trial} step {step}"
            );
            assert_eq!(
                multiset(&snapshot(&*computed_chain.borrow())),
                multiset(&snapshot(&*occ.borrow())),
                "[computed_chain] trial {trial} step {step}"
            );
        }
    }
}

/// One random mutation to either join parent (insert/key-update/val-update/
/// delete), shared by the single-change and batched join drivers.
fn join_random_op(
    rng: &mut Lcg,
    left: &Rc<RefCell<Table>>,
    right: &Rc<RefCell<Table>>,
    next_lid: &mut i64,
    next_rid: &mut i64,
) {
    let llen = left.borrow().len();
    let rlen = right.borrow().len();
    // Cap total rows: with a key space of 4 the many-to-many join output grows
    // quadratically, so keep the parents small for speed. When over cap, delete.
    if llen + rlen >= 24 {
        if llen >= rlen && llen > 0 {
            left.borrow_mut().delete_row(rng.below(llen)).unwrap();
        } else if rlen > 0 {
            right.borrow_mut().delete_row(rng.below(rlen)).unwrap();
        }
        return;
    }
    let roll = rng.pct();
    if (llen == 0 && rlen == 0) || roll < 28 {
        let id = *next_lid;
        *next_lid += 1;
        let mut row: Row = HashMap::new();
        row.insert("lid".to_string(), ColumnValue::Int64(id));
        row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
        row.insert("lval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
        left.borrow_mut().append_row(row).unwrap();
    } else if roll < 54 {
        let id = *next_rid;
        *next_rid += 1;
        let mut row: Row = HashMap::new();
        row.insert("rid".to_string(), ColumnValue::Int64(id));
        row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
        row.insert("rval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
        right.borrow_mut().append_row(row).unwrap();
    } else if roll < 66 && llen > 0 {
        let idx = rng.below(llen);
        left.borrow_mut().set_value(idx, "k", ColumnValue::Int32(rng.below(4) as i32)).unwrap();
    } else if roll < 78 && rlen > 0 {
        let idx = rng.below(rlen);
        right.borrow_mut().set_value(idx, "k", ColumnValue::Int32(rng.below(4) as i32)).unwrap();
    } else if roll < 84 && llen > 0 {
        let idx = rng.below(llen);
        left.borrow_mut().set_value(idx, "lval", ColumnValue::Int32(rng.below(1000) as i32)).unwrap();
    } else if roll < 90 && rlen > 0 {
        let idx = rng.below(rlen);
        right.borrow_mut().set_value(idx, "rval", ColumnValue::Int32(rng.below(1000) as i32)).unwrap();
    } else if roll < 95 && llen > 0 {
        let idx = rng.below(llen);
        left.borrow_mut().delete_row(idx).unwrap();
    } else if rlen > 0 {
        let idx = rng.below(rlen);
        right.borrow_mut().delete_row(idx).unwrap();
    } else {
        let id = *next_lid;
        *next_lid += 1;
        let mut row: Row = HashMap::new();
        row.insert("lid".to_string(), ColumnValue::Int64(id));
        row.insert("k".to_string(), ColumnValue::Int32(rng.below(4) as i32));
        row.insert("lval".to_string(), ColumnValue::Int32(rng.below(1000) as i32));
        left.borrow_mut().append_row(row).unwrap();
    }
}

#[test]
fn differential_join_batched_fuzz() {
    let join_types = [
        ("left", JoinType::Left),
        ("inner", JoinType::Inner),
        ("right", JoinType::Right),
        ("full", JoinType::Full),
    ];
    for (jt_name, jt) in join_types {
        for trial in 0..30u64 {
            let mut rng = Lcg(0x0F1E_2D3C_4B5A_6978 ^ trial.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let left = Rc::new(RefCell::new(Table::new("left".to_string(), left_schema())));
            let right = Rc::new(RefCell::new(Table::new("right".to_string(), right_schema())));
            let mut joined = JoinView::new(
                "j".to_string(), left.clone(), right.clone(),
                "k".to_string(), "k".to_string(), jt,
            ).unwrap();
            let mut next_lid: i64 = 0;
            let mut next_rid: i64 = 0;

            for step in 0..120usize {
                // Multi-change batch (1..=5 mutations) before one sync().
                let batch = 1 + rng.below(5);
                for _ in 0..batch {
                    join_random_op(&mut rng, &left, &right, &mut next_lid, &mut next_rid);
                }
                joined.sync();

                let ol = clone_table(&left, left_schema());
                let or = clone_table(&right, right_schema());
                let ojoined = JoinView::new(
                    "oj".to_string(), ol.clone(), or.clone(),
                    "k".to_string(), "k".to_string(), jt,
                ).unwrap();

                let live = multiset(&snapshot(&joined));
                let oracle = multiset(&snapshot(&ojoined));
                assert_eq!(
                    live, oracle,
                    "[join_batched:{jt_name}] trial {trial} step {step}: incremental != rebuild\nlive={live:#?}\noracle={oracle:#?}"
                );
            }
        }
    }
}
