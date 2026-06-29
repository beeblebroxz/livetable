//! Build real engine views (`FilterView`/`SortedView`/`AggregateView`) from
//! the wire `ViewNodeSpec` payloads. Pure functions — no actix, no shared
//! state — so they unit-test directly.

use std::cell::RefCell;
use std::rc::Rc;

use crate::messages::{AggSpec, SortKeySpec};
use crate::readable::ReadableTable;
use crate::view::{AggregateFunction, AggregateView, FilterView, SortKey, SortedView};

/// Map an engine-syntax aggregate op string to an `AggregateFunction`.
///
/// Accepts `sum`, `count`, `avg`/`average`/`mean`, `min`, `max`, `median`,
/// the `pNN` shorthand (`p95` → 95th percentile), and the explicit
/// `percentile(x)` form with `x` in `0.0..=1.0`. Case-insensitive.
pub fn parse_agg_function(op: &str) -> Result<AggregateFunction, String> {
    let op = op.trim().to_lowercase();
    match op.as_str() {
        "sum" => return Ok(AggregateFunction::Sum),
        "count" => return Ok(AggregateFunction::Count),
        "avg" | "average" | "mean" => return Ok(AggregateFunction::Avg),
        "min" => return Ok(AggregateFunction::Min),
        "max" => return Ok(AggregateFunction::Max),
        "median" => return Ok(AggregateFunction::Median),
        _ => {}
    }

    // `percentile(x)` with x in 0.0..=1.0 (checked before pNN: both start 'p').
    if let Some(inner) = op.strip_prefix("percentile(").and_then(|s| s.strip_suffix(')')) {
        let p: f64 = inner
            .trim()
            .parse()
            .map_err(|_| format!("percentile() needs a number, got '{}'", inner.trim()))?;
        if !(0.0..=1.0).contains(&p) {
            return Err(format!("percentile fraction must be in 0.0..=1.0, got {p}"));
        }
        return Ok(AggregateFunction::Percentile(p));
    }

    // `pNN` shorthand with NN in 0..=100.
    if let Some(rest) = op.strip_prefix('p') {
        if let Ok(n) = rest.parse::<f64>() {
            if (0.0..=100.0).contains(&n) {
                return Ok(AggregateFunction::Percentile(n / 100.0));
            }
        }
    }

    Err(format!("unknown aggregate op '{op}'"))
}

/// Build a `FilterView` from a `filter_expr` predicate string.
pub fn build_filter(
    parent: Rc<RefCell<dyn ReadableTable>>,
    id: &str,
    predicate: &str,
) -> Result<Rc<RefCell<FilterView>>, String> {
    let expr = crate::expr::parse_expr(predicate)?;
    let view = FilterView::new(id.to_string(), parent, move |row| {
        crate::expr::eval_expr(&expr, row)
    });
    Ok(Rc::new(RefCell::new(view)))
}

/// Build a `SortedView` from sort-key specs.
pub fn build_sort(
    parent: Rc<RefCell<dyn ReadableTable>>,
    id: &str,
    keys: &[SortKeySpec],
) -> Result<Rc<RefCell<SortedView>>, String> {
    if keys.is_empty() {
        return Err("sort needs at least one key".to_string());
    }
    let sort_keys: Vec<SortKey> = keys
        .iter()
        .map(|k| {
            if k.descending {
                SortKey::descending(&k.column)
            } else {
                SortKey::ascending(&k.column)
            }
        })
        .collect();
    Ok(Rc::new(RefCell::new(SortedView::new(
        id.to_string(),
        parent,
        sort_keys,
    )?)))
}

/// Build an `AggregateView` (GROUP BY) from group-by columns and agg specs.
///
/// Every aggregate requires an explicit source column. `count` is SQL
/// `COUNT(col)` — the count of non-null values of that column (the engine has
/// no row-count aggregate); for a column with no nulls this equals the group's
/// row count.
pub fn build_group(
    parent: Rc<RefCell<dyn ReadableTable>>,
    id: &str,
    group_by: &[String],
    aggs: &[AggSpec],
) -> Result<Rc<RefCell<AggregateView>>, String> {
    if group_by.is_empty() {
        return Err("group needs at least one group-by column".to_string());
    }
    let specs = aggs
        .iter()
        .map(|a| {
            let func = parse_agg_function(&a.op)?;
            let col = a
                .column
                .clone()
                .ok_or_else(|| format!("aggregate '{}' ({}) requires a column", a.alias, a.op))?;
            Ok((a.alias.clone(), col, func))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Rc::new(RefCell::new(AggregateView::new(
        id.to_string(),
        parent,
        group_by.to_vec(),
        specs,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::{ColumnType, ColumnValue};
    use crate::table::{Schema, Table};
    use std::collections::HashMap;

    #[test]
    fn parses_agg_ops() {
        assert_eq!(parse_agg_function("sum").unwrap(), AggregateFunction::Sum);
        assert_eq!(parse_agg_function("AVG").unwrap(), AggregateFunction::Avg);
        assert_eq!(parse_agg_function("median").unwrap(), AggregateFunction::Median);
        assert_eq!(parse_agg_function("p95").unwrap(), AggregateFunction::Percentile(0.95));
        assert_eq!(
            parse_agg_function("percentile(0.25)").unwrap(),
            AggregateFunction::Percentile(0.25)
        );
        assert!(parse_agg_function("bogus").is_err());
        assert!(parse_agg_function("percentile(2.0)").is_err());
    }

    #[test]
    fn builds_filter_group_chain() {
        let schema = Schema::new(vec![
            ("region".to_string(), ColumnType::String, false),
            ("amount".to_string(), ColumnType::Float64, true),
        ]);
        let base = Rc::new(RefCell::new(Table::new("t".to_string(), schema)));
        for (region, amount) in [("West", 600.0), ("West", 400.0), ("East", 800.0)] {
            let mut row = HashMap::new();
            row.insert("region".to_string(), ColumnValue::String(region.to_string()));
            row.insert("amount".to_string(), ColumnValue::Float64(amount));
            base.borrow_mut().append_row(row).unwrap();
        }

        let filter = build_filter(base.clone(), "f", "amount >= 500").unwrap();
        let group = build_group(
            filter.clone(),
            "g",
            &["region".to_string()],
            &[
                AggSpec { alias: "total".into(), op: "sum".into(), column: Some("amount".into()) },
                AggSpec { alias: "n".into(), op: "count".into(), column: Some("amount".into()) },
            ],
        )
        .unwrap();

        // Filter keeps the two >=500 rows (West 600, East 800).
        assert_eq!(filter.borrow().len(), 2);
        // Two groups; West total=600 n=1, East total=800 n=1.
        let g = group.borrow();
        let mut seen = std::collections::HashMap::new();
        for i in 0..g.len() {
            let row = g.get_row(i).unwrap();
            let region = match row.get("region") {
                Some(ColumnValue::String(s)) => s.clone(),
                other => panic!("bad region {other:?}"),
            };
            seen.insert(region, (row.get("total").cloned(), row.get("n").cloned()));
        }
        assert_eq!(seen.len(), 2);
        assert_eq!(seen["West"].0, Some(ColumnValue::Float64(600.0)));
        assert_eq!(seen["West"].1, Some(ColumnValue::Int64(1)));
        assert_eq!(seen["East"].0, Some(ColumnValue::Float64(800.0)));
    }
}
