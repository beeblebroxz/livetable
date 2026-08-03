//! Build real engine views (`FilterView`/`SortedView`/`AggregateView`) from
//! the wire `ViewNodeSpec` payloads. Pure functions — no actix, no shared
//! state — so they unit-test directly.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use crate::messages::{AggSpec, SortKeySpec, ViewKindSpec, ViewNodeSpec};
use crate::readable::ReadableTable;
use crate::view::{AggregateFunction, AggregateView, FilterView, SortKey, SortedView};

/// Resource limits for one client-defined pipeline. These keep validation
/// deterministic before any views are allocated.
pub const MAX_PIPELINE_NODES: usize = 32;
pub const MAX_PIPELINE_EXPRESSION_BYTES: usize = 4096;
pub const MAX_PIPELINE_NODE_ID_BYTES: usize = 64;
pub const MAX_PIPELINE_FIELD_BYTES: usize = 128;
pub const MAX_PIPELINE_SORT_KEYS: usize = 16;
pub const MAX_PIPELINE_GROUP_KEYS: usize = 16;
pub const MAX_PIPELINE_AGGREGATES: usize = 32;

fn validate_nonempty_bounded(value: &str, label: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!(
            "{label} exceeds the {max_bytes}-byte protocol limit"
        ));
    }
    Ok(())
}

fn validate_node_id(id: &str) -> Result<(), String> {
    validate_nonempty_bounded(id, "node id", MAX_PIPELINE_NODE_ID_BYTES)?;
    if id == "base" {
        return Err("node id 'base' is reserved".to_string());
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(format!(
            "node id '{id}' may contain only letters, numbers, '_' and '-'"
        ));
    }
    Ok(())
}

/// Validate the connection-local DAG shape and bounded wire inputs before
/// allocating real views. A node may reference only `base` or an earlier node,
/// which makes cycles impossible and fixes construction order.
pub fn validate_pipeline_spec(nodes: &[ViewNodeSpec]) -> Result<(), String> {
    if nodes.len() > MAX_PIPELINE_NODES {
        return Err(format!(
            "pipeline has {} nodes; maximum is {MAX_PIPELINE_NODES}",
            nodes.len()
        ));
    }

    let mut known_sources = HashSet::from(["base".to_string()]);
    for node in nodes {
        validate_node_id(&node.id)?;
        if known_sources.contains(&node.id) {
            return Err(format!("duplicate pipeline node id '{}'", node.id));
        }
        if !known_sources.contains(&node.source_id) {
            return Err(format!(
                "node '{}' references unknown or later source '{}'",
                node.id, node.source_id
            ));
        }

        match &node.kind {
            ViewKindSpec::Filter { predicate } => {
                validate_nonempty_bounded(
                    predicate,
                    "filter predicate",
                    MAX_PIPELINE_EXPRESSION_BYTES,
                )?;
            }
            ViewKindSpec::Sort { keys } => {
                if keys.is_empty() {
                    return Err(format!("sort node '{}' needs at least one key", node.id));
                }
                if keys.len() > MAX_PIPELINE_SORT_KEYS {
                    return Err(format!(
                        "sort node '{}' has {} keys; maximum is {MAX_PIPELINE_SORT_KEYS}",
                        node.id,
                        keys.len()
                    ));
                }
                let mut columns = HashSet::new();
                for key in keys {
                    validate_nonempty_bounded(
                        &key.column,
                        "sort column",
                        MAX_PIPELINE_FIELD_BYTES,
                    )?;
                    if !columns.insert(&key.column) {
                        return Err(format!(
                            "sort node '{}' repeats column '{}'",
                            node.id, key.column
                        ));
                    }
                }
            }
            ViewKindSpec::Group { group_by, aggs } => {
                if group_by.is_empty() {
                    return Err(format!(
                        "group node '{}' needs at least one group-by column",
                        node.id
                    ));
                }
                if group_by.len() > MAX_PIPELINE_GROUP_KEYS {
                    return Err(format!(
                        "group node '{}' has {} group-by columns; maximum is {MAX_PIPELINE_GROUP_KEYS}",
                        node.id,
                        group_by.len()
                    ));
                }
                if aggs.is_empty() {
                    return Err(format!(
                        "group node '{}' needs at least one aggregate",
                        node.id
                    ));
                }
                if aggs.len() > MAX_PIPELINE_AGGREGATES {
                    return Err(format!(
                        "group node '{}' has {} aggregates; maximum is {MAX_PIPELINE_AGGREGATES}",
                        node.id,
                        aggs.len()
                    ));
                }

                let mut group_columns = HashSet::new();
                for column in group_by {
                    validate_nonempty_bounded(column, "group-by column", MAX_PIPELINE_FIELD_BYTES)?;
                    if !group_columns.insert(column) {
                        return Err(format!(
                            "group node '{}' repeats group-by column '{}'",
                            node.id, column
                        ));
                    }
                }

                let mut aliases = HashSet::new();
                for agg in aggs {
                    validate_nonempty_bounded(
                        &agg.alias,
                        "aggregate alias",
                        MAX_PIPELINE_FIELD_BYTES,
                    )?;
                    validate_nonempty_bounded(
                        &agg.column,
                        "aggregate column",
                        MAX_PIPELINE_FIELD_BYTES,
                    )?;
                    parse_agg_function(&agg.op)?;
                    if !aliases.insert(&agg.alias) {
                        return Err(format!(
                            "group node '{}' repeats aggregate alias '{}'",
                            node.id, agg.alias
                        ));
                    }
                }
            }
        }

        known_sources.insert(node.id.clone());
    }

    Ok(())
}

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
    if let Some(inner) = op
        .strip_prefix("percentile(")
        .and_then(|s| s.strip_suffix(')'))
    {
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
            if a.column.trim().is_empty() {
                return Err(format!(
                    "aggregate '{}' ({}) requires a column",
                    a.alias, a.op
                ));
            }
            Ok((a.alias.clone(), a.column.clone(), func))
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

    fn valid_pipeline() -> Vec<ViewNodeSpec> {
        vec![
            ViewNodeSpec {
                id: "filtered".into(),
                source_id: "base".into(),
                kind: ViewKindSpec::Filter {
                    predicate: "amount >= 500".into(),
                },
            },
            ViewNodeSpec {
                id: "ranked".into(),
                source_id: "filtered".into(),
                kind: ViewKindSpec::Sort {
                    keys: vec![SortKeySpec {
                        column: "amount".into(),
                        descending: true,
                    }],
                },
            },
            ViewNodeSpec {
                id: "totals".into(),
                source_id: "ranked".into(),
                kind: ViewKindSpec::Group {
                    group_by: vec!["region".into()],
                    aggs: vec![AggSpec {
                        alias: "total".into(),
                        op: "sum".into(),
                        column: "amount".into(),
                    }],
                },
            },
        ]
    }

    #[test]
    fn validates_ordered_pipeline_dag() {
        assert!(validate_pipeline_spec(&valid_pipeline()).is_ok());
        assert!(validate_pipeline_spec(&[]).is_ok());
    }

    #[test]
    fn rejects_duplicate_reserved_and_forward_node_ids() {
        let mut duplicate = valid_pipeline();
        duplicate[1].id = "filtered".into();
        assert!(validate_pipeline_spec(&duplicate)
            .unwrap_err()
            .contains("duplicate"));

        let mut reserved = valid_pipeline();
        reserved[0].id = "base".into();
        assert!(validate_pipeline_spec(&reserved)
            .unwrap_err()
            .contains("reserved"));

        let mut forward = valid_pipeline();
        forward[0].source_id = "ranked".into();
        assert!(validate_pipeline_spec(&forward)
            .unwrap_err()
            .contains("unknown or later"));
    }

    #[test]
    fn rejects_unbounded_or_incomplete_pipeline_specs() {
        let mut too_many = Vec::new();
        for i in 0..=MAX_PIPELINE_NODES {
            too_many.push(ViewNodeSpec {
                id: format!("f{i}"),
                source_id: "base".into(),
                kind: ViewKindSpec::Filter {
                    predicate: "amount >= 0".into(),
                },
            });
        }
        assert!(validate_pipeline_spec(&too_many)
            .unwrap_err()
            .contains("maximum"));

        let mut blank_filter = valid_pipeline();
        blank_filter[0].kind = ViewKindSpec::Filter {
            predicate: " ".into(),
        };
        assert!(validate_pipeline_spec(&blank_filter)
            .unwrap_err()
            .contains("must not be empty"));

        let mut blank_count_column = valid_pipeline();
        let ViewKindSpec::Group { aggs, .. } = &mut blank_count_column[2].kind else {
            panic!("expected group node")
        };
        aggs[0] = AggSpec {
            alias: "count".into(),
            op: "count".into(),
            column: "".into(),
        };
        assert!(validate_pipeline_spec(&blank_count_column)
            .unwrap_err()
            .contains("aggregate column"));
    }

    #[test]
    fn parses_agg_ops() {
        assert_eq!(parse_agg_function("sum").unwrap(), AggregateFunction::Sum);
        assert_eq!(parse_agg_function("AVG").unwrap(), AggregateFunction::Avg);
        assert_eq!(
            parse_agg_function("median").unwrap(),
            AggregateFunction::Median
        );
        assert_eq!(
            parse_agg_function("p95").unwrap(),
            AggregateFunction::Percentile(0.95)
        );
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
            row.insert(
                "region".to_string(),
                ColumnValue::String(region.to_string()),
            );
            row.insert("amount".to_string(), ColumnValue::Float64(amount));
            base.borrow_mut().append_row(row).unwrap();
        }

        let filter = build_filter(base.clone(), "f", "amount >= 500").unwrap();
        let group = build_group(
            filter.clone(),
            "g",
            &["region".to_string()],
            &[
                AggSpec {
                    alias: "total".into(),
                    op: "sum".into(),
                    column: "amount".into(),
                },
                AggSpec {
                    alias: "n".into(),
                    op: "count".into(),
                    column: "amount".into(),
                },
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
