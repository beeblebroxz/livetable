//! Single-threaded owner for server tables and connection-local view pipelines.
//!
//! Engine views use `Rc<RefCell<...>>`, so this type deliberately stays off
//! shared worker state. The WebSocket layer hosts it in one Actix actor and
//! sends commands to that actor from every connection.

use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::column::{ColumnType, ColumnValue};
use crate::messages::{ServerMessage, ViewKindSpec, ViewNodeSpec, WireTableRow, WireViewRow};
use crate::pipeline_spec::{build_filter, build_group, build_sort, validate_pipeline_spec};
use crate::readable::ReadableTable;
use crate::table::{Schema, Table};
use crate::view::TickableTable;

pub type ConnId = u64;
type JsonRow = HashMap<String, JsonValue>;

#[derive(Debug, Clone)]
pub struct NodeSnapshot {
    pub pipeline_generation: u32,
    pub node_id: String,
    pub source_id: String,
    pub kind: String,
    pub seq: u64,
    pub columns: Vec<String>,
    pub rows: Vec<WireViewRow>,
}

pub type PipelineBuildResult = Result<NodeSnapshot, (String, String)>;

struct ViewNode {
    id: String,
    source_id: String,
    kind: String,
    view: Rc<RefCell<dyn ReadableTable>>,
    last_seq: u64,
}

struct Pipeline {
    generation: u32,
    nodes: Vec<ViewNode>,
    base_last_seq: u64,
}

struct BaseState {
    table: Rc<RefCell<Table>>,
    tickable: TickableTable,
    row_ids: Vec<u64>,
    next_row_id: u64,
    pipelines: HashMap<ConnId, Pipeline>,
}

impl BaseState {
    fn with_seed_data(table: Table) -> Self {
        let row_count = table.len() as u64;
        let table = Rc::new(RefCell::new(table));
        let tickable = TickableTable::new(table.clone());
        Self {
            table,
            tickable,
            row_ids: (1..=row_count).collect(),
            next_row_id: row_count + 1,
            pipelines: HashMap::new(),
        }
    }

    fn row_index_by_id(&self, row_id: u64) -> Option<usize> {
        self.row_ids
            .iter()
            .position(|candidate| *candidate == row_id)
    }

    fn base_seq(&self) -> u64 {
        self.table.borrow().changeset().total_len() as u64
    }

    fn base_snapshot(&self, generation: u32) -> Result<NodeSnapshot, String> {
        let table = self.table.borrow();
        let columns = table.column_names();
        let mut rows = Vec::with_capacity(table.len());
        for (index, row_id) in self.row_ids.iter().enumerate() {
            rows.push(WireViewRow {
                row_id: Some(*row_id),
                row: row_to_json(&table.get_row(index)?),
            });
        }
        Ok(NodeSnapshot {
            pipeline_generation: generation,
            node_id: "base".to_string(),
            source_id: "base".to_string(),
            kind: "base".to_string(),
            seq: table.changeset().total_len() as u64,
            columns,
            rows,
        })
    }
}

/// Server-side table/view state. This type is intentionally `!Send`.
pub struct TableEngine {
    bases: HashMap<String, BaseState>,
}

impl Default for TableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TableEngine {
    pub fn new() -> Self {
        let schema = Schema::new(vec![
            ("region".to_string(), ColumnType::String, false),
            ("product".to_string(), ColumnType::String, false),
            ("amount".to_string(), ColumnType::Float64, false),
        ]);
        let mut demo = Table::new("demo".to_string(), schema);

        let mut row = HashMap::new();
        row.insert(
            "region".to_string(),
            ColumnValue::String("West".to_string()),
        );
        row.insert(
            "product".to_string(),
            ColumnValue::String("Widget".to_string()),
        );
        row.insert("amount".to_string(), ColumnValue::Float64(100.5));
        demo.append_row(row).expect("valid demo seed row");

        let mut row = HashMap::new();
        row.insert(
            "region".to_string(),
            ColumnValue::String("East".to_string()),
        );
        row.insert(
            "product".to_string(),
            ColumnValue::String("Gadget".to_string()),
        );
        row.insert("amount".to_string(), ColumnValue::Float64(200.75));
        demo.append_row(row).expect("valid demo seed row");
        demo.clear_changeset();

        Self {
            bases: HashMap::from([("demo".to_string(), BaseState::with_seed_data(demo))]),
        }
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        self.bases.contains_key(table_name)
    }

    pub fn query_table(&self, table_name: &str) -> Result<ServerMessage, String> {
        let base = self
            .bases
            .get(table_name)
            .ok_or_else(|| format!("Table '{table_name}' not found"))?;
        let table = base.table.borrow();
        let columns = table.column_names();
        let mut rows = Vec::with_capacity(table.len());
        for (index, row_id) in base.row_ids.iter().enumerate() {
            rows.push(WireTableRow {
                row_id: *row_id,
                row: row_to_json(&table.get_row(index)?),
            });
        }
        Ok(ServerMessage::TableData {
            table_name: table_name.to_string(),
            seq: table.changeset().total_len() as u64,
            columns,
            rows,
        })
    }

    pub fn insert_row(&mut self, table_name: &str, row: JsonRow) -> Result<ServerMessage, String> {
        let base = self
            .bases
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{table_name}' not found"))?;
        let converted = convert_row_for_schema(base.table.borrow().schema(), &row)?;
        let index = base.table.borrow().len();
        let row_id = base.next_row_id;
        base.table.borrow_mut().append_row(converted.clone())?;
        base.next_row_id += 1;
        base.row_ids.push(row_id);
        Ok(ServerMessage::RowInserted {
            table_name: table_name.to_string(),
            seq: base.base_seq(),
            index,
            row_id,
            row: row_to_json(&converted),
        })
    }

    pub fn update_cell(
        &mut self,
        table_name: &str,
        row_id: u64,
        column: &str,
        value: &JsonValue,
    ) -> Result<ServerMessage, String> {
        let base = self
            .bases
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{table_name}' not found"))?;
        let row_index = base
            .row_index_by_id(row_id)
            .ok_or_else(|| format!("Row '{row_id}' not found"))?;
        let table = base.table.borrow();
        let col_type = table
            .schema()
            .get_column_type(column)
            .ok_or_else(|| format!("Column '{column}' not found"))?;
        let nullable = table.schema().is_column_nullable(column).unwrap_or(false);
        let converted = json_to_column_value_typed(value, col_type, nullable)
            .map_err(|err| format!("Column '{column}': {err}"))?;
        drop(table);
        base.table
            .borrow_mut()
            .set_value(row_index, column, converted.clone())?;
        Ok(ServerMessage::CellUpdated {
            table_name: table_name.to_string(),
            seq: base.base_seq(),
            row_id,
            column: column.to_string(),
            value: column_value_to_json(&converted),
        })
    }

    pub fn delete_row(&mut self, table_name: &str, row_id: u64) -> Result<ServerMessage, String> {
        let base = self
            .bases
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{table_name}' not found"))?;
        let row_index = base
            .row_index_by_id(row_id)
            .ok_or_else(|| format!("Row '{row_id}' not found"))?;
        base.table.borrow_mut().delete_row(row_index)?;
        base.row_ids.remove(row_index);
        Ok(ServerMessage::RowDeleted {
            table_name: table_name.to_string(),
            seq: base.base_seq(),
            row_id,
        })
    }

    /// Define or replace one connection's pipeline and return its initial
    /// snapshots. Structural validation is atomic: on failure, the previous
    /// pipeline remains installed. A node build error installs the valid
    /// prefix and returns an error for the first invalid node.
    pub fn set_pipeline(
        &mut self,
        conn: ConnId,
        table_name: &str,
        pipeline_generation: u32,
        specs: &[ViewNodeSpec],
    ) -> Vec<PipelineBuildResult> {
        if let Err(message) = validate_pipeline_spec(specs) {
            return vec![Err(("pipeline".to_string(), message))];
        }

        let Some(base) = self.bases.get_mut(table_name) else {
            return vec![Err((
                "pipeline".to_string(),
                format!("Table '{table_name}' not found"),
            ))];
        };

        let base_source: Rc<RefCell<dyn ReadableTable>> = base.table.clone();
        let mut sources = HashMap::from([("base".to_string(), base_source)]);
        let mut nodes = Vec::with_capacity(specs.len());
        let mut results = Vec::with_capacity(specs.len() + 1);

        match base.base_snapshot(pipeline_generation) {
            Ok(snapshot) => results.push(Ok(snapshot)),
            Err(message) => {
                return vec![Err(("base".to_string(), message))];
            }
        }

        for spec in specs {
            let parent = sources
                .get(&spec.source_id)
                .expect("validated source must exist")
                .clone();
            let built: Result<(String, Rc<RefCell<dyn ReadableTable>>), String> = match &spec.kind {
                ViewKindSpec::Filter { predicate } => build_filter(parent, &spec.id, predicate)
                    .map(|view| {
                        base.tickable.register_filter(&view);
                        let readable: Rc<RefCell<dyn ReadableTable>> = view;
                        ("filter".to_string(), readable)
                    }),
                ViewKindSpec::Sort { keys } => build_sort(parent, &spec.id, keys).map(|view| {
                    base.tickable.register_sorted(&view);
                    let readable: Rc<RefCell<dyn ReadableTable>> = view;
                    ("sort".to_string(), readable)
                }),
                ViewKindSpec::Group { group_by, aggs } => {
                    build_group(parent, &spec.id, group_by, aggs).map(|view| {
                        base.tickable.register_aggregate(&view);
                        let readable: Rc<RefCell<dyn ReadableTable>> = view;
                        ("group".to_string(), readable)
                    })
                }
            };

            let (kind, view) = match built {
                Ok(built) => built,
                Err(message) => {
                    results.push(Err((spec.id.clone(), message)));
                    break;
                }
            };
            let seq = view.borrow().version();
            match snapshot_view(
                pipeline_generation,
                &spec.id,
                &spec.source_id,
                &kind,
                seq,
                &view,
            ) {
                Ok(snapshot) => results.push(Ok(snapshot)),
                Err(message) => {
                    results.push(Err((spec.id.clone(), message)));
                    break;
                }
            }
            sources.insert(spec.id.clone(), view.clone());
            nodes.push(ViewNode {
                id: spec.id.clone(),
                source_id: spec.source_id.clone(),
                kind,
                view,
                last_seq: seq,
            });
        }

        let base_last_seq = base.base_seq();
        base.pipelines.insert(
            conn,
            Pipeline {
                generation: pipeline_generation,
                nodes,
                base_last_seq,
            },
        );
        results
    }

    /// Propagate one pending base mutation and collect every node whose
    /// sequence advanced. The caller must invoke this once per mutation.
    pub fn tick_and_collect(&mut self, table_name: &str) -> HashMap<ConnId, Vec<NodeSnapshot>> {
        let Some(base) = self.bases.get_mut(table_name) else {
            return HashMap::new();
        };
        base.tickable.tick();
        let base_seq = base.base_seq();
        let base_table = base.table.clone();
        let row_ids = base.row_ids.clone();
        let mut collected = HashMap::new();

        for (conn, pipeline) in &mut base.pipelines {
            let mut snapshots = Vec::new();
            if base_seq > pipeline.base_last_seq {
                if let Ok(snapshot) =
                    snapshot_base(pipeline.generation, base_seq, &base_table, &row_ids)
                {
                    snapshots.push(snapshot);
                }
                pipeline.base_last_seq = base_seq;
            }
            for node in &mut pipeline.nodes {
                let seq = node.view.borrow().version();
                if seq != node.last_seq {
                    if let Ok(snapshot) = snapshot_view(
                        pipeline.generation,
                        &node.id,
                        &node.source_id,
                        &node.kind,
                        seq,
                        &node.view,
                    ) {
                        snapshots.push(snapshot);
                    }
                    node.last_seq = seq;
                }
            }
            if !snapshots.is_empty() {
                collected.insert(*conn, snapshots);
            }
        }
        collected
    }

    pub fn drop_connection(&mut self, conn: ConnId) {
        for base in self.bases.values_mut() {
            base.pipelines.remove(&conn);
        }
    }
}

fn snapshot_base(
    generation: u32,
    seq: u64,
    table: &Rc<RefCell<Table>>,
    row_ids: &[u64],
) -> Result<NodeSnapshot, String> {
    let table = table.borrow();
    let mut rows = Vec::with_capacity(table.len());
    for (index, row_id) in row_ids.iter().enumerate() {
        rows.push(WireViewRow {
            row_id: Some(*row_id),
            row: row_to_json(&table.get_row(index)?),
        });
    }
    Ok(NodeSnapshot {
        pipeline_generation: generation,
        node_id: "base".to_string(),
        source_id: "base".to_string(),
        kind: "base".to_string(),
        seq,
        columns: table.column_names(),
        rows,
    })
}

fn snapshot_view(
    generation: u32,
    node_id: &str,
    source_id: &str,
    kind: &str,
    seq: u64,
    view: &Rc<RefCell<dyn ReadableTable>>,
) -> Result<NodeSnapshot, String> {
    let view = view.borrow();
    let mut rows = Vec::with_capacity(view.len());
    for index in 0..view.len() {
        rows.push(WireViewRow {
            row_id: None,
            row: row_to_json(&view.get_row(index)?),
        });
    }
    Ok(NodeSnapshot {
        pipeline_generation: generation,
        node_id: node_id.to_string(),
        source_id: source_id.to_string(),
        kind: kind.to_string(),
        seq,
        columns: view.column_names(),
        rows,
    })
}

pub(crate) fn column_value_to_json(value: &ColumnValue) -> JsonValue {
    match value {
        ColumnValue::Int32(value) => JsonValue::Number((*value).into()),
        ColumnValue::Int64(value) => JsonValue::Number((*value).into()),
        ColumnValue::Float32(value) => serde_json::Number::from_f64(*value as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ColumnValue::Float64(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ColumnValue::String(value) => JsonValue::String(value.clone()),
        ColumnValue::Bool(value) => JsonValue::Bool(*value),
        ColumnValue::Date(days) => JsonValue::String(format_date_from_days(*days)),
        ColumnValue::DateTime(millis) => JsonValue::String(format_datetime_from_millis(*millis)),
        ColumnValue::Null => JsonValue::Null,
    }
}

fn row_to_json(row: &HashMap<String, ColumnValue>) -> JsonRow {
    row.iter()
        .map(|(key, value)| (key.clone(), column_value_to_json(value)))
        .collect()
}

pub(crate) fn json_to_column_value_typed(
    value: &JsonValue,
    col_type: ColumnType,
    nullable: bool,
) -> Result<ColumnValue, String> {
    if value.is_null() {
        return if nullable {
            Ok(ColumnValue::Null)
        } else {
            Err("NULL value for non-nullable column".to_string())
        };
    }
    match col_type {
        ColumnType::Int32 => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(ColumnValue::Int32)
            .ok_or_else(|| "Expected INT32 number".to_string()),
        ColumnType::Int64 => value
            .as_i64()
            .map(ColumnValue::Int64)
            .ok_or_else(|| "Expected INT64 number".to_string()),
        ColumnType::Float32 => value
            .as_f64()
            .map(|value| ColumnValue::Float32(value as f32))
            .ok_or_else(|| "Expected FLOAT32 number".to_string()),
        ColumnType::Float64 => value
            .as_f64()
            .map(ColumnValue::Float64)
            .ok_or_else(|| "Expected FLOAT64 number".to_string()),
        ColumnType::String => value
            .as_str()
            .map(|value| ColumnValue::String(value.to_string()))
            .ok_or_else(|| "Expected STRING value".to_string()),
        ColumnType::Bool => value
            .as_bool()
            .map(ColumnValue::Bool)
            .ok_or_else(|| "Expected BOOL value".to_string()),
        ColumnType::Date => match value {
            JsonValue::Number(number) => number
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .map(ColumnValue::Date)
                .ok_or_else(|| "Expected DATE as days-since-epoch integer".to_string()),
            JsonValue::String(value) => parse_date(value)
                .map(ColumnValue::Date)
                .ok_or_else(|| "Expected DATE string in YYYY-MM-DD format".to_string()),
            _ => Err("Expected DATE value".to_string()),
        },
        ColumnType::DateTime => match value {
            JsonValue::Number(number) => number
                .as_i64()
                .map(ColumnValue::DateTime)
                .ok_or_else(|| "Expected DATETIME as millis-since-epoch integer".to_string()),
            JsonValue::String(value) => parse_datetime(value)
                .map(ColumnValue::DateTime)
                .ok_or_else(|| "Expected DATETIME string in ISO format".to_string()),
            _ => Err("Expected DATETIME value".to_string()),
        },
    }
}

pub(crate) fn convert_row_for_schema(
    schema: &Schema,
    row: &JsonRow,
) -> Result<HashMap<String, ColumnValue>, String> {
    for key in row.keys() {
        if schema.get_column_index(key).is_none() {
            return Err(format!("Unknown column '{key}'"));
        }
    }
    let mut converted = HashMap::new();
    for index in 0..schema.len() {
        let (name, col_type, nullable) = schema
            .get_column_info(index)
            .expect("schema index must be valid");
        let value = row
            .get(name)
            .ok_or_else(|| format!("Missing value for column '{name}'"))?;
        converted.insert(
            name.to_string(),
            json_to_column_value_typed(value, col_type, nullable)
                .map_err(|err| format!("Column '{name}': {err}"))?,
        );
    }
    Ok(converted)
}

fn ymd_from_days(days: i32) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

fn format_date_from_days(days: i32) -> String {
    let (year, month, day) = ymd_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn format_datetime_from_millis(millis: i64) -> String {
    let days = millis.div_euclid(86_400_000) as i32;
    let time = millis.rem_euclid(86_400_000) as u32;
    let (year, month, day) = ymd_from_days(days);
    let hour = time / 3_600_000;
    let minute = (time % 3_600_000) / 60_000;
    let second = (time % 60_000) / 1000;
    let millis = time % 1000;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn parse_date(value: &str) -> Option<i32> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_from_ymd(year, month, day))
}

fn parse_datetime(value: &str) -> Option<i64> {
    let (date, time) = value
        .split_once('T')
        .or_else(|| value.split_once(' '))
        .unwrap_or((value, ""));
    let days = parse_date(date)?;
    if time.is_empty() {
        return Some(days as i64 * 86_400_000);
    }
    let time = time.trim_end_matches('Z');
    let (time, millis) = match time.split_once('.') {
        Some((time, fraction)) => {
            let fraction = &fraction[..fraction.len().min(3)];
            let millis = format!("{fraction:0<3}").parse::<u32>().ok()?;
            (time, millis)
        }
        None => (time, 0),
    };
    let mut parts = time.split(':');
    let hour: u32 = parts.next()?.parse().ok()?;
    let minute: u32 = parts.next()?.parse().ok()?;
    let second: u32 = parts.next().map(str::parse).transpose().ok()?.unwrap_or(0);
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(
        days as i64 * 86_400_000
            + hour as i64 * 3_600_000
            + minute as i64 * 60_000
            + second as i64 * 1000
            + millis as i64,
    )
}

fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 {
        year / 400
    } else {
        (year - 399) / 400
    };
    let year_of_era = (year - era * 400) as u32;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146097 + day_of_era as i32 - 719468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{AggSpec, SortKeySpec};
    use serde_json::json;

    #[derive(Clone)]
    struct ShadowRow {
        id: u64,
        region: String,
        product: String,
        amount: f64,
    }

    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, limit: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (self.0 as usize) % limit
        }
    }

    fn pipeline_specs() -> Vec<ViewNodeSpec> {
        vec![
            ViewNodeSpec {
                id: "filtered".into(),
                source_id: "base".into(),
                kind: ViewKindSpec::Filter {
                    predicate: "amount >= 150".into(),
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
    fn builds_pipeline_and_ticks_after_mutation() {
        let mut engine = TableEngine::new();
        let initial = engine.set_pipeline(7, "demo", 3, &pipeline_specs());
        assert_eq!(initial.len(), 4);
        assert!(initial.iter().all(Result::is_ok));
        let base = initial[0].as_ref().unwrap();
        assert_eq!(base.node_id, "base");
        assert!(base.rows.iter().all(|row| row.row_id.is_some()));
        let derived = initial[1].as_ref().unwrap();
        assert!(derived.rows.iter().all(|row| row.row_id.is_none()));

        engine
            .insert_row(
                "demo",
                HashMap::from([
                    ("region".into(), json!("West")),
                    ("product".into(), json!("Premium")),
                    ("amount".into(), json!(300.0)),
                ]),
            )
            .unwrap();
        let collected = engine.tick_and_collect("demo");
        let snapshots = &collected[&7];
        assert_eq!(snapshots.len(), 4);
        assert_eq!(snapshots[0].node_id, "base");
        let totals = snapshots
            .iter()
            .find(|snapshot| snapshot.node_id == "totals")
            .unwrap();
        let west = totals
            .rows
            .iter()
            .find(|row| row.row["region"] == json!("West"))
            .unwrap();
        assert_eq!(west.row["total"], json!(300.0));
    }

    #[test]
    fn pipelines_are_connection_local_and_generation_scoped() {
        let mut engine = TableEngine::new();
        let first = engine.set_pipeline(1, "demo", 10, &pipeline_specs());
        assert!(first.iter().all(Result::is_ok));
        let second = engine.set_pipeline(2, "demo", 20, &[]);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].as_ref().unwrap().pipeline_generation, 20);

        engine
            .update_cell("demo", 1, "amount", &json!(500.0))
            .unwrap();
        let collected = engine.tick_and_collect("demo");
        assert_eq!(collected[&1].len(), 4);
        assert_eq!(collected[&2].len(), 1);
        assert!(collected[&1]
            .iter()
            .all(|snapshot| snapshot.pipeline_generation == 10));
        assert!(collected[&2]
            .iter()
            .all(|snapshot| snapshot.pipeline_generation == 20));

        engine.drop_connection(1);
        engine.delete_row("demo", 2).unwrap();
        let collected = engine.tick_and_collect("demo");
        assert!(!collected.contains_key(&1));
        assert!(collected.contains_key(&2));
    }

    #[test]
    fn structural_failure_preserves_previous_pipeline() {
        let mut engine = TableEngine::new();
        engine.set_pipeline(1, "demo", 1, &pipeline_specs());
        let invalid = vec![ViewNodeSpec {
            id: "bad".into(),
            source_id: "later".into(),
            kind: ViewKindSpec::Filter {
                predicate: "amount > 0".into(),
            },
        }];
        let result = engine.set_pipeline(1, "demo", 2, &invalid);
        assert!(result[0].is_err());

        engine
            .update_cell("demo", 1, "amount", &json!(500.0))
            .unwrap();
        let snapshots = engine.tick_and_collect("demo");
        assert_eq!(snapshots[&1].len(), 4);
        assert!(snapshots[&1]
            .iter()
            .all(|snapshot| snapshot.pipeline_generation == 1));
    }

    #[test]
    fn query_and_mutations_keep_stable_ids_and_monotonic_sequences() {
        let mut engine = TableEngine::new();
        let ServerMessage::TableData { seq, rows, .. } = engine.query_table("demo").unwrap() else {
            panic!("expected table data")
        };
        assert_eq!(seq, 2);
        assert_eq!(
            rows.iter().map(|row| row.row_id).collect::<Vec<_>>(),
            [1, 2]
        );

        let ServerMessage::RowDeleted { seq: deleted, .. } = engine.delete_row("demo", 1).unwrap()
        else {
            panic!("expected deletion")
        };
        engine.tick_and_collect("demo");
        let ServerMessage::RowInserted {
            seq: inserted,
            row_id,
            ..
        } = engine
            .insert_row(
                "demo",
                HashMap::from([
                    ("region".into(), json!("North")),
                    ("product".into(), json!("New")),
                    ("amount".into(), json!(50.0)),
                ]),
            )
            .unwrap()
        else {
            panic!("expected insertion")
        };
        assert!(inserted > deleted);
        assert_eq!(row_id, 3);
    }

    #[test]
    fn rejects_bad_node_build_and_installs_valid_prefix() {
        let mut engine = TableEngine::new();
        let mut specs = pipeline_specs();
        let ViewKindSpec::Sort { keys } = &mut specs[1].kind else {
            panic!("expected sort")
        };
        keys[0].column = "missing".into();
        let result = engine.set_pipeline(4, "demo", 8, &specs);
        assert_eq!(result.len(), 3);
        assert!(result[0].is_ok());
        assert!(result[1].is_ok());
        assert!(result[2].as_ref().unwrap_err().1.contains("not found"));

        engine
            .update_cell("demo", 1, "amount", &json!(500.0))
            .unwrap();
        let snapshots = engine.tick_and_collect("demo");
        assert_eq!(snapshots[&4].len(), 2);
    }

    #[test]
    fn differential_pipeline_snapshots_match_shadow_model() {
        const REGIONS: [&str; 4] = ["West", "East", "North", "South"];

        for trial in 0..10_u64 {
            let mut engine = TableEngine::new();
            assert!(engine
                .set_pipeline(1, "demo", 1, &pipeline_specs())
                .iter()
                .all(Result::is_ok));
            let mut shadow = vec![
                ShadowRow {
                    id: 1,
                    region: "West".into(),
                    product: "Widget".into(),
                    amount: 100.5,
                },
                ShadowRow {
                    id: 2,
                    region: "East".into(),
                    product: "Gadget".into(),
                    amount: 200.75,
                },
            ];
            let mut next_id = 3_u64;
            let mut rng = Lcg(0xA5A5_1234_9876_FEDC ^ trial);

            for step in 0..50_usize {
                let roll = rng.below(100);
                if shadow.is_empty() || roll < 40 {
                    let region = REGIONS[rng.below(REGIONS.len())].to_string();
                    let product = format!("P{next_id}");
                    let amount = rng.below(900) as f64 + step as f64 / 100.0;
                    engine
                        .insert_row(
                            "demo",
                            HashMap::from([
                                ("region".into(), json!(region)),
                                ("product".into(), json!(product)),
                                ("amount".into(), json!(amount)),
                            ]),
                        )
                        .unwrap();
                    shadow.push(ShadowRow {
                        id: next_id,
                        region,
                        product,
                        amount,
                    });
                    next_id += 1;
                } else if roll < 70 {
                    let index = rng.below(shadow.len());
                    let amount = rng.below(900) as f64 + (step + 1) as f64 / 100.0;
                    engine
                        .update_cell("demo", shadow[index].id, "amount", &json!(amount))
                        .unwrap();
                    shadow[index].amount = amount;
                } else if roll < 85 {
                    let index = rng.below(shadow.len());
                    let region = REGIONS[rng.below(REGIONS.len())].to_string();
                    engine
                        .update_cell("demo", shadow[index].id, "region", &json!(region))
                        .unwrap();
                    shadow[index].region = region;
                } else {
                    let index = rng.below(shadow.len());
                    let removed = shadow.remove(index);
                    engine.delete_row("demo", removed.id).unwrap();
                }

                let collected = engine.tick_and_collect("demo");
                let snapshots = &collected[&1];
                assert_eq!(snapshots.len(), 4, "trial {trial}, step {step}");

                let base = snapshots
                    .iter()
                    .find(|snapshot| snapshot.node_id == "base")
                    .unwrap();
                assert_eq!(
                    base.rows
                        .iter()
                        .map(|row| row.row_id.unwrap())
                        .collect::<Vec<_>>(),
                    shadow.iter().map(|row| row.id).collect::<Vec<_>>(),
                    "base ids at trial {trial}, step {step}"
                );
                for (actual, expected) in base.rows.iter().zip(&shadow) {
                    assert_eq!(actual.row["region"], json!(expected.region));
                    assert_eq!(actual.row["product"], json!(expected.product));
                    assert_eq!(actual.row["amount"], json!(expected.amount));
                }

                let expected_filter: Vec<f64> = shadow
                    .iter()
                    .filter(|row| row.amount >= 150.0)
                    .map(|row| row.amount)
                    .collect();
                let filtered = snapshots
                    .iter()
                    .find(|snapshot| snapshot.node_id == "filtered")
                    .unwrap();
                assert_eq!(
                    filtered
                        .rows
                        .iter()
                        .map(|row| row.row["amount"].as_f64().unwrap())
                        .collect::<Vec<_>>(),
                    expected_filter,
                    "filter at trial {trial}, step {step}"
                );

                let mut expected_sort = expected_filter;
                expected_sort.sort_by(|left, right| right.total_cmp(left));
                let ranked = snapshots
                    .iter()
                    .find(|snapshot| snapshot.node_id == "ranked")
                    .unwrap();
                assert_eq!(
                    ranked
                        .rows
                        .iter()
                        .map(|row| row.row["amount"].as_f64().unwrap())
                        .collect::<Vec<_>>(),
                    expected_sort,
                    "sort at trial {trial}, step {step}"
                );

                let mut expected_groups: HashMap<String, f64> = HashMap::new();
                for row in shadow.iter().filter(|row| row.amount >= 150.0) {
                    *expected_groups.entry(row.region.clone()).or_default() += row.amount;
                }
                let totals = snapshots
                    .iter()
                    .find(|snapshot| snapshot.node_id == "totals")
                    .unwrap();
                let actual_groups: HashMap<String, f64> = totals
                    .rows
                    .iter()
                    .map(|row| {
                        (
                            row.row["region"].as_str().unwrap().to_string(),
                            row.row["total"].as_f64().unwrap(),
                        )
                    })
                    .collect();
                assert_eq!(
                    actual_groups.len(),
                    expected_groups.len(),
                    "group count at trial {trial}, step {step}"
                );
                for (region, expected) in expected_groups {
                    let actual = actual_groups[&region];
                    assert!(
                        (actual - expected).abs() < 1e-9,
                        "group {region} at trial {trial}, step {step}: {actual} != {expected}"
                    );
                }
            }
        }
    }
}
