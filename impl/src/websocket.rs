/// WebSocket server for real-time table updates
use actix::prelude::*;
use actix_web_actors::ws;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::column::{ColumnType, ColumnValue};
use crate::messages::{ClientMessage, ServerMessage, WireTableRow};
use crate::table::{Schema, Table};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
type JsonRow = HashMap<String, JsonValue>;

struct TableState {
    table: Table,
    row_ids: Vec<u64>,
    next_row_id: u64,
}

impl TableState {
    fn with_seed_data(table: Table) -> Self {
        let row_count = table.len() as u64;
        Self {
            table,
            row_ids: (1..=row_count).collect(),
            next_row_id: row_count + 1,
        }
    }

    fn row_index_by_id(&self, row_id: u64) -> Option<usize> {
        self.row_ids
            .iter()
            .position(|candidate| *candidate == row_id)
    }

    fn insert_row(
        &mut self,
        row: HashMap<String, ColumnValue>,
    ) -> Result<(usize, u64, JsonRow), String> {
        let index = self.table.len();
        let row_id = self.next_row_id;
        self.next_row_id += 1;
        self.table.append_row(row.clone())?;
        self.row_ids.insert(index, row_id);
        Ok((index, row_id, row_to_json(&row)))
    }

    fn update_cell(
        &mut self,
        row_id: u64,
        column: &str,
        value: ColumnValue,
    ) -> Result<JsonValue, String> {
        let row_index = self
            .row_index_by_id(row_id)
            .ok_or_else(|| format!("Row '{}' not found", row_id))?;
        self.table.set_value(row_index, column, value.clone())?;
        Ok(column_value_to_json(&value))
    }

    fn delete_row(&mut self, row_id: u64) -> Result<(), String> {
        let row_index = self
            .row_index_by_id(row_id)
            .ok_or_else(|| format!("Row '{}' not found", row_id))?;
        self.table.delete_row(row_index)?;
        self.row_ids.remove(row_index);
        Ok(())
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared state for all WebSocket connections
pub struct AppState {
    tables: Arc<Mutex<HashMap<String, TableState>>>,
    subscribers: Arc<Mutex<HashMap<String, Vec<Addr<TableWebSocket>>>>>,
}

impl AppState {
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        // Create a demo table
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("value".to_string(), ColumnType::Float64, false),
        ]);

        let mut demo_table = Table::new("demo".to_string(), schema);

        // Add some initial data
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row1.insert("value".to_string(), ColumnValue::Float64(100.5));
        demo_table.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row2.insert("value".to_string(), ColumnValue::Float64(200.75));
        demo_table.append_row(row2).unwrap();
        demo_table.clear_changeset();

        tables.insert("demo".to_string(), TableState::with_seed_data(demo_table));

        Self {
            tables: Arc::new(Mutex::new(tables)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Subscribe a WebSocket connection to a table
    pub fn subscribe(&self, table_name: &str, addr: Addr<TableWebSocket>) {
        let mut subscribers = self.subscribers.lock().unwrap();
        let subs = subscribers.entry(table_name.to_string()).or_default();
        // Prevent duplicate subscriptions from the same actor
        if !subs.contains(&addr) {
            subs.push(addr);
        }
        println!(
            "[subscribe] {} now has {} subscriber(s)",
            table_name,
            subs.len()
        );
    }

    /// Broadcast a message to all subscribers of a table
    pub fn broadcast(&self, table_name: &str, msg: ServerMessage) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if let Some(addrs) = subscribers.get_mut(table_name) {
            println!("[broadcast] {} -> {} subscribers", table_name, addrs.len());
            // Prune dead/full addresses: retain only subscribers that accepted the message
            addrs.retain(|addr| addr.try_send(BroadcastMessage(msg.clone())).is_ok());
        } else {
            println!("[broadcast] {} -> no subscribers!", table_name);
        }
    }

    /// Unsubscribe a WebSocket connection from a table
    pub fn unsubscribe(&self, table_name: &str, addr: &Addr<TableWebSocket>) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if let Some(subs) = subscribers.get_mut(table_name) {
            subs.retain(|a| a != addr);
            println!(
                "[unsubscribe] {} now has {} subscriber(s)",
                table_name,
                subs.len()
            );
        }
    }

    fn send_error(ctx: &mut ws::WebsocketContext<TableWebSocket>, message: String) {
        ctx.text(serde_json::to_string(&ServerMessage::Error { message }).unwrap());
    }

    pub fn query_table(&self, table_name: &str) -> Result<ServerMessage, String> {
        let tables = self.tables.lock().unwrap();
        let table_state = tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let (columns, rows) = table_to_json(table_state)?;
        Ok(ServerMessage::TableData {
            table_name: table_name.to_string(),
            columns,
            rows,
        })
    }

    pub fn insert_row(
        &self,
        table_name: &str,
        row: HashMap<String, JsonValue>,
    ) -> Result<ServerMessage, String> {
        let mut tables = self.tables.lock().unwrap();
        let table_state = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let converted_row = convert_row_for_schema(table_state.table.schema(), &row)?;
        let (index, row_id, json_row) = table_state.insert_row(converted_row)?;

        Ok(ServerMessage::RowInserted {
            table_name: table_name.to_string(),
            index,
            row_id,
            row: json_row,
        })
    }

    pub fn update_cell(
        &self,
        table_name: &str,
        row_id: u64,
        column: &str,
        value: &JsonValue,
    ) -> Result<ServerMessage, String> {
        let mut tables = self.tables.lock().unwrap();
        let table_state = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let col_type = table_state
            .table
            .schema()
            .get_column_type(column)
            .ok_or_else(|| format!("Column '{}' not found", column))?;
        let nullable = table_state
            .table
            .schema()
            .is_column_nullable(column)
            .unwrap_or(false);
        let col_value = json_to_column_value_typed(value, col_type, nullable)
            .map_err(|e| format!("Column '{}': {}", column, e))?;
        let json_value = table_state.update_cell(row_id, column, col_value)?;

        Ok(ServerMessage::CellUpdated {
            table_name: table_name.to_string(),
            row_id,
            column: column.to_string(),
            value: json_value,
        })
    }

    pub fn delete_row(&self, table_name: &str, row_id: u64) -> Result<ServerMessage, String> {
        let mut tables = self.tables.lock().unwrap();
        let table_state = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        table_state.delete_row(row_id)?;

        Ok(ServerMessage::RowDeleted {
            table_name: table_name.to_string(),
            row_id,
        })
    }
}

/// Message to broadcast to clients
#[derive(Message)]
#[rtype(result = "()")]
struct BroadcastMessage(ServerMessage);

/// WebSocket connection actor
pub struct TableWebSocket {
    hb: Instant,
    state: actix_web::web::Data<AppState>,
    subscribed_table: Option<String>,
}

impl TableWebSocket {
    pub fn new(state: actix_web::web::Data<AppState>) -> Self {
        Self {
            hb: Instant::now(),
            state,
            subscribed_table: None,
        }
    }

    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                println!("WebSocket Client heartbeat failed, disconnecting!");
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn handle_client_message(&mut self, msg: ClientMessage, ctx: &mut ws::WebsocketContext<Self>) {
        match msg {
            ClientMessage::Subscribe { table_name } => {
                // Only allow subscriptions to existing tables
                let table_exists = self.state.tables.lock().unwrap().contains_key(&table_name);
                if !table_exists {
                    ctx.text(
                        serde_json::to_string(&ServerMessage::Error {
                            message: format!("Table '{}' not found", table_name),
                        })
                        .unwrap(),
                    );
                    return;
                }

                // If re-subscribing to a different table, remove the old subscription first.
                if let Some(prev_table) = self.subscribed_table.as_ref() {
                    if prev_table != &table_name {
                        self.state.unsubscribe(prev_table, &ctx.address());
                    }
                }

                self.subscribed_table = Some(table_name.clone());
                self.state.subscribe(&table_name, ctx.address());

                let response = ServerMessage::Subscribed {
                    table_name: table_name.clone(),
                };
                ctx.text(serde_json::to_string(&response).unwrap());
            }

            ClientMessage::Query { table_name } => match self.state.query_table(&table_name) {
                Ok(response) => ctx.text(serde_json::to_string(&response).unwrap()),
                Err(err) => AppState::send_error(ctx, err),
            },

            ClientMessage::InsertRow { table_name, row } => {
                match self.state.insert_row(&table_name, row) {
                    Ok(response) => self.state.broadcast(&table_name, response),
                    Err(err) => AppState::send_error(ctx, err),
                }
            }

            ClientMessage::UpdateCell {
                table_name,
                row_id,
                column,
                value,
            } => match self.state.update_cell(&table_name, row_id, &column, &value) {
                Ok(response) => self.state.broadcast(&table_name, response),
                Err(err) => AppState::send_error(ctx, err),
            },

            ClientMessage::DeleteRow { table_name, row_id } => {
                match self.state.delete_row(&table_name, row_id) {
                    Ok(response) => self.state.broadcast(&table_name, response),
                    Err(err) => AppState::send_error(ctx, err),
                }
            }
        }
    }
}

impl Actor for TableWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        // Clean up subscription when connection closes
        if let Some(ref table_name) = self.subscribed_table {
            self.state.unsubscribe(table_name, &ctx.address());
        }
        Running::Stop
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for TableWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(client_msg) => {
                    self.handle_client_message(client_msg, ctx);
                }
                Err(e) => {
                    ctx.text(
                        serde_json::to_string(&ServerMessage::Error {
                            message: format!("Invalid message format: {}", e),
                        })
                        .unwrap(),
                    );
                }
            },
            Ok(ws::Message::Binary(_)) => {
                println!("Unexpected binary message");
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => ctx.stop(),
        }
    }
}

impl Handler<BroadcastMessage> for TableWebSocket {
    type Result = ();

    fn handle(&mut self, msg: BroadcastMessage, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg.0).unwrap());
    }
}

/// Convert days since Unix epoch to (year, month, day)
fn ymd_from_days(days: i32) -> (i32, u32, u32) {
    // Shift to March 1, year 0 epoch (simplifies leap year calculation)
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month in [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Format a date (days since epoch) as ISO 8601 date string (YYYY-MM-DD)
fn format_date_from_days(days: i32) -> String {
    let (year, month, day) = ymd_from_days(days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// Format a datetime (milliseconds since epoch) as ISO 8601 datetime string
fn format_datetime_from_millis(ms: i64) -> String {
    let ms_per_day: i64 = 86_400_000;
    let days = ms.div_euclid(ms_per_day) as i32;
    let time_ms = ms.rem_euclid(ms_per_day) as u32;
    let (year, month, day) = ymd_from_days(days);
    let hours = time_ms / 3_600_000;
    let minutes = (time_ms % 3_600_000) / 60_000;
    let seconds = (time_ms % 60_000) / 1000;
    let millis = time_ms % 1000;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hours, minutes, seconds, millis
    )
}

/// Convert ColumnValue to JSON Value
fn column_value_to_json(cv: &ColumnValue) -> JsonValue {
    match cv {
        ColumnValue::Int32(v) => JsonValue::Number((*v).into()),
        ColumnValue::Int64(v) => JsonValue::Number((*v).into()),
        ColumnValue::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ColumnValue::Float64(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ColumnValue::String(v) => JsonValue::String(v.clone()),
        ColumnValue::Bool(v) => JsonValue::Bool(*v),
        ColumnValue::Date(days) => {
            // Convert days since Unix epoch to ISO 8601 date string (YYYY-MM-DD)
            JsonValue::String(format_date_from_days(*days))
        }
        ColumnValue::DateTime(millis) => {
            // Convert milliseconds since Unix epoch to ISO 8601 datetime string
            JsonValue::String(format_datetime_from_millis(*millis))
        }
        ColumnValue::Null => JsonValue::Null,
    }
}

fn table_to_json(table_state: &TableState) -> Result<(Vec<String>, Vec<WireTableRow>), String> {
    let columns: Vec<String> = table_state
        .table
        .schema()
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::with_capacity(table_state.table.len());
    for (row_idx, row_id) in table_state.row_ids.iter().enumerate() {
        rows.push(WireTableRow {
            row_id: *row_id,
            row: row_to_json(&table_state.table.get_row(row_idx)?),
        });
    }

    Ok((columns, rows))
}

/// Convert row to JSON
fn row_to_json(row: &HashMap<String, ColumnValue>) -> JsonRow {
    row.iter()
        .map(|(k, v)| (k.clone(), column_value_to_json(v)))
        .collect()
}

/// Convert JSON Value to ColumnValue
fn json_to_column_value_typed(
    value: &JsonValue,
    col_type: ColumnType,
    nullable: bool,
) -> Result<ColumnValue, String> {
    if matches!(value, JsonValue::Null) {
        if nullable {
            return Ok(ColumnValue::Null);
        }
        return Err("NULL value for non-nullable column".to_string());
    }

    match col_type {
        ColumnType::Int32 => match value {
            JsonValue::Number(n) => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(ColumnValue::Int32)
                .ok_or_else(|| "Expected INT32 number".to_string()),
            _ => Err("Expected INT32 number".to_string()),
        },
        ColumnType::Int64 => match value {
            JsonValue::Number(n) => n
                .as_i64()
                .map(ColumnValue::Int64)
                .ok_or_else(|| "Expected INT64 number".to_string()),
            _ => Err("Expected INT64 number".to_string()),
        },
        ColumnType::Float32 => match value {
            JsonValue::Number(n) => n
                .as_f64()
                .map(|v| ColumnValue::Float32(v as f32))
                .ok_or_else(|| "Expected FLOAT32 number".to_string()),
            _ => Err("Expected FLOAT32 number".to_string()),
        },
        ColumnType::Float64 => match value {
            JsonValue::Number(n) => n
                .as_f64()
                .map(ColumnValue::Float64)
                .ok_or_else(|| "Expected FLOAT64 number".to_string()),
            _ => Err("Expected FLOAT64 number".to_string()),
        },
        ColumnType::String => match value {
            JsonValue::String(s) => Ok(ColumnValue::String(s.clone())),
            _ => Err("Expected STRING value".to_string()),
        },
        ColumnType::Bool => match value {
            JsonValue::Bool(b) => Ok(ColumnValue::Bool(*b)),
            _ => Err("Expected BOOL value".to_string()),
        },
        ColumnType::Date => match value {
            JsonValue::Number(n) => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(ColumnValue::Date)
                .ok_or_else(|| "Expected DATE as days-since-epoch integer".to_string()),
            JsonValue::String(s) => parse_date(s)
                .map(ColumnValue::Date)
                .ok_or_else(|| "Expected DATE string in YYYY-MM-DD format".to_string()),
            _ => Err("Expected DATE value".to_string()),
        },
        ColumnType::DateTime => match value {
            JsonValue::Number(n) => n
                .as_i64()
                .map(ColumnValue::DateTime)
                .ok_or_else(|| "Expected DATETIME as millis-since-epoch integer".to_string()),
            JsonValue::String(s) => parse_datetime(s)
                .map(ColumnValue::DateTime)
                .ok_or_else(|| "Expected DATETIME string in ISO format".to_string()),
            _ => Err("Expected DATETIME value".to_string()),
        },
    }
}

/// Validate a JSON row against schema and convert it to typed column values.
fn convert_row_for_schema(
    schema: &Schema,
    row: &HashMap<String, JsonValue>,
) -> Result<HashMap<String, ColumnValue>, String> {
    for key in row.keys() {
        if schema.get_column_index(key).is_none() {
            return Err(format!("Unknown column '{}'", key));
        }
    }

    let mut converted = HashMap::new();
    for i in 0..schema.len() {
        if let Some((col_name, col_type, nullable)) = schema.get_column_info(i) {
            let value = row
                .get(col_name)
                .ok_or_else(|| format!("Missing value for column '{}'", col_name))?;
            let converted_value = json_to_column_value_typed(value, col_type, nullable)
                .map_err(|e| format!("Column '{}': {}", col_name, e))?;
            converted.insert(col_name.to_string(), converted_value);
        }
    }

    Ok(converted)
}

/// Parse an ISO 8601 date string (YYYY-MM-DD) to days since epoch.
fn parse_date(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_from_ymd(year, month, day))
}

/// Parse an ISO 8601 datetime string to milliseconds since epoch.
fn parse_datetime(s: &str) -> Option<i64> {
    let (date_part, time_part) = if s.contains('T') {
        let parts: Vec<&str> = s.splitn(2, 'T').collect();
        if parts.len() != 2 {
            return None;
        }
        (parts[0], parts[1])
    } else if s.contains(' ') {
        let parts: Vec<&str> = s.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return None;
        }
        (parts[0], parts[1])
    } else {
        return parse_date(s).map(|d| (d as i64) * 86_400_000);
    };

    let days = parse_date(date_part)?;
    let time_part = time_part.trim_end_matches('Z');
    let (time_str, ms) = if time_part.contains('.') {
        let parts: Vec<&str> = time_part.splitn(2, '.').collect();
        let ms_str = parts.get(1)?;
        let ms: u32 = if ms_str.len() >= 3 {
            ms_str[..3].parse().ok()?
        } else {
            format!("{:0<3}", ms_str).parse().ok()?
        };
        (parts[0], ms)
    } else {
        (time_part, 0)
    };

    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() < 2 {
        return None;
    }
    let hour: u32 = time_parts[0].parse().ok()?;
    let minute: u32 = time_parts[1].parse().ok()?;
    let second: u32 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let time_ms =
        (hour as i64) * 3_600_000 + (minute as i64) * 60_000 + (second as i64) * 1000 + (ms as i64);

    Some((days as i64) * 86_400_000 + time_ms)
}

/// Convert (year, month, day) to days since Unix epoch.
fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32) - 719468
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_convert_row_for_schema_success() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
            ("value".to_string(), ColumnType::Float64, false),
        ]);

        let mut row = HashMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("name".to_string(), json!("Alice"));
        row.insert("value".to_string(), json!(42.5));

        let converted = convert_row_for_schema(&schema, &row).expect("row should convert");
        assert_eq!(converted.get("id"), Some(&ColumnValue::Int32(1)));
        assert_eq!(
            converted.get("name"),
            Some(&ColumnValue::String("Alice".to_string()))
        );
        assert_eq!(converted.get("value"), Some(&ColumnValue::Float64(42.5)));
    }

    #[test]
    fn test_convert_row_for_schema_rejects_unknown_column() {
        let schema = Schema::new(vec![("id".to_string(), ColumnType::Int32, false)]);

        let mut row = HashMap::new();
        row.insert("id".to_string(), json!(1));
        row.insert("extra".to_string(), json!(123));

        let err = convert_row_for_schema(&schema, &row).unwrap_err();
        assert!(err.contains("Unknown column 'extra'"));
    }

    #[test]
    fn test_convert_row_for_schema_rejects_missing_column() {
        let schema = Schema::new(vec![
            ("id".to_string(), ColumnType::Int32, false),
            ("name".to_string(), ColumnType::String, false),
        ]);

        let mut row = HashMap::new();
        row.insert("id".to_string(), json!(1));

        let err = convert_row_for_schema(&schema, &row).unwrap_err();
        assert!(err.contains("Missing value for column 'name'"));
    }

    #[test]
    fn test_json_to_column_value_typed_respects_nullability() {
        let err =
            json_to_column_value_typed(&JsonValue::Null, ColumnType::Int32, false).unwrap_err();
        assert!(err.contains("NULL value for non-nullable column"));

        let v = json_to_column_value_typed(&JsonValue::Null, ColumnType::Int32, true).unwrap();
        assert_eq!(v, ColumnValue::Null);
    }

    #[test]
    fn test_app_state_initial_demo_table_clears_seed_changes() {
        let state = AppState::new();
        let tables = state.tables.lock().unwrap();
        let demo = tables.get("demo").expect("demo table should exist");

        assert_eq!(demo.table.len(), 2);
        assert_eq!(demo.table.changeset().len(), 0);
        assert_eq!(demo.row_ids, vec![1, 2]);
        assert_eq!(demo.next_row_id, 3);
    }

    #[test]
    fn test_app_state_mutations_use_stable_row_ids() {
        let state = AppState::new();

        let insert_response = state
            .insert_row(
                "demo",
                HashMap::from([
                    ("id".to_string(), json!(3)),
                    ("name".to_string(), json!("Charlie")),
                    ("value".to_string(), json!(300.25)),
                ]),
            )
            .expect("insert should succeed");
        let inserted_row_id = match insert_response {
            ServerMessage::RowInserted { index, row_id, .. } => {
                assert_eq!(index, 2);
                row_id
            }
            _ => panic!("expected row inserted response"),
        };
        assert_eq!(inserted_row_id, 3);

        let update_response = state
            .update_cell("demo", 2, "value", &json!(250.5))
            .expect("update should succeed");
        assert!(matches!(
            update_response,
            ServerMessage::CellUpdated { row_id: 2, .. }
        ));

        let delete_response = state.delete_row("demo", 1).expect("delete should succeed");
        assert!(matches!(
            delete_response,
            ServerMessage::RowDeleted { row_id: 1, .. }
        ));

        let tables = state.tables.lock().unwrap();
        let demo = tables.get("demo").expect("demo table should exist");
        assert_eq!(demo.table.len(), 2);
        assert_eq!(demo.table.changeset().len(), 3);
        assert_eq!(demo.row_ids, vec![2, 3]);
        assert_eq!(
            demo.table.get_value(0, "name").unwrap(),
            ColumnValue::String("Bob".to_string())
        );
        assert_eq!(
            demo.table.get_value(0, "value").unwrap(),
            ColumnValue::Float64(250.5)
        );
        assert_eq!(
            demo.table.get_value(1, "name").unwrap(),
            ColumnValue::String("Charlie".to_string())
        );
    }

    #[test]
    fn test_query_table_returns_json_from_core_table() {
        let state = AppState::new();
        state
            .insert_row(
                "demo",
                HashMap::from([
                    ("id".to_string(), json!(3)),
                    ("name".to_string(), json!("Charlie")),
                    ("value".to_string(), json!(300.25)),
                ]),
            )
            .unwrap();

        let response = state.query_table("demo").expect("query should succeed");
        let ServerMessage::TableData { columns, rows, .. } = response else {
            panic!("expected table data response");
        };

        assert_eq!(columns, vec!["id", "name", "value"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2].row_id, 3);
        assert_eq!(rows[2].row.get("name"), Some(&json!("Charlie")));
        assert_eq!(rows[2].row.get("value"), Some(&json!(300.25)));
    }

    #[test]
    fn test_app_state_rejects_unknown_row_id() {
        let state = AppState::new();

        let update_err = state
            .update_cell("demo", 999, "value", &json!(250.5))
            .unwrap_err();
        assert!(update_err.contains("Row '999' not found"));

        let delete_err = state.delete_row("demo", 999).unwrap_err();
        assert!(delete_err.contains("Row '999' not found"));
    }
}
