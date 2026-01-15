/// WebSocket server for real-time table updates
use actix::prelude::*;
use actix_web_actors::ws;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::column::{ColumnType, ColumnValue};
use crate::messages::{ClientMessage, ServerMessage};
use crate::table::Schema;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Simple table data storage (thread-safe)
#[derive(Clone)]
pub struct TableData {
    pub name: String,
    pub schema: Schema,
    pub rows: Vec<HashMap<String, ColumnValue>>,
}

impl TableData {
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            name,
            schema,
            rows: Vec::new(),
        }
    }

    /// Convert to JSON format
    pub fn to_json(&self) -> (Vec<String>, Vec<HashMap<String, JsonValue>>) {
        let columns: Vec<String> = self
            .schema
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows: Vec<HashMap<String, JsonValue>> = self
            .rows
            .iter()
            .map(|row| row_to_json(row))
            .collect();

        (columns, rows)
    }
}

/// Shared state for all WebSocket connections
pub struct AppState {
    pub tables: Arc<Mutex<HashMap<String, TableData>>>,
    pub subscribers: Arc<Mutex<HashMap<String, Vec<Addr<TableWebSocket>>>>>,
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

        let mut demo_table = TableData::new("demo".to_string(), schema);

        // Add some initial data
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), ColumnValue::Int32(1));
        row1.insert("name".to_string(), ColumnValue::String("Alice".to_string()));
        row1.insert("value".to_string(), ColumnValue::Float64(100.5));
        demo_table.rows.push(row1);

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), ColumnValue::Int32(2));
        row2.insert("name".to_string(), ColumnValue::String("Bob".to_string()));
        row2.insert("value".to_string(), ColumnValue::Float64(200.75));
        demo_table.rows.push(row2);

        tables.insert("demo".to_string(), demo_table);

        Self {
            tables: Arc::new(Mutex::new(tables)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get or create a table
    pub fn get_or_create_table(&self, name: &str) -> TableData {
        let mut tables = self.tables.lock().unwrap();
        tables
            .entry(name.to_string())
            .or_insert_with(|| {
                let schema = Schema::new(vec![
                    ("id".to_string(), ColumnType::Int32, false),
                    ("name".to_string(), ColumnType::String, false),
                    ("value".to_string(), ColumnType::Float64, false),
                ]);
                TableData::new(name.to_string(), schema)
            })
            .clone()
    }

    /// Subscribe a WebSocket connection to a table
    pub fn subscribe(&self, table_name: &str, addr: Addr<TableWebSocket>) {
        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers
            .entry(table_name.to_string())
            .or_insert_with(Vec::new)
            .push(addr);
    }

    /// Broadcast a message to all subscribers of a table
    pub fn broadcast(&self, table_name: &str, msg: ServerMessage) {
        let subscribers = self.subscribers.lock().unwrap();
        if let Some(addrs) = subscribers.get(table_name) {
            for addr in addrs {
                addr.do_send(BroadcastMessage(msg.clone()));
            }
        }
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
                self.subscribed_table = Some(table_name.clone());
                self.state.subscribe(&table_name, ctx.address());

                let response = ServerMessage::Subscribed {
                    table_name: table_name.clone(),
                };
                ctx.text(serde_json::to_string(&response).unwrap());
            }

            ClientMessage::Query { table_name } => {
                let table_data = self.state.get_or_create_table(&table_name);
                let (columns, rows) = table_data.to_json();

                let response = ServerMessage::TableData {
                    table_name,
                    columns,
                    rows,
                };
                ctx.text(serde_json::to_string(&response).unwrap());
            }

            ClientMessage::InsertRow { table_name, row } => {
                let mut tables = self.state.tables.lock().unwrap();

                if let Some(table_data) = tables.get_mut(&table_name) {
                    let converted_row: HashMap<String, ColumnValue> = row
                        .iter()
                        .filter_map(|(k, v)| json_to_column_value(v).map(|cv| (k.clone(), cv)))
                        .collect();

                    let index = table_data.rows.len();
                    table_data.rows.push(converted_row.clone());

                    drop(tables); // Release lock

                    let response = ServerMessage::RowInserted {
                        table_name: table_name.clone(),
                        index,
                        row: row_to_json(&converted_row),
                    };

                    self.state.broadcast(&table_name, response);
                } else {
                    ctx.text(
                        serde_json::to_string(&ServerMessage::Error {
                            message: "Table not found".to_string(),
                        })
                        .unwrap(),
                    );
                }
            }

            ClientMessage::UpdateCell {
                table_name,
                row_index,
                column,
                value,
            } => {
                let mut tables = self.state.tables.lock().unwrap();

                if let Some(table_data) = tables.get_mut(&table_name) {
                    if row_index < table_data.rows.len() {
                        if let Some(col_value) = json_to_column_value(&value) {
                            table_data.rows[row_index].insert(column.clone(), col_value);

                            drop(tables); // Release lock

                            let response = ServerMessage::CellUpdated {
                                table_name: table_name.clone(),
                                row_index,
                                column,
                                value,
                            };

                            self.state.broadcast(&table_name, response);
                        } else {
                            ctx.text(
                                serde_json::to_string(&ServerMessage::Error {
                                    message: "Invalid value type".to_string(),
                                })
                                .unwrap(),
                            );
                        }
                    } else {
                        ctx.text(
                            serde_json::to_string(&ServerMessage::Error {
                                message: "Row index out of bounds".to_string(),
                            })
                            .unwrap(),
                        );
                    }
                }
            }

            ClientMessage::DeleteRow {
                table_name,
                row_index,
            } => {
                let mut tables = self.state.tables.lock().unwrap();

                if let Some(table_data) = tables.get_mut(&table_name) {
                    if row_index < table_data.rows.len() {
                        table_data.rows.remove(row_index);

                        drop(tables); // Release lock

                        let response = ServerMessage::RowDeleted {
                            table_name: table_name.clone(),
                            index: row_index,
                        };

                        self.state.broadcast(&table_name, response);
                    } else {
                        ctx.text(
                            serde_json::to_string(&ServerMessage::Error {
                                message: "Row index out of bounds".to_string(),
                            })
                            .unwrap(),
                        );
                    }
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
            Ok(ws::Message::Text(text)) => {
                match serde_json::from_str::<ClientMessage>(&text) {
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
                }
            }
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

/// Convert ColumnValue to JSON Value
fn column_value_to_json(cv: &ColumnValue) -> JsonValue {
    match cv {
        ColumnValue::Int32(v) => JsonValue::Number((*v).into()),
        ColumnValue::Int64(v) => JsonValue::Number((*v).into()),
        ColumnValue::Float32(v) => {
            JsonValue::Number(serde_json::Number::from_f64(*v as f64).unwrap())
        }
        ColumnValue::Float64(v) => JsonValue::Number(serde_json::Number::from_f64(*v).unwrap()),
        ColumnValue::String(v) => JsonValue::String(v.clone()),
        ColumnValue::Bool(v) => JsonValue::Bool(*v),
        ColumnValue::Null => JsonValue::Null,
    }
}

/// Convert row to JSON
fn row_to_json(row: &HashMap<String, ColumnValue>) -> HashMap<String, JsonValue> {
    row.iter()
        .map(|(k, v)| (k.clone(), column_value_to_json(v)))
        .collect()
}

/// Convert JSON Value to ColumnValue
fn json_to_column_value(value: &JsonValue) -> Option<ColumnValue> {
    match value {
        JsonValue::Null => Some(ColumnValue::Null),
        JsonValue::Bool(b) => Some(ColumnValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Some(ColumnValue::Int32(i as i32))
                } else {
                    Some(ColumnValue::Int64(i))
                }
            } else if let Some(f) = n.as_f64() {
                Some(ColumnValue::Float64(f))
            } else {
                None
            }
        }
        JsonValue::String(s) => Some(ColumnValue::String(s.clone())),
        _ => None,
    }
}
