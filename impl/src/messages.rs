/// WebSocket message types for client-server communication
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone)]
pub struct WireTableRow {
    pub row_id: u64,
    pub row: HashMap<String, JsonValue>,
}

/// Messages sent from client to server
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Subscribe to table updates
    Subscribe { table_name: String },

    /// Request current table data
    Query { table_name: String },

    /// Insert a new row
    InsertRow {
        table_name: String,
        row: HashMap<String, JsonValue>,
    },

    /// Update a single cell
    UpdateCell {
        table_name: String,
        row_id: u64,
        column: String,
        value: JsonValue,
    },

    /// Delete a row
    DeleteRow { table_name: String, row_id: u64 },
}

/// Messages sent from server to client
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Full table data in response to Query
    TableData {
        table_name: String,
        columns: Vec<String>,
        rows: Vec<WireTableRow>,
    },

    /// A row was inserted
    RowInserted {
        table_name: String,
        index: usize,
        row_id: u64,
        row: HashMap<String, JsonValue>,
    },

    /// A cell was updated
    CellUpdated {
        table_name: String,
        row_id: u64,
        column: String,
        value: JsonValue,
    },

    /// A row was deleted
    RowDeleted { table_name: String, row_id: u64 },

    /// Subscription confirmed
    Subscribed { table_name: String },

    /// Error occurred
    Error { message: String },
}
