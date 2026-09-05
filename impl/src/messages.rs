/// WebSocket message types for client-server communication
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone)]
pub struct WireTableRow {
    pub row_id: u64,
    pub row: HashMap<String, JsonValue>,
}

/// A row in a derived-view snapshot. Derived rows do not always have a stable
/// identity (group aggregates are the clearest example), so their row id is
/// explicitly nullable instead of using an integer sentinel that JavaScript
/// cannot represent safely.
#[derive(Debug, Serialize, Clone)]
pub struct WireViewRow {
    pub row_id: Option<u64>,
    pub row: HashMap<String, JsonValue>,
}

/// Ordered edits in a node's own row coordinates. Indices refer to the state
/// at that step in the batch, not the final view. Derived identities stay null.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ViewChange {
    RowInserted {
        index: usize,
        row: WireViewRow,
    },
    RowDeleted {
        index: usize,
    },
    CellUpdated {
        index: usize,
        column: String,
        value: JsonValue,
    },
}

/// Maximum operations in one delta and in the engine's pending base journal.
pub const MAX_VIEW_DELTA_CHANGES: usize = 512;

/// One sort key in a `Sort` view spec.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SortKeySpec {
    pub column: String,
    pub descending: bool,
}

/// One aggregate in a `Group` view spec. `op` is an engine-syntax op string
/// (`sum|avg|min|max|count|median|pNN|percentile(x)`); `column` is the source
/// column. `count` means `COUNT(column)` (non-null count), not row count.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggSpec {
    pub alias: String,
    pub op: String,
    pub column: String,
}

/// The kind-specific payload of a pipeline node, internally tagged on `kind`
/// and flattened into `ViewNodeSpec`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ViewKindSpec {
    Filter {
        predicate: String,
    },
    Sort {
        keys: Vec<SortKeySpec>,
    },
    Group {
        group_by: Vec<String>,
        aggs: Vec<AggSpec>,
    },
}

/// One node in a client-defined derived-view pipeline. `source_id` of `"base"`
/// means the root table; otherwise it references an earlier node's `id`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ViewNodeSpec {
    pub id: String,
    pub source_id: String,
    #[serde(flatten)]
    pub kind: ViewKindSpec,
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

    /// Define/replace this connection's derived-view pipeline over `table_name`.
    /// Re-sent on every expression edit (the server rebuilds the affected views).
    SetPipeline {
        table_name: String,
        /// Client-selected generation for this complete pipeline definition.
        /// Echoed by ViewData/ViewError so a client can discard responses from
        /// an older definition after rapid edits or reconnects.
        pipeline_generation: u32,
        nodes: Vec<ViewNodeSpec>,
    },

    /// Rebaseline one installed node without rebuilding the pipeline. The
    /// generation must still be current for this connection and table.
    QueryView {
        table_name: String,
        pipeline_generation: u32,
        node_id: String,
    },
}

/// Messages sent from server to client
///
/// `TableData` and flat-table deltas carry a `seq`: the table's total
/// change count (`Changeset::total_len`) captured in the same serialized engine
/// operation as the snapshot or mutation it describes. Because that counter is monotonic, a
/// client can reconcile a snapshot with concurrently-broadcast deltas: any
/// delta whose `seq` is <= the snapshot's `seq` is already reflected in the
/// snapshot and must be dropped, while a delta with a greater `seq` is newer
/// and must be applied. Without this tag the snapshot/delta overlap is
/// ambiguous and a racing insert can be applied twice.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Full table data in response to Query
    TableData {
        table_name: String,
        /// Change count at which this snapshot was taken.
        seq: u64,
        columns: Vec<String>,
        rows: Vec<WireTableRow>,
    },

    /// A row was inserted
    RowInserted {
        table_name: String,
        /// Change count after this insert was applied.
        seq: u64,
        index: usize,
        row_id: u64,
        row: HashMap<String, JsonValue>,
    },

    /// A cell was updated
    CellUpdated {
        table_name: String,
        /// Change count after this update was applied.
        seq: u64,
        row_id: u64,
        column: String,
        value: JsonValue,
    },

    /// A row was deleted
    RowDeleted {
        table_name: String,
        /// Change count after this delete was applied.
        seq: u64,
        row_id: u64,
    },

    /// Subscription confirmed
    Subscribed {
        table_name: String,
        /// Wire-protocol version, so clients can detect a mismatched server
        /// instead of failing mysteriously on unknown message shapes.
        protocol_version: u32,
    },

    /// Initial, fallback, or requested snapshot of one pipeline node.
    /// Rows without a stable identity carry a null
    /// `row_id`; the `base` node carries real row ids so the client can
    /// edit/delete.
    ViewData {
        table_name: String,
        /// Generation from the SetPipeline request that created this node.
        pipeline_generation: u32,
        node_id: String,
        source_id: String,
        kind: String,
        /// Connection/generation/node-local delivery sequence, starting at zero.
        /// Not a view version, changeset cursor, or flat-table sequence.
        seq: u64,
        columns: Vec<String>,
        rows: Vec<WireViewRow>,
    },

    /// Apply the entire ordered batch atomically, only on exactly `from_seq`.
    /// Duplicate/old deliveries may be ignored; a gap requires QueryView.
    ViewDelta {
        table_name: String,
        pipeline_generation: u32,
        node_id: String,
        from_seq: u64,
        seq: u64,
        changes: Vec<ViewChange>,
    },

    /// Periodic delivery watermarks detect a lost final delta or snapshot even
    /// when there are no further mutations. They carry no row payloads.
    PipelineStatus {
        table_name: String,
        pipeline_generation: u32,
        sequences: HashMap<String, u64>,
    },

    /// A pipeline node failed to build/evaluate (e.g. a bad expression).
    ViewError {
        table_name: String,
        /// Generation from the SetPipeline request that failed.
        pipeline_generation: u32,
        node_id: String,
        message: String,
    },

    /// Error occurred
    Error { message: String },
}

/// Current server→client wire-protocol version. Bump on breaking changes to
/// message shapes or semantics.
pub const PROTOCOL_VERSION: u32 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribed_carries_protocol_version() {
        let msg = ServerMessage::Subscribed {
            table_name: "demo".to_string(),
            protocol_version: PROTOCOL_VERSION,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"protocol_version\":3"), "got: {}", json);
        assert!(json.contains("\"type\":\"Subscribed\""), "got: {}", json);
    }

    #[test]
    fn set_pipeline_deserializes_tagged_specs() {
        let json = r#"{"type":"SetPipeline","table_name":"demo","pipeline_generation":12,"nodes":[
            {"id":"f","source_id":"base","kind":"filter","predicate":"amount >= 500"},
            {"id":"s","source_id":"f","kind":"sort","keys":[{"column":"amount","descending":true}]},
            {"id":"g","source_id":"s","kind":"group","group_by":["region"],
             "aggs":[{"alias":"total","op":"sum","column":"amount"},
                     {"alias":"p95","op":"p95","column":"amount"}]}
        ]}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        match msg {
            ClientMessage::SetPipeline {
                table_name,
                pipeline_generation,
                nodes,
            } => {
                assert_eq!(table_name, "demo");
                assert_eq!(pipeline_generation, 12);
                assert_eq!(nodes.len(), 3);
                assert_eq!(nodes[0].id, "f");
                assert_eq!(nodes[0].source_id, "base");
                match &nodes[0].kind {
                    ViewKindSpec::Filter { predicate } => assert_eq!(predicate, "amount >= 500"),
                    other => panic!("expected filter, got {:?}", other),
                }
                match &nodes[2].kind {
                    ViewKindSpec::Group { group_by, aggs } => {
                        assert_eq!(group_by, &vec!["region".to_string()]);
                        assert_eq!(aggs.len(), 2);
                        assert_eq!(aggs[1].op, "p95");
                    }
                    other => panic!("expected group, got {:?}", other),
                }
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_pipeline_requires_generation_and_aggregate_column() {
        let missing_generation = r#"{"type":"SetPipeline","table_name":"demo","nodes":[]}"#;
        assert!(serde_json::from_str::<ClientMessage>(missing_generation).is_err());

        let missing_column = r#"{"type":"SetPipeline","table_name":"demo","pipeline_generation":1,"nodes":[
            {"id":"g","source_id":"base","kind":"group","group_by":["region"],
             "aggs":[{"alias":"count","op":"count"}]}
        ]}"#;
        assert!(serde_json::from_str::<ClientMessage>(missing_column).is_err());
    }

    #[test]
    fn view_data_serializes_with_seq() {
        let msg = ServerMessage::ViewData {
            table_name: "demo".into(),
            pipeline_generation: 12,
            node_id: "g".into(),
            source_id: "s".into(),
            kind: "group".into(),
            seq: 7,
            columns: vec!["region".into(), "total".into()],
            rows: vec![WireViewRow {
                row_id: None,
                row: HashMap::from([
                    ("region".into(), JsonValue::String("West".into())),
                    ("total".into(), JsonValue::from(500)),
                ]),
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"ViewData\""), "got: {}", json);
        assert!(json.contains("\"seq\":7"), "got: {}", json);
        assert!(json.contains("\"pipeline_generation\":12"), "got: {}", json);
        assert!(json.contains("\"node_id\":\"g\""), "got: {}", json);
        assert!(json.contains("\"row_id\":null"), "got: {}", json);
    }

    #[test]
    fn view_error_serializes() {
        let msg = ServerMessage::ViewError {
            table_name: "demo".into(),
            pipeline_generation: 13,
            node_id: "f".into(),
            message: "bad expr".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"ViewError\""), "got: {}", json);
        assert!(json.contains("\"pipeline_generation\":13"), "got: {}", json);
        assert!(json.contains("\"node_id\":\"f\""), "got: {}", json);
    }
}
