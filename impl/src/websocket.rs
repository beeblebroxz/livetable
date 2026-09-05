//! WebSocket transport and the single-threaded actor that owns `TableEngine`.

use actix::prelude::*;
use actix_web_actors::ws;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::engine::{ConnId, NodeSnapshot, TableEngine};
use crate::messages::{ClientMessage, LabAction, ServerMessage, ViewNodeSpec, PROTOCOL_VERSION};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const PIPELINE_STATUS_INTERVAL: Duration = Duration::from_secs(1);
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);
type JsonRow = HashMap<String, JsonValue>;

fn serialize_ws_message(message: &ServerMessage) -> String {
    serde_json::to_string(message).unwrap_or_else(|error| {
        let safe = error.to_string().replace('"', "'").replace('\\', "/");
        format!(
            r#"{{"type":"Error","message":"Server serialization failure: {}"}}"#,
            safe
        )
    })
}

fn snapshot_message(table_name: &str, snapshot: NodeSnapshot) -> ServerMessage {
    snapshot.into_message(table_name)
}

/// The one outbound path to a WebSocket (or an actor-test probe).
#[derive(Message, Clone)]
#[rtype(result = "()")]
pub(crate) struct BroadcastMessage(pub ServerMessage);

type ClientRecipient = Recipient<BroadcastMessage>;

#[derive(Message)]
#[rtype(result = "()")]
struct SubscribeBase {
    table_name: String,
    conn: ConnId,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Query {
    table_name: String,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Insert {
    table_name: String,
    row: JsonRow,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Update {
    table_name: String,
    row_id: u64,
    column: String,
    value: JsonValue,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Delete {
    table_name: String,
    row_id: u64,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetPipeline {
    conn: ConnId,
    table_name: String,
    pipeline_generation: u32,
    nodes: Vec<ViewNodeSpec>,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct QueryView {
    conn: ConnId,
    table_name: String,
    pipeline_generation: u32,
    node_id: String,
    requester: ClientRecipient,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Disconnect {
    conn: ConnId,
}

#[derive(Message)]
#[rtype(result = "()")]
struct LabCommand {
    request_id: u32,
    action: LabAction,
    requester: ClientRecipient,
}

/// Owns all core tables and `Rc`-based views on one Actix thread.
pub struct TableEngineActor {
    engine: TableEngine,
    base_subscribers: HashMap<String, Vec<(ConnId, ClientRecipient)>>,
    connections: HashMap<ConnId, ClientRecipient>,
}

impl Default for TableEngineActor {
    fn default() -> Self {
        Self::new()
    }
}

impl TableEngineActor {
    pub fn new() -> Self {
        Self {
            engine: TableEngine::new(),
            base_subscribers: HashMap::new(),
            connections: HashMap::new(),
        }
    }

    fn send(recipient: &ClientRecipient, message: ServerMessage) {
        let _ = recipient.try_send(BroadcastMessage(message));
    }

    fn send_error(recipient: &ClientRecipient, message: String) {
        Self::send(recipient, ServerMessage::Error { message });
    }

    fn broadcast_base(&mut self, table_name: &str, message: ServerMessage) {
        if let Some(subscribers) = self.base_subscribers.get_mut(table_name) {
            subscribers.retain(|(_, recipient)| {
                recipient
                    .try_send(BroadcastMessage(message.clone()))
                    .is_ok()
            });
        }
    }

    fn propagate_views(&mut self, table_name: &str) {
        let collected = self.engine.tick_and_collect(table_name);
        for (conn, messages) in collected {
            let Some(recipient) = self.connections.get(&conn) else {
                continue;
            };
            for message in messages {
                Self::send(recipient, message);
            }
        }
    }

    fn finish_mutation(
        &mut self,
        table_name: &str,
        requester: &ClientRecipient,
        result: Result<ServerMessage, String>,
    ) {
        match result {
            Ok(message) => {
                self.broadcast_base(table_name, message);
                self.propagate_views(table_name);
            }
            Err(message) => Self::send_error(requester, message),
        }
    }
}

impl Actor for TableEngineActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(PIPELINE_STATUS_INTERVAL, |actor, _| {
            for (conn, message) in actor.engine.pipeline_statuses() {
                if let Some(recipient) = actor.connections.get(&conn) {
                    Self::send(recipient, message);
                }
            }
        });
    }
}

impl Handler<SubscribeBase> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: SubscribeBase, _ctx: &mut Self::Context) {
        if !self.engine.has_table(&message.table_name) {
            Self::send_error(
                &message.requester,
                format!("Table '{}' not found", message.table_name),
            );
            return;
        }

        for subscribers in self.base_subscribers.values_mut() {
            subscribers.retain(|(conn, _)| *conn != message.conn);
        }
        self.connections
            .insert(message.conn, message.requester.clone());
        self.base_subscribers
            .entry(message.table_name.clone())
            .or_default()
            .push((message.conn, message.requester.clone()));
        Self::send(
            &message.requester,
            ServerMessage::Subscribed {
                table_name: message.table_name,
                protocol_version: PROTOCOL_VERSION,
            },
        );
    }
}

impl Handler<Query> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: Query, _ctx: &mut Self::Context) {
        match self.engine.query_table(&message.table_name) {
            Ok(response) => Self::send(&message.requester, response),
            Err(error) => Self::send_error(&message.requester, error),
        }
    }
}

impl Handler<LabCommand> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: LabCommand, _ctx: &mut Self::Context) {
        match self.engine.lab_command(&message.action) {
            Ok((flat, mutations, rows, step)) => {
                if matches!(message.action, LabAction::Reset { .. }) {
                    // Flat clients also receive a monotonic, coherent baseline.
                    if let Ok(snapshot) = self.engine.query_table("lab") {
                        self.broadcast_base("lab", snapshot);
                    }
                } else {
                    for update in flat { self.broadcast_base("lab", update); }
                }
                self.propagate_views("lab");
                Self::send(&message.requester, ServerMessage::LabComplete {
                    request_id: message.request_id, rows, step, mutations,
                });
            }
            Err(error) => Self::send(&message.requester, ServerMessage::LabError {
                request_id: message.request_id, message: error,
            }),
        }
    }
}

impl Handler<Insert> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: Insert, _ctx: &mut Self::Context) {
        let result = self.engine.insert_row(&message.table_name, message.row);
        self.finish_mutation(&message.table_name, &message.requester, result);
    }
}

impl Handler<Update> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: Update, _ctx: &mut Self::Context) {
        let result = self.engine.update_cell(
            &message.table_name,
            message.row_id,
            &message.column,
            &message.value,
        );
        self.finish_mutation(&message.table_name, &message.requester, result);
    }
}

impl Handler<Delete> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: Delete, _ctx: &mut Self::Context) {
        let result = self.engine.delete_row(&message.table_name, message.row_id);
        self.finish_mutation(&message.table_name, &message.requester, result);
    }
}

impl Handler<SetPipeline> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: SetPipeline, _ctx: &mut Self::Context) {
        self.connections
            .insert(message.conn, message.requester.clone());
        let results = self.engine.set_pipeline(
            message.conn,
            &message.table_name,
            message.pipeline_generation,
            &message.nodes,
        );
        for result in results {
            let response = match result {
                Ok(snapshot) => snapshot_message(&message.table_name, snapshot),
                Err((node_id, error)) => ServerMessage::ViewError {
                    table_name: message.table_name.clone(),
                    pipeline_generation: message.pipeline_generation,
                    node_id,
                    message: error,
                },
            };
            Self::send(&message.requester, response);
        }
    }
}

impl Handler<Disconnect> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: Disconnect, _ctx: &mut Self::Context) {
        self.connections.remove(&message.conn);
        for subscribers in self.base_subscribers.values_mut() {
            subscribers.retain(|(conn, _)| *conn != message.conn);
        }
        self.engine.drop_connection(message.conn);
    }
}

impl Handler<QueryView> for TableEngineActor {
    type Result = ();

    fn handle(&mut self, message: QueryView, _ctx: &mut Self::Context) {
        let response = self
            .engine
            .query_view(
                message.conn,
                &message.table_name,
                message.pipeline_generation,
                &message.node_id,
            )
            .unwrap_or_else(|error| ServerMessage::ViewError {
                table_name: message.table_name,
                pipeline_generation: message.pipeline_generation,
                node_id: message.node_id,
                message: error,
            });
        Self::send(&message.requester, response);
    }
}

/// Cloneable application state shared by HTTP workers. The contained address
/// routes every operation back to the one engine actor.
pub struct AppState {
    engine: Addr<TableEngineActor>,
}

impl AppState {
    pub fn new() -> Self {
        Self::with_engine(TableEngine::new())
    }

    /// Start the actor with a pre-seeded engine, on the current Actix thread.
    pub fn with_engine(engine: TableEngine) -> Self {
        Self {
            engine: TableEngineActor {
                engine,
                ..TableEngineActor::new()
            }
            .start(),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TableWebSocket {
    hb: Instant,
    conn_id: ConnId,
    state: actix_web::web::Data<AppState>,
}

impl TableWebSocket {
    pub fn new(state: actix_web::web::Data<AppState>) -> Self {
        Self {
            hb: Instant::now(),
            conn_id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
            state,
        }
    }

    fn heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |actor, ctx| {
            if Instant::now().duration_since(actor.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }

    fn handle_client_message(
        &mut self,
        message: ClientMessage,
        ctx: &mut ws::WebsocketContext<Self>,
    ) {
        let requester = ctx.address().recipient();
        match message {
            ClientMessage::LabCommand { request_id, action } => {
                self.state.engine.do_send(LabCommand { request_id, action, requester });
            }
            ClientMessage::Subscribe { table_name } => {
                self.state.engine.do_send(SubscribeBase {
                    table_name,
                    conn: self.conn_id,
                    requester,
                });
            }
            ClientMessage::Query { table_name } => {
                self.state.engine.do_send(Query {
                    table_name,
                    requester,
                });
            }
            ClientMessage::InsertRow { table_name, row } => {
                self.state.engine.do_send(Insert {
                    table_name,
                    row,
                    requester,
                });
            }
            ClientMessage::UpdateCell {
                table_name,
                row_id,
                column,
                value,
            } => {
                self.state.engine.do_send(Update {
                    table_name,
                    row_id,
                    column,
                    value,
                    requester,
                });
            }
            ClientMessage::DeleteRow { table_name, row_id } => {
                self.state.engine.do_send(Delete {
                    table_name,
                    row_id,
                    requester,
                });
            }
            ClientMessage::SetPipeline {
                table_name,
                pipeline_generation,
                nodes,
            } => {
                self.state.engine.do_send(SetPipeline {
                    conn: self.conn_id,
                    table_name,
                    pipeline_generation,
                    nodes,
                    requester,
                });
            }
            ClientMessage::QueryView {
                table_name,
                pipeline_generation,
                node_id,
            } => {
                self.state.engine.do_send(QueryView {
                    conn: self.conn_id,
                    table_name,
                    pipeline_generation,
                    node_id,
                    requester,
                });
            }
        }
    }
}

impl Actor for TableWebSocket {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        self.state.engine.do_send(Disconnect { conn: self.conn_id });
        Running::Stop
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for TableWebSocket {
    fn handle(&mut self, message: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match message {
            Ok(ws::Message::Ping(message)) => {
                self.hb = Instant::now();
                ctx.pong(&message);
            }
            Ok(ws::Message::Pong(_)) => self.hb = Instant::now(),
            Ok(ws::Message::Text(text)) => match serde_json::from_str::<ClientMessage>(&text) {
                Ok(message) => self.handle_client_message(message, ctx),
                Err(error) => ctx.text(serialize_ws_message(&ServerMessage::Error {
                    message: format!("Invalid message format: {error}"),
                })),
            },
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Binary(_)) | Ok(ws::Message::Continuation(_)) => {}
            Ok(ws::Message::Nop) => {}
            Err(_) => ctx.stop(),
        }
    }
}

impl Handler<BroadcastMessage> for TableWebSocket {
    type Result = ();

    fn handle(&mut self, message: BroadcastMessage, ctx: &mut Self::Context) {
        ctx.text(serialize_ws_message(&message.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{AggSpec, ViewKindSpec};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    struct Probe {
        messages: Arc<Mutex<Vec<ServerMessage>>>,
    }

    impl Actor for Probe {
        type Context = Context<Self>;
    }

    impl Handler<BroadcastMessage> for Probe {
        type Result = ();

        fn handle(&mut self, message: BroadcastMessage, _ctx: &mut Self::Context) {
            self.messages.lock().unwrap().push(message.0);
        }
    }

    #[actix::test]
    async fn actor_builds_pipeline_and_streams_updates() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let requester = Probe {
            messages: received.clone(),
        }
        .start()
        .recipient();
        let engine = TableEngineActor::new().start();

        engine
            .send(SetPipeline {
                conn: 42,
                table_name: "demo".into(),
                pipeline_generation: 9,
                nodes: vec![
                    ViewNodeSpec {
                        id: "filtered".into(),
                        source_id: "base".into(),
                        kind: ViewKindSpec::Filter {
                            predicate: "amount >= 150".into(),
                        },
                    },
                    ViewNodeSpec {
                        id: "totals".into(),
                        source_id: "filtered".into(),
                        kind: ViewKindSpec::Group {
                            group_by: vec!["region".into()],
                            aggs: vec![AggSpec {
                                alias: "total".into(),
                                op: "sum".into(),
                                column: "amount".into(),
                            }],
                        },
                    },
                ],
                requester: requester.clone(),
            })
            .await
            .unwrap();
        engine
            .send(Insert {
                table_name: "demo".into(),
                row: HashMap::from([
                    ("region".into(), json!("West")),
                    ("product".into(), json!("Premium")),
                    ("amount".into(), json!(300.0)),
                ]),
                requester,
            })
            .await
            .unwrap();

        actix::clock::sleep(Duration::from_millis(10)).await;
        let messages = received.lock().unwrap();
        let totals: Vec<_> = messages
            .iter()
            .filter(|message| {
                matches!(message, ServerMessage::ViewData { node_id, .. } if node_id == "totals")
            })
            .collect();
        assert_eq!(totals.len(), 2, "initial snapshot plus mutation snapshot");
        let ServerMessage::ViewData {
            pipeline_generation,
            rows,
            ..
        } = totals[1]
        else {
            unreachable!()
        };
        assert_eq!(*pipeline_generation, 9);
        assert!(rows
            .iter()
            .any(|row| { row.row["region"] == json!("West") && row.row["total"] == json!(300.0) }));
    }

    #[actix::test]
    async fn actor_preserves_base_protocol_and_reports_pipeline_errors() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let requester = Probe {
            messages: received.clone(),
        }
        .start()
        .recipient();
        let engine = TableEngineActor::new().start();

        engine
            .send(SubscribeBase {
                table_name: "demo".into(),
                conn: 5,
                requester: requester.clone(),
            })
            .await
            .unwrap();
        engine
            .send(Query {
                table_name: "demo".into(),
                requester: requester.clone(),
            })
            .await
            .unwrap();
        engine
            .send(SetPipeline {
                conn: 5,
                table_name: "demo".into(),
                pipeline_generation: 2,
                nodes: vec![ViewNodeSpec {
                    id: "bad".into(),
                    source_id: "base".into(),
                    kind: ViewKindSpec::Filter {
                        predicate: "amount >>> 2".into(),
                    },
                }],
                requester,
            })
            .await
            .unwrap();

        actix::clock::sleep(Duration::from_millis(10)).await;
        let messages = received.lock().unwrap();
        assert!(messages.iter().any(|message| matches!(
            message,
            ServerMessage::Subscribed {
                protocol_version: 3,
                ..
            }
        )));
        assert!(messages
            .iter()
            .any(|message| matches!(message, ServerMessage::TableData { .. })));
        assert!(messages.iter().any(|message| matches!(
            message,
            ServerMessage::ViewError {
                pipeline_generation: 2,
                node_id,
                ..
            } if node_id == "bad"
        )));
    }
}
