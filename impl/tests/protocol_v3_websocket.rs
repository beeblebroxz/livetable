#![cfg(feature = "server")]

use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use tokio::io::{
    split, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    ReadHalf, WriteHalf,
};
use tokio::net::TcpStream;

struct RawWebSocket {
    reader: BufReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
    snapshots: HashMap<String, JsonValue>,
    generation: u64,
    drop_next_node: Option<String>,
}

impl RawWebSocket {
    async fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = split(stream);
        let request = format!(
            "GET /ws HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        writer.write_all(request.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(reader);
        let mut status = String::new();
        reader.read_line(&mut status).await.unwrap();
        assert!(
            status.contains("101 Switching Protocols"),
            "unexpected upgrade response: {status}"
        );
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
        }

        Self {
            reader,
            writer,
            snapshots: HashMap::new(),
            generation: 0,
            drop_next_node: None,
        }
    }

    async fn send_json(&mut self, value: JsonValue) {
        write_client_frame(&mut self.writer, 0x1, value.to_string().as_bytes()).await;
    }

    async fn receive_json(&mut self) -> JsonValue {
        loop {
            let (opcode, payload) =
                tokio::time::timeout(Duration::from_secs(2), read_server_frame(&mut self.reader))
                    .await
                    .expect("timed out waiting for a server WebSocket frame");
            match opcode {
                0x1 => {
                    let message: JsonValue = serde_json::from_slice(&payload).unwrap();
                    self.reconstruct(&message);
                    return message;
                }
                0x9 => write_client_frame(&mut self.writer, 0xA, &payload).await,
                0x8 => panic!("server closed WebSocket before test completed"),
                other => panic!("unexpected WebSocket opcode {other}"),
            }
        }
    }

    async fn receive_view(&mut self, node_id: &str) -> JsonValue {
        for _ in 0..16 {
            let message = self.receive_json().await;
            if (message["type"] == "ViewData" || message["type"] == "ViewDelta")
                && message["node_id"] == node_id
            {
                return message;
            }
        }
        panic!("did not receive ViewData for node '{node_id}'");
    }

    fn reconstruct(&mut self, message: &JsonValue) {
        let Some(kind @ ("ViewData" | "ViewDelta")) = message["type"].as_str() else {
            return;
        };
        let node = message["node_id"].as_str().unwrap();
        if self.drop_next_node.as_deref() == Some(node) {
            self.drop_next_node = None;
            return;
        }
        let generation = message["pipeline_generation"].as_u64().unwrap();
        if generation < self.generation {
            return;
        }
        if generation > self.generation {
            self.generation = generation;
            self.snapshots.clear();
        }
        if kind == "ViewData" {
            self.snapshots.insert(node.into(), message.clone());
        } else {
            let snapshot = self.snapshots.get_mut(node).expect("delta needs baseline");
            assert_eq!(snapshot["seq"], message["from_seq"]);
            let rows = snapshot["rows"].as_array_mut().unwrap();
            for change in message["changes"].as_array().unwrap() {
                let index = change["index"].as_u64().unwrap() as usize;
                match change["type"].as_str().unwrap() {
                    "RowInserted" => rows.insert(index, change["row"].clone()),
                    "RowDeleted" => {
                        rows.remove(index);
                    }
                    "CellUpdated" => {
                        rows[index]["row"][change["column"].as_str().unwrap()] =
                            change["value"].clone();
                    }
                    other => panic!("unknown operation {other}"),
                }
            }
            snapshot["seq"] = message["seq"].clone();
        }
    }

    async fn close(&mut self) {
        write_client_frame(&mut self.writer, 0x8, &[]).await;
    }
}

async fn write_client_frame<W>(writer: &mut W, opcode: u8, payload: &[u8])
where
    W: AsyncWrite + Unpin,
{
    let mask = [0x12, 0x34, 0x56, 0x78];
    let mut frame = vec![0x80 | opcode];
    match payload.len() {
        len if len < 126 => frame.push(0x80 | len as u8),
        len if u16::try_from(len).is_ok() => {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[index % mask.len()]),
    );
    writer.write_all(&frame).await.unwrap();
}

async fn read_server_frame<R>(reader: &mut R) -> (u8, Vec<u8>)
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; 2];
    reader.read_exact(&mut header).await.unwrap();
    assert_ne!(header[0] & 0x80, 0, "fragmented frames are not expected");
    let opcode = header[0] & 0x0F;
    let masked = header[1] & 0x80 != 0;
    let mut length = (header[1] & 0x7F) as u64;
    if length == 126 {
        let mut extended = [0_u8; 2];
        reader.read_exact(&mut extended).await.unwrap();
        length = u16::from_be_bytes(extended) as u64;
    } else if length == 127 {
        let mut extended = [0_u8; 8];
        reader.read_exact(&mut extended).await.unwrap();
        length = u64::from_be_bytes(extended);
    }
    let mask = if masked {
        let mut mask = [0_u8; 4];
        reader.read_exact(&mut mask).await.unwrap();
        Some(mask)
    } else {
        None
    };
    let mut payload = vec![0_u8; usize::try_from(length).unwrap()];
    reader.read_exact(&mut payload).await.unwrap();
    if let Some(mask) = mask {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % mask.len()];
        }
    }
    (opcode, payload)
}

#[actix::test]
async fn protocol_v3_pipeline_crosses_real_websocket_boundary() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = livetable::server::server_from_listener(listener).unwrap();
    let handle = server.handle();
    let server_task = actix::spawn(server);

    let mut socket = RawWebSocket::connect(address).await;
    socket
        .send_json(json!({"type": "Subscribe", "table_name": "demo"}))
        .await;
    socket
        .send_json(json!({
            "type": "SetPipeline",
            "table_name": "demo",
            "pipeline_generation": 41,
            "nodes": [
                {"id": "f", "source_id": "base", "kind": "filter", "predicate": "amount >= 500"},
                {"id": "s", "source_id": "f", "kind": "sort", "keys": [{"column": "amount", "descending": true}]},
                {"id": "g", "source_id": "s", "kind": "group", "group_by": ["region"],
                 "aggs": [{"alias": "total", "op": "sum", "column": "amount"}]}
            ]
        }))
        .await;

    let mut initial_nodes = HashSet::new();
    let mut subscribed = false;
    while initial_nodes.len() < 4 || !subscribed {
        let message = socket.receive_json().await;
        match message["type"].as_str() {
            Some("Subscribed") => {
                assert_eq!(message["protocol_version"], 3);
                subscribed = true;
            }
            Some("ViewData") => {
                assert_eq!(message["pipeline_generation"], 41);
                initial_nodes.insert(message["node_id"].as_str().unwrap().to_string());
            }
            other => panic!("unexpected initial message {other:?}: {message}"),
        }
    }
    assert_eq!(
        initial_nodes,
        HashSet::from(["base".into(), "f".into(), "s".into(), "g".into()])
    );

    // Deliberately lose the filter's final update; periodic watermarks must
    // reveal the gap even without a subsequent mutation.
    socket.drop_next_node = Some("f".into());
    socket
        .send_json(json!({
            "type": "InsertRow",
            "table_name": "demo",
            "row": {"region": "West", "product": "Integration", "amount": 700.0}
        }))
        .await;
    let inserted_group = socket.receive_view("g").await;
    let west = inserted_group["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["row"]["region"] == "West")
        .unwrap();
    assert_eq!(west["row"]["total"], 700.0);
    assert_eq!(
        socket.snapshots["base"]["rows"].as_array().unwrap().len(),
        3
    );
    assert_eq!(socket.snapshots["s"]["rows"][0]["row"]["amount"], 700.0);
    assert_eq!(socket.snapshots["f"]["seq"], 0);
    loop {
        let message = socket.receive_json().await;
        if message["type"] == "PipelineStatus" {
            assert_eq!(message["pipeline_generation"], 41);
            assert_eq!(message["sequences"]["f"], 1);
            break;
        }
    }
    socket.send_json(json!({"type":"QueryView", "table_name":"demo", "pipeline_generation":41, "node_id":"f"})).await;
    let repaired = socket.receive_view("f").await;
    assert_eq!(repaired["type"], "ViewData");
    assert_eq!(repaired["seq"], 2);
    assert_eq!(repaired["rows"][0]["row"]["amount"], 700.0);

    socket
        .send_json(json!({
            "type": "UpdateCell",
            "table_name": "demo",
            "row_id": 3,
            "column": "amount",
            "value": 100.0
        }))
        .await;
    let updated_group = socket.receive_view("g").await;
    assert!(updated_group["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["row"]["region"] != "West"));

    socket
        .send_json(json!({
            "type": "DeleteRow",
            "table_name": "demo",
            "row_id": 3
        }))
        .await;
    let deleted_base = socket.receive_view("base").await;
    assert_eq!(deleted_base["type"], "ViewDelta");
    assert!(socket.snapshots["base"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["row_id"] != 3));

    // Querying a node advances only that node's sequence; normal deltas above
    // still applied correctly after the repair. Replacement resets baselines.
    socket.send_json(json!({"type":"SetPipeline", "table_name":"demo", "pipeline_generation":42, "nodes":[]})).await;
    let replacement = socket.receive_view("base").await;
    assert_eq!(replacement["pipeline_generation"], 42);
    assert_eq!(replacement["seq"], 0);
    assert_eq!(socket.snapshots.len(), 1);
    socket.send_json(json!({"type":"QueryView", "table_name":"demo", "pipeline_generation":41, "node_id":"f"})).await;
    loop {
        let message = socket.receive_json().await;
        if message["type"] == "ViewError" {
            assert_eq!(message["pipeline_generation"], 41);
            break;
        }
    }

    // An independent connection gets its own generation/baseline and current
    // stable base IDs, not the first connection's delivery sequence.
    let mut second = RawWebSocket::connect(address).await;
    second
        .send_json(
            json!({"type":"SetPipeline", "table_name":"demo", "pipeline_generation":1, "nodes":[]}),
        )
        .await;
    let independent = second.receive_view("base").await;
    assert_eq!(independent["seq"], 0);
    assert_eq!(independent["rows"], replacement["rows"]);
    second.close().await;
    drop(second);

    socket.close().await;
    drop(socket);
    handle.stop(true).await;
    server_task.await.unwrap().unwrap();
}
