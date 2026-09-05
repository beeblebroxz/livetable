//! Ephemeral, pre-seeded server for benchmarks/pipeline_delivery.mjs.
use livetable::engine::TableEngine;
use serde_json::json;
use std::collections::HashMap;
use std::net::TcpListener;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let size: usize = std::env::args()
        .nth(1)
        .expect("row count")
        .parse()
        .expect("integer row count");
    assert!(size >= 100);
    let mut engine = TableEngine::new();
    for index in 2..size {
        engine
            .insert_row(
                "demo",
                HashMap::from([
                    ("region".into(), json!(format!("region{}", index % 8))),
                    ("product".into(), json!(format!("product{index}"))),
                    (
                        "amount".into(),
                        json!(if index % 2 == 0 {
                            500.0 + (index % 100) as f64
                        } else {
                            100.0
                        }),
                    ),
                ]),
            )
            .unwrap();
        if index % 256 == 0 {
            engine.tick_and_collect("demo");
        }
    }
    engine.tick_and_collect("demo");
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    println!(
        "{}",
        json!({"address":listener.local_addr()?.to_string(), "rows":size})
    );
    livetable::server::server_from_listener_with_engine(listener, engine)?.await
}
