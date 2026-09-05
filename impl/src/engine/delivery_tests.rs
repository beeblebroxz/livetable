use super::tests::{apply_deliveries, pipeline_specs};
use super::*;
use serde_json::json;

fn install(
    engine: &mut TableEngine,
    conn: ConnId,
    generation: u32,
    specs: &[ViewNodeSpec],
) -> HashMap<String, NodeSnapshot> {
    let mut client = HashMap::new();
    apply_deliveries(
        &mut client,
        engine
            .set_pipeline(conn, "demo", generation, specs)
            .into_iter()
            .map(|result| result.unwrap().into_message("demo"))
            .collect(),
    );
    client
}

fn insert(engine: &mut TableEngine, amount: f64) -> u64 {
    match engine
        .insert_row(
            "demo",
            HashMap::from([
                ("region".into(), json!("West")),
                ("product".into(), json!("\u{0}雪")),
                ("amount".into(), json!(amount)),
            ]),
        )
        .unwrap()
    {
        ServerMessage::RowInserted { row_id, .. } => row_id,
        _ => unreachable!(),
    }
}

fn assert_matches(client: &HashMap<String, NodeSnapshot>, fresh: &HashMap<String, NodeSnapshot>) {
    for (id, expected) in fresh {
        let actual = &client[id];
        assert_eq!(actual.columns, expected.columns, "columns for {id}");
        if id == "totals" {
            let groups = |node: &NodeSnapshot| {
                node.rows
                    .iter()
                    .map(|row| {
                        (
                            row.row["region"].as_str().unwrap().to_string(),
                            row.row["total"].as_f64().unwrap(),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            };
            let actual = groups(actual);
            let expected = groups(expected);
            assert_eq!(actual.len(), expected.len());
            for (key, value) in expected {
                assert!((actual[&key] - value).abs() < 1e-8);
            }
        } else {
            assert_eq!(
                serde_json::to_value(&actual.rows).unwrap(),
                serde_json::to_value(&expected.rows).unwrap(),
                "rows for {id}"
            );
        }
    }
}

#[test]
fn excluded_edits_send_no_filter_or_sort_payload_and_keep_baselines() {
    let mut engine = TableEngine::new();
    let mut client = install(&mut engine, 1, 1, &pipeline_specs());
    for value in [50.0, 75.0, 125.0] {
        engine
            .update_cell("demo", 1, "amount", &json!(value))
            .unwrap();
        let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
        assert_eq!(messages.len(), 2, "base delta plus aggregate snapshot only");
        assert!(
            matches!(&messages[0], ServerMessage::ViewDelta { node_id, from_seq, .. }
            if node_id == "base" && *from_seq == client["base"].seq)
        );
        apply_deliveries(&mut client, messages);
        assert_eq!(client["filtered"].seq, 0);
        assert_eq!(client["ranked"].seq, 0);
    }
    engine
        .update_cell("demo", 1, "amount", &json!(600.0))
        .unwrap();
    let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
    assert_eq!(messages.len(), 4);
    assert!(matches!(
        &messages[1],
        ServerMessage::ViewDelta {
            from_seq: 0,
            seq: 1,
            ..
        }
    ));
    apply_deliveries(&mut client, messages);
    assert_matches(&client, &install(&mut engine, 2, 1, &pipeline_specs()));
    assert!(engine.tick_and_collect("demo").is_empty());
}

#[test]
fn mixed_batch_preserves_step_coordinates_and_deleted_insert_ids() {
    let mut engine = TableEngine::new();
    let mut client = install(&mut engine, 1, 1, &pipeline_specs());
    let transient = insert(&mut engine, 900.0);
    engine
        .update_cell("demo", 2, "amount", &json!(800.0))
        .unwrap();
    engine.delete_row("demo", 1).unwrap();
    engine
        .update_cell("demo", transient, "amount", &json!(1.0))
        .unwrap();
    engine.delete_row("demo", transient).unwrap();
    let retained = insert(&mut engine, 700.0);
    let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
    assert!(matches!(&messages[0], ServerMessage::ViewDelta { changes, .. } if changes.len() == 6));
    apply_deliveries(&mut client, messages);
    assert_eq!(
        client["base"]
            .rows
            .iter()
            .map(|row| row.row_id.unwrap())
            .collect::<Vec<_>>(),
        [2, retained]
    );
    assert_matches(&client, &install(&mut engine, 2, 1, &pipeline_specs()));
}

#[test]
fn oversized_batches_rebaseline_and_resume_deltas() {
    for size in [257, MAX_VIEW_DELTA_CHANGES + 1] {
        let mut engine = TableEngine::new();
        let mut client = install(&mut engine, 1, 1, &pipeline_specs());
        for step in 0..size {
            engine
                .update_cell("demo", 2, "amount", &json!(300.0 + step as f64))
                .unwrap();
        }
        assert!(engine.bases["demo"].pending_base.len() <= MAX_VIEW_DELTA_CHANGES);
        let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
        assert!(
            matches!(&messages[1], ServerMessage::ViewData { node_id, seq: 1, .. } if node_id == "filtered")
        );
        assert!(
            matches!(&messages[2], ServerMessage::ViewData { node_id, seq: 1, .. } if node_id == "ranked")
        );
        assert_eq!(
            matches!(&messages[0], ServerMessage::ViewData { .. }),
            size > MAX_VIEW_DELTA_CHANGES
        );
        apply_deliveries(&mut client, messages);
        engine
            .update_cell("demo", 2, "amount", &json!(400.0))
            .unwrap();
        let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
        assert!(matches!(
            &messages[1],
            ServerMessage::ViewDelta {
                from_seq: 1,
                seq: 2,
                ..
            }
        ));
        apply_deliveries(&mut client, messages);
        assert_matches(&client, &install(&mut engine, 2, 1, &pipeline_specs()));
    }
}

#[test]
fn query_view_repairs_lost_delivery_and_is_connection_local() {
    let mut engine = TableEngine::new();
    let mut first = install(&mut engine, 1, 4, &pipeline_specs());
    let mut second = install(&mut engine, 2, 9, &pipeline_specs());
    engine
        .update_cell("demo", 2, "amount", &json!(999.0))
        .unwrap();
    let mut collected = engine.tick_and_collect("demo");
    // Drop all deliveries to first, including the last delta.
    apply_deliveries(&mut second, collected.remove(&2).unwrap());
    let statuses = engine.pipeline_statuses();
    let (_, status) = statuses.iter().find(|(conn, _)| *conn == 1).unwrap();
    let ServerMessage::PipelineStatus {
        pipeline_generation: 4,
        sequences,
        ..
    } = status
    else {
        panic!("status");
    };
    assert_eq!(sequences["filtered"], 1);
    assert!(sequences["filtered"] > first["filtered"].seq);
    for node in ["base", "filtered", "ranked", "totals"] {
        let message = engine.query_view(1, "demo", 4, node).unwrap();
        assert!(matches!(&message, ServerMessage::ViewData { seq: 2, .. }));
        apply_deliveries(&mut first, vec![message]);
    }
    engine
        .update_cell("demo", 2, "amount", &json!(650.0))
        .unwrap();
    let mut collected = engine.tick_and_collect("demo");
    apply_deliveries(&mut first, collected.remove(&1).unwrap());
    apply_deliveries(&mut second, collected.remove(&2).unwrap());
    assert_eq!(first["filtered"].seq, 3);
    assert_eq!(second["filtered"].seq, 2);
    assert_matches(&first, &second);
}

#[test]
fn generation_and_query_validation_do_not_reset_live_delivery() {
    let mut engine = TableEngine::new();
    install(&mut engine, 1, 4, &pipeline_specs());
    for generation in [3, 4] {
        assert!(engine.set_pipeline(1, "demo", generation, &[])[0].is_err());
    }
    assert!(engine.query_view(2, "demo", 4, "base").is_err());
    assert!(engine.query_view(1, "missing", 4, "base").is_err());
    assert!(engine.query_view(1, "demo", 3, "base").is_err());
    assert!(engine.query_view(1, "demo", 4, "missing").is_err());
    install(&mut engine, 1, 5, &[]);
    assert!(engine.query_view(1, "demo", 4, "base").is_err());
    assert!(engine.query_view(1, "demo", 5, "filtered").is_err());
    engine.drop_connection(1);
    assert!(engine.pipeline_statuses().is_empty());
    let reconnected = install(&mut engine, 2, 1, &pipeline_specs());
    assert!(reconnected.values().all(|snapshot| snapshot.seq == 0));
}

#[test]
fn connection_created_mid_batch_does_not_replay_its_initial_rows() {
    let mut engine = TableEngine::new();
    let mut first = install(&mut engine, 1, 1, &pipeline_specs());
    insert(&mut engine, 700.0);
    let mut second = install(&mut engine, 2, 1, &pipeline_specs());
    engine.delete_row("demo", 1).unwrap();
    let mut collected = engine.tick_and_collect("demo");
    apply_deliveries(&mut first, collected.remove(&1).unwrap());
    apply_deliveries(&mut second, collected.remove(&2).unwrap());
    assert_matches(&first, &second);
}

#[test]
fn rejected_writes_and_noop_ticks_emit_nothing() {
    let mut engine = TableEngine::new();
    install(&mut engine, 1, 1, &pipeline_specs());
    assert!(engine
        .update_cell("demo", 1, "amount", &json!(null))
        .is_err());
    assert!(engine.delete_row("demo", 999).is_err());
    assert!(engine.insert_row("demo", HashMap::new()).is_err());
    assert!(engine.tick_and_collect("demo").is_empty());
    let statuses = engine.pipeline_statuses();
    let ServerMessage::PipelineStatus { sequences, .. } = &statuses[0].1 else {
        panic!("status");
    };
    assert!(sequences.values().all(|seq| *seq == 0));
}

#[test]
fn seeded_mixed_batches_match_fresh_pipelines() {
    for seed in 0..8_u64 {
        let mut rng = seed + 123;
        let mut random = |limit: usize| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            (rng >> 32) as usize % limit
        };
        let mut engine = TableEngine::new();
        let mut client = install(&mut engine, 1, 1, &pipeline_specs());
        let mut ids = vec![1, 2];
        for batch in 0..80 {
            for _ in 0..1 + random(12) {
                match random(5) {
                    0 | 1 if !ids.is_empty() => {
                        let id = ids[random(ids.len())];
                        engine
                            .update_cell("demo", id, "amount", &json!(random(10) as f64 * 100.0))
                            .unwrap();
                    }
                    2 if !ids.is_empty() => {
                        let index = random(ids.len());
                        engine.delete_row("demo", ids.remove(index)).unwrap();
                    }
                    3 if !ids.is_empty() => {
                        let id = ids[random(ids.len())];
                        engine
                            .update_cell(
                                "demo",
                                id,
                                "region",
                                &json!(if random(2) == 0 { "East" } else { "West" }),
                            )
                            .unwrap();
                    }
                    _ => ids.push(insert(&mut engine, random(10) as f64 * 100.0)),
                }
            }
            let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
            apply_deliveries(&mut client, messages);
            let fresh = install(&mut engine, 99, batch + 1, &pipeline_specs());
            assert_matches(&client, &fresh);
            engine.drop_connection(99);
        }
    }
}

#[test]
fn group_descendants_keep_snapshot_fallback() {
    let mut engine = TableEngine::new();
    let mut specs = pipeline_specs();
    specs.push(ViewNodeSpec {
        id: "group_filter".into(),
        source_id: "totals".into(),
        kind: ViewKindSpec::Filter {
            predicate: "total > 0".into(),
        },
    });
    let mut client = install(&mut engine, 1, 1, &specs);
    engine
        .update_cell("demo", 2, "amount", &json!(400.0))
        .unwrap();
    let messages = engine.tick_and_collect("demo").remove(&1).unwrap();
    assert!(messages.iter().any(|message| matches!(message, ServerMessage::ViewData { node_id, .. } if node_id == "group_filter")));
    apply_deliveries(&mut client, messages);
    assert_matches(&client, &install(&mut engine, 2, 1, &specs));
}
