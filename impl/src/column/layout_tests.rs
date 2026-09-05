use super::*;
use std::collections::HashMap;

fn same(actual: &ColumnValue, expected: &ColumnValue) {
    match (actual, expected) {
        (ColumnValue::Float32(a), ColumnValue::Float32(b)) => assert_eq!(a.to_bits(), b.to_bits()),
        (ColumnValue::Float64(a), ColumnValue::Float64(b)) => assert_eq!(a.to_bits(), b.to_bits()),
        _ => assert_eq!(actual, expected),
    }
}

fn sample(ty: ColumnType, n: usize, nullable: bool) -> ColumnValue {
    if nullable && n.is_multiple_of(7) {
        return ColumnValue::Null;
    }
    match ty {
        ColumnType::Int32 => ColumnValue::Int32([i32::MIN, 0, i32::MAX, -1][n % 4]),
        ColumnType::Int64 => ColumnValue::Int64([i64::MIN, 0, i64::MAX, 1 << 54][n % 4]),
        ColumnType::Float32 => ColumnValue::Float32(
            [
                -0.0,
                0.0,
                f32::INFINITY,
                f32::NEG_INFINITY,
                f32::from_bits(0x7fc0_1234),
                1.25,
            ][n % 6],
        ),
        ColumnType::Float64 => ColumnValue::Float64(
            [
                -0.0,
                0.0,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::from_bits(0x7ff8_0000_0000_1234),
                1.25,
            ][n % 6],
        ),
        ColumnType::String => ColumnValue::String(["", "repeat", "雪🌱", "a\0b"][n % 4].into()),
        ColumnType::Bool => ColumnValue::Bool(n.is_multiple_of(2)),
        ColumnType::Date => ColumnValue::Date([i32::MIN, -1, 0, i32::MAX][n % 4]),
        ColumnType::DateTime => ColumnValue::DateTime([i64::MIN, -1, 0, i64::MAX][n % 4]),
    }
}

fn check(column: &Column, expected: &[ColumnValue]) {
    assert_eq!(column.len(), expected.len());
    assert_eq!(column.is_empty(), expected.is_empty());
    assert_eq!(column.data.len(), expected.len());
    if let Some(flags) = &column.null_flags {
        assert_eq!(flags.len(), expected.len());
    }
    for (i, value) in expected.iter().enumerate() {
        same(&column.get(i).unwrap(), value);
        assert_eq!(column.is_null_at(i), value.is_null());
        assert_eq!(column.is_null(i).unwrap(), value.is_null());
        let number = match value {
            ColumnValue::Int32(v) => Some(*v as f64),
            ColumnValue::Int64(v) => Some(*v as f64),
            ColumnValue::Float32(v) => Some(*v as f64),
            ColumnValue::Float64(v) => Some(*v),
            _ => None,
        };
        assert_eq!(
            column.get_f64(i).map(f64::to_bits),
            number.map(f64::to_bits)
        );
    }
    let iterated: Vec<_> = column.iter().collect();
    assert_eq!(iterated.len(), expected.len());
    for (a, b) in iterated.iter().zip(expected) {
        same(a, b);
    }
    assert!(column.get(expected.len()).is_err());
    assert_eq!(column.get_f64(expected.len()), None);
    assert!(!column.is_null_at(expected.len()));
    if column.is_nullable() {
        assert!(column.is_null(expected.len()).is_err());
    } else {
        assert_eq!(column.is_null(expected.len()), Ok(false));
    }
    if column.uses_interning() {
        let mut counts = HashMap::new();
        for value in expected {
            if let ColumnValue::String(s) = value {
                *counts.entry(s.as_str()).or_insert(0) += 1;
            }
        }
        let interner = column.interner().unwrap().lock().unwrap();
        assert_eq!(interner.len(), counts.len());
        assert_eq!(
            interner.stats().total_references,
            counts.values().sum::<u64>()
        );
        for (string, count) in counts {
            assert_eq!(
                interner.ref_count(interner.string_to_id[string]) as u64,
                count
            );
        }
    }
}

#[test]
fn all_types_and_backends_match_value_model() {
    for ty in [
        ColumnType::Int32,
        ColumnType::Int64,
        ColumnType::Float32,
        ColumnType::Float64,
        ColumnType::String,
        ColumnType::Bool,
        ColumnType::Date,
        ColumnType::DateTime,
    ] {
        for tiered in [false, true] {
            for nullable in [false, true] {
                for interned in [false, true] {
                    let interner = interned.then(|| Arc::new(Mutex::new(StringInterner::new())));
                    let mut column =
                        Column::new_with_interner("values".into(), ty, nullable, tiered, interner);
                    let mut expected = Vec::new();
                    for i in 0..130 {
                        let value = sample(ty, i, nullable);
                        column.append(value.clone()).unwrap();
                        expected.push(value);
                    }
                    let mut state = 12345u64;
                    for step in 0..1000 {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let index = (state >> 32) as usize % (expected.len() + 1);
                        let value = sample(ty, (state >> 16) as usize, nullable);
                        match state % 5 {
                            0 => {
                                column.insert(index, value.clone()).unwrap();
                                expected.insert(index, value);
                            }
                            1 if index < expected.len() => {
                                same(&column.delete(index).unwrap(), &expected.remove(index));
                            }
                            2 if index < expected.len() => {
                                column.set(index, value.clone()).unwrap();
                                expected[index] = value;
                            }
                            _ => {
                                column.append(value.clone()).unwrap();
                                expected.push(value);
                            }
                        }
                        if step % 71 == 0 {
                            check(&column, &expected);
                        }
                    }
                    check(&column, &expected);
                    let wrong = if ty == ColumnType::String {
                        ColumnValue::Bool(false)
                    } else {
                        ColumnValue::String("wrong".into())
                    };
                    assert!(column.append(wrong.clone()).is_err());
                    assert!(column.insert(0, wrong.clone()).is_err());
                    assert!(column.set(0, wrong).is_err());
                    for index in [expected.len(), usize::MAX] {
                        assert!(column.set(index, sample(ty, 1, nullable)).is_err());
                        assert!(column.delete(index).is_err());
                    }
                    assert!(column.insert(usize::MAX, sample(ty, 1, nullable)).is_err());
                    if !nullable {
                        assert!(column.append(ColumnValue::Null).is_err());
                        assert!(column.insert(0, ColumnValue::Null).is_err());
                        assert!(column.set(0, ColumnValue::Null).is_err());
                    }
                    check(&column, &expected);
                    column.truncate_to(65);
                    expected.truncate(65);
                    check(&column, &expected);
                    column.truncate_to(0);
                    check(&column, &[]);
                }
            }
        }
    }
}

#[test]
fn native_widths_and_no_interned_placeholder_buffer() {
    for tiered in [false, true] {
        let mut ints = Column::new_with_options("i".into(), ColumnType::Int32, false, tiered);
        ints.append(ColumnValue::Int32(42)).unwrap();
        let ColumnData::Int32(data) = &ints.data else {
            panic!("not a typed integer buffer")
        };
        assert_eq!(std::mem::size_of_val(data.get_ref(0).unwrap()), 4);
        assert!(ints.null_flags.is_none());
        let mut floats = Column::new_with_options("f".into(), ColumnType::Float64, true, tiered);
        floats.append(ColumnValue::Float64(1.0)).unwrap();
        let ColumnData::Float64(data) = &floats.data else {
            panic!("not a typed float buffer")
        };
        assert_eq!(std::mem::size_of_val(data.get_ref(0).unwrap()), 8);
        let interner = Arc::new(Mutex::new(StringInterner::new()));
        let mut strings =
            Column::new_with_interner("s".into(), ColumnType::String, true, tiered, Some(interner));
        strings.append(ColumnValue::String("".into())).unwrap();
        strings.append(ColumnValue::Null).unwrap();
        let ColumnData::StringIds(ids) = &strings.data else {
            panic!("interned column must store only IDs")
        };
        assert_eq!(ids.len(), 2);
        assert_eq!(std::mem::size_of_val(ids.get_ref(0).unwrap()), 4);
        assert_eq!(
            ids.get(0).unwrap(),
            0,
            "ID zero is a valid string, not NULL"
        );
        assert_eq!(ids.get(1).unwrap(), NULL_STRING_ID);
    }
}

#[test]
fn shared_interner_references_survive_updates_and_column_drop() {
    for tiered in [false, true] {
        let interner = Arc::new(Mutex::new(StringInterner::new()));
        let mut first = Column::new_with_interner(
            "a".into(),
            ColumnType::String,
            true,
            tiered,
            Some(interner.clone()),
        );
        let mut second = Column::new_with_interner(
            "b".into(),
            ColumnType::String,
            true,
            tiered,
            Some(interner.clone()),
        );
        first.append(ColumnValue::String("shared".into())).unwrap();
        first.append(ColumnValue::Null).unwrap();
        second.append(ColumnValue::String("shared".into())).unwrap();
        first.set(0, ColumnValue::String("shared".into())).unwrap();
        assert_eq!(interner.lock().unwrap().stats().total_references, 2);
        first.set(1, ColumnValue::String("new".into())).unwrap();
        first.set(1, ColumnValue::Null).unwrap();
        drop(first);
        assert_eq!(second.get(0).unwrap(), ColumnValue::String("shared".into()));
        assert_eq!(interner.lock().unwrap().stats().total_references, 1);
        drop(second);
        assert_eq!(interner.lock().unwrap().stats().total_references, 0);
        assert!(interner.lock().unwrap().is_empty());
    }
}

#[test]
fn poisoned_interner_leaves_all_mutations_atomic() {
    for tiered in [false, true] {
        let interner = Arc::new(Mutex::new(StringInterner::new()));
        let mut column = Column::new_with_interner(
            "s".into(),
            ColumnType::String,
            true,
            tiered,
            Some(interner.clone()),
        );
        let expected = [ColumnValue::String("original".into()), ColumnValue::Null];
        for value in &expected {
            column.append(value.clone()).unwrap();
        }
        let _ = std::panic::catch_unwind(|| {
            let _lock = interner.lock().unwrap();
            panic!("test interner failure");
        });
        for value in [ColumnValue::Null, ColumnValue::String("new".into())] {
            assert!(column.append(value.clone()).is_err());
            assert!(column.insert(1, value.clone()).is_err());
            assert!(column.set(0, value.clone()).is_err());
            assert!(column.set(1, value).is_err());
        }
        assert!(column.delete(0).is_err());
        assert!(column.delete(1).is_err());
        assert_eq!(column.len(), 2);
        assert_eq!(column.null_flags.as_ref().unwrap().len(), 2);
        interner.clear_poison();
        check(&column, &expected);
    }
}
