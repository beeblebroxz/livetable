// ============================================================================
// Date/Time Helper Functions
// ============================================================================

/// Convert a Python datetime.date to days since Unix epoch (1970-01-01)
fn date_to_days_since_epoch(date: &Bound<'_, PyAny>) -> PyResult<i32> {
    let year: i32 = date.getattr("year")?.extract()?;
    let month: u32 = date.getattr("month")?.extract()?;
    let day: u32 = date.getattr("day")?.extract()?;

    let days = days_from_ymd(year, month, day);
    Ok(days)
}

/// Convert a Python datetime.datetime to milliseconds since Unix epoch
fn datetime_to_ms_since_epoch(dt: &Bound<'_, PyAny>) -> PyResult<i64> {
    let year: i32 = dt.getattr("year")?.extract()?;
    let month: u32 = dt.getattr("month")?.extract()?;
    let day: u32 = dt.getattr("day")?.extract()?;
    let hour: u32 = dt.getattr("hour")?.extract()?;
    let minute: u32 = dt.getattr("minute")?.extract()?;
    let second: u32 = dt.getattr("second")?.extract()?;
    let microsecond: u32 = dt.getattr("microsecond")?.extract()?;

    let days = days_from_ymd(year, month, day) as i64;
    let ms = days * 24 * 60 * 60 * 1000
        + (hour as i64) * 60 * 60 * 1000
        + (minute as i64) * 60 * 1000
        + (second as i64) * 1000
        + (microsecond as i64) / 1000;
    Ok(ms)
}

/// Convert days since epoch back to (year, month, day)
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
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Convert (year, month, day) to days since Unix epoch
fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let m = month;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32) - 719468
}

/// Convert milliseconds since epoch to (year, month, day, hour, minute, second, microsecond)
fn datetime_from_ms(ms: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let ms_per_day: i64 = 24 * 60 * 60 * 1000;
    let days = ms.div_euclid(ms_per_day) as i32;
    let remaining_ms = ms.rem_euclid(ms_per_day);

    let (year, month, day) = ymd_from_days(days);
    let hour = (remaining_ms / (60 * 60 * 1000)) as u32;
    let minute = ((remaining_ms % (60 * 60 * 1000)) / (60 * 1000)) as u32;
    let second = ((remaining_ms % (60 * 1000)) / 1000) as u32;
    let microsecond = ((remaining_ms % 1000) * 1000) as u32;

    (year, month, day, hour, minute, second, microsecond)
}

// ============================================================================
// Core Type Conversions
// ============================================================================

/// Python-exposed ColumnType enum
#[pyclass(name = "ColumnType")]
#[derive(Clone, Copy)]
pub struct PyColumnType {
    inner: RustColumnType,
}

#[pymethods]
impl PyColumnType {
    #[classattr]
    const INT32: PyColumnType = PyColumnType {
        inner: RustColumnType::Int32,
    };

    #[classattr]
    const INT64: PyColumnType = PyColumnType {
        inner: RustColumnType::Int64,
    };

    #[classattr]
    const FLOAT32: PyColumnType = PyColumnType {
        inner: RustColumnType::Float32,
    };

    #[classattr]
    const FLOAT64: PyColumnType = PyColumnType {
        inner: RustColumnType::Float64,
    };

    #[classattr]
    const STRING: PyColumnType = PyColumnType {
        inner: RustColumnType::String,
    };

    #[classattr]
    const BOOL: PyColumnType = PyColumnType {
        inner: RustColumnType::Bool,
    };

    #[classattr]
    const DATE: PyColumnType = PyColumnType {
        inner: RustColumnType::Date,
    };

    #[classattr]
    const DATETIME: PyColumnType = PyColumnType {
        inner: RustColumnType::DateTime,
    };

    fn __repr__(&self) -> String {
        match self.inner {
            RustColumnType::Int32 => "ColumnType.INT32".to_string(),
            RustColumnType::Int64 => "ColumnType.INT64".to_string(),
            RustColumnType::Float32 => "ColumnType.FLOAT32".to_string(),
            RustColumnType::Float64 => "ColumnType.FLOAT64".to_string(),
            RustColumnType::String => "ColumnType.STRING".to_string(),
            RustColumnType::Bool => "ColumnType.BOOL".to_string(),
            RustColumnType::Date => "ColumnType.DATE".to_string(),
            RustColumnType::DateTime => "ColumnType.DATETIME".to_string(),
        }
    }
}

impl PyColumnType {
    fn to_rust(self) -> RustColumnType {
        self.inner
    }

    fn from_rust(col_type: RustColumnType) -> Self {
        PyColumnType { inner: col_type }
    }
}

/// Convert Python value to ColumnValue using known column type (faster than guessing).
fn py_to_column_value_typed(
    value: &Bound<'_, PyAny>,
    expected_type: RustColumnType,
    nullable: bool,
) -> PyResult<RustColumnValue> {
    if value.is_none() {
        if nullable {
            return Ok(RustColumnValue::Null);
        }
        return Err(PyValueError::new_err("NULL value for non-nullable column"));
    }

    match expected_type {
        RustColumnType::Int32 => value
            .extract::<i32>()
            .map(RustColumnValue::Int32)
            .map_err(|_| PyValueError::new_err("Expected INT32 value")),
        RustColumnType::Int64 => {
            if let Ok(v) = value.extract::<i64>() {
                Ok(RustColumnValue::Int64(v))
            } else if let Ok(v) = value.extract::<i32>() {
                Ok(RustColumnValue::Int64(v as i64))
            } else {
                Err(PyValueError::new_err("Expected INT64 value"))
            }
        }
        RustColumnType::Float32 => value
            .extract::<f32>()
            .map(RustColumnValue::Float32)
            .or_else(|_| {
                value
                    .extract::<f64>()
                    .map(|v| RustColumnValue::Float32(v as f32))
            })
            .map_err(|_| PyValueError::new_err("Expected FLOAT32 value")),
        RustColumnType::Float64 => value
            .extract::<f64>()
            .map(RustColumnValue::Float64)
            .map_err(|_| PyValueError::new_err("Expected FLOAT64 value")),
        RustColumnType::String => value
            .extract::<String>()
            .map(RustColumnValue::String)
            .map_err(|_| PyValueError::new_err("Expected STRING value")),
        RustColumnType::Bool => value
            .extract::<bool>()
            .map(RustColumnValue::Bool)
            .map_err(|_| PyValueError::new_err("Expected BOOL value")),
        RustColumnType::Date => {
            if let Ok(days) = value.extract::<i32>() {
                return Ok(RustColumnValue::Date(days));
            }
            Python::with_gil(|py| {
                let datetime_mod = py.import_bound("datetime")?;
                let date_type = datetime_mod.getattr("date")?;
                let datetime_type = datetime_mod.getattr("datetime")?;

                if value.is_instance(&datetime_type)? {
                    let date_obj = value.call_method0("date")?;
                    let days = date_to_days_since_epoch(&date_obj)?;
                    Ok(RustColumnValue::Date(days))
                } else if value.is_instance(&date_type)? {
                    let days = date_to_days_since_epoch(value)?;
                    Ok(RustColumnValue::Date(days))
                } else {
                    Err(PyValueError::new_err(
                        "Expected DATE value (datetime.date, datetime.datetime, or integer days since epoch)",
                    ))
                }
            })
        }
        RustColumnType::DateTime => {
            if let Ok(ms) = value.extract::<i64>() {
                return Ok(RustColumnValue::DateTime(ms));
            }
            Python::with_gil(|py| {
                let datetime_mod = py.import_bound("datetime")?;
                let datetime_type = datetime_mod.getattr("datetime")?;
                let date_type = datetime_mod.getattr("date")?;

                if value.is_instance(&datetime_type)? {
                    let ms = datetime_to_ms_since_epoch(value)?;
                    Ok(RustColumnValue::DateTime(ms))
                } else if value.is_instance(&date_type)? {
                    let days = date_to_days_since_epoch(value)?;
                    let ms = (days as i64) * 24 * 60 * 60 * 1000;
                    Ok(RustColumnValue::DateTime(ms))
                } else {
                    Err(PyValueError::new_err(
                        "Expected DATETIME value (datetime.datetime, datetime.date, or integer milliseconds since epoch)",
                    ))
                }
            })
        }
    }
}

/// Convert ColumnValue to Python object
fn column_value_to_py(py: Python, value: &RustColumnValue) -> PyResult<PyObject> {
    match value {
        RustColumnValue::Int32(v) => Ok(v.to_object(py)),
        RustColumnValue::Int64(v) => Ok(v.to_object(py)),
        RustColumnValue::Float32(v) => Ok(v.to_object(py)),
        RustColumnValue::Float64(v) => Ok(v.to_object(py)),
        RustColumnValue::String(v) => Ok(v.to_object(py)),
        RustColumnValue::Bool(v) => Ok(v.to_object(py)),
        RustColumnValue::Date(days) => {
            let (year, month, day) = ymd_from_days(*days);
            let datetime_mod = py.import_bound("datetime")?;
            let date_class = datetime_mod.getattr("date")?;
            let date_obj = date_class.call1((year, month, day))?;
            Ok(date_obj.to_object(py))
        }
        RustColumnValue::DateTime(ms) => {
            let (year, month, day, hour, minute, second, microsecond) = datetime_from_ms(*ms);
            let datetime_mod = py.import_bound("datetime")?;
            let datetime_class = datetime_mod.getattr("datetime")?;
            let dt_obj =
                datetime_class.call1((year, month, day, hour, minute, second, microsecond))?;
            Ok(dt_obj.to_object(py))
        }
        RustColumnValue::Null => Ok(py.None()),
    }
}

/// Extract a string or list of strings from a Python object.
fn extract_string_or_list(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = value.extract::<String>() {
        return Ok(vec![s]);
    }

    if let Ok(list) = value.downcast::<PyList>() {
        let mut result = Vec::with_capacity(list.len());
        for item in list.iter() {
            let s: String = item
                .extract()
                .map_err(|_| PyValueError::new_err("All items in key list must be strings"))?;
            result.push(s);
        }
        return Ok(result);
    }

    Err(PyValueError::new_err(
        "Key must be a string or list of strings",
    ))
}

/// Convert pandas dtype string to ColumnType
fn dtype_str_to_column_type(dtype_str: &str) -> RustColumnType {
    match dtype_str {
        "int32" => RustColumnType::Int32,
        "int64" | "Int64" => RustColumnType::Int64,
        "float32" => RustColumnType::Float32,
        "float64" => RustColumnType::Float64,
        "bool" | "boolean" => RustColumnType::Bool,
        s if s.starts_with("datetime64") => RustColumnType::DateTime,
        _ => RustColumnType::String,
    }
}

/// Convert pandas value to ColumnValue, handling NaN and special types
fn pandas_value_to_column_value(
    py: Python,
    value: &Bound<'_, PyAny>,
    expected_type: RustColumnType,
) -> PyResult<RustColumnValue> {
    if value.is_none() {
        return Ok(RustColumnValue::Null);
    }

    let is_na = || -> bool {
        if let Ok(pandas) = py.import_bound("pandas") {
            if let Ok(is_na_fn) = pandas.getattr("isna") {
                if let Ok(result) = is_na_fn.call1((value,)) {
                    if let Ok(is_null) = result.extract::<bool>() {
                        return is_null;
                    }
                }
            }
        }
        false
    };

    if is_na() {
        return Ok(RustColumnValue::Null);
    }

    match expected_type {
        RustColumnType::Bool => {
            if let Ok(v) = value.extract::<bool>() {
                return Ok(RustColumnValue::Bool(v));
            }
        }
        RustColumnType::Int32 => {
            if let Ok(v) = value.extract::<i32>() {
                return Ok(RustColumnValue::Int32(v));
            }
            if let Ok(v) = value.extract::<i64>() {
                if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
                    return Ok(RustColumnValue::Int32(v as i32));
                }
            }
        }
        RustColumnType::Int64 => {
            if let Ok(v) = value.extract::<i64>() {
                return Ok(RustColumnValue::Int64(v));
            }
        }
        RustColumnType::Float32 => {
            if let Ok(v) = value.extract::<f32>() {
                if v.is_nan() {
                    return Ok(RustColumnValue::Null);
                }
                return Ok(RustColumnValue::Float32(v));
            }
            if let Ok(v) = value.extract::<f64>() {
                if v.is_nan() {
                    return Ok(RustColumnValue::Null);
                }
                return Ok(RustColumnValue::Float32(v as f32));
            }
        }
        RustColumnType::Float64 => {
            if let Ok(v) = value.extract::<f64>() {
                if v.is_nan() {
                    return Ok(RustColumnValue::Null);
                }
                return Ok(RustColumnValue::Float64(v));
            }
        }
        RustColumnType::String => {
            if let Ok(v) = value.extract::<String>() {
                return Ok(RustColumnValue::String(v));
            }
            if let Ok(v) = value.str() {
                if let Ok(s) = v.extract::<String>() {
                    return Ok(RustColumnValue::String(s));
                }
            }
        }
        RustColumnType::Date => {
            if let Ok(days) = value.extract::<i32>() {
                return Ok(RustColumnValue::Date(days));
            }
            if let Ok(days) = date_to_days_since_epoch(value) {
                return Ok(RustColumnValue::Date(days));
            }
        }
        RustColumnType::DateTime => {
            if let Ok(ms) = value.extract::<i64>() {
                return Ok(RustColumnValue::DateTime(ms));
            }
            if let Ok(ms) = datetime_to_ms_since_epoch(value) {
                return Ok(RustColumnValue::DateTime(ms));
            }
            if let Ok(ts_method) = value.getattr("timestamp") {
                if let Ok(ts_result) = ts_method.call0() {
                    if let Ok(seconds) = ts_result.extract::<f64>() {
                        let ms = (seconds * 1000.0) as i64;
                        return Ok(RustColumnValue::DateTime(ms));
                    }
                }
            }
        }
    }

    Err(PyValueError::new_err(format!(
        "Cannot convert pandas value to {:?}",
        expected_type
    )))
}
