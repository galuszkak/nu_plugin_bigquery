use base64::Engine;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use nu_protocol::{Record, Span, Value};

use crate::client::{TableFieldSchema, TableRow, TableSchema};

/// Convert BigQuery query result rows into Nushell Values using schema type information.
pub fn rows_to_values(schema: &TableSchema, rows: &[TableRow], span: Span) -> Vec<Value> {
    let fields = match &schema.fields {
        Some(f) => f,
        None => return Vec::new(),
    };

    rows.iter()
        .map(|row| row_to_value(fields, row, span))
        .collect()
}

fn row_to_value(fields: &[TableFieldSchema], row: &TableRow, span: Span) -> Value {
    let cells = match row.f.as_ref() {
        Some(c) => c,
        None => {
            // Row with no cells — produce a record with all Nothing values
            let mut record = Record::new();
            for field in fields {
                let name = field.name.as_deref().unwrap_or("unknown").to_string();
                record.push(name, Value::nothing(span));
            }
            return Value::record(record, span);
        }
    };
    let mut record = Record::new();

    for (i, field) in fields.iter().enumerate() {
        let name = field.name.as_deref().unwrap_or("unknown").to_string();
        let bq_type = field.r#type.as_deref().unwrap_or("STRING");
        let mode = field.mode.as_deref().unwrap_or("NULLABLE");
        let sub_fields = field.fields.as_deref();

        let cell_value = cells.get(i).and_then(|c| c.v.as_ref());
        let nu_value = convert_cell(cell_value, bq_type, mode, sub_fields, span);
        record.push(name, nu_value);
    }

    Value::record(record, span)
}

/// Convert a single BigQuery cell value to a Nushell Value.
fn convert_cell(
    value: Option<&serde_json::Value>,
    bq_type: &str,
    mode: &str,
    sub_fields: Option<&[TableFieldSchema]>,
    span: Span,
) -> Value {
    // Handle null
    let json_val = match value {
        None | Some(serde_json::Value::Null) => return Value::nothing(span),
        Some(v) => v,
    };

    // Handle REPEATED (array) mode
    if mode == "REPEATED" {
        if let serde_json::Value::Array(arr) = json_val {
            let items: Vec<Value> = arr
                .iter()
                .map(|item| {
                    // REPEATED items are wrapped in {"v": value}
                    let inner = if let serde_json::Value::Object(obj) = item {
                        obj.get("v")
                    } else {
                        Some(item)
                    };
                    convert_cell(inner, bq_type, "NULLABLE", sub_fields, span)
                })
                .collect();
            return Value::list(items, span);
        }
        return Value::nothing(span);
    }

    match bq_type.to_uppercase().as_str() {
        "STRING" | "GEOGRAPHY" | "JSON" => convert_string(json_val, span),
        "BYTES" => convert_bytes(json_val, span),
        "INTEGER" | "INT64" => convert_int(json_val, span),
        "FLOAT" | "FLOAT64" => convert_float(json_val, span),
        "NUMERIC" | "BIGNUMERIC" => convert_numeric(json_val, span),
        "BOOLEAN" | "BOOL" => convert_bool(json_val, span),
        "TIMESTAMP" => convert_timestamp(json_val, span),
        "DATE" => convert_date(json_val, span),
        "TIME" => convert_string(json_val, span), // No Nushell Time type
        "DATETIME" => convert_datetime(json_val, span),
        "RECORD" | "STRUCT" => convert_record(json_val, sub_fields, span),
        _ => convert_string(json_val, span), // Fallback
    }
}

fn convert_string(val: &serde_json::Value, span: Span) -> Value {
    match val {
        serde_json::Value::String(s) => Value::string(s.clone(), span),
        other => Value::string(other.to_string(), span),
    }
}

fn convert_bytes(val: &serde_json::Value, span: Span) -> Value {
    if let serde_json::Value::String(s) = val {
        match base64::engine::general_purpose::STANDARD.decode(s) {
            Ok(bytes) => Value::binary(bytes, span),
            Err(_) => Value::string(s.clone(), span),
        }
    } else {
        Value::nothing(span)
    }
}

fn convert_int(val: &serde_json::Value, span: Span) -> Value {
    match val {
        serde_json::Value::String(s) => match s.parse::<i64>() {
            Ok(n) => Value::int(n, span),
            Err(_) => Value::string(s.clone(), span),
        },
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::int(i, span)
            } else if let Some(f) = n.as_f64() {
                Value::int(f as i64, span)
            } else {
                Value::string(n.to_string(), span)
            }
        }
        _ => Value::nothing(span),
    }
}

fn convert_float(val: &serde_json::Value, span: Span) -> Value {
    match val {
        serde_json::Value::String(s) => match s.parse::<f64>() {
            Ok(f) => Value::float(f, span),
            Err(_) => Value::string(s.clone(), span),
        },
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Value::float(f, span)
            } else {
                Value::string(n.to_string(), span)
            }
        }
        _ => Value::nothing(span),
    }
}

fn convert_numeric(val: &serde_json::Value, span: Span) -> Value {
    // Preserve precision by keeping as string
    match val {
        serde_json::Value::String(s) => Value::string(s.clone(), span),
        other => Value::string(other.to_string(), span),
    }
}

fn convert_bool(val: &serde_json::Value, span: Span) -> Value {
    match val {
        serde_json::Value::Bool(b) => Value::bool(*b, span),
        serde_json::Value::String(s) => match s.to_lowercase().as_str() {
            "true" => Value::bool(true, span),
            "false" => Value::bool(false, span),
            _ => Value::string(s.clone(), span),
        },
        _ => Value::nothing(span),
    }
}

fn convert_timestamp(val: &serde_json::Value, span: Span) -> Value {
    // BigQuery REST API returns timestamps as Unix epoch seconds (float string)
    // e.g., "1.634567890123E9"
    if let serde_json::Value::String(s) = val {
        if let Ok(epoch_secs) = s.parse::<f64>() {
            let secs = epoch_secs as i64;
            let nanos = ((epoch_secs - secs as f64) * 1_000_000_000.0) as u32;
            if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
                let fixed: DateTime<FixedOffset> =
                    dt.with_timezone(&FixedOffset::east_opt(0).unwrap());
                return Value::date(fixed, span);
            }
        }
        // Try ISO 8601 format as fallback
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Value::date(dt, span);
        }
        Value::string(s.clone(), span)
    } else {
        Value::nothing(span)
    }
}

fn convert_date(val: &serde_json::Value, span: Span) -> Value {
    // BigQuery DATE format: "YYYY-MM-DD"
    if let serde_json::Value::String(s) = val {
        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            let dt = date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .with_timezone(&FixedOffset::east_opt(0).unwrap());
            return Value::date(dt, span);
        }
        Value::string(s.clone(), span)
    } else {
        Value::nothing(span)
    }
}

fn convert_datetime(val: &serde_json::Value, span: Span) -> Value {
    // BigQuery DATETIME format: "YYYY-MM-DD HH:MM:SS.FFFFFF" (no timezone)
    if let serde_json::Value::String(s) = val {
        // Try with fractional seconds
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            let dt = ndt
                .and_utc()
                .with_timezone(&FixedOffset::east_opt(0).unwrap());
            return Value::date(dt, span);
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
            let dt = ndt
                .and_utc()
                .with_timezone(&FixedOffset::east_opt(0).unwrap());
            return Value::date(dt, span);
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            let dt = ndt
                .and_utc()
                .with_timezone(&FixedOffset::east_opt(0).unwrap());
            return Value::date(dt, span);
        }
        Value::string(s.clone(), span)
    } else {
        Value::nothing(span)
    }
}

fn convert_record(
    val: &serde_json::Value,
    sub_fields: Option<&[TableFieldSchema]>,
    span: Span,
) -> Value {
    // RECORD/STRUCT values come as {"f": [{"v": val1}, {"v": val2}, ...]}
    let obj = match val {
        serde_json::Value::Object(o) => o,
        _ => return Value::nothing(span),
    };

    let cells = match obj.get("f").and_then(|f| f.as_array()) {
        Some(arr) => arr,
        None => return Value::nothing(span),
    };

    let fields = match sub_fields {
        Some(f) => f,
        None => return Value::nothing(span),
    };

    let mut record = Record::new();
    for (i, field) in fields.iter().enumerate() {
        let name = field.name.as_deref().unwrap_or("unknown").to_string();
        let bq_type = field.r#type.as_deref().unwrap_or("STRING");
        let mode = field.mode.as_deref().unwrap_or("NULLABLE");
        let nested_fields = field.fields.as_deref();

        let cell_value = cells
            .get(i)
            .and_then(|c| c.as_object())
            .and_then(|o| o.get("v"));
        let nu_value = convert_cell(cell_value, bq_type, mode, nested_fields, span);
        record.push(name, nu_value);
    }

    Value::record(record, span)
}

/// Format bytes as a human-readable string.
pub fn format_bytes(bytes_str: &str) -> String {
    if let Ok(bytes) = bytes_str.parse::<u64>() {
        if bytes < 1024 {
            format!("{bytes} B")
        } else if bytes < 1024 * 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else if bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes < 1024 * 1024 * 1024 * 1024 {
            format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else {
            format!(
                "{:.2} TB",
                bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0)
            )
        }
    } else {
        bytes_str.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::TableCell;

    #[test]
    fn test_convert_int_from_string() {
        let val = serde_json::Value::String("42".to_string());
        let result = convert_int(&val, Span::test_data());
        assert_eq!(result, Value::test_int(42));
    }

    #[test]
    fn test_convert_float_from_string() {
        let val = serde_json::Value::String("3.5".to_string());
        let result = convert_float(&val, Span::test_data());
        assert_eq!(result, Value::test_float(3.5));
    }

    #[test]
    fn test_convert_bool_from_string() {
        let val = serde_json::Value::String("true".to_string());
        let result = convert_bool(&val, Span::test_data());
        assert_eq!(result, Value::test_bool(true));
    }

    #[test]
    fn test_convert_null() {
        let result = convert_cell(None, "STRING", "NULLABLE", None, Span::test_data());
        assert_eq!(result, Value::nothing(Span::test_data()));
    }

    #[test]
    fn test_convert_date() {
        let val = serde_json::Value::String("2024-01-15".to_string());
        let result = convert_date(&val, Span::test_data());
        match result {
            Value::Date { .. } => {} // OK
            other => panic!("Expected Date, got {:?}", other),
        }
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes("500"), "500 B");
        assert_eq!(format_bytes("1536"), "1.5 KB");
        assert_eq!(format_bytes("1073741824"), "1.00 GB");
    }

    #[test]
    fn test_convert_timestamp_epoch() {
        // Unix epoch as float string (BigQuery REST API format)
        let val = serde_json::Value::String("1.7e+09".to_string());
        let result = convert_timestamp(&val, Span::test_data());
        match result {
            Value::Date { .. } => {}
            other => panic!("Expected Date for timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_datetime_string() {
        let val = serde_json::Value::String("2024-01-15 14:30:00".to_string());
        let result = convert_datetime(&val, Span::test_data());
        match result {
            Value::Date { .. } => {}
            other => panic!("Expected Date for datetime, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_bytes_base64() {
        let val = serde_json::Value::String("SGVsbG8=".to_string()); // "Hello"
        let result = convert_bytes(&val, Span::test_data());
        assert_eq!(result, Value::binary(b"Hello".to_vec(), Span::test_data()));
    }

    #[test]
    fn test_convert_numeric_preserves_precision() {
        let val = serde_json::Value::String("123456789.123456789".to_string());
        let result = convert_numeric(&val, Span::test_data());
        assert_eq!(result, Value::test_string("123456789.123456789"));
    }

    #[test]
    fn test_convert_repeated_field() {
        let arr = serde_json::json!([
            {"v": "10"},
            {"v": "20"},
            {"v": "30"}
        ]);
        let result = convert_cell(Some(&arr), "INTEGER", "REPEATED", None, Span::test_data());
        match result {
            Value::List { vals, .. } => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Value::test_int(10));
                assert_eq!(vals[1], Value::test_int(20));
                assert_eq!(vals[2], Value::test_int(30));
            }
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_record_nested() {
        let sub_fields = vec![
            TableFieldSchema {
                name: Some("street".to_string()),
                r#type: Some("STRING".to_string()),
                mode: Some("NULLABLE".to_string()),
                description: None,
                fields: None,
            },
            TableFieldSchema {
                name: Some("zip".to_string()),
                r#type: Some("INTEGER".to_string()),
                mode: Some("NULLABLE".to_string()),
                description: None,
                fields: None,
            },
        ];

        let val = serde_json::json!({
            "f": [
                {"v": "123 Main St"},
                {"v": "90210"}
            ]
        });

        let result = convert_record(&val, Some(&sub_fields), Span::test_data());
        match result {
            Value::Record { val, .. } => {
                assert_eq!(val.len(), 2);
                assert_eq!(
                    val.get("street").unwrap(),
                    &Value::test_string("123 Main St")
                );
                assert_eq!(val.get("zip").unwrap(), &Value::test_int(90210));
            }
            other => panic!("Expected Record, got {:?}", other),
        }
    }

    #[test]
    fn test_rows_to_values_full_pipeline() {
        let schema = TableSchema {
            fields: Some(vec![
                TableFieldSchema {
                    name: Some("id".to_string()),
                    r#type: Some("INTEGER".to_string()),
                    mode: Some("REQUIRED".to_string()),
                    description: None,
                    fields: None,
                },
                TableFieldSchema {
                    name: Some("name".to_string()),
                    r#type: Some("STRING".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: None,
                },
                TableFieldSchema {
                    name: Some("active".to_string()),
                    r#type: Some("BOOLEAN".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: None,
                },
            ]),
        };

        let rows = vec![
            TableRow {
                f: Some(vec![
                    TableCell {
                        v: Some(serde_json::json!("1")),
                    },
                    TableCell {
                        v: Some(serde_json::json!("Alice")),
                    },
                    TableCell {
                        v: Some(serde_json::json!("true")),
                    },
                ]),
            },
            TableRow {
                f: Some(vec![
                    TableCell {
                        v: Some(serde_json::json!("2")),
                    },
                    TableCell {
                        v: Some(serde_json::json!("Bob")),
                    },
                    TableCell {
                        v: Some(serde_json::Value::Null),
                    },
                ]),
            },
        ];

        let values = rows_to_values(&schema, &rows, Span::test_data());
        assert_eq!(values.len(), 2);

        // Check first row
        match &values[0] {
            Value::Record { val, .. } => {
                assert_eq!(val.get("id").unwrap(), &Value::test_int(1));
                assert_eq!(val.get("name").unwrap(), &Value::test_string("Alice"));
                assert_eq!(val.get("active").unwrap(), &Value::test_bool(true));
            }
            other => panic!("Expected Record, got {:?}", other),
        }

        // Check second row — null becomes Nothing
        match &values[1] {
            Value::Record { val, .. } => {
                assert_eq!(val.get("id").unwrap(), &Value::test_int(2));
                assert_eq!(val.get("name").unwrap(), &Value::test_string("Bob"));
                assert_eq!(
                    val.get("active").unwrap(),
                    &Value::nothing(Span::test_data())
                );
            }
            other => panic!("Expected Record, got {:?}", other),
        }
    }

    #[test]
    fn test_row_with_missing_f_produces_nothing_record() {
        let schema = TableSchema {
            fields: Some(vec![
                TableFieldSchema {
                    name: Some("id".to_string()),
                    r#type: Some("INTEGER".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: None,
                },
                TableFieldSchema {
                    name: Some("name".to_string()),
                    r#type: Some("STRING".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: None,
                },
            ]),
        };

        // Row with f: None should NOT be dropped
        let rows = vec![
            TableRow {
                f: Some(vec![
                    TableCell {
                        v: Some(serde_json::json!("1")),
                    },
                    TableCell {
                        v: Some(serde_json::json!("Alice")),
                    },
                ]),
            },
            TableRow { f: None },
        ];

        let values = rows_to_values(&schema, &rows, Span::test_data());
        assert_eq!(values.len(), 2, "Row with missing f should not be dropped");

        // Second row should be all Nothing
        match &values[1] {
            Value::Record { val, .. } => {
                assert_eq!(val.get("id").unwrap(), &Value::nothing(Span::test_data()));
                assert_eq!(val.get("name").unwrap(), &Value::nothing(Span::test_data()));
            }
            other => panic!("Expected Record, got {:?}", other),
        }
    }
}
