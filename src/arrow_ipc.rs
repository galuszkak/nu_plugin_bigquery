use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use nu_protocol::LabeledError;
use tempfile::NamedTempFile;

use crate::client::{TableFieldSchema, TableRow, TableSchema};

/// Convert BigQuery rows to an Arrow IPC file. Returns the temp file path.
pub fn write_arrow_ipc(schema: &TableSchema, rows: &[TableRow]) -> Result<String, LabeledError> {
    let bq_fields = schema.fields.as_deref().unwrap_or(&[]);
    let arrow_schema = bq_schema_to_arrow(bq_fields)?;

    let batch = build_record_batch(&arrow_schema, bq_fields, rows)?;

    let tmp = NamedTempFile::with_suffix(".arrow").map_err(|e| {
        LabeledError::new("Failed to create temp file")
            .with_help(format!("Could not create Arrow IPC temp file: {e}"))
    })?;

    let path = tmp.path().to_string_lossy().to_string();

    // Separate the file handle from the temp path; keep temp_path alive so its
    // Drop impl auto-deletes the file if any write step below fails.
    let (file, temp_path) = tmp.into_parts();

    let mut writer = FileWriter::try_new(file, &arrow_schema).map_err(|e| {
        LabeledError::new("Failed to write Arrow IPC")
            .with_help(format!("Arrow IPC writer error: {e}"))
    })?;

    writer.write(&batch).map_err(|e| {
        LabeledError::new("Failed to write Arrow IPC batch")
            .with_help(format!("Arrow IPC write error: {e}"))
    })?;

    writer.finish().map_err(|e| {
        LabeledError::new("Failed to finalize Arrow IPC file")
            .with_help(format!("Arrow IPC finish error: {e}"))
    })?;

    // Only persist the file after all writes succeed; on any error above,
    // temp_path's Drop handler automatically cleans up the file on disk.
    temp_path.keep().map_err(|e| {
        LabeledError::new("Failed to persist temp file")
            .with_help(format!("Could not keep Arrow IPC temp file: {e}"))
    })?;

    Ok(path)
}

fn bq_type_to_arrow(bq_type: &str, bq_fields: Option<&[TableFieldSchema]>) -> DataType {
    match bq_type.to_uppercase().as_str() {
        "STRING" | "GEOGRAPHY" | "JSON" | "TIME" => DataType::Utf8,
        "BYTES" => DataType::Binary,
        "INTEGER" | "INT64" => DataType::Int64,
        "FLOAT" | "FLOAT64" => DataType::Float64,
        "NUMERIC" | "BIGNUMERIC" => DataType::Utf8, // Preserve precision
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "DATE" => DataType::Date32,
        "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "RECORD" | "STRUCT" => {
            let sub_fields = bq_fields.unwrap_or(&[]);
            let arrow_fields: Vec<Field> = sub_fields
                .iter()
                .map(|f| {
                    let name = f.name.as_deref().unwrap_or("unknown");
                    let dt = bq_type_to_arrow(
                        f.r#type.as_deref().unwrap_or("STRING"),
                        f.fields.as_deref(),
                    );
                    let nullable = f.mode.as_deref() != Some("REQUIRED");
                    Field::new(name, dt, nullable)
                })
                .collect();
            DataType::Struct(arrow_fields.into())
        }
        _ => DataType::Utf8,
    }
}

fn bq_schema_to_arrow(fields: &[TableFieldSchema]) -> Result<Schema, LabeledError> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let name = f.name.as_deref().unwrap_or("unknown");
            let bq_type = f.r#type.as_deref().unwrap_or("STRING");
            let mode = f.mode.as_deref().unwrap_or("NULLABLE");
            let nullable = mode != "REQUIRED";

            let data_type = if mode == "REPEATED" {
                // REPEATED RECORD/STRUCT: serialize as JSON strings since building
                // nested Arrow struct arrays from BQ wire format is not supported
                let inner_type = match bq_type.to_uppercase().as_str() {
                    "RECORD" | "STRUCT" => DataType::Utf8,
                    _ => bq_type_to_arrow(bq_type, f.fields.as_deref()),
                };
                DataType::List(Arc::new(Field::new("item", inner_type, true)))
            } else {
                bq_type_to_arrow(bq_type, f.fields.as_deref())
            };

            Field::new(name, data_type, nullable)
        })
        .collect();

    Ok(Schema::new(arrow_fields))
}

fn build_record_batch(
    arrow_schema: &Schema,
    bq_fields: &[TableFieldSchema],
    rows: &[TableRow],
) -> Result<RecordBatch, LabeledError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(bq_fields.len());

    for (col_idx, field) in bq_fields.iter().enumerate() {
        let bq_type = field.r#type.as_deref().unwrap_or("STRING");
        let mode = field.mode.as_deref().unwrap_or("NULLABLE");

        // Extract cell values for this column across all rows
        let cell_values: Vec<Option<&serde_json::Value>> = rows
            .iter()
            .map(|row| {
                row.f
                    .as_ref()
                    .and_then(|cells| cells.get(col_idx))
                    .and_then(|cell| cell.v.as_ref())
                    .and_then(|v| if v.is_null() { None } else { Some(v) })
            })
            .collect();

        let array = if mode == "REPEATED" {
            build_list_array(&cell_values, bq_type, field.fields.as_deref())?
        } else {
            build_column_array(&cell_values, bq_type, field.fields.as_deref())?
        };

        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns).map_err(|e| {
        LabeledError::new("Failed to build Arrow RecordBatch")
            .with_help(format!("Arrow error: {e}"))
    })
}

fn build_column_array(
    values: &[Option<&serde_json::Value>],
    bq_type: &str,
    sub_fields: Option<&[TableFieldSchema]>,
) -> Result<ArrayRef, LabeledError> {
    match bq_type.to_uppercase().as_str() {
        "INTEGER" | "INT64" => {
            let arr: Int64Array = values.iter().map(|v| v.and_then(json_to_i64)).collect();
            Ok(Arc::new(arr))
        }
        "FLOAT" | "FLOAT64" => {
            let arr: Float64Array = values.iter().map(|v| v.and_then(json_to_f64)).collect();
            Ok(Arc::new(arr))
        }
        "BOOLEAN" | "BOOL" => {
            let arr: BooleanArray = values.iter().map(|v| v.and_then(json_to_bool)).collect();
            Ok(Arc::new(arr))
        }
        "TIMESTAMP" => {
            // Timestamps as microseconds since epoch
            let arr: TimestampMicrosecondArray = values
                .iter()
                .map(|v| v.and_then(json_to_timestamp_us))
                .collect::<TimestampMicrosecondArray>()
                .with_timezone("UTC");
            Ok(Arc::new(arr))
        }
        "DATE" => {
            // Date32 = days since epoch
            let arr: Date32Array = values.iter().map(|v| v.and_then(json_to_date32)).collect();
            Ok(Arc::new(arr))
        }
        "DATETIME" => {
            let arr: TimestampMicrosecondArray = values
                .iter()
                .map(|v| v.and_then(json_to_datetime_us))
                .collect();
            Ok(Arc::new(arr))
        }
        "BYTES" => {
            let arr: BinaryArray = values
                .iter()
                .map(|v| {
                    v.and_then(|v| v.as_str())
                        .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
                })
                .collect::<Vec<_>>()
                .iter()
                .map(|v| v.as_deref())
                .collect::<BinaryArray>();
            Ok(Arc::new(arr))
        }
        "RECORD" | "STRUCT" => {
            let child_fields = sub_fields.unwrap_or(&[]);

            // Build each child column by extracting sub-field values from the
            // BQ wire format: {"f": [{"v": val0}, {"v": val1}, ...]}
            let mut child_arrays: Vec<(Arc<Field>, ArrayRef)> =
                Vec::with_capacity(child_fields.len());

            for (child_idx, child_field) in child_fields.iter().enumerate() {
                let child_name = child_field.name.as_deref().unwrap_or("unknown");
                let child_bq_type = child_field.r#type.as_deref().unwrap_or("STRING");
                let child_mode = child_field.mode.as_deref().unwrap_or("NULLABLE");
                let child_nullable = child_mode != "REQUIRED";

                // Extract this child's values across all rows
                let child_values: Vec<Option<&serde_json::Value>> = values
                    .iter()
                    .map(|row_val| {
                        row_val
                            .and_then(|v| v.get("f"))
                            .and_then(|f| f.as_array())
                            .and_then(|arr| arr.get(child_idx))
                            .and_then(|cell| cell.get("v"))
                            .and_then(|v| if v.is_null() { None } else { Some(v) })
                    })
                    .collect();

                let child_arr = build_column_array(
                    &child_values,
                    child_bq_type,
                    child_field.fields.as_deref(),
                )?;

                let child_dt = bq_type_to_arrow(child_bq_type, child_field.fields.as_deref());
                let field = Arc::new(Field::new(child_name, child_dt, child_nullable));
                child_arrays.push((field, child_arr));
            }

            // Build null bitmap: a row is null if its value was None
            let null_buffer: arrow::buffer::NullBuffer =
                values.iter().map(|v| v.is_some()).collect();

            let (fields, arrays): (Vec<_>, Vec<_>) = child_arrays.into_iter().unzip();

            let struct_array = StructArray::try_new(fields.into(), arrays, Some(null_buffer))
                .map_err(|e| {
                    LabeledError::new("Failed to build StructArray")
                        .with_help(format!("Arrow error: {e}"))
                })?;

            Ok(Arc::new(struct_array) as ArrayRef)
        }
        _ => {
            // STRING, NUMERIC, BIGNUMERIC, GEOGRAPHY, JSON, TIME, unknown
            let arr: StringArray = values.iter().map(|v| v.map(json_to_string)).collect();
            Ok(Arc::new(arr))
        }
    }
}

fn unwrap_repeated_items(val: &serde_json::Value) -> Vec<Option<&serde_json::Value>> {
    match val {
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(|item| {
                // BQ wraps repeated items as {"v": <value>}
                let inner = item.as_object().and_then(|o| o.get("v")).unwrap_or(item);
                if inner.is_null() { None } else { Some(inner) }
            })
            .collect(),
        _ => vec![],
    }
}

fn build_list_array(
    values: &[Option<&serde_json::Value>],
    item_type: &str,
    _sub_fields: Option<&[TableFieldSchema]>,
) -> Result<ArrayRef, LabeledError> {
    match item_type.to_uppercase().as_str() {
        "INTEGER" | "INT64" => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for val in values {
                match val {
                    Some(v) => {
                        for item in unwrap_repeated_items(v) {
                            match item.and_then(json_to_i64) {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "FLOAT" | "FLOAT64" => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for val in values {
                match val {
                    Some(v) => {
                        for item in unwrap_repeated_items(v) {
                            match item.and_then(json_to_f64) {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "BOOLEAN" | "BOOL" => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for val in values {
                match val {
                    Some(v) => {
                        for item in unwrap_repeated_items(v) {
                            match item.and_then(json_to_bool) {
                                Some(b) => builder.values().append_value(b),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        "RECORD" | "STRUCT" => {
            // Nested structs in repeated fields — fall back to JSON string representation
            let mut builder = ListBuilder::new(StringBuilder::new());
            for val in values {
                match val {
                    Some(v) => {
                        for item in unwrap_repeated_items(v) {
                            builder.values().append_value(json_to_string(
                                item.unwrap_or(&serde_json::Value::Null),
                            ));
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // STRING, NUMERIC, BIGNUMERIC, GEOGRAPHY, JSON, TIME, BYTES, DATE, DATETIME, TIMESTAMP, unknown
            let mut builder = ListBuilder::new(StringBuilder::new());
            for val in values {
                match val {
                    Some(v) => {
                        for item in unwrap_repeated_items(v) {
                            builder.values().append_value(json_to_string(
                                item.unwrap_or(&serde_json::Value::Null),
                            ));
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

// --- JSON value extraction helpers ---

fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn json_to_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn json_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn json_to_bool(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) => match s.to_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn json_to_timestamp_us(v: &serde_json::Value) -> Option<i64> {
    // BQ REST API returns timestamps as float epoch seconds: "1.634567890123E9"
    if let serde_json::Value::String(s) = v
        && let Ok(secs) = s.parse::<f64>()
    {
        return Some((secs * 1_000_000.0) as i64);
    }
    None
}

fn json_to_date32(v: &serde_json::Value) -> Option<i32> {
    // "YYYY-MM-DD" → days since 1970-01-01
    if let serde_json::Value::String(s) = v
        && let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
    {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
        return Some((date - epoch).num_days() as i32);
    }
    None
}

fn json_to_datetime_us(v: &serde_json::Value) -> Option<i64> {
    // "YYYY-MM-DD HH:MM:SS.FFFFFF" → microseconds since epoch (no timezone)
    if let serde_json::Value::String(s) = v {
        for fmt in &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
        ] {
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
                return Some(ndt.and_utc().timestamp_micros());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::TableCell;

    fn make_schema(fields: Vec<(&str, &str)>) -> TableSchema {
        TableSchema {
            fields: Some(
                fields
                    .into_iter()
                    .map(|(name, typ)| TableFieldSchema {
                        name: Some(name.to_string()),
                        r#type: Some(typ.to_string()),
                        mode: Some("NULLABLE".to_string()),
                        description: None,
                        fields: None,
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn test_write_arrow_ipc_basic() {
        let schema = make_schema(vec![
            ("id", "INTEGER"),
            ("name", "STRING"),
            ("score", "FLOAT"),
            ("active", "BOOLEAN"),
        ]);

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
                        v: Some(serde_json::json!("98.5")),
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
                    TableCell {
                        v: Some(serde_json::json!("false")),
                    },
                ]),
            },
        ];

        let path = write_arrow_ipc(&schema, &rows).unwrap();
        assert!(path.ends_with(".arrow"));
        assert!(std::path::Path::new(&path).exists());

        // Read back and verify
        let file = std::fs::File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let arrow_schema = reader.schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Int64);
        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(*arrow_schema.field(1).data_type(), DataType::Utf8);

        // Clean up
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_arrow_ipc_with_dates() {
        let schema = make_schema(vec![("created", "DATE"), ("updated", "TIMESTAMP")]);

        let rows = vec![TableRow {
            f: Some(vec![
                TableCell {
                    v: Some(serde_json::json!("2024-01-15")),
                },
                TableCell {
                    v: Some(serde_json::json!("1.7e+09")),
                },
            ]),
        }];

        let path = write_arrow_ipc(&schema, &rows).unwrap();
        assert!(std::path::Path::new(&path).exists());

        let file = std::fs::File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        assert_eq!(reader.schema().fields().len(), 2);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_bq_schema_to_arrow() {
        let fields = vec![
            TableFieldSchema {
                name: Some("id".to_string()),
                r#type: Some("INTEGER".to_string()),
                mode: Some("REQUIRED".to_string()),
                description: None,
                fields: None,
            },
            TableFieldSchema {
                name: Some("tags".to_string()),
                r#type: Some("STRING".to_string()),
                mode: Some("REPEATED".to_string()),
                description: None,
                fields: None,
            },
        ];

        let schema = bq_schema_to_arrow(&fields).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert!(!schema.field(0).is_nullable()); // REQUIRED
        assert!(matches!(schema.field(1).data_type(), DataType::List(_)));
    }

    #[test]
    fn test_write_arrow_ipc_repeated_int() {
        // REPEATED INTEGER should produce List<Int64>, not List<String>
        let schema = TableSchema {
            fields: Some(vec![
                TableFieldSchema {
                    name: Some("name".to_string()),
                    r#type: Some("STRING".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: None,
                },
                TableFieldSchema {
                    name: Some("scores".to_string()),
                    r#type: Some("INTEGER".to_string()),
                    mode: Some("REPEATED".to_string()),
                    description: None,
                    fields: None,
                },
            ]),
        };

        let rows = vec![TableRow {
            f: Some(vec![
                TableCell {
                    v: Some(serde_json::json!("Alice")),
                },
                TableCell {
                    v: Some(serde_json::json!([
                        {"v": "10"},
                        {"v": "20"},
                        {"v": "30"}
                    ])),
                },
            ]),
        }];

        let path = write_arrow_ipc(&schema, &rows).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let arrow_schema = reader.schema();

        // Verify the list inner type is Int64, not Utf8
        match arrow_schema.field(1).data_type() {
            DataType::List(inner) => {
                assert_eq!(*inner.data_type(), DataType::Int64);
            }
            other => panic!("Expected List, got {other:?}"),
        }

        // Read back the batch and verify values
        let mut reader =
            arrow::ipc::reader::FileReader::try_new(std::fs::File::open(&path).unwrap(), None)
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let list_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let inner = list_col.value(0);
        let int_arr = inner.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 10);
        assert_eq!(int_arr.value(1), 20);
        assert_eq!(int_arr.value(2), 30);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_arrow_ipc_repeated_bool() {
        let schema = TableSchema {
            fields: Some(vec![TableFieldSchema {
                name: Some("flags".to_string()),
                r#type: Some("BOOLEAN".to_string()),
                mode: Some("REPEATED".to_string()),
                description: None,
                fields: None,
            }]),
        };

        let rows = vec![TableRow {
            f: Some(vec![TableCell {
                v: Some(serde_json::json!([
                    {"v": "true"},
                    {"v": "false"}
                ])),
            }]),
        }];

        let path = write_arrow_ipc(&schema, &rows).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();

        match reader.schema().field(0).data_type() {
            DataType::List(inner) => {
                assert_eq!(*inner.data_type(), DataType::Boolean);
            }
            other => panic!("Expected List, got {other:?}"),
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_arrow_ipc_record_struct() {
        // Non-repeated RECORD column should produce a proper StructArray
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
                    name: Some("address".to_string()),
                    r#type: Some("RECORD".to_string()),
                    mode: Some("NULLABLE".to_string()),
                    description: None,
                    fields: Some(vec![
                        TableFieldSchema {
                            name: Some("street".to_string()),
                            r#type: Some("STRING".to_string()),
                            mode: Some("NULLABLE".to_string()),
                            description: None,
                            fields: None,
                        },
                        TableFieldSchema {
                            name: Some("city".to_string()),
                            r#type: Some("STRING".to_string()),
                            mode: Some("NULLABLE".to_string()),
                            description: None,
                            fields: None,
                        },
                    ]),
                },
            ]),
        };

        // BQ wire format for RECORD: {"f": [{"v": "street_val"}, {"v": "city_val"}]}
        let rows = vec![
            TableRow {
                f: Some(vec![
                    TableCell {
                        v: Some(serde_json::json!("1")),
                    },
                    TableCell {
                        v: Some(serde_json::json!({
                            "f": [
                                {"v": "123 Main St"},
                                {"v": "Springfield"}
                            ]
                        })),
                    },
                ]),
            },
            TableRow {
                f: Some(vec![
                    TableCell {
                        v: Some(serde_json::json!("2")),
                    },
                    TableCell {
                        v: Some(serde_json::Value::Null),
                    },
                ]),
            },
        ];

        let path = write_arrow_ipc(&schema, &rows).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let arrow_schema = reader.schema();

        // Verify the schema has a Struct type for the address column
        match arrow_schema.field(1).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "street");
                assert_eq!(fields[1].name(), "city");
            }
            other => panic!("Expected Struct, got {other:?}"),
        }

        // Read back and verify the actual values
        let mut reader =
            arrow::ipc::reader::FileReader::try_new(std::fs::File::open(&path).unwrap(), None)
                .unwrap();
        let batch = reader.next().unwrap().unwrap();

        // Row 0: address is present
        let struct_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(struct_col.is_valid(0)); // row 0 has data
        assert!(struct_col.is_null(1)); // row 1 is null

        let street_col = struct_col
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(street_col.value(0), "123 Main St");

        let city_col = struct_col
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(city_col.value(0), "Springfield");

        std::fs::remove_file(&path).ok();
    }
}
