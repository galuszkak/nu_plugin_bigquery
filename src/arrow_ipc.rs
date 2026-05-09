use arrow::array::*;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

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

pub(crate) struct StorageArrowDecoder {
    decoder: arrow::ipc::reader::StreamDecoder,
    schema: SchemaRef,
}

impl StorageArrowDecoder {
    pub(crate) fn new(schema_bytes: Vec<u8>) -> Result<Self, nu_protocol::LabeledError> {
        let reader = arrow::ipc::reader::StreamReader::try_new(
            std::io::Cursor::new(schema_bytes.clone()),
            None,
        )
        .map_err(storage_arrow_error)?;
        let schema = reader.schema();

        let mut decoder = arrow::ipc::reader::StreamDecoder::new();
        let mut buffer = arrow::buffer::Buffer::from(schema_bytes);
        let _ = decoder.decode(&mut buffer).map_err(storage_arrow_error)?;

        Ok(Self { decoder, schema })
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub(crate) fn decode_batch(
        &mut self,
        batch_bytes: Vec<u8>,
    ) -> Result<Vec<RecordBatch>, nu_protocol::LabeledError> {
        let mut buffer = arrow::buffer::Buffer::from(batch_bytes);
        let mut batches = Vec::new();

        while !buffer.is_empty() {
            if let Some(batch) = self
                .decoder
                .decode(&mut buffer)
                .map_err(storage_arrow_error)?
            {
                batches.push(batch);
            }
        }

        Ok(batches)
    }
}

fn storage_arrow_error(e: arrow::error::ArrowError) -> nu_protocol::LabeledError {
    nu_protocol::LabeledError::new("Failed to decode Arrow IPC from BigQuery Storage")
        .with_help(e.to_string())
}

pub(crate) fn create_arrow_ipc_file_writer(
    schema: &Schema,
) -> Result<(FileWriter<std::fs::File>, std::path::PathBuf), nu_protocol::LabeledError> {
    let temp_file = NamedTempFile::with_suffix(".arrow").map_err(|e| {
        nu_protocol::LabeledError::new("Failed to create temporary file").with_help(e.to_string())
    })?;
    let (file, path) = temp_file.keep().map_err(|e| {
        nu_protocol::LabeledError::new("Failed to keep temporary file").with_help(e.to_string())
    })?;

    let writer = FileWriter::try_new(file, schema).map_err(|e| {
        nu_protocol::LabeledError::new("Failed to create Arrow IPC writer").with_help(e.to_string())
    })?;

    Ok((writer, path))
}

pub(crate) fn arrow_batch_to_nu_values(
    batch: &arrow::record_batch::RecordBatch,
    span: nu_protocol::Span,
) -> Result<Vec<nu_protocol::Value>, nu_protocol::LabeledError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();

    let mut rows = Vec::with_capacity(num_rows);
    let mut cols_vals: Vec<Vec<nu_protocol::Value>> = vec![Vec::with_capacity(num_rows); num_cols];

    #[allow(clippy::needless_range_loop)]
    for c in 0..num_cols {
        let col = batch.column(c);
        let field = schema.field(c);

        #[allow(clippy::needless_range_loop)]
        for r in 0..num_rows {
            if col.is_null(r) {
                cols_vals[c].push(nu_protocol::Value::nothing(span));
                continue;
            }

            let val = arrow_value_to_nu(col.as_ref(), r, field.data_type(), span);
            cols_vals[c].push(val);
        }
    }

    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    #[allow(clippy::needless_range_loop)]
    for r in 0..num_rows {
        let mut row_record = nu_protocol::Record::with_capacity(num_cols);
        #[allow(clippy::needless_range_loop)]
        for c in 0..num_cols {
            row_record.push(col_names[c].clone(), cols_vals[c][r].clone());
        }
        rows.push(nu_protocol::Value::record(row_record, span));
    }

    Ok(rows)
}

fn downcast_or_fallback<A: arrow::array::Array + 'static>(
    col: &dyn arrow::array::Array,
    row_idx: usize,
    span: nu_protocol::Span,
    f: impl FnOnce(&A, usize) -> nu_protocol::Value,
) -> nu_protocol::Value {
    col.as_any()
        .downcast_ref::<A>()
        .map(|arr| f(arr, row_idx))
        .unwrap_or_else(|| nu_protocol::Value::string(format!("{:?}", col.data_type()), span))
}

fn arrow_value_to_nu(
    col: &dyn arrow::array::Array,
    row_idx: usize,
    data_type: &arrow::datatypes::DataType,
    span: nu_protocol::Span,
) -> nu_protocol::Value {
    use arrow::datatypes::DataType;

    if col.is_null(row_idx) {
        return nu_protocol::Value::nothing(span);
    }

    match data_type {
        DataType::Int8 => downcast_or_fallback::<Int8Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::Int16 => downcast_or_fallback::<Int16Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::Int32 => downcast_or_fallback::<Int32Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::Int64 => downcast_or_fallback::<Int64Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i), span)
        }),
        DataType::UInt8 => downcast_or_fallback::<UInt8Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::UInt16 => downcast_or_fallback::<UInt16Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::UInt32 => downcast_or_fallback::<UInt32Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::UInt64 => downcast_or_fallback::<UInt64Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::int(arr.value(i) as i64, span)
        }),
        DataType::Float32 => downcast_or_fallback::<Float32Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::float(arr.value(i) as f64, span)
        }),
        DataType::Float64 => downcast_or_fallback::<Float64Array>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::float(arr.value(i), span)
        }),
        DataType::Boolean => downcast_or_fallback::<BooleanArray>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::bool(arr.value(i), span)
        }),
        DataType::Utf8 => downcast_or_fallback::<StringArray>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::string(arr.value(i).to_string(), span)
        }),
        DataType::LargeUtf8 => {
            downcast_or_fallback::<LargeStringArray>(col, row_idx, span, |arr, i| {
                nu_protocol::Value::string(arr.value(i).to_string(), span)
            })
        }
        DataType::Date32 => downcast_or_fallback::<Date32Array>(col, row_idx, span, |arr, i| {
            let days = arr.value(i) as i64;
            let seconds = days * 86400;
            if let Some(dt) = chrono::DateTime::from_timestamp(seconds, 0) {
                nu_protocol::Value::date(dt.into(), span)
            } else {
                nu_protocol::Value::string(format!("Date32({})", days), span)
            }
        }),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            downcast_or_fallback::<TimestampMicrosecondArray>(col, row_idx, span, |arr, i| {
                let micros = arr.value(i);
                let seconds = micros / 1_000_000;
                let nanos = (micros % 1_000_000) * 1000;
                if let Some(dt) = chrono::DateTime::from_timestamp(seconds, nanos as u32) {
                    nu_protocol::Value::date(dt.into(), span)
                } else {
                    nu_protocol::Value::string(format!("Timestamp(us, {})", micros), span)
                }
            })
        }
        DataType::List(field) => downcast_or_fallback::<ListArray>(col, row_idx, span, |arr, i| {
            let values_arr = arr.value(i);
            let mut items = Vec::with_capacity(values_arr.len());
            for j in 0..values_arr.len() {
                items.push(arrow_value_to_nu(
                    values_arr.as_ref(),
                    j,
                    field.data_type(),
                    span,
                ));
            }
            nu_protocol::Value::list(items, span)
        }),
        DataType::LargeList(field) => {
            downcast_or_fallback::<LargeListArray>(col, row_idx, span, |arr, i| {
                let values_arr = arr.value(i);
                let mut items = Vec::with_capacity(values_arr.len());
                for j in 0..values_arr.len() {
                    items.push(arrow_value_to_nu(
                        values_arr.as_ref(),
                        j,
                        field.data_type(),
                        span,
                    ));
                }
                nu_protocol::Value::list(items, span)
            })
        }
        DataType::Struct(fields) => {
            downcast_or_fallback::<StructArray>(col, row_idx, span, |arr, i| {
                let mut record = nu_protocol::Record::with_capacity(fields.len());
                for (j, field) in fields.iter().enumerate() {
                    let val = arrow_value_to_nu(arr.column(j).as_ref(), i, field.data_type(), span);
                    record.push(field.name().clone(), val);
                }
                nu_protocol::Value::record(record, span)
            })
        }
        DataType::Binary => downcast_or_fallback::<BinaryArray>(col, row_idx, span, |arr, i| {
            nu_protocol::Value::binary(arr.value(i).to_vec(), span)
        }),
        DataType::LargeBinary => {
            downcast_or_fallback::<LargeBinaryArray>(col, row_idx, span, |arr, i| {
                nu_protocol::Value::binary(arr.value(i).to_vec(), span)
            })
        }
        // Fallback: cast to string
        _ => {
            if let Ok(str_arr) = arrow::compute::cast(col, &DataType::Utf8) {
                if let Some(arr) = str_arr.as_any().downcast_ref::<StringArray>() {
                    nu_protocol::Value::string(arr.value(row_idx).to_string(), span)
                } else {
                    nu_protocol::Value::string(format!("{:?}", col.data_type()), span)
                }
            } else {
                nu_protocol::Value::string(format!("{:?}", col.data_type()), span)
            }
        }
    }
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

#[cfg(test)]
mod tests_arrow_to_nu {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use nu_protocol::{Span, Value};
    use std::sync::Arc;

    #[test]
    fn test_arrow_batch_to_nu_values_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float64, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("str_col", DataType::Utf8, true),
        ]));

        let int_arr = Int64Array::from(vec![Some(42), None, Some(-10)]);
        let float_arr = Float64Array::from(vec![Some(9.99), Some(-2.5), None]);
        let bool_arr = BooleanArray::from(vec![None, Some(true), Some(false)]);
        let str_arr = StringArray::from(vec![Some("hello"), Some("world"), None]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(int_arr),
                Arc::new(float_arr),
                Arc::new(bool_arr),
                Arc::new(str_arr),
            ],
        )
        .unwrap();

        let span = Span::test_data();
        let result = arrow_batch_to_nu_values(&batch, span).unwrap();
        assert_eq!(result.len(), 3);

        // Row 0
        let row0 = result[0].as_record().unwrap();
        assert_eq!(row0.get("int_col").unwrap(), &Value::int(42, span));
        assert_eq!(row0.get("float_col").unwrap(), &Value::float(9.99, span));
        assert_eq!(row0.get("bool_col").unwrap(), &Value::nothing(span));
        assert_eq!(row0.get("str_col").unwrap(), &Value::string("hello", span));

        // Row 1
        let row1 = result[1].as_record().unwrap();
        assert_eq!(row1.get("int_col").unwrap(), &Value::nothing(span));
        assert_eq!(row1.get("float_col").unwrap(), &Value::float(-2.5, span));
        assert_eq!(row1.get("bool_col").unwrap(), &Value::bool(true, span));
        assert_eq!(row1.get("str_col").unwrap(), &Value::string("world", span));

        // Row 2
        let row2 = result[2].as_record().unwrap();
        assert_eq!(row2.get("int_col").unwrap(), &Value::int(-10, span));
        assert_eq!(row2.get("float_col").unwrap(), &Value::nothing(span));
        assert_eq!(row2.get("bool_col").unwrap(), &Value::bool(false, span));
        assert_eq!(row2.get("str_col").unwrap(), &Value::nothing(span));
    }

    #[test]
    fn test_arrow_batch_to_nu_values_datetime() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date_col", DataType::Date32, true),
            Field::new(
                "ts_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        // Date32: days since epoch. 1970-01-02 is 1 day.
        let date_arr = Date32Array::from(vec![Some(1), None]);

        // Timestamp(us): microseconds since epoch. 1 second is 1_000_000 microseconds.
        let ts_arr = TimestampMicrosecondArray::from(vec![None, Some(1_000_000)]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(date_arr), Arc::new(ts_arr)]).unwrap();

        let span = Span::test_data();
        let result = arrow_batch_to_nu_values(&batch, span).unwrap();
        assert_eq!(result.len(), 2);

        // Row 0
        let row0 = result[0].as_record().unwrap();
        match row0.get("date_col").unwrap() {
            Value::Date { val, .. } => {
                assert_eq!(val.format("%Y-%m-%d").to_string(), "1970-01-02");
            }
            _ => panic!("Expected Date value"),
        }
        assert_eq!(row0.get("ts_col").unwrap(), &Value::nothing(span));

        // Row 1
        let row1 = result[1].as_record().unwrap();
        assert_eq!(row1.get("date_col").unwrap(), &Value::nothing(span));
        match row1.get("ts_col").unwrap() {
            Value::Date { val, .. } => {
                assert_eq!(
                    val.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "1970-01-01 00:00:01"
                );
            }
            _ => panic!("Expected Date value"),
        }
    }

    #[test]
    fn test_arrow_batch_to_nu_values_nested() {
        use arrow::datatypes::Fields;

        let inner_fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "struct_col",
                DataType::Struct(Fields::from(inner_fields.clone())),
                true,
            ),
        ]));

        let mut list_builder = ListBuilder::new(Int64Builder::new());
        list_builder.values().append_value(1);
        list_builder.values().append_value(2);
        list_builder.append(true);
        list_builder.append(false);
        let list_arr = list_builder.finish();

        let a_arr = Arc::new(Int64Array::from(vec![Some(100), Some(200)])) as Arc<dyn Array>;
        let b_arr = Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as Arc<dyn Array>;
        let struct_arr =
            StructArray::try_new(Fields::from(inner_fields), vec![a_arr, b_arr], None).unwrap();

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(list_arr), Arc::new(struct_arr)]).unwrap();

        let span = Span::test_data();
        let result = arrow_batch_to_nu_values(&batch, span).unwrap();
        assert_eq!(result.len(), 2);

        let row0 = result[0].as_record().unwrap();
        let list_val = row0.get("list_col").unwrap().as_list().unwrap();
        assert_eq!(list_val, &[Value::int(1, span), Value::int(2, span)]);

        let struct_val = row0.get("struct_col").unwrap().as_record().unwrap();
        assert_eq!(struct_val.get("a").unwrap(), &Value::int(100, span));
        assert_eq!(struct_val.get("b").unwrap(), &Value::string("x", span));
    }

    #[test]
    fn test_storage_arrow_decoder_reads_split_schema_and_batch_messages() {
        use arrow::ipc::writer::{
            CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();

        let options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let data_gen = IpcDataGenerator::default();

        let schema_message = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema.as_ref(),
            &mut dictionary_tracker,
            &options,
        );
        let mut schema_bytes = Vec::new();
        write_message(&mut schema_bytes, schema_message, &options).unwrap();

        let (_dictionaries, batch_message) = data_gen
            .encode(
                &batch,
                &mut dictionary_tracker,
                &options,
                &mut CompressionContext::default(),
            )
            .unwrap();
        let mut batch_bytes = Vec::new();
        write_message(&mut batch_bytes, batch_message, &options).unwrap();

        let mut decoder = StorageArrowDecoder::new(schema_bytes).unwrap();
        let decoded = decoder.decode_batch(batch_bytes).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
        assert_eq!(decoded[0].schema().field(0).name(), "id");
    }

    #[test]
    fn test_create_arrow_ipc_file_writer_allows_empty_file() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let (mut writer, path) = create_arrow_ipc_file_writer(&schema).unwrap();
        writer.finish().unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();

        assert_eq!(reader.schema().field(0).name(), "id");
        assert!(reader.next().is_none());

        std::fs::remove_file(&path).ok();
    }
}
