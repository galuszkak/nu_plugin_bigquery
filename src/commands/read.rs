use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Signature, SyntaxShape, Type, Value};

use crate::plugin::BigQueryPlugin;

use super::{create_client, parse_table_ref};

// ---------------------------------------------------------------------------
// bq read
// ---------------------------------------------------------------------------

pub struct BqRead;

impl SimplePluginCommand for BqRead {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq read"
    }

    fn signature(&self) -> Signature {
        read_signature("bq read")
    }

    fn description(&self) -> &str {
        "Read rows directly from a BigQuery table (no SQL required)"
    }

    fn extra_description(&self) -> &str {
        "Reads data from a BigQuery table with server-side column selection.\n\
         Use --filter for server-side row filtering (SQL WHERE clause syntax).\n\
         Use --arrow to write results as Arrow IPC file (returns path for `polars open $in`)."
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bigquery", "gcp", "google", "table", "data", "download"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_read(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// bigquery read (alias)
// ---------------------------------------------------------------------------

pub struct BigqueryRead;

impl SimplePluginCommand for BigqueryRead {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery read"
    }

    fn signature(&self) -> Signature {
        read_signature("bigquery read")
    }

    fn description(&self) -> &str {
        "Read rows directly from a BigQuery table (no SQL required)"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bq", "gcp", "google", "table", "data", "download"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_read(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn read_signature(name: &str) -> Signature {
    Signature::build(name)
        .required(
            "table",
            SyntaxShape::String,
            "Table reference: dataset.table or project.dataset.table",
        )
        .named(
            "columns",
            SyntaxShape::List(Box::new(SyntaxShape::String)),
            "Columns to read (server-side projection)",
            None,
        )
        .named(
            "filter",
            SyntaxShape::String,
            "Row filter expression (SQL WHERE syntax, e.g., \"ts > '2024-06-01'\")",
            None,
        )
        .named(
            "max-results",
            SyntaxShape::Int,
            "Maximum number of rows to return",
            Some('n'),
        )
        .named("project", SyntaxShape::String, "GCP project ID", Some('p'))
        .named(
            "credentials",
            SyntaxShape::Filepath,
            "Path to service account key JSON file",
            Some('c'),
        )
        .switch(
            "arrow",
            "Write results as Arrow IPC file, return path (for `polars open $in`)",
            Some('a'),
        )
        .input_output_type(Type::Nothing, Type::table())
        .category(Category::Database)
}

fn run_read(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    call: &EvaluatedCall,
) -> Result<Value, LabeledError> {
    let table_ref: String = call.req(0)?;
    let columns: Option<Vec<String>> = call.get_flag("columns")?;
    let filter: Option<String> = call.get_flag("filter")?;
    let max_results: Option<i64> = call.get_flag("max-results")?;
    let project: Option<String> = call.get_flag("project")?;
    let credentials: Option<String> = call.get_flag("credentials")?;
    let arrow_mode = call.has_flag("arrow")?;
    let span = call.head;

    if let Some(n) = max_results
        && n < 0
    {
        return Err(
            LabeledError::new("--max-results must be non-negative").with_help(format!("Got {n}"))
        );
    }

    let (ref_project, dataset_id, table_id) = parse_table_ref(&table_ref)?;
    let effective_project = project.or(ref_project);
    let client = create_client(plugin, engine, credentials, effective_project, span)?;

    plugin.runtime.block_on(async {
        let mut storage_client = client.create_storage_client().await?;

        // 1. Create a ReadSession
        use googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1::{
            CreateReadSessionRequest, ReadSession, DataFormat, read_session::TableReadOptions,
            ReadRowsRequest
        };

        let parent = format!("projects/{}", client.project());
        let table = format!("projects/{}/datasets/{}/tables/{}", client.project(), dataset_id, table_id);

        let mut read_options = TableReadOptions::default();
        if let Some(ref cols) = columns {
            read_options.selected_fields = cols.clone();
        }
        if let Some(ref f) = filter {
            read_options.row_restriction = f.clone();
        }

        let session = ReadSession {
            table: table.clone(),
            data_format: DataFormat::Arrow.into(),
            read_options: Some(read_options),
            ..Default::default()
        };

        let create_req = CreateReadSessionRequest {
            parent: parent.clone(),
            read_session: Some(session),
            max_stream_count: 1, // Start with single stream for simplicity
            ..Default::default()
        };

        let session_resp = storage_client
            .create_read_session(tonic::Request::new(create_req))
            .await
            .map_err(|e| {
                LabeledError::new("Failed to create BigQuery Read Session")
                    .with_help(format!("gRPC error: {}", e))
            })?
            .into_inner();

        let streams = session_resp.streams;
        if streams.is_empty() {
            return Ok(Value::list(vec![], span));
        }

        // 2. ReadRows from the stream
        let stream_name = streams[0].name.clone();
        let read_rows_req = ReadRowsRequest {
            read_stream: stream_name,
            offset: 0,
        };

        let mut row_stream = storage_client
            .read_rows(tonic::Request::new(read_rows_req))
            .await
            .map_err(|e| {
                LabeledError::new("Failed to read rows from BigQuery Storage")
                    .with_help(format!("gRPC error: {}", e))
            })?
            .into_inner();

        // 3. Collect and process Arrow IPC data
        let mut all_arrow_batches = Vec::new();
        // Since the schema is embedded in the first read rows response or ReadSession,
        // we can extract it from `session_resp.arrow_schema`
        let arrow_schema_bytes = match session_resp.schema {
            Some(googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1::read_session::Schema::ArrowSchema(schema)) => schema.serialized_schema,
            _ => return Err(LabeledError::new("Expected Arrow schema from BigQuery Storage API")),
        };

        let mut total_rows = 0;
        while let Some(resp) = row_stream.message().await.map_err(|e| {
            LabeledError::new("Error reading from stream").with_help(format!("gRPC error: {}", e))
        })? {
            if let Some(googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1::read_rows_response::Rows::ArrowRecordBatch(batch)) = resp.rows {
                let row_count = resp.row_count;
                if let Some(max) = max_results
                    && total_rows >= max {
                        break;
                    }
                all_arrow_batches.push(batch.serialized_record_batch);
                total_rows += row_count;
            }
        }

        // Decode Arrow IPC messages
        crate::arrow_ipc::decode_storage_api_arrow(arrow_schema_bytes, all_arrow_batches, arrow_mode, max_results, span)
    })
}

#[cfg(test)]
mod tests {
    use crate::commands::parse_table_ref;

    #[test]
    fn test_parse_table_ref_two_parts() {
        let (proj, ds, tbl) = parse_table_ref("my_dataset.my_table").unwrap();
        assert_eq!(proj, None);
        assert_eq!(ds, "my_dataset");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_parse_table_ref_three_parts() {
        let (proj, ds, tbl) = parse_table_ref("proj.my_dataset.my_table").unwrap();
        assert_eq!(proj, Some("proj".to_string()));
        assert_eq!(ds, "my_dataset");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn test_parse_table_ref_invalid() {
        assert!(parse_table_ref("only_one").is_err());
        assert!(parse_table_ref("a.b.c.d").is_err());
    }
}
