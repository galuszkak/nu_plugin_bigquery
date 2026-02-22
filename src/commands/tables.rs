use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Record, Signature, SyntaxShape, Type, Value};

use crate::convert;
use crate::plugin::BigQueryPlugin;

use super::create_client;

// ---------------------------------------------------------------------------
// bq tables
// ---------------------------------------------------------------------------

pub struct BqTables;

impl SimplePluginCommand for BqTables {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq tables"
    }

    fn signature(&self) -> Signature {
        tables_signature("bq tables")
    }

    fn description(&self) -> &str {
        "List tables in a BigQuery dataset"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bigquery", "gcp", "google", "list"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_tables(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// bigquery tables (alias)
// ---------------------------------------------------------------------------

pub struct BigqueryTables;

impl SimplePluginCommand for BigqueryTables {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery tables"
    }

    fn signature(&self) -> Signature {
        tables_signature("bigquery tables")
    }

    fn description(&self) -> &str {
        "List tables in a BigQuery dataset"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bq", "gcp", "google", "list"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_tables(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn tables_signature(name: &str) -> Signature {
    Signature::build(name)
        .required(
            "dataset",
            SyntaxShape::String,
            "Dataset ID to list tables from",
        )
        .named("project", SyntaxShape::String, "GCP project ID", Some('p'))
        .named(
            "credentials",
            SyntaxShape::Filepath,
            "Path to service account key JSON file",
            Some('c'),
        )
        .input_output_type(Type::Nothing, Type::table())
        .category(Category::Database)
}

fn run_tables(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    call: &EvaluatedCall,
) -> Result<Value, LabeledError> {
    let dataset: String = call.req(0)?;
    let project: Option<String> = call.get_flag("project")?;
    let credentials: Option<String> = call.get_flag("credentials")?;
    let span = call.head;

    let client = create_client(plugin, engine, credentials, project, span)?;

    plugin.runtime.block_on(async {
        // Use INFORMATION_SCHEMA to get num_rows, num_bytes in a single query
        let sql = format!(
            "SELECT table_name, table_type, \
             CAST(row_count AS INT64) AS num_rows, \
             CAST(size_bytes AS INT64) AS num_bytes, \
             creation_time \
             FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`",
            project = client.project(),
            dataset = dataset,
        );

        let response = client.query(&sql, None, None, false, None).await;

        match response {
            Ok(resp) => {
                // Successfully queried INFORMATION_SCHEMA — collect all pages
                let (schema, all_rows) =
                    super::query::collect_all_rows(&client, resp, None, None).await?;
                let values = convert::rows_to_values(&schema, &all_rows, span);
                Ok(Value::list(values, span))
            }
            Err(e) => {
                // Only fall back to tables.list API for permission errors (403).
                // Propagate other errors (wrong dataset, syntax, network, etc.)
                let msg = e.msg.to_lowercase();
                let is_permission_error = msg.contains("403")
                    || msg.contains("access denied")
                    || msg.contains("permission");
                if !is_permission_error {
                    return Err(e);
                }

                let response = client.list_tables(&dataset).await?;

                let rows: Vec<Value> = response
                    .tables
                    .unwrap_or_default()
                    .iter()
                    .map(|tbl| {
                        let table_id = tbl
                            .table_reference
                            .as_ref()
                            .and_then(|r| r.table_id.clone())
                            .unwrap_or_default();
                        let table_type = tbl.r#type.clone().unwrap_or_default();
                        let creation_time = tbl.creation_time.clone().unwrap_or_default();

                        let mut record = Record::new();
                        record.push("table_name", Value::string(&table_id, span));
                        record.push("table_type", Value::string(&table_type, span));
                        record.push("num_rows", Value::nothing(span));
                        record.push("num_bytes", Value::nothing(span));
                        record.push("creation_time", Value::string(&creation_time, span));
                        Value::record(record, span)
                    })
                    .collect();

                Ok(Value::list(rows, span))
            }
        }
    })
}
