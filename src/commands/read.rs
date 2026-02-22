use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Signature, SyntaxShape, Type, Value};

use crate::arrow_ipc;
use crate::convert;
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
        // When --filter is specified, we need to use a SQL query since
        // the tabledata.list REST API doesn't support row filtering.
        if filter.is_some() {
            return run_read_via_query(ReadQueryParams {
                client: &client,
                dataset_id: &dataset_id,
                table_id: &table_id,
                columns: columns.as_deref(),
                filter: filter.as_deref(),
                max_results,
                arrow_mode,
                span,
            })
            .await;
        }

        // No filter — use tabledata.list API for direct table reads
        let selected_fields = columns.as_ref().map(|cols| cols.join(","));

        // Get the table schema for type conversion
        let table_meta = client.get_table(&dataset_id, &table_id).await?;
        let full_schema = table_meta.schema.ok_or_else(|| {
            LabeledError::new("No schema found for table").with_help(format!(
                "Table {dataset_id}.{table_id} has no schema information."
            ))
        })?;

        // Filter schema to selected columns if specified
        let schema = if let Some(ref sel) = selected_fields {
            let selected: Vec<&str> = sel.split(',').collect();
            let filtered_fields = full_schema
                .fields
                .as_ref()
                .map(|fields| {
                    fields
                        .iter()
                        .filter(|f| {
                            f.name
                                .as_deref()
                                .map(|n| selected.contains(&n))
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            crate::client::TableSchema {
                fields: Some(filtered_fields),
            }
        } else {
            full_schema
        };

        // Read table data with pagination
        let mut all_raw_rows = Vec::new();
        let mut page_token: Option<String> = None;
        let max = max_results.map(|n| n as u64);

        loop {
            let response = client
                .read_table_data(
                    &dataset_id,
                    &table_id,
                    selected_fields.as_deref(),
                    page_token.as_deref(),
                    max,
                )
                .await?;

            all_raw_rows.extend(response.rows.unwrap_or_default());

            // Stop if we've hit the requested max
            if let Some(limit) = max_results
                && all_raw_rows.len() >= limit as usize
            {
                all_raw_rows.truncate(limit as usize);
                break;
            }

            match response.page_token {
                Some(pt) => page_token = Some(pt),
                None => break,
            }
        }

        if arrow_mode {
            let path = arrow_ipc::write_arrow_ipc(&schema, &all_raw_rows)?;
            Ok(Value::string(path, span))
        } else {
            let values = convert::rows_to_values(&schema, &all_raw_rows, span);
            Ok(Value::list(values, span))
        }
    })
}

struct ReadQueryParams<'a> {
    client: &'a crate::client::BigQueryClient,
    dataset_id: &'a str,
    table_id: &'a str,
    columns: Option<&'a [String]>,
    filter: Option<&'a str>,
    max_results: Option<i64>,
    arrow_mode: bool,
    span: nu_protocol::Span,
}

/// When --filter is used, we convert the read into a SQL query since the
/// tabledata.list API doesn't support row filtering.
async fn run_read_via_query(params: ReadQueryParams<'_>) -> Result<Value, LabeledError> {
    let ReadQueryParams {
        client,
        dataset_id,
        table_id,
        columns,
        filter,
        max_results,
        arrow_mode,
        span,
    } = params;
    let project = client.project();
    let select_clause = match columns {
        Some(cols) if !cols.is_empty() => cols.join(", "),
        _ => "*".to_string(),
    };

    let mut sql = format!("SELECT {select_clause} FROM `{project}.{dataset_id}.{table_id}`");

    if let Some(f) = filter {
        sql.push_str(&format!(" WHERE {f}"));
    }

    if let Some(limit) = max_results {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    let max_u64 = max_results.map(|n| n as u64);
    let response = client.query(&sql, None, max_u64, false, None).await?;

    // Reuse the shared pagination + job-polling logic from bq query
    let (schema, all_raw_rows) =
        crate::commands::query::collect_all_rows(client, response, None, max_results).await?;

    if arrow_mode {
        let path = arrow_ipc::write_arrow_ipc(&schema, &all_raw_rows)?;
        Ok(Value::string(path, span))
    } else {
        let values = convert::rows_to_values(&schema, &all_raw_rows, span);
        Ok(Value::list(values, span))
    }
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
