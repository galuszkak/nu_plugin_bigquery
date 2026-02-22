use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Record, Signature, SyntaxShape, Type, Value};

use crate::arrow_ipc;
use crate::client::{QueryResponse, TableSchema};
use crate::convert;
use crate::plugin::BigQueryPlugin;

use super::create_client;

// ---------------------------------------------------------------------------
// bq query
// ---------------------------------------------------------------------------

pub struct BqQuery;

impl SimplePluginCommand for BqQuery {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq query"
    }

    fn signature(&self) -> Signature {
        query_signature("bq query")
    }

    fn description(&self) -> &str {
        "Execute a SQL query against Google BigQuery"
    }

    fn extra_description(&self) -> &str {
        "Returns results as a Nushell table by default.\n\
         Use --arrow to write results as Arrow IPC file (returns path for `polars open $in`).\n\
         Use --dry-run to see bytes processed without executing."
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bigquery", "sql", "gcp", "google", "cloud", "data"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_query(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// bigquery query (alias)
// ---------------------------------------------------------------------------

pub struct BigqueryQuery;

impl SimplePluginCommand for BigqueryQuery {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery query"
    }

    fn signature(&self) -> Signature {
        query_signature("bigquery query")
    }

    fn description(&self) -> &str {
        "Execute a SQL query against Google BigQuery"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bq", "sql", "gcp", "google", "cloud", "data"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_query(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn query_signature(name: &str) -> Signature {
    Signature::build(name)
        .required("sql", SyntaxShape::String, "SQL query to execute")
        .named("project", SyntaxShape::String, "GCP project ID", Some('p'))
        .named(
            "credentials",
            SyntaxShape::Filepath,
            "Path to service account key JSON file",
            Some('c'),
        )
        .named(
            "location",
            SyntaxShape::String,
            "BigQuery processing location (e.g., US, EU)",
            Some('l'),
        )
        .named(
            "max-results",
            SyntaxShape::Int,
            "Maximum number of rows to return",
            Some('n'),
        )
        .named(
            "timeout",
            SyntaxShape::Int,
            "Query timeout in milliseconds",
            None,
        )
        .switch(
            "arrow",
            "Write results as Arrow IPC file, return path (for `polars open $in`)",
            Some('a'),
        )
        .switch("dry-run", "Show bytes processed without executing", None)
        .input_output_type(Type::Nothing, Type::table())
        .category(Category::Database)
}

fn run_query(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    call: &EvaluatedCall,
) -> Result<Value, LabeledError> {
    let sql: String = call.req(0)?;
    let project: Option<String> = call.get_flag("project")?;
    let credentials: Option<String> = call.get_flag("credentials")?;
    let location: Option<String> = call.get_flag("location")?;
    let max_results: Option<i64> = call.get_flag("max-results")?;
    let timeout: Option<i64> = call.get_flag("timeout")?;
    let arrow_mode = call.has_flag("arrow")?;
    let dry_run = call.has_flag("dry-run")?;

    let span = call.head;

    if let Some(n) = max_results
        && n < 0
    {
        return Err(
            LabeledError::new("--max-results must be non-negative").with_help(format!("Got {n}"))
        );
    }
    if let Some(t) = timeout
        && t < 0
    {
        return Err(
            LabeledError::new("--timeout must be non-negative").with_help(format!("Got {t}"))
        );
    }

    let client = create_client(plugin, engine, credentials, project, span)?;

    plugin.runtime.block_on(async {
        let response = client
            .query(
                &sql,
                location.as_deref(),
                max_results.map(|n| n as u64),
                dry_run,
                timeout.map(|t| t as u64),
            )
            .await?;

        // Dry run: return cost estimation
        if dry_run {
            let bytes_processed = response.total_bytes_processed.as_deref().unwrap_or("0");
            let record = Record::from_raw_cols_vals(
                vec![
                    "bytes_processed".to_string(),
                    "bytes_processed_human".to_string(),
                ],
                vec![
                    Value::int(bytes_processed.parse::<i64>().unwrap_or(0), span),
                    Value::string(convert::format_bytes(bytes_processed), span),
                ],
                span,
                span,
            )?;
            return Ok(Value::record(record, span));
        }

        // Collect all rows (with pagination and job polling)
        let (schema, all_raw_rows) =
            collect_all_rows(&client, response, location.as_deref(), max_results).await?;

        if arrow_mode {
            // Write Arrow IPC file, return path
            let path = arrow_ipc::write_arrow_ipc(&schema, &all_raw_rows)?;
            Ok(Value::string(path, span))
        } else {
            // Convert to Nushell table
            let values = convert::rows_to_values(&schema, &all_raw_rows, span);
            Ok(Value::list(values, span))
        }
    })
}

/// Collect all result rows across pagination and job polling.
/// Returns the schema and raw TableRow data.
pub(crate) async fn collect_all_rows(
    client: &crate::client::BigQueryClient,
    response: QueryResponse,
    location: Option<&str>,
    max_results: Option<i64>,
) -> Result<(TableSchema, Vec<crate::client::TableRow>), LabeledError> {
    // If job not complete, poll until done
    if response.job_complete != Some(true) {
        let job_ref = response.job_reference.as_ref().ok_or_else(|| {
            LabeledError::new("Query job did not complete and no job reference was returned")
        })?;
        let job_id = job_ref
            .job_id
            .as_deref()
            .ok_or_else(|| LabeledError::new("No job ID in response"))?;

        return poll_all_rows(client, job_id, location, max_results).await;
    }

    let schema = response.schema.ok_or_else(|| {
        LabeledError::new("No schema in query response")
            .with_help("The query returned no schema information.")
    })?;

    let mut all_rows: Vec<crate::client::TableRow> = response.rows.unwrap_or_default();

    // Fetch remaining pages, capping at max_results total
    if let Some(job_ref) = &response.job_reference
        && let Some(job_id) = &job_ref.job_id
    {
        let mut page_token = response.page_token;
        while let Some(pt) = page_token.take() {
            if let Some(limit) = max_results
                && all_rows.len() >= limit as usize
            {
                break;
            }
            let page = client
                .get_query_results(job_id, location, Some(&pt), None)
                .await?;

            all_rows.extend(page.rows.unwrap_or_default());
            page_token = page.page_token;
        }
    }

    if let Some(limit) = max_results {
        all_rows.truncate(limit as usize);
    }

    Ok((schema, all_rows))
}

/// Maximum time (in seconds) to wait for a BigQuery job to complete during polling.
const MAX_POLL_SECS: u64 = 600;

/// Poll a job until complete, then paginate all remaining rows.
/// Used when the initial query response has jobComplete: false, and also
/// called from run_read_via_query for filtered reads.
pub(crate) async fn poll_all_rows(
    client: &crate::client::BigQueryClient,
    job_id: &str,
    location: Option<&str>,
    max_results: Option<i64>,
) -> Result<(TableSchema, Vec<crate::client::TableRow>), LabeledError> {
    let mut all_rows = Vec::new();
    let mut page_token: Option<String> = None;
    let mut schema_ref: Option<TableSchema> = None;
    let poll_start = std::time::Instant::now();

    // Phase 1: Poll until the job reports complete, collecting the first page of results
    loop {
        let response = client
            .get_query_results(job_id, location, page_token.as_deref(), None)
            .await?;

        if let Some(s) = response.schema {
            schema_ref = Some(s);
        }

        if response.job_complete == Some(true) {
            all_rows.extend(response.rows.unwrap_or_default());
            page_token = response.page_token;
            break;
        }

        if poll_start.elapsed().as_secs() >= MAX_POLL_SECS {
            return Err(
                LabeledError::new("BigQuery job polling timed out").with_help(format!(
                    "Job {job_id} did not complete within {MAX_POLL_SECS} seconds. \
                     The job may still be running in BigQuery."
                )),
            );
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    // Phase 2: Paginate remaining results
    while let Some(pt) = page_token.take() {
        if let Some(limit) = max_results
            && all_rows.len() >= limit as usize
        {
            break;
        }
        let response = client
            .get_query_results(job_id, location, Some(&pt), None)
            .await?;

        all_rows.extend(response.rows.unwrap_or_default());
        page_token = response.page_token;
    }

    if let Some(limit) = max_results {
        all_rows.truncate(limit as usize);
    }

    let schema = schema_ref.ok_or_else(|| LabeledError::new("No schema in query results"))?;

    Ok((schema, all_rows))
}
