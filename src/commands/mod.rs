mod datasets;
mod help;
mod query;
mod read;
mod schema;
mod tables;

pub use datasets::{BigqueryDatasets, BqDatasets};
pub use help::{Bigquery, Bq};
pub use query::{BigqueryQuery, BqQuery};
pub use read::{BigqueryRead, BqRead};
pub use schema::{BigquerySchema, BqSchema};
pub use tables::{BigqueryTables, BqTables};

use nu_plugin::EngineInterface;
use nu_protocol::{LabeledError, Span};

use crate::auth;
use crate::client::BigQueryClient;
use crate::plugin::BigQueryPlugin;

/// Parse a table reference like "dataset.table" or "project.dataset.table".
/// Returns (project, dataset_id, table_id). Project is None for 2-part refs.
pub(crate) fn parse_table_ref(
    table_ref: &str,
) -> Result<(Option<String>, String, String), LabeledError> {
    let parts: Vec<&str> = table_ref.split('.').collect();
    match parts.len() {
        2 => Ok((None, parts[0].to_string(), parts[1].to_string())),
        3 => Ok((
            Some(parts[0].to_string()),
            parts[1].to_string(),
            parts[2].to_string(),
        )),
        _ => Err(
            LabeledError::new("Invalid table reference").with_help(format!(
                "Expected format: dataset.table or project.dataset.table, got: '{table_ref}'"
            )),
        ),
    }
}

/// Common helper: create an authenticated BigQueryClient from command flags + env.
pub(crate) fn create_client(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    credentials: Option<String>,
    project: Option<String>,
    span: Span,
) -> Result<BigQueryClient, LabeledError> {
    plugin.runtime.block_on(async {
        let provider = auth::resolve_auth(credentials.as_deref(), engine).await?;
        let project_id =
            auth::resolve_project(project.as_deref(), engine, provider.as_ref(), span).await?;
        Ok(BigQueryClient::new(provider, project_id))
    })
}
