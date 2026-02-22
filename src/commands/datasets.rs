use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Record, Signature, SyntaxShape, Type, Value};

use crate::plugin::BigQueryPlugin;

use super::create_client;

// ---------------------------------------------------------------------------
// bq datasets
// ---------------------------------------------------------------------------

pub struct BqDatasets;

impl SimplePluginCommand for BqDatasets {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq datasets"
    }

    fn signature(&self) -> Signature {
        datasets_signature("bq datasets")
    }

    fn description(&self) -> &str {
        "List BigQuery datasets in the project"
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
        run_datasets(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// bigquery datasets (alias)
// ---------------------------------------------------------------------------

pub struct BigqueryDatasets;

impl SimplePluginCommand for BigqueryDatasets {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery datasets"
    }

    fn signature(&self) -> Signature {
        datasets_signature("bigquery datasets")
    }

    fn description(&self) -> &str {
        "List BigQuery datasets in the project"
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
        run_datasets(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn datasets_signature(name: &str) -> Signature {
    Signature::build(name)
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

fn run_datasets(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    call: &EvaluatedCall,
) -> Result<Value, LabeledError> {
    let project: Option<String> = call.get_flag("project")?;
    let credentials: Option<String> = call.get_flag("credentials")?;
    let span = call.head;

    let client = create_client(plugin, engine, credentials, project, span)?;

    plugin.runtime.block_on(async {
        let response = client.list_datasets().await?;

        let rows: Vec<Value> = response
            .datasets
            .unwrap_or_default()
            .iter()
            .map(|ds| {
                let dataset_id = ds
                    .dataset_reference
                    .as_ref()
                    .and_then(|r| r.dataset_id.clone())
                    .unwrap_or_default();
                let id = ds.id.clone().unwrap_or_default();
                let location = ds.location.clone().unwrap_or_default();
                let friendly_name = ds.friendly_name.clone().unwrap_or_default();

                let mut record = Record::new();
                record.push("name", Value::string(&dataset_id, span));
                record.push("id", Value::string(&id, span));
                record.push("location", Value::string(&location, span));
                record.push("friendly_name", Value::string(&friendly_name, span));
                Value::record(record, span)
            })
            .collect();

        Ok(Value::list(rows, span))
    })
}
