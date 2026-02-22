use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Signature, Type, Value};

use crate::plugin::BigQueryPlugin;

// ---------------------------------------------------------------------------
// bq (parent command — shows help)
// ---------------------------------------------------------------------------

pub struct Bq;

impl SimplePluginCommand for Bq {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq"
    }

    fn signature(&self) -> Signature {
        Signature::build("bq")
            .input_output_type(Type::Nothing, Type::Nothing)
            .category(Category::Database)
    }

    fn description(&self) -> &str {
        "Query Google BigQuery from Nushell"
    }

    fn extra_description(&self) -> &str {
        HELP_TEXT
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bigquery", "gcp", "google", "cloud", "sql", "data"]
    }

    fn run(
        &self,
        _plugin: &BigQueryPlugin,
        _engine: &EngineInterface,
        _call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        Err(LabeledError::new("Missing subcommand")
            .with_help(format!("Usage: bq <subcommand>\n\n{HELP_TEXT}")))
    }
}

// ---------------------------------------------------------------------------
// bigquery (parent command — shows help)
// ---------------------------------------------------------------------------

pub struct Bigquery;

impl SimplePluginCommand for Bigquery {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery"
    }

    fn signature(&self) -> Signature {
        Signature::build("bigquery")
            .input_output_type(Type::Nothing, Type::Nothing)
            .category(Category::Database)
    }

    fn description(&self) -> &str {
        "Query Google BigQuery from Nushell"
    }

    fn extra_description(&self) -> &str {
        HELP_TEXT
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bq", "gcp", "google", "cloud", "sql", "data"]
    }

    fn run(
        &self,
        _plugin: &BigQueryPlugin,
        _engine: &EngineInterface,
        _call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        Err(LabeledError::new("Missing subcommand")
            .with_help(format!("Usage: bigquery <subcommand>\n\n{HELP_TEXT}")))
    }
}

const HELP_TEXT: &str = "\
Available subcommands:
  query      Execute a SQL query against BigQuery
  read       Read rows directly from a BigQuery table
  datasets   List datasets in the project
  tables     List tables in a dataset
  schema     Inspect the schema of a table

Authentication (zero-config by default):
  # Just works if `gcloud auth application-default login` was run
  bq query \"SELECT 1\"

  # Explicit service account
  bq query --credentials /path/to/sa-key.json \"SELECT 1\"

  # Project override
  bq query --project my-project \"SELECT 1\"

Run `bq <subcommand> --help` for details on each command.";
