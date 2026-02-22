use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Record, Signature, SyntaxShape, Type, Value};

use crate::client::TableFieldSchema;
use crate::plugin::BigQueryPlugin;

use super::{create_client, parse_table_ref};

// ---------------------------------------------------------------------------
// bq schema
// ---------------------------------------------------------------------------

pub struct BqSchema;

impl SimplePluginCommand for BqSchema {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bq schema"
    }

    fn signature(&self) -> Signature {
        schema_signature("bq schema")
    }

    fn description(&self) -> &str {
        "Inspect the schema of a BigQuery table"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bigquery", "gcp", "google", "describe", "columns"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_schema(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// bigquery schema (alias)
// ---------------------------------------------------------------------------

pub struct BigquerySchema;

impl SimplePluginCommand for BigquerySchema {
    type Plugin = BigQueryPlugin;

    fn name(&self) -> &str {
        "bigquery schema"
    }

    fn signature(&self) -> Signature {
        schema_signature("bigquery schema")
    }

    fn description(&self) -> &str {
        "Inspect the schema of a BigQuery table"
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["bq", "gcp", "google", "describe", "columns"]
    }

    fn run(
        &self,
        plugin: &BigQueryPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: &Value,
    ) -> Result<Value, LabeledError> {
        run_schema(plugin, engine, call)
    }
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn schema_signature(name: &str) -> Signature {
    Signature::build(name)
        .required(
            "table",
            SyntaxShape::String,
            "Table reference: dataset.table or project.dataset.table",
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

fn run_schema(
    plugin: &BigQueryPlugin,
    engine: &EngineInterface,
    call: &EvaluatedCall,
) -> Result<Value, LabeledError> {
    let table_ref: String = call.req(0)?;
    let project: Option<String> = call.get_flag("project")?;
    let credentials: Option<String> = call.get_flag("credentials")?;
    let span = call.head;

    let (ref_project, dataset_id, table_id) = parse_table_ref(&table_ref)?;
    let effective_project = project.or(ref_project);
    let client = create_client(plugin, engine, credentials, effective_project, span)?;

    plugin.runtime.block_on(async {
        let table = client.get_table(&dataset_id, &table_id).await?;

        let fields = table.schema.and_then(|s| s.fields).unwrap_or_default();

        let rows = flatten_fields(&fields, "", span);
        Ok(Value::list(rows, span))
    })
}

/// Flatten schema fields into rows, including nested RECORD fields with dot notation.
fn flatten_fields(
    fields: &[TableFieldSchema],
    prefix: &str,
    span: nu_protocol::Span,
) -> Vec<Value> {
    let mut rows = Vec::new();

    for field in fields {
        let name = field.name.as_deref().unwrap_or("unknown");
        let full_name = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}.{name}")
        };

        let field_type = field.r#type.as_deref().unwrap_or("UNKNOWN");
        let mode = field.mode.as_deref().unwrap_or("NULLABLE");
        let description = field.description.as_deref().unwrap_or("");

        let mut record = Record::new();
        record.push("name", Value::string(&full_name, span));
        record.push("type", Value::string(field_type, span));
        record.push("mode", Value::string(mode, span));
        record.push("description", Value::string(description, span));
        rows.push(Value::record(record, span));

        // Recurse into RECORD/STRUCT sub-fields
        if (field_type == "RECORD" || field_type == "STRUCT")
            && let Some(sub_fields) = &field.fields
        {
            rows.extend(flatten_fields(sub_fields, &full_name, span));
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use nu_protocol::Span;

    #[test]
    fn test_flatten_fields_simple() {
        let fields = vec![
            TableFieldSchema {
                name: Some("id".to_string()),
                r#type: Some("INTEGER".to_string()),
                mode: Some("REQUIRED".to_string()),
                description: Some("Primary key".to_string()),
                fields: None,
            },
            TableFieldSchema {
                name: Some("name".to_string()),
                r#type: Some("STRING".to_string()),
                mode: Some("NULLABLE".to_string()),
                description: None,
                fields: None,
            },
        ];

        let rows = flatten_fields(&fields, "", Span::test_data());
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_flatten_fields_nested_record() {
        let fields = vec![TableFieldSchema {
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
        }];

        let rows = flatten_fields(&fields, "", Span::test_data());
        // 1 for address + 2 for sub-fields
        assert_eq!(rows.len(), 3);
    }
}
