#![allow(dead_code)] // API response fields are deserialized from JSON; not all are read directly

use std::sync::Arc;

use gcp_auth::TokenProvider;
use nu_protocol::LabeledError;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::auth;

const BQ_BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// BigQuery REST API client.
pub struct BigQueryClient {
    http: Client,
    provider: Arc<dyn TokenProvider>,
    project: String,
}

impl BigQueryClient {
    pub fn new(provider: Arc<dyn TokenProvider>, project: String) -> Self {
        let http = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            http,
            provider,
            project,
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    async fn bearer_token(&self) -> Result<String, LabeledError> {
        auth::get_token(self.provider.as_ref()).await
    }

    /// Execute a SQL query and return the full response including schema.
    pub async fn query(
        &self,
        sql: &str,
        location: Option<&str>,
        max_results: Option<u64>,
        dry_run: bool,
        timeout_ms: Option<u64>,
    ) -> Result<QueryResponse, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!("{}/projects/{}/queries", BQ_BASE_URL, self.project);

        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: location.map(String::from),
            max_results,
            dry_run: if dry_run { Some(true) } else { None },
            timeout_ms,
        };

        let resp = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<QueryResponse>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }

    /// Get remaining results for a query job (pagination).
    pub async fn get_query_results(
        &self,
        job_id: &str,
        location: Option<&str>,
        page_token: Option<&str>,
        max_results: Option<u64>,
    ) -> Result<GetQueryResultsResponse, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!(
            "{}/projects/{}/queries/{}",
            BQ_BASE_URL, self.project, job_id
        );

        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(loc) = location {
            params.push(("location", loc.to_string()));
        }
        if let Some(pt) = page_token {
            params.push(("pageToken", pt.to_string()));
        }
        if let Some(mr) = max_results {
            params.push(("maxResults", mr.to_string()));
        }

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .query(&params)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<GetQueryResultsResponse>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }

    /// List datasets in the project.
    pub async fn list_datasets(&self) -> Result<DatasetListResponse, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!("{}/projects/{}/datasets", BQ_BASE_URL, self.project);

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<DatasetListResponse>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }

    /// List tables in a dataset.
    pub async fn list_tables(&self, dataset_id: &str) -> Result<TableListResponse, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!(
            "{}/projects/{}/datasets/{}/tables",
            BQ_BASE_URL, self.project, dataset_id
        );

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<TableListResponse>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }

    /// Read rows from a table using tabledata.list API.
    pub async fn read_table_data(
        &self,
        dataset_id: &str,
        table_id: &str,
        selected_fields: Option<&str>,
        page_token: Option<&str>,
        max_results: Option<u64>,
    ) -> Result<TableDataListResponse, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!(
            "{}/projects/{}/datasets/{}/tables/{}/data",
            BQ_BASE_URL, self.project, dataset_id, table_id
        );

        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(fields) = selected_fields {
            params.push(("selectedFields", fields.to_string()));
        }
        if let Some(pt) = page_token {
            params.push(("pageToken", pt.to_string()));
        }
        if let Some(mr) = max_results {
            params.push(("maxResults", mr.to_string()));
        }

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .query(&params)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<TableDataListResponse>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }

    /// Get table metadata (including schema).
    pub async fn get_table(
        &self,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<TableResource, LabeledError> {
        let token = self.bearer_token().await?;
        let url = format!(
            "{}/projects/{}/datasets/{}/tables/{}",
            BQ_BASE_URL, self.project, dataset_id, table_id
        );

        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                LabeledError::new("BigQuery request failed").with_help(format!("HTTP error: {e}"))
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(parse_bq_error(status.as_u16(), &body));
        }

        resp.json::<TableResource>().await.map_err(|e| {
            LabeledError::new("Failed to parse BigQuery response")
                .with_help(format!("JSON parse error: {e}"))
        })
    }
}

fn parse_bq_error(status: u16, body: &str) -> LabeledError {
    if let Ok(err_resp) = serde_json::from_str::<ErrorResponse>(body)
        && let Some(err) = err_resp.error
    {
        let msg = format!("BigQuery API error ({}): {}", err.code, err.message);
        let mut labeled = LabeledError::new(msg);
        if let Some(errors) = err.errors {
            let details: Vec<String> = errors
                .iter()
                .map(|e| {
                    format!(
                        "- {}: {}",
                        e.reason.as_deref().unwrap_or("unknown"),
                        e.message.as_deref().unwrap_or("")
                    )
                })
                .collect();
            if !details.is_empty() {
                labeled = labeled.with_help(details.join("\n"));
            }
        }
        return labeled;
    }
    LabeledError::new(format!("BigQuery API error (HTTP {status})"))
        .with_help(body.chars().take(500).collect::<String>())
}

// --- API request/response types ---

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryRequest {
    query: String,
    use_legacy_sql: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_results: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dry_run: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub schema: Option<TableSchema>,
    pub rows: Option<Vec<TableRow>>,
    pub total_rows: Option<String>,
    pub total_bytes_processed: Option<String>,
    pub job_complete: Option<bool>,
    pub job_reference: Option<JobReference>,
    pub page_token: Option<String>,
    pub cache_hit: Option<bool>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetQueryResultsResponse {
    pub schema: Option<TableSchema>,
    pub rows: Option<Vec<TableRow>>,
    pub total_rows: Option<String>,
    pub page_token: Option<String>,
    pub job_complete: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub fields: Option<Vec<TableFieldSchema>>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TableFieldSchema {
    pub name: Option<String>,
    pub r#type: Option<String>,
    pub mode: Option<String>,
    pub description: Option<String>,
    pub fields: Option<Vec<TableFieldSchema>>,
}

#[derive(Deserialize, Debug)]
pub struct TableRow {
    pub f: Option<Vec<TableCell>>,
}

#[derive(Deserialize, Debug)]
pub struct TableCell {
    pub v: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobReference {
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub location: Option<String>,
}

// --- Table data list types (tabledata.list) ---

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableDataListResponse {
    pub total_rows: Option<String>,
    pub page_token: Option<String>,
    pub rows: Option<Vec<TableRow>>,
}

// --- Dataset list types ---

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DatasetListResponse {
    pub datasets: Option<Vec<DatasetListItem>>,
    pub next_page_token: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DatasetListItem {
    pub dataset_reference: Option<DatasetReference>,
    pub friendly_name: Option<String>,
    pub id: Option<String>,
    pub location: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DatasetReference {
    pub dataset_id: Option<String>,
    pub project_id: Option<String>,
}

// --- Table list types ---

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableListResponse {
    pub tables: Option<Vec<TableListItem>>,
    pub next_page_token: Option<String>,
    pub total_items: Option<i64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableListItem {
    pub table_reference: Option<TableReference>,
    pub friendly_name: Option<String>,
    pub id: Option<String>,
    pub r#type: Option<String>,
    pub creation_time: Option<String>,
    pub expiration_time: Option<String>,
    // Note: tables.list returns row/byte counts in a nested "view" in some API versions,
    // but the standard response doesn't include them directly. We fetch via get_table if needed.
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableReference {
    pub project_id: Option<String>,
    pub dataset_id: Option<String>,
    pub table_id: Option<String>,
}

// --- Table resource (for schema) ---

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TableResource {
    pub table_reference: Option<TableReference>,
    pub schema: Option<TableSchema>,
    pub num_rows: Option<String>,
    pub num_bytes: Option<String>,
    pub creation_time: Option<String>,
    pub last_modified_time: Option<String>,
    pub r#type: Option<String>,
    pub description: Option<String>,
}

// --- Error response ---

#[derive(Deserialize)]
struct ErrorResponse {
    error: Option<BqApiError>,
}

#[derive(Deserialize)]
struct BqApiError {
    code: u16,
    message: String,
    errors: Option<Vec<BqApiErrorDetail>>,
}

#[derive(Deserialize)]
struct BqApiErrorDetail {
    reason: Option<String>,
    message: Option<String>,
}
