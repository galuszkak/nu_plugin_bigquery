use std::sync::Arc;

use gcp_auth::TokenProvider;
use nu_plugin::EngineInterface;
use nu_protocol::{LabeledError, Span};

const BQ_SCOPES: &[&str] = &["https://www.googleapis.com/auth/bigquery"];

/// Resolve a GCP token provider.
///
/// Resolution order:
/// 1. `--credentials` flag (loaded directly via `CustomServiceAccount::from_file`)
/// 2. `$env.GOOGLE_APPLICATION_CREDENTIALS` from Nushell environment (same direct load)
/// 3. Application Default Credentials (gcloud auth, metadata server, gcloud CLI)
pub async fn resolve_auth(
    credentials_path: Option<&str>,
    engine: &EngineInterface,
) -> Result<Arc<dyn TokenProvider>, LabeledError> {
    // 1. Explicit --credentials flag
    if let Some(path) = credentials_path {
        return load_credentials_from_file(path);
    }

    // 2. Nushell env var — load the file directly instead of forwarding to
    //    process env (std::env::set_var is unsound in multi-threaded contexts).
    if let Some(path) = get_env_string(engine, "GOOGLE_APPLICATION_CREDENTIALS")
        && !path.is_empty()
    {
        return load_credentials_from_file(&path);
    }

    // 3. Provider chain: process env GOOGLE_APPLICATION_CREDENTIALS → ADC → gcloud → metadata
    gcp_auth::provider().await.map_err(|e| {
        LabeledError::new("BigQuery authentication failed").with_help(format!(
            "No valid credentials found. Error: {e}\n\n\
             Try one of:\n\
             - Run `gcloud auth application-default login`\n\
             - Pass --credentials /path/to/credentials.json\n\
             - Set $env.GOOGLE_APPLICATION_CREDENTIALS = \"/path/to/credentials.json\""
        ))
    })
}

/// Load a service account key file directly, avoiding `std::env::set_var`.
fn load_credentials_from_file(path: &str) -> Result<Arc<dyn TokenProvider>, LabeledError> {
    if !std::path::Path::new(path).exists() {
        return Err(LabeledError::new("Credentials file not found")
            .with_help(format!("No file at '{path}'")));
    }
    let sa = gcp_auth::CustomServiceAccount::from_file(path).map_err(|e| {
        LabeledError::new("Failed to load credentials").with_help(format!(
            "Could not parse credentials at '{path}': {e}\n\
             Ensure the file is a valid service account key JSON."
        ))
    })?;
    Ok(Arc::new(sa))
}

/// Get a Bearer token string for BigQuery API calls.
pub async fn get_token(provider: &dyn TokenProvider) -> Result<String, LabeledError> {
    let token = provider.token(BQ_SCOPES).await.map_err(|e| {
        LabeledError::new("Failed to obtain access token").with_help(format!(
            "Could not get a BigQuery access token: {e}\n\
             Your credentials may have expired. Try `gcloud auth application-default login`."
        ))
    })?;
    Ok(token.as_str().to_string())
}

/// Resolve the GCP project ID.
///
/// Resolution order:
/// 1. `--project` flag
/// 2. `$env.BQ_PROJECT`
/// 3. `$env.GOOGLE_CLOUD_PROJECT`
/// 4. `$env.GCLOUD_PROJECT`
/// 5. Detect from TokenProvider
pub async fn resolve_project(
    project_flag: Option<&str>,
    engine: &EngineInterface,
    provider: &dyn TokenProvider,
    _span: Span,
) -> Result<String, LabeledError> {
    // 1. Explicit --project flag
    if let Some(p) = project_flag {
        return Ok(p.to_string());
    }

    // 2-4. Environment variables
    for var in &["BQ_PROJECT", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"] {
        if let Some(val) = get_env_string(engine, var)
            && !val.is_empty()
        {
            return Ok(val);
        }
    }

    // 5. Detect from token provider
    match provider.project_id().await {
        Ok(project_id) => Ok(project_id.to_string()),
        Err(_) => Err(LabeledError::new("No project ID specified").with_help(
            "Provide a project ID using one of:\n\
             - --project (-p) flag\n\
             - $env.BQ_PROJECT\n\
             - $env.GOOGLE_CLOUD_PROJECT\n\
             - Service account key file (contains project_id)",
        )),
    }
}

/// Helper to get a Nushell env var as a String.
fn get_env_string(engine: &EngineInterface, name: &str) -> Option<String> {
    engine
        .get_env_var(name)
        .ok()
        .flatten()
        .and_then(|v| v.as_str().ok().map(|s| s.to_string()))
}
