use std::path::PathBuf;

use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use nu_plugin::EngineInterface;
use nu_protocol::{LabeledError, Span};

const BQ_SCOPES: [&str; 1] = ["https://www.googleapis.com/auth/bigquery"];

/// Resolve a GCP token provider.
///
/// Resolution order:
/// 1. `--credentials` flag (loaded directly via `CustomServiceAccount::from_file`)
/// 2. `$env.GOOGLE_APPLICATION_CREDENTIALS` from Nushell environment (same direct load)
/// 3. Application Default Credentials (gcloud auth, metadata server, gcloud CLI)
pub async fn resolve_auth(
    credentials_path: Option<&str>,
    engine: &EngineInterface,
) -> Result<AccessTokenCredentials, LabeledError> {
    // 1. Explicit --credentials flag
    if let Some(path) = credentials_path {
        return load_credentials_from_file(path).await;
    }

    // 2. Nushell env var
    if let Some(path) = get_env_string(engine, "GOOGLE_APPLICATION_CREDENTIALS")
        && !path.is_empty()
    {
        return load_credentials_from_file(&path).await;
    }

    // 3. Application Default Credentials
    Builder::default()
        .with_scopes(BQ_SCOPES)
        .build_access_token_credentials()
        .map_err(|e| {
            LabeledError::new("BigQuery authentication failed").with_help(format!(
                "No valid credentials found. Error: {e}\n\n\
                 Try one of:\n\
                 - Run `gcloud auth application-default login`\n\
                 - Pass --credentials /path/to/credentials.json\n\
                 - Set $env.GOOGLE_APPLICATION_CREDENTIALS = \"/path/to/credentials.json\""
            ))
        })
}

async fn load_credentials_from_file(path: &str) -> Result<AccessTokenCredentials, LabeledError> {
    if !std::path::Path::new(path).exists() {
        return Err(LabeledError::new("Credentials file not found")
            .with_help(format!("No file at '{path}'")));
    }

    let contents = std::fs::read_to_string(path).map_err(|e| {
        LabeledError::new("Failed to read credentials file")
            .with_help(format!("Could not read file at '{path}': {e}"))
    })?;

    let json: serde_json::Value = serde_json::from_str(&contents).map_err(|e| {
        LabeledError::new("Failed to parse credentials").with_help(format!(
            "Could not parse JSON in credentials at '{path}': {e}\n\
             Ensure the file is valid JSON."
        ))
    })?;

    // Determine the credential type directly from the JSON.
    // `google_cloud_auth` doesn't expose a generic builder that accepts a JSON object,
    // so we manually delegate to the appropriate sub-builder (e.g., service_account, authorized_user).
    let type_str = json.get("type").and_then(|v| v.as_str()).unwrap_or("");

    let credentials = match type_str {
        "service_account" => google_cloud_auth::credentials::service_account::Builder::new(json)
            .with_access_specifier(
                google_cloud_auth::credentials::service_account::AccessSpecifier::from_scopes(
                    BQ_SCOPES,
                ),
            )
            .build_access_token_credentials()
            .map_err(|e| {
                LabeledError::new("Failed to build credentials")
                    .with_help(format!("Could not build credentials from '{path}': {e}"))
            }),
        "external_account" => google_cloud_auth::credentials::external_account::Builder::new(json)
            .with_scopes(BQ_SCOPES)
            .build_access_token_credentials()
            .map_err(|e| {
                LabeledError::new("Failed to build credentials")
                    .with_help(format!("Could not build credentials from '{path}': {e}"))
            }),
        // Application default credentials often have type "authorized_user" for user accounts
        "authorized_user" => google_cloud_auth::credentials::user_account::Builder::new(json)
            .with_scopes(BQ_SCOPES)
            .build_access_token_credentials()
            .map_err(|e| {
                LabeledError::new("Failed to build credentials")
                    .with_help(format!("Could not build credentials from '{path}': {e}"))
            }),
        _ => Err(LabeledError::new("Failed to build credentials")
            .with_help(format!("Unsupported credential type: {}", type_str))),
    }?;

    Ok(credentials)
}

/// Get a Bearer token string for BigQuery API calls.
pub async fn get_token(provider: &AccessTokenCredentials) -> Result<String, LabeledError> {
    let token = provider.access_token().await.map_err(|e| {
        LabeledError::new("Failed to obtain access token").with_help(format!(
            "Could not get a BigQuery access token: {e}\n\
             Your credentials may have expired. Try `gcloud auth application-default login`."
        ))
    })?;
    Ok(token.token)
}

/// Resolve the GCP project ID.
///
/// Resolution order:
/// 1. `--project` flag
/// 2. `$env.BQ_PROJECT`
/// 3. `$env.GOOGLE_CLOUD_PROJECT`
/// 4. `$env.GCLOUD_PROJECT`
/// 5. Detect from credentials file
pub async fn resolve_project(
    project_flag: Option<&str>,
    engine: &EngineInterface,
    credentials_path: Option<&str>,
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

    // 5. Detect from credentials file
    if let Some(project_id) = extract_project_id_from_credentials(credentials_path, engine) {
        return Ok(project_id);
    }

    Err(LabeledError::new("No project ID specified").with_help(
        "Provide a project ID using one of:\n\
         - --project (-p) flag\n\
         - $env.BQ_PROJECT\n\
         - $env.GOOGLE_CLOUD_PROJECT\n\
         - Service account key file (contains project_id)\n\
         - ADC file (contains quota_project_id)",
    ))
}

fn extract_project_id_from_credentials(
    credentials_path: Option<&str>,
    engine: &EngineInterface,
) -> Option<String> {
    let path = if let Some(path) = credentials_path {
        PathBuf::from(path)
    } else if let Some(path) = get_env_string(engine, "GOOGLE_APPLICATION_CREDENTIALS") {
        PathBuf::from(path)
    } else if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        PathBuf::from(path)
    } else {
        #[cfg(target_os = "windows")]
        let p = std::env::var("APPDATA")
            .ok()
            .map(|root| PathBuf::from(root).join("gcloud/application_default_credentials.json"));

        #[cfg(not(target_os = "windows"))]
        let p = std::env::var("HOME").ok().map(|root| {
            PathBuf::from(root).join(".config/gcloud/application_default_credentials.json")
        });

        p?
    };

    #[allow(clippy::collapsible_if)]
    if let Ok(contents) = std::fs::read_to_string(&path) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&contents) {
            // Service accounts and some others have `project_id`.
            if let Some(project_id) = json.get("project_id").and_then(|v| v.as_str()) {
                return Some(project_id.to_string());
            }
            // User accounts (ADC) often have `quota_project_id`.
            if let Some(quota_project_id) = json.get("quota_project_id").and_then(|v| v.as_str()) {
                return Some(quota_project_id.to_string());
            }
        }
    }
    None
}

/// Helper to get a Nushell env var as a String.
fn get_env_string(engine: &EngineInterface, name: &str) -> Option<String> {
    engine
        .get_env_var(name)
        .ok()
        .flatten()
        .and_then(|v| v.as_str().ok().map(|s| s.to_string()))
}
