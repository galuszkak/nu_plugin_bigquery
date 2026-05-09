use googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1::big_query_read_client::BigQueryReadClient;
use tonic::{transport::{Channel, ClientTlsConfig}, metadata::MetadataValue, service::Interceptor};
use nu_protocol::LabeledError;

#[derive(Clone)]
pub struct AuthInterceptor {
    token_metadata: MetadataValue<tonic::metadata::Ascii>,
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token_metadata.clone());
        Ok(request)
    }
}

pub type StorageClient =
    BigQueryReadClient<tonic::codegen::InterceptedService<Channel, AuthInterceptor>>;

pub async fn create_storage_client(token: String) -> Result<StorageClient, LabeledError> {
    let tls_config = ClientTlsConfig::new().with_enabled_roots();

    let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
        .tls_config(tls_config)
        .map_err(|e| LabeledError::new(format!("Failed to setup TLS: {}", e)))?
        .connect()
        .await
        .map_err(|e| {
            LabeledError::new(format!("Failed to connect to BigQuery Storage API: {}", e))
        })?;

    let token_header = format!("Bearer {}", token);
    let token_metadata = MetadataValue::try_from(token_header)
        .map_err(|e| LabeledError::new(format!("Invalid metadata: {}", e)))?;

    let interceptor = AuthInterceptor { token_metadata };
    let client = BigQueryReadClient::with_interceptor(channel, interceptor);

    Ok(client)
}
