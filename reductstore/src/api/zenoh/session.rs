// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::zenoh::{
    attachments, queryable::QueryablePipeline, subscriber::SubscriberPipeline,
};
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::{ComponentError, StateKeeper};
use bytes::Bytes;
use log::{debug, error, info, warn};
use reduct_base::error::ErrorCode;
use reduct_base::io::ReadRecord;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

use zenoh::config::Config;
use zenoh::sample::Sample;
use zenoh::Session;

/// Holds temporary credential files to keep them alive for the session duration.
///
/// When inline TLS certificates or auth dictionaries are provided via environment variables,
/// they are written to temporary files. This struct holds those files to prevent
/// them from being deleted while the Zenoh session is active.
#[allow(dead_code)]
struct CredentialFiles {
    tls_root_ca: Option<NamedTempFile>,
    tls_connect_cert: Option<NamedTempFile>,
    tls_connect_key: Option<NamedTempFile>,
    auth_dictionary: Option<NamedTempFile>,
}

/// Runs the Zenoh session, creating subscribers and queryables based on configuration.
pub(crate) async fn run_session(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), SessionError> {
    info!(
        "Starting Zenoh API runtime: bucket='{}', sub_keyexprs={}, query_keyexprs={}",
        config.bucket,
        config.sub_keyexprs.as_deref().unwrap_or("<disabled>"),
        config.query_keyexprs.as_deref().unwrap_or("<disabled>")
    );

    validate_label_codec()?;

    let components = {
        let mut logged_wait = false;
        loop {
            match state_keeper.get_anonymous().await {
                Ok(components) => break components,
                Err(err) if err.status() == ErrorCode::ServiceUnavailable => {
                    if !logged_wait {
                        info!("Zenoh API waiting for server components to initialize");
                        logged_wait = true;
                    }
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(err) => return Err(SessionError::Component(err)),
            }
        }
    };

    // Ensure the target bucket exists (auto-create if not)
    ensure_bucket_exists(&components, &config.bucket).await?;

    // Build Zenoh configuration (including credential files that must stay alive)
    let (zenoh_config, _credential_files) = build_zenoh_config(&config)?;

    // Open Zenoh session
    info!("Opening Zenoh session...");
    let session = zenoh::open(zenoh_config)
        .await
        .map_err(|e| SessionError::ZenohOpen(e.to_string()))?;

    info!("Zenoh session opened successfully");

    // Create pipelines
    let subscriber_pipeline = Arc::new(SubscriberPipeline::new(
        config.clone(),
        Arc::clone(&components),
    ));
    let queryable_pipeline = Arc::new(QueryablePipeline::new(
        config.clone(),
        Arc::clone(&components),
    ));

    // Bootstrap pipelines (validation)
    subscriber_pipeline
        .bootstrap()
        .await
        .map_err(SessionError::Subscriber)?;
    queryable_pipeline
        .bootstrap()
        .await
        .map_err(SessionError::Queryable)?;

    // Spawn subscriber tasks (only if sub_keyexprs is set)
    let subscriber_handles = if config.sub_keyexprs.is_some() {
        spawn_subscribers(&session, &config, Arc::clone(&subscriber_pipeline)).await?
    } else {
        Vec::new()
    };

    // Spawn queryable tasks (only if query_keyexprs is set)
    let queryable_handles = if config.query_keyexprs.is_some() {
        spawn_queryables(&session, &config, queryable_pipeline).await?
    } else {
        Vec::new()
    };

    info!(
        "Zenoh API runtime started: {} subscribers, {} queryables",
        subscriber_handles.len(),
        queryable_handles.len()
    );

    // Wait for shutdown signal
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Zenoh API runtime received shutdown signal");
                    break;
                }
            }
        }
    }

    // Clean shutdown - close session
    info!("Closing Zenoh session...");
    session
        .close()
        .await
        .map_err(|e| SessionError::ZenohClose(e.to_string()))?;

    info!("Zenoh API runtime terminated gracefully");
    Ok(())
}

/// Ensures the target bucket exists, creating it with default settings if not.
async fn ensure_bucket_exists(
    components: &Arc<crate::core::components::Components>,
    bucket_name: &str,
) -> Result<(), SessionError> {
    match components.storage.get_bucket(bucket_name).await {
        Ok(_) => {
            info!("Zenoh target bucket '{}' exists", bucket_name);
            Ok(())
        }
        Err(_) => {
            info!(
                "Zenoh target bucket '{}' does not exist, creating...",
                bucket_name
            );
            components
                .storage
                .create_bucket(bucket_name, BucketSettings::default())
                .await
                .map_err(|e| {
                    SessionError::InvalidConfig(format!(
                        "Failed to create bucket '{}': {}",
                        bucket_name, e
                    ))
                })?;
            info!("Zenoh target bucket '{}' created successfully", bucket_name);
            Ok(())
        }
    }
}

fn build_zenoh_config(config: &ZenohApiConfig) -> Result<(Config, CredentialFiles), SessionError> {
    // Build base config: inline config > config file path > error
    let mut zenoh_config = if let Some(ref inline_config) = config.config_inline {
        info!("Building Zenoh config from inline string");
        parse_inline_config(inline_config)?
    } else if let Some(ref config_path) = config.config_path {
        info!("Loading Zenoh config from file: {}", config_path);
        load_config_file(config_path)?
    } else {
        return Err(SessionError::InvalidConfig(
            "Either RS_ZENOH_CONFIG or RS_ZENOH_CONFIG_PATH must be set".to_string(),
        ));
    };

    // Create credential files and inject TLS/auth config
    let credential_files = inject_credentials(&mut zenoh_config, config)?;

    Ok((zenoh_config, credential_files))
}

/// Creates temp files for inline credentials and injects paths into Zenoh config.
fn inject_credentials(
    zenoh_config: &mut Config,
    config: &ZenohApiConfig,
) -> Result<CredentialFiles, SessionError> {
    let mut cred_files = CredentialFiles {
        tls_root_ca: None,
        tls_connect_cert: None,
        tls_connect_key: None,
        auth_dictionary: None,
    };

    // TLS Root CA Certificate (for validating server cert)
    if let Some(ref cert_content) = config.tls_root_ca_cert {
        let temp_file = write_credential_file("zenoh_root_ca", ".pem", cert_content)?;
        let path = temp_file.path().to_string_lossy().to_string();
        info!("Injecting TLS root CA certificate from inline config");
        zenoh_config
            .insert_json5(
                "transport/link/tls/root_ca_certificate",
                &format!("\"{}\"", path),
            )
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set TLS root CA path: {}", e))
            })?;
        cred_files.tls_root_ca = Some(temp_file);
    }

    // TLS Connect Certificate (for mTLS client authentication)
    if let Some(ref cert_content) = config.tls_connect_cert {
        let temp_file = write_credential_file("zenoh_connect_cert", ".pem", cert_content)?;
        let path = temp_file.path().to_string_lossy().to_string();
        info!("Injecting mTLS client certificate from inline config");
        zenoh_config
            .insert_json5(
                "transport/link/tls/connect_certificate",
                &format!("\"{}\"", path),
            )
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set mTLS client cert path: {}", e))
            })?;
        cred_files.tls_connect_cert = Some(temp_file);
    }

    // TLS Connect Private Key (for mTLS client authentication)
    if let Some(ref key_content) = config.tls_connect_key {
        let temp_file = write_credential_file("zenoh_connect_key", ".pem", key_content)?;
        let path = temp_file.path().to_string_lossy().to_string();
        info!("Injecting mTLS client private key from inline config");
        zenoh_config
            .insert_json5(
                "transport/link/tls/connect_private_key",
                &format!("\"{}\"", path),
            )
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set mTLS client key path: {}", e))
            })?;
        cred_files.tls_connect_key = Some(temp_file);
    }

    // User/Password Dictionary (for routers/peers accepting connections)
    if let Some(ref dict_content) = config.auth_dictionary {
        let temp_file = write_credential_file("zenoh_auth_dict", ".txt", dict_content)?;
        let path = temp_file.path().to_string_lossy().to_string();
        info!("Injecting auth dictionary from inline config");
        zenoh_config
            .insert_json5(
                "transport/auth/usrpwd/dictionary_file",
                &format!("\"{}\"", path),
            )
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set auth dictionary path: {}", e))
            })?;
        cred_files.auth_dictionary = Some(temp_file);
    }

    Ok(cred_files)
}

/// Writes credential content to a temporary file.
fn write_credential_file(
    prefix: &str,
    suffix: &str,
    content: &str,
) -> Result<NamedTempFile, SessionError> {
    let mut temp_file = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(suffix)
        .tempfile()
        .map_err(|e| {
            SessionError::InvalidConfig(format!("Failed to create temp file for {}: {}", prefix, e))
        })?;

    temp_file.write_all(content.as_bytes()).map_err(|e| {
        SessionError::InvalidConfig(format!(
            "Failed to write credential content for {}: {}",
            prefix, e
        ))
    })?;

    temp_file.flush().map_err(|e| {
        SessionError::InvalidConfig(format!("Failed to flush credential file {}: {}", prefix, e))
    })?;

    debug!(
        "Created credential temp file: {}",
        temp_file.path().display()
    );

    Ok(temp_file)
}

/// Parses an inline config string like "mode=client;peer=localhost:7447"
fn parse_inline_config(inline: &str) -> Result<Config, SessionError> {
    let mut zenoh_config = Config::default();

    for part in inline.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim().to_lowercase();
            let value = value.trim();

            match key.as_str() {
                "mode" => {
                    zenoh_config
                        .insert_json5("mode", &format!("\"{}\"", value))
                        .map_err(|e| {
                            SessionError::InvalidConfig(format!("Failed to set mode: {}", e))
                        })?;
                }
                "peer" | "connect" => {
                    // Support comma-separated peers
                    let endpoints: Vec<&str> = value.split(',').map(|s| s.trim()).collect();
                    let endpoints_json = serde_json::to_string(&endpoints).map_err(|e| {
                        SessionError::InvalidConfig(format!(
                            "Failed to serialize connect endpoints: {}",
                            e
                        ))
                    })?;
                    zenoh_config
                        .insert_json5("connect/endpoints", &endpoints_json)
                        .map_err(|e| {
                            SessionError::InvalidConfig(format!(
                                "Failed to set connect endpoints: {}",
                                e
                            ))
                        })?;
                }
                "listen" => {
                    let endpoints: Vec<&str> = value.split(',').map(|s| s.trim()).collect();
                    let endpoints_json = serde_json::to_string(&endpoints).map_err(|e| {
                        SessionError::InvalidConfig(format!(
                            "Failed to serialize listen endpoints: {}",
                            e
                        ))
                    })?;
                    zenoh_config
                        .insert_json5("listen/endpoints", &endpoints_json)
                        .map_err(|e| {
                            SessionError::InvalidConfig(format!(
                                "Failed to set listen endpoints: {}",
                                e
                            ))
                        })?;
                }
                _ => {
                    warn!("Unknown inline config key '{}', ignoring", key);
                }
            }
        } else {
            warn!("Invalid inline config part '{}', expected key=value", part);
        }
    }

    Ok(zenoh_config)
}

/// Loads a Zenoh config from a JSON5 file.
fn load_config_file(path: &str) -> Result<Config, SessionError> {
    let path = Path::new(path);
    if !path.exists() {
        return Err(SessionError::InvalidConfig(format!(
            "Config file does not exist: {}",
            path.display()
        )));
    }

    Config::from_file(path).map_err(|e| {
        SessionError::InvalidConfig(format!(
            "Failed to load config file '{}': {}",
            path.display(),
            e
        ))
    })
}

async fn spawn_subscribers(
    session: &Session,
    config: &ZenohApiConfig,
    pipeline: Arc<SubscriberPipeline>,
) -> Result<Vec<tokio::task::JoinHandle<()>>, SessionError> {
    let mut handles = Vec::new();

    // Use the configured key expression (guaranteed to be Some by caller)
    let key_expr = config.sub_keyexprs.as_ref().unwrap();

    info!("Declaring Zenoh subscriber on key expression: {}", key_expr);

    let subscriber = session.declare_subscriber(key_expr).await.map_err(|e| {
        SessionError::Subscriber(format!(
            "Failed to declare subscriber on '{}': {}",
            key_expr, e
        ))
    })?;

    let pipeline_clone = Arc::clone(&pipeline);
    let key_expr_clone = key_expr.clone();

    let handle = tokio::spawn(async move {
        loop {
            match subscriber.recv_async().await {
                Ok(sample) => {
                    if let Err(e) = handle_sample(&pipeline_clone, sample).await {
                        warn!(
                            "Failed to handle Zenoh sample on '{}': {}",
                            key_expr_clone, e
                        );
                    }
                }
                Err(e) => {
                    error!("Subscriber '{}' recv error: {}", key_expr_clone, e);
                    break;
                }
            }
        }
    });

    handles.push(handle);

    Ok(handles)
}

async fn handle_sample(
    pipeline: &SubscriberPipeline,
    sample: Sample,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let key_expr = sample.key_expr().as_str();
    let payload = Bytes::from(sample.payload().to_bytes().to_vec());

    // Extract attachment if present (labels)
    let attachment = sample.attachment().map(|att| att.to_bytes().to_vec());

    // Extract encoding from Zenoh sample (defaults to application/octet-stream)
    let content_type = sample.encoding().to_string();

    // Extract timestamp from Zenoh sample if available
    let timestamp = sample.timestamp().map(|ts| {
        // Convert Zenoh timestamp to microseconds
        let ntp64 = ts.get_time().as_u64();
        // NTP64 to Unix microseconds conversion
        // NTP epoch is 1900-01-01, Unix epoch is 1970-01-01
        // Difference is 2208988800 seconds
        const NTP_TO_UNIX_OFFSET: u64 = 2_208_988_800;
        let secs = (ntp64 >> 32).saturating_sub(NTP_TO_UNIX_OFFSET);
        let frac = ntp64 & 0xFFFF_FFFF;
        let micros = (frac * 1_000_000) >> 32;
        secs * 1_000_000 + micros
    });

    debug!(
        "Received Zenoh sample: key={}, bytes={}, encoding={}, has_attachment={}, timestamp={:?}",
        key_expr,
        payload.len(),
        content_type,
        attachment.is_some(),
        timestamp
    );

    pipeline
        .handle_sample(key_expr, payload, attachment, timestamp, content_type)
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

async fn spawn_queryables(
    session: &Session,
    config: &ZenohApiConfig,
    pipeline: Arc<QueryablePipeline>,
) -> Result<Vec<tokio::task::JoinHandle<()>>, SessionError> {
    let mut handles = Vec::new();

    let key_expr = config.query_keyexprs.as_ref().unwrap();
    info!("Declaring Zenoh queryable on key expression: {}", key_expr);

    let queryable = session
        .declare_queryable(key_expr.as_str())
        .await
        .map_err(|e| SessionError::Queryable(format!("Failed to declare queryable: {}", e)))?;

    let handle = tokio::spawn(async move {
        loop {
            match queryable.recv_async().await {
                Ok(query) => {
                    let key_expr = query.key_expr().as_str().to_string();
                    let params = expand_query_params(query.selector().parameters());

                    debug!(
                        "Received Zenoh query: key={}, params={:?}",
                        key_expr, params
                    );

                    match pipeline.handle_query(&key_expr, &params).await {
                        Ok(result) => {
                            if let Err(e) = send_query_reply(&query, result).await {
                                warn!("Failed to send query reply: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("Query handler error for '{}': {}", key_expr, e);
                            if let Err(reply_err) = query
                                .reply_err(zenoh::bytes::ZBytes::from(e.to_string().into_bytes()))
                                .await
                            {
                                warn!("Failed to send error reply: {}", reply_err);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Queryable recv error: {}", e);
                    break;
                }
            }
        }
    });

    handles.push(handle);
    Ok(handles)
}

fn expand_query_params(params: &zenoh::query::Parameters) -> HashMap<String, String> {
    let mut expanded = HashMap::new();
    for (key, value) in params.iter() {
        let raw = value.to_string();
        if raw.contains('&') {
            let mut first = true;
            for part in raw.split('&') {
                if part.is_empty() {
                    continue;
                }
                if first {
                    expanded.insert((*key).to_string(), part.to_string());
                    first = false;
                    continue;
                }
                if let Some((extra_key, extra_value)) = part.split_once('=') {
                    expanded.insert(extra_key.to_string(), extra_value.to_string());
                } else {
                    expanded.insert(part.to_string(), String::new());
                }
            }
        } else {
            expanded.insert((*key).to_string(), raw);
        }
    }
    expanded
}

async fn send_query_reply(
    query: &zenoh::query::Query,
    result: crate::api::zenoh::queryable::QueryResult,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    use crate::api::zenoh::queryable::QueryResult;

    match result {
        QueryResult::Record(mut reader) => {
            // Read the record content using the ReadRecord trait
            let mut data = Vec::new();
            while let Some(chunk_result) = reader.read_chunk() {
                match chunk_result {
                    Ok(chunk) => data.extend_from_slice(&chunk),
                    Err(e) => return Err(Box::new(e)),
                }
            }

            // Get labels and content_type from the record meta
            let labels = reader.meta().labels().clone();
            let content_type = reader.meta().content_type();
            let attachment = if labels.is_empty() {
                None
            } else {
                Some(attachments::serialize_labels(&labels)?)
            };

            // Build reply with encoding from content_type
            let key_expr = query.key_expr().clone();
            let mut reply_builder = query
                .reply(key_expr, data)
                .encoding(zenoh::bytes::Encoding::from(content_type));

            if let Some(att) = attachment {
                reply_builder = reply_builder.attachment(att);
            }

            reply_builder.await.map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn Error + Send + Sync>
            })?;
        }
        QueryResult::Stream {
            receiver,
            io_config,
        } => {
            // For streaming queries, send multiple replies
            let query_rx = receiver.upgrade().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn Error + Send + Sync>
            })?;

            let mut count = 0;
            let limit = io_config.batch_max_records;

            loop {
                let mut record = {
                    let mut rx = query_rx.write().await.map_err(|e| {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )) as Box<dyn Error + Send + Sync>
                    })?;

                    match rx.recv().await {
                        Some(Ok(record)) => record,
                        Some(Err(e)) => return Err(Box::new(e)),
                        None => break,
                    }
                };

                // Read record data using the ReadRecord trait
                let mut data = Vec::new();
                while let Some(chunk_result) = record.read_chunk() {
                    match chunk_result {
                        Ok(chunk) => data.extend_from_slice(&chunk),
                        Err(e) => return Err(Box::new(e)),
                    }
                }

                // Get labels and content_type from the record meta
                let labels = record.meta().labels().clone();
                let content_type = record.meta().content_type();
                let attachment = if labels.is_empty() {
                    None
                } else {
                    Some(attachments::serialize_labels(&labels)?)
                };

                // Build reply with encoding from content_type
                let key_expr = query.key_expr().clone();
                let mut reply_builder = query
                    .reply(key_expr, data)
                    .encoding(zenoh::bytes::Encoding::from(content_type));

                if let Some(att) = attachment {
                    reply_builder = reply_builder.attachment(att);
                }

                reply_builder.await.map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )) as Box<dyn Error + Send + Sync>
                })?;

                count += 1;
                if count >= limit {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn validate_label_codec() -> Result<(), SessionError> {
    let mut labels = Labels::new();
    labels.insert("codec".into(), "probe".into());

    let encoded = attachments::serialize_labels(&labels)?;
    let decoded = attachments::deserialize_labels(&encoded)?;

    if decoded != labels {
        return Err(SessionError::AttachmentCodecMismatch);
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) enum SessionError {
    Component(ComponentError),
    Subscriber(String),
    Queryable(String),
    AttachmentCodec(serde_json::Error),
    AttachmentCodecMismatch,
    ZenohOpen(String),
    ZenohClose(String),
    InvalidConfig(String),
}

impl Display for SessionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Component(err) => write!(f, "{}", err),
            SessionError::Subscriber(err) => write!(f, "Subscriber pipeline error: {}", err),
            SessionError::Queryable(err) => write!(f, "Queryable pipeline error: {}", err),
            SessionError::AttachmentCodec(err) => {
                write!(f, "Failed to serialize labels: {}", err)
            }
            SessionError::AttachmentCodecMismatch => {
                write!(f, "Label codec roundtrip produced mismatched values")
            }
            SessionError::ZenohOpen(err) => write!(f, "Failed to open Zenoh session: {}", err),
            SessionError::ZenohClose(err) => write!(f, "Failed to close Zenoh session: {}", err),
            SessionError::InvalidConfig(err) => write!(f, "Invalid Zenoh configuration: {}", err),
        }
    }
}

impl Error for SessionError {}

impl From<serde_json::Error> for SessionError {
    fn from(value: serde_json::Error) -> Self {
        SessionError::AttachmentCodec(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::fs;

    #[rstest]
    fn writes_credential_file_with_correct_content() {
        let content = "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----";
        let temp_file = write_credential_file("test_cert", ".pem", content).unwrap();

        let read_content = fs::read_to_string(temp_file.path()).unwrap();
        assert_eq!(read_content, content);
    }

    #[rstest]
    fn writes_credential_file_uses_prefix_and_suffix() {
        let content = "test";
        let temp_file = write_credential_file("my_prefix", ".txt", content).unwrap();

        let filename = temp_file.path().file_name().unwrap().to_string_lossy();
        assert!(filename.starts_with("my_prefix"));
        assert!(filename.ends_with(".txt"));
    }

    #[rstest]
    fn injects_tls_root_ca_into_config() {
        let mut zenoh_config = Config::default();
        let api_config = ZenohApiConfig {
            tls_root_ca_cert: Some("root-ca-content".to_string()),
            ..Default::default()
        };

        let cred_files = inject_credentials(&mut zenoh_config, &api_config).unwrap();

        // Credential file should exist
        assert!(cred_files.tls_root_ca.is_some());
        let temp_file = cred_files.tls_root_ca.as_ref().unwrap();
        let read_content = fs::read_to_string(temp_file.path()).unwrap();
        assert_eq!(read_content, "root-ca-content");
    }

    #[rstest]
    fn injects_mtls_credentials_into_config() {
        let mut zenoh_config = Config::default();
        let api_config = ZenohApiConfig {
            tls_connect_cert: Some("client-cert".to_string()),
            tls_connect_key: Some("client-key".to_string()),
            ..Default::default()
        };

        let cred_files = inject_credentials(&mut zenoh_config, &api_config).unwrap();

        assert!(cred_files.tls_connect_cert.is_some());
        assert!(cred_files.tls_connect_key.is_some());

        let cert_content =
            fs::read_to_string(cred_files.tls_connect_cert.as_ref().unwrap().path()).unwrap();
        let key_content =
            fs::read_to_string(cred_files.tls_connect_key.as_ref().unwrap().path()).unwrap();
        assert_eq!(cert_content, "client-cert");
        assert_eq!(key_content, "client-key");
    }

    #[rstest]
    fn injects_auth_dictionary_into_config() {
        let mut zenoh_config = Config::default();
        let api_config = ZenohApiConfig {
            auth_dictionary: Some("user1:pass1\nuser2:pass2".to_string()),
            ..Default::default()
        };

        let cred_files = inject_credentials(&mut zenoh_config, &api_config).unwrap();

        assert!(cred_files.auth_dictionary.is_some());
        let read_content =
            fs::read_to_string(cred_files.auth_dictionary.as_ref().unwrap().path()).unwrap();
        assert_eq!(read_content, "user1:pass1\nuser2:pass2");
    }

    #[rstest]
    fn no_credentials_leaves_config_unchanged() {
        let mut zenoh_config = Config::default();
        let api_config = ZenohApiConfig::default();

        let cred_files = inject_credentials(&mut zenoh_config, &api_config).unwrap();

        assert!(cred_files.tls_root_ca.is_none());
        assert!(cred_files.tls_connect_cert.is_none());
        assert!(cred_files.tls_connect_key.is_none());
        assert!(cred_files.auth_dictionary.is_none());
    }
}
