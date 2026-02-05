// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::zenoh::{
    attachments, queryable::QueryablePipeline, subscriber::SubscriberPipeline,
};
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::{ComponentError, StateKeeper};
use bytes::Bytes;
use log::{debug, error, info, warn};
use reduct_base::io::ReadRecord;
use reduct_base::Labels;
use reduct_base::error::ErrorCode;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

use zenoh::config::Config;
use zenoh::sample::Sample;
use zenoh::Session;

/// Runs the Zenoh session, creating subscribers and queryables based on configuration.
pub(crate) async fn run_session(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), SessionError> {
    info!(
        "Starting Zenoh API runtime: prefix='{}', listen={:?}, connect={:?}, mode={:?}",
        config.key_prefix, config.listen_endpoints, config.connect_endpoints, config.mode
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

    // Build Zenoh configuration
    let zenoh_config = build_zenoh_config(&config)?;

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

    // Spawn subscriber tasks
    let subscriber_handles =
        spawn_subscribers(&session, &config, Arc::clone(&subscriber_pipeline)).await?;

    // Spawn queryable tasks
    let queryable_handles = if config.enable_queryable {
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

fn build_zenoh_config(config: &ZenohApiConfig) -> Result<Config, SessionError> {
    let mut zenoh_config = Config::default();

    // Set mode if specified
    if let Some(ref mode) = config.mode {
        zenoh_config
            .insert_json5("mode", &format!("\"{}\"", mode))
            .map_err(|e| SessionError::InvalidConfig(format!("Failed to set mode: {}", e)))?;
    }

    // Set listen endpoints
    if !config.listen_endpoints.is_empty() {
        let endpoints_json = serde_json::to_string(&config.listen_endpoints).map_err(|e| {
            SessionError::InvalidConfig(format!("Failed to serialize listen endpoints: {}", e))
        })?;
        zenoh_config
            .insert_json5("listen/endpoints", &endpoints_json)
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set listen endpoints: {}", e))
            })?;
    }

    // Set connect endpoints
    if !config.connect_endpoints.is_empty() {
        let endpoints_json = serde_json::to_string(&config.connect_endpoints).map_err(|e| {
            SessionError::InvalidConfig(format!("Failed to serialize connect endpoints: {}", e))
        })?;
        zenoh_config
            .insert_json5("connect/endpoints", &endpoints_json)
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to set connect endpoints: {}", e))
            })?;
    }

    // Disable multicast scouting if requested
    if config.disable_multicast_scouting {
        zenoh_config
            .insert_json5("scouting/multicast/enabled", "false")
            .map_err(|e| {
                SessionError::InvalidConfig(format!("Failed to disable multicast: {}", e))
            })?;
    }

    Ok(zenoh_config)
}

async fn spawn_subscribers(
    session: &Session,
    config: &ZenohApiConfig,
    pipeline: Arc<SubscriberPipeline>,
) -> Result<Vec<tokio::task::JoinHandle<()>>, SessionError> {
    let mut handles = Vec::new();

    // Subscribe to patterns from config, or use default prefix pattern
    let patterns: Vec<String> = if config.subscribe_patterns.is_empty() {
        vec![format!("{}/**", config.key_prefix)]
    } else {
        config.subscribe_patterns.clone()
    };

    for pattern in patterns {
        info!("Declaring Zenoh subscriber on key expression: {}", pattern);

        let subscriber = session.declare_subscriber(&pattern).await.map_err(|e| {
            SessionError::Subscriber(format!(
                "Failed to declare subscriber on '{}': {}",
                pattern, e
            ))
        })?;

        let pipeline_clone = Arc::clone(&pipeline);
        let pattern_clone = pattern.clone();

        let handle = tokio::spawn(async move {
            loop {
                match subscriber.recv_async().await {
                    Ok(sample) => {
                        if let Err(e) = handle_sample(&pipeline_clone, sample).await {
                            warn!(
                                "Failed to handle Zenoh sample on '{}': {}",
                                pattern_clone, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("Subscriber '{}' recv error: {}", pattern_clone, e);
                        break;
                    }
                }
            }
        });

        handles.push(handle);
    }

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
        "Received Zenoh sample: key={}, bytes={}, has_attachment={}, timestamp={:?}",
        key_expr,
        payload.len(),
        attachment.is_some(),
        timestamp
    );

    pipeline
        .handle_sample(key_expr, payload, attachment, timestamp)
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

async fn spawn_queryables(
    session: &Session,
    config: &ZenohApiConfig,
    pipeline: Arc<QueryablePipeline>,
) -> Result<Vec<tokio::task::JoinHandle<()>>, SessionError> {
    let mut handles = Vec::new();

    // Declare queryable on the key prefix to handle all queries
    let key_expr = format!("{}/**", config.key_prefix);
    info!("Declaring Zenoh queryable on key expression: {}", key_expr);

    let queryable = session
        .declare_queryable(&key_expr)
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

            // Get labels from the record meta
            let labels = reader.meta().labels().clone();
            let attachment = if labels.is_empty() {
                None
            } else {
                Some(attachments::serialize_labels(&labels)?)
            };

            // Build reply
            let key_expr = query.key_expr().clone();
            let mut reply_builder = query.reply(key_expr, data);

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

                // Get labels from the record meta
                let labels = record.meta().labels().clone();
                let attachment = if labels.is_empty() {
                    None
                } else {
                    Some(attachments::serialize_labels(&labels)?)
                };

                // Build reply
                let key_expr = query.key_expr().clone();
                let mut reply_builder = query.reply(key_expr, data);

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
