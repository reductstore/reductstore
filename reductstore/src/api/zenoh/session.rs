// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::components::{ComponentError, StateKeeper};
use crate::api::zenoh::routing::BucketRouter;
use crate::api::zenoh::{
    attachments, attachments::QueryAttachments, queryable::QueryablePipeline,
    subscriber::IngestError, subscriber::SubscriberPipeline,
};
use crate::cfg::zenoh::{ZenohApiConfig, ZenohBlock, ZenohQueryableBlock, ZenohQueryableLocality};
use bytes::Bytes;
use log::{debug, error, info, warn};
use reduct_base::error::ErrorCode;
use reduct_base::io::ReadRecord;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::Labels;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration, Instant};

use zenoh::config::Config;
use zenoh::key_expr::KeyExpr;
use zenoh::sample::Sample;
use zenoh::time::{Timestamp, TimestampId, NTP64};
use zenoh::Session;

#[allow(dead_code)]
struct CredentialFiles {
    tls_root_ca: Option<NamedTempFile>,
    tls_connect_cert: Option<NamedTempFile>,
    tls_connect_key: Option<NamedTempFile>,
    auth_dictionary: Option<NamedTempFile>,
}

pub(crate) async fn run_session(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), SessionError> {
    info!("Starting Zenoh API runtime: {}", config);

    let subscriber_routers = build_routers(&config.subscribers, "subscriber")?;
    let queryable_routers = build_routers(
        &config
            .queryables
            .iter()
            .map(|block| block.route.clone())
            .collect::<Vec<_>>(),
        "queryable",
    )?;

    validate_keyexprs(&config)?;
    warn_on_overlapping_keyexprs(&config);

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

    let mut declared_buckets = HashSet::new();
    for router in subscriber_routers.iter().chain(queryable_routers.iter()) {
        if let Some(bucket) = router.declared_bucket() {
            if declared_buckets.insert(bucket.to_string()) {
                ensure_bucket_exists(&components, bucket).await?;
            }
        }
    }

    let server_info = components
        .storage
        .info()
        .await
        .map_err(|err| SessionError::InvalidConfig(err.to_string()))?;
    info!(
        "Zenoh API runtime ready (storage version {})",
        server_info.version
    );

    let (zenoh_config, _credential_files) = build_zenoh_config(&config)?;

    info!("Opening Zenoh session...");
    let session = zenoh::open(zenoh_config)
        .await
        .map_err(|e| SessionError::ZenohOpen(e.to_string()))?;

    info!("Zenoh session opened successfully");

    let mut handles = Vec::new();
    let mut declared = 0;

    for (block, router) in config.subscribers.iter().zip(subscriber_routers) {
        let pipeline = Arc::new(SubscriberPipeline::new(router, Arc::clone(&components)));
        info!(
            "Zenoh subscriber block '{}': keyexprs=[{}] {}",
            block.id,
            block.keyexprs.join(","),
            pipeline.describe()
        );
        declared += spawn_subscriber_block(&session, block, pipeline, &mut handles).await;
    }

    for (block, router) in config.queryables.iter().zip(queryable_routers) {
        let pipeline = Arc::new(QueryablePipeline::new(router, Arc::clone(&components)));
        info!(
            "Zenoh queryable block '{}': keyexprs=[{}] locality={} {}",
            block.route.id,
            block.route.keyexprs.join(","),
            block.locality,
            pipeline.describe()
        );
        declared += spawn_queryable_block(&session, block, pipeline, &mut handles).await;
    }

    let configured = config.subscribers.len() + config.queryables.len();
    if declared == 0 && configured > 0 {
        shutdown(session, handles).await.ok();
        return Err(SessionError::NoBlocksStarted(format!(
            "all {} configured block(s) failed to declare",
            configured
        )));
    }

    info!("Zenoh API runtime started: {} declaration(s)", declared);

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

    shutdown(session, handles).await?;

    info!("Zenoh API runtime terminated gracefully");
    Ok(())
}

async fn shutdown(session: Session, handles: Vec<JoinHandle<()>>) -> Result<(), SessionError> {
    for handle in handles {
        handle.abort();
    }

    info!("Closing Zenoh session...");
    session
        .close()
        .await
        .map_err(|e| SessionError::ZenohClose(e.to_string()))
}

fn build_routers(
    blocks: &[ZenohBlock],
    kind: &str,
) -> Result<Vec<Arc<BucketRouter>>, SessionError> {
    blocks
        .iter()
        .map(|block| {
            let router = BucketRouter::from_block(block);
            router.validate().map_err(|err| {
                SessionError::InvalidConfig(format!("{} block '{}': {}", kind, block.id, err))
            })?;

            for warning in router.config_warnings() {
                warn!("Zenoh {} block '{}': {}", kind, block.id, warning);
            }

            Ok(Arc::new(router))
        })
        .collect()
}

fn validate_keyexprs(config: &ZenohApiConfig) -> Result<(), SessionError> {
    for (kind, block) in blocks_with_kind(config) {
        for key_expr in &block.keyexprs {
            KeyExpr::try_from(key_expr.as_str()).map_err(|err| {
                SessionError::InvalidConfig(format!(
                    "{} block '{}': invalid key expression '{}': {}",
                    kind, block.id, key_expr, err
                ))
            })?;
        }
    }
    Ok(())
}

// zenoh delivers a sample to every matching subscriber, so overlapping keyexprs store it twice
fn warn_on_overlapping_keyexprs(config: &ZenohApiConfig) {
    for (block_a, key_a, block_b, key_b) in keyexpr_pairs(&config.subscribers) {
        if keyexprs_intersect(key_a, key_b) {
            warn!(
                "Zenoh subscriber key expressions '{}' (block '{}') and '{}' (block '{}') \
                 intersect, so matching samples are written more than once",
                key_a, block_a, key_b, block_b
            );
        }
    }

    let queryable_routes: Vec<ZenohBlock> = config
        .queryables
        .iter()
        .map(|block| block.route.clone())
        .collect();
    for (block_a, key_a, block_b, key_b) in keyexpr_pairs(&queryable_routes) {
        if keyexprs_intersect(key_a, key_b) {
            debug!(
                "Zenoh queryable key expressions '{}' (block '{}') and '{}' (block '{}') intersect",
                key_a, block_a, key_b, block_b
            );
        }
    }
}

fn keyexprs_intersect(a: &str, b: &str) -> bool {
    match (KeyExpr::try_from(a), KeyExpr::try_from(b)) {
        (Ok(a), Ok(b)) => a.intersects(&b),
        _ => false,
    }
}

fn keyexpr_pairs(blocks: &[ZenohBlock]) -> Vec<(&str, &str, &str, &str)> {
    let flat: Vec<(&str, &str)> = blocks
        .iter()
        .flat_map(|block| {
            block
                .keyexprs
                .iter()
                .map(move |key| (block.id.as_str(), key.as_str()))
        })
        .collect();

    let mut pairs = Vec::new();
    for (index, (block_a, key_a)) in flat.iter().enumerate() {
        for (block_b, key_b) in flat.iter().skip(index + 1) {
            pairs.push((*block_a, *key_a, *block_b, *key_b));
        }
    }
    pairs
}

fn blocks_with_kind(config: &ZenohApiConfig) -> Vec<(&'static str, &ZenohBlock)> {
    config
        .subscribers
        .iter()
        .map(|block| ("subscriber", block))
        .chain(
            config
                .queryables
                .iter()
                .map(|block| ("queryable", &block.route)),
        )
        .collect()
}

async fn ensure_bucket_exists(
    components: &Arc<crate::api::Components>,
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

    let credential_files = inject_credentials(&mut zenoh_config, config)?;

    Ok((zenoh_config, credential_files))
}

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

fn parse_inline_config(inline: &str) -> Result<Config, SessionError> {
    let trimmed = inline.trim();

    // If it looks like JSON5
    if trimmed.starts_with('{') {
        return Config::from_json5(trimmed)
            .map_err(|e| SessionError::InvalidConfig(format!("Invalid JSON5 config: {}", e)));
    }

    // Otherwise, parse simple key=value;key=value format
    let mut zenoh_config = Config::default();

    for part in inline.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim();
            let value = value.trim();

            let json_value = if value == "true"
                || value == "false"
                || value.parse::<i64>().is_ok()
                || value.parse::<f64>().is_ok()
            {
                value.to_string()
            } else if value.starts_with('[') && value.ends_with(']') {
                // Array: quote each element as a string
                let inner = &value[1..value.len() - 1];
                let elements: Vec<String> = inner
                    .split(',')
                    .map(|s| format!("\"{}\"", s.trim()))
                    .collect();
                format!("[{}]", elements.join(","))
            } else {
                format!("\"{}\"", value)
            };

            zenoh_config.insert_json5(key, &json_value).map_err(|e| {
                SessionError::InvalidConfig(format!("Invalid config '{}={}': {}", key, value, e))
            })?;
        } else {
            return Err(SessionError::InvalidConfig(format!(
                "Invalid config part '{}', expected key=value",
                part
            )));
        }
    }

    Ok(zenoh_config)
}

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

async fn spawn_subscriber_block(
    session: &Session,
    block: &ZenohBlock,
    pipeline: Arc<SubscriberPipeline>,
    handles: &mut Vec<JoinHandle<()>>,
) -> usize {
    let mut declared = 0;

    for key_expr in &block.keyexprs {
        info!("Declaring Zenoh subscriber on key expression: {}", key_expr);

        let subscriber = match session.declare_subscriber(key_expr.as_str()).await {
            Ok(subscriber) => subscriber,
            Err(err) => {
                error!(
                    "Zenoh subscriber block '{}' failed to declare '{}': {}",
                    block.id, key_expr, err
                );
                continue;
            }
        };

        let pipeline = Arc::clone(&pipeline);
        let key_expr = key_expr.clone();

        handles.push(tokio::spawn(async move {
            loop {
                match subscriber.recv_async().await {
                    Ok(sample) => match handle_sample(&pipeline, sample).await {
                        Ok(()) => {}
                        // routing rejections are already reported by the pipeline, throttled
                        Err(IngestFailure::Routing) => {}
                        Err(IngestFailure::Other(err)) => {
                            warn!("Failed to handle Zenoh sample on '{}': {}", key_expr, err)
                        }
                    },
                    Err(err) => {
                        error!("Subscriber '{}' recv error: {}", key_expr, err);
                        break;
                    }
                }
            }
        }));

        declared += 1;
    }

    declared
}

enum IngestFailure {
    Routing,
    Other(Box<dyn Error + Send + Sync>),
}

async fn handle_sample(pipeline: &SubscriberPipeline, sample: Sample) -> Result<(), IngestFailure> {
    let key_expr = sample.key_expr().as_str();
    let payload = Bytes::from(sample.payload().to_bytes().to_vec());

    let attachment = sample.attachment().map(|att| att.to_bytes().to_vec());
    let content_type = sample.encoding().to_string();

    let (timestamp, source_labels) = match sample.timestamp() {
        Some(ts) => {
            let micros = ts
                .get_time()
                .to_system_time()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;

            let mut labels = Labels::new();
            labels.insert(ZENOH_SOURCE_ID_LABEL.into(), ts.get_id().to_string());
            labels.insert(ZENOH_TS_LABEL.into(), ts.get_time().as_u64().to_string());

            (Some(micros), labels)
        }
        None => (None, Labels::new()),
    };

    debug!(
        "Received Zenoh sample: key={}, bytes={}, encoding={}, has_attachment={}, timestamp={:?}",
        key_expr,
        payload.len(),
        content_type,
        attachment.is_some(),
        timestamp
    );

    pipeline
        .handle_sample(
            key_expr,
            payload,
            attachment,
            timestamp,
            content_type,
            source_labels,
        )
        .await
        .map_err(|err| match err {
            IngestError::Routing(_) => IngestFailure::Routing,
            other => IngestFailure::Other(Box::new(other) as Box<dyn Error + Send + Sync>),
        })
}

async fn spawn_queryable_block(
    session: &Session,
    block: &ZenohQueryableBlock,
    pipeline: Arc<QueryablePipeline>,
    handles: &mut Vec<JoinHandle<()>>,
) -> usize {
    let mut declared = 0;
    let allowed_origin = to_zenoh_locality(block.locality);

    for key_expr in &block.route.keyexprs {
        info!(
            "Declaring Zenoh queryable on key expression: {} (locality={})",
            key_expr, block.locality
        );

        let queryable = match session
            .declare_queryable(key_expr.as_str())
            .allowed_origin(allowed_origin)
            .await
        {
            Ok(queryable) => queryable,
            Err(err) => {
                error!(
                    "Zenoh queryable block '{}' failed to declare '{}': {}",
                    block.route.id, key_expr, err
                );
                continue;
            }
        };

        let pipeline = Arc::clone(&pipeline);
        handles.push(tokio::spawn(async move {
            loop {
                match queryable.recv_async().await {
                    Ok(query) => {
                        let key_expr = query.key_expr().as_str().to_string();
                        let params = expand_query_params(query.selector().parameters());
                        let query_attachments = parse_query_attachments(query.attachment());

                        debug!(
                            "Received Zenoh query: key={}, params={:?}, has_when={}",
                            key_expr,
                            params,
                            query_attachments.when.is_some(),
                        );

                        if let Err(e) = pipeline.check_api_request().await {
                            warn!("Query request limit exceeded for '{}': {}", key_expr, e);
                            if let Err(reply_err) = query
                                .reply_err(zenoh::bytes::ZBytes::from(e.to_string().into_bytes()))
                                .await
                            {
                                warn!("Failed to send error reply: {}", reply_err);
                            }
                            continue;
                        }

                        match pipeline
                            .handle_query(&key_expr, &params, &query_attachments)
                            .await
                        {
                            Ok(result) => {
                                if let Err(e) = send_query_reply(&query, result, &pipeline).await {
                                    warn!("Failed to send query reply: {}", e);
                                    if let Err(reply_err) = query
                                        .reply_err(zenoh::bytes::ZBytes::from(
                                            e.to_string().into_bytes(),
                                        ))
                                        .await
                                    {
                                        warn!("Failed to send error reply: {}", reply_err);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Query handler error for '{}': {}", key_expr, e);
                                if let Err(reply_err) = query
                                    .reply_err(zenoh::bytes::ZBytes::from(
                                        e.public_message().into_bytes(),
                                    ))
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
        }));

        declared += 1;
    }

    declared
}

fn to_zenoh_locality(locality: ZenohQueryableLocality) -> zenoh::sample::Locality {
    match locality {
        ZenohQueryableLocality::SessionLocal => zenoh::sample::Locality::SessionLocal,
        ZenohQueryableLocality::Remote => zenoh::sample::Locality::Remote,
        ZenohQueryableLocality::Any => zenoh::sample::Locality::Any,
    }
}

fn expand_query_params(params: &zenoh::query::Parameters) -> HashMap<String, String> {
    params
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn parse_query_attachments(attachment: Option<&zenoh::bytes::ZBytes>) -> QueryAttachments {
    attachment
        .and_then(|att| {
            let bytes = att.to_bytes();
            match attachments::deserialize_query_attachments(&bytes) {
                Ok(parsed) => Some(parsed),
                Err(e) => {
                    debug!("Failed to parse query attachments: {}", e);
                    None
                }
            }
        })
        .unwrap_or_default()
}

async fn send_query_reply(
    query: &zenoh::query::Query,
    result: crate::api::zenoh::queryable::QueryResult,
    pipeline: &QueryablePipeline,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    use crate::api::zenoh::queryable::QueryResult;

    match result {
        QueryResult::Record(mut reader) => {
            let mut data = Vec::new();
            while let Some(chunk_result) = reader.read_chunk() {
                match chunk_result {
                    Ok(chunk) => data.extend_from_slice(&chunk),
                    Err(e) => return Err(Box::new(e)),
                }
            }

            pipeline.check_egress(data.len() as u64).await?;

            let meta = reader.meta();
            let labels = meta.labels().clone();
            let content_type = meta.content_type();
            let record_timestamp = meta.timestamp();
            let reply_timestamp = build_reply_timestamp(&labels, record_timestamp);
            let attachment = attachments::serialize_labels(&labels)?;

            let key_expr = query.key_expr().clone();
            let mut reply_builder = query
                .reply(key_expr, data)
                .encoding(zenoh::bytes::Encoding::from(content_type))
                .attachment(attachment);

            if let Some(ts) = reply_timestamp {
                reply_builder = reply_builder.timestamp(ts);
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
            let query_rx = receiver.upgrade().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn Error + Send + Sync>
            })?;

            let mut count = 0;
            let limit = io_config.batch_max_records;
            let start_time = Instant::now();

            loop {
                if start_time.elapsed() > io_config.batch_timeout {
                    debug!("Batch timeout reached after {} records", count);
                    break;
                }

                let mut record = {
                    let mut rx = query_rx.write().await.map_err(|e| {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )) as Box<dyn Error + Send + Sync>
                    })?;

                    match timeout(io_config.batch_records_timeout, rx.recv()).await {
                        Ok(Some(Ok(record))) => record,
                        Ok(Some(Err(e))) if e.status == ErrorCode::NoContent => break,
                        Ok(Some(Err(e))) => return Err(Box::new(e)),
                        Ok(None) => break,
                        Err(_) => {
                            debug!("Record receive timeout, continuing");
                            continue;
                        }
                    }
                };

                let mut data = Vec::new();
                while let Some(chunk_result) = record.read_chunk() {
                    match chunk_result {
                        Ok(chunk) => data.extend_from_slice(&chunk),
                        Err(e) => return Err(Box::new(e)),
                    }
                }

                pipeline.check_egress(data.len() as u64).await?;

                let meta = record.meta();
                let labels = meta.labels().clone();
                let content_type = meta.content_type();
                let record_timestamp = meta.timestamp();
                let reply_timestamp = build_reply_timestamp(&labels, record_timestamp);
                let attachment = attachments::serialize_labels(&labels)?;

                let key_expr = query.key_expr().clone();
                let mut reply_builder = query
                    .reply(key_expr, data)
                    .encoding(zenoh::bytes::Encoding::from(content_type))
                    .attachment(attachment);

                if let Some(ts) = reply_timestamp {
                    reply_builder = reply_builder.timestamp(ts);
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

const ZENOH_TS_LABEL: &str = "zenoh_ts_ntp64";
const ZENOH_SOURCE_ID_LABEL: &str = "zenoh_source_id";
const FALLBACK_ZENOH_SOURCE_ID: u128 = 1;

fn build_reply_timestamp(labels: &Labels, record_timestamp_us: u64) -> Option<Timestamp> {
    parse_timestamp_from_labels(labels)
        .or_else(|| timestamp_from_microseconds(labels, record_timestamp_us))
}

fn parse_timestamp_from_labels(labels: &Labels) -> Option<Timestamp> {
    let ntp_raw = labels.get(ZENOH_TS_LABEL)?;
    let source_id_raw = labels.get(ZENOH_SOURCE_ID_LABEL)?;

    let ntp = match NTP64::from_str(ntp_raw) {
        Ok(value) => value,
        Err(err) => {
            debug!(
                "Failed to parse label '{}'='{}' as NTP64: {}",
                ZENOH_TS_LABEL, ntp_raw, err.cause
            );
            return None;
        }
    };

    let source_id = match TimestampId::from_str(source_id_raw) {
        Ok(value) => value,
        Err(err) => {
            debug!(
                "Failed to parse label '{}'='{}' as zenoh ID: {}",
                ZENOH_SOURCE_ID_LABEL, source_id_raw, err.cause
            );
            return None;
        }
    };

    Some(Timestamp::new(ntp, source_id))
}

fn timestamp_from_microseconds(labels: &Labels, record_timestamp_us: u64) -> Option<Timestamp> {
    let source_id = labels
        .get(ZENOH_SOURCE_ID_LABEL)
        .and_then(|raw| match TimestampId::from_str(raw) {
            Ok(value) => Some(value),
            Err(err) => {
                debug!(
                    "Failed to parse label '{}'='{}' as zenoh ID: {}",
                    ZENOH_SOURCE_ID_LABEL, raw, err.cause
                );
                None
            }
        })
        .or_else(|| match TimestampId::try_from(FALLBACK_ZENOH_SOURCE_ID) {
            Ok(value) => Some(value),
            Err(err) => {
                debug!("Failed to build fallback zenoh timestamp ID: {}", err);
                None
            }
        })?;

    let duration = StdDuration::from_micros(record_timestamp_us);
    Some(Timestamp::new(NTP64::from(duration), source_id))
}

#[derive(Debug)]
pub(crate) enum SessionError {
    Component(ComponentError),
    ZenohOpen(String),
    ZenohClose(String),
    InvalidConfig(String),
    NoBlocksStarted(String),
}

impl Display for SessionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Component(err) => write!(f, "{}", err),
            SessionError::ZenohOpen(err) => write!(f, "Failed to open Zenoh session: {}", err),
            SessionError::ZenohClose(err) => write!(f, "Failed to close Zenoh session: {}", err),
            SessionError::InvalidConfig(err) => write!(f, "Invalid Zenoh configuration: {}", err),
            SessionError::NoBlocksStarted(err) => {
                write!(f, "No Zenoh blocks could be started: {}", err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::zenoh::ZenohBucketRouting;
    use rstest::rstest;
    use std::fs;
    use std::time::Duration as StdDuration;

    fn subscriber_block(id: &str, keyexprs: &[&str], bucket: Option<&str>) -> ZenohBlock {
        ZenohBlock {
            id: id.to_string(),
            keyexprs: keyexprs.iter().map(|k| k.to_string()).collect(),
            routing: ZenohBucketRouting::Static,
            bucket: bucket.map(|name| name.to_string()),
            bucket_allowlist: Vec::new(),
            allow_bucket_creation: false,
        }
    }

    fn config_with(subscribers: Vec<ZenohBlock>, queryables: Vec<ZenohBlock>) -> ZenohApiConfig {
        ZenohApiConfig {
            subscribers,
            queryables: queryables
                .into_iter()
                .map(|route| ZenohQueryableBlock {
                    route,
                    locality: ZenohQueryableLocality::default(),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[rstest]
    fn build_routers_rejects_a_bucket_outside_its_allowlist() {
        let block = ZenohBlock {
            bucket_allowlist: vec!["site_*".to_string()],
            routing: ZenohBucketRouting::KeyPrefix,
            ..subscriber_block("3", &["**"], Some("zenoh_misc"))
        };

        let err = build_routers(&[block], "subscriber").unwrap_err();
        assert!(matches!(err, SessionError::InvalidConfig(_)));
        assert!(err.to_string().contains("subscriber block '3'"));
    }

    #[rstest]
    fn build_routers_rejects_a_system_bucket() {
        let err = build_routers(
            &[subscriber_block("0", &["**"], Some("$system"))],
            "queryable",
        )
        .unwrap_err();
        assert!(err.to_string().contains("queryable block '0'"));
    }

    #[rstest]
    fn build_routers_accepts_valid_blocks() {
        let routers = build_routers(
            &[subscriber_block("0", &["raw/**"], Some("raw_data"))],
            "subscriber",
        )
        .unwrap();
        assert_eq!(routers.len(), 1);
        assert_eq!(routers[0].declared_bucket(), Some("raw_data"));
    }

    #[rstest]
    fn validate_keyexprs_rejects_malformed_expressions() {
        let config = config_with(
            vec![subscriber_block("0", &["raw/***"], Some("raw"))],
            vec![],
        );
        let err = validate_keyexprs(&config).unwrap_err();
        assert!(err.to_string().contains("invalid key expression"));
    }

    #[rstest]
    fn validate_keyexprs_accepts_wildcards() {
        let config = config_with(
            vec![subscriber_block("0", &["site_$*/**", "**"], Some("raw"))],
            vec![subscriber_block("0", &["a/*/b"], Some("raw"))],
        );
        assert!(validate_keyexprs(&config).is_ok());
    }

    #[rstest]
    #[case("**", "a/b", true)]
    #[case("a/**", "a/b", true)]
    #[case("a/**", "b/**", false)]
    #[case("site_$*/**", "cell_$*/**", false)]
    fn detects_intersecting_keyexprs(#[case] a: &str, #[case] b: &str, #[case] expected: bool) {
        assert_eq!(keyexprs_intersect(a, b), expected);
    }

    #[rstest]
    fn keyexpr_pairs_covers_within_and_across_blocks() {
        let blocks = vec![
            subscriber_block("0", &["a/**", "b/**"], Some("bucket_a")),
            subscriber_block("1", &["c/**"], Some("bucket_b")),
        ];

        let pairs = keyexpr_pairs(&blocks);
        assert_eq!(pairs.len(), 3);
        assert!(pairs.contains(&("0", "a/**", "0", "b/**")));
        assert!(pairs.contains(&("0", "a/**", "1", "c/**")));
        assert!(pairs.contains(&("0", "b/**", "1", "c/**")));
    }

    fn build_labels_with_timestamp() -> Labels {
        let mut labels = Labels::new();
        let ts = NTP64::from(StdDuration::from_secs(42));
        let id = TimestampId::try_from(99u128).unwrap();
        labels.insert(ZENOH_TS_LABEL.into(), ts.to_string());
        labels.insert(ZENOH_SOURCE_ID_LABEL.into(), id.to_string());
        labels
    }

    #[test]
    fn reply_timestamp_prefers_label_values() {
        let labels = build_labels_with_timestamp();
        let record_ts = 1;

        let result = build_reply_timestamp(&labels, record_ts).unwrap();
        let expected_time = NTP64::from(StdDuration::from_secs(42));
        let expected_id = TimestampId::try_from(99u128).unwrap();

        assert_eq!(result.get_time().as_u64(), expected_time.as_u64());
        assert_eq!(result.get_id().to_string(), expected_id.to_string());
    }

    #[test]
    fn timestamp_from_microseconds_uses_label_source_id() {
        let mut labels = Labels::new();
        let source_id = TimestampId::try_from(123u128).unwrap();
        labels.insert(ZENOH_SOURCE_ID_LABEL.into(), source_id.to_string());

        let result = timestamp_from_microseconds(&labels, 500_000).unwrap();

        assert_eq!(result.get_id().to_string(), source_id.to_string());
    }

    #[test]
    fn timestamp_from_microseconds_falls_back_when_label_missing() {
        let labels = Labels::new();
        let fallback_id = TimestampId::try_from(FALLBACK_ZENOH_SOURCE_ID).unwrap();

        let result = timestamp_from_microseconds(&labels, 750_000).unwrap();

        assert_eq!(result.get_id().to_string(), fallback_id.to_string());
    }

    #[test]
    fn to_zenoh_locality_covers_all_variants() {
        assert!(matches!(
            to_zenoh_locality(ZenohQueryableLocality::SessionLocal),
            zenoh::sample::Locality::SessionLocal
        ));
        assert!(matches!(
            to_zenoh_locality(ZenohQueryableLocality::Remote),
            zenoh::sample::Locality::Remote
        ));
        assert!(matches!(
            to_zenoh_locality(ZenohQueryableLocality::Any),
            zenoh::sample::Locality::Any
        ));
    }

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

    #[rstest]
    fn parses_json5_inline_config() {
        let config = r#"{ mode: "client" }"#;
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn parses_simple_key_value_config() {
        let config = "mode=client";
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn parses_multiple_key_value_pairs() {
        let config = "mode=client;scouting/multicast/enabled=false";
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn parses_array_values_with_endpoints() {
        let config = "mode=router;listen/endpoints=[tcp/127.0.0.1:7447]";
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn parses_array_with_multiple_endpoints() {
        let config = "connect/endpoints=[tcp/10.0.0.1:7447, tcp/10.0.0.2:7447]";
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn ignores_empty_parts_in_config() {
        let config = "mode=client;;";
        let result = parse_inline_config(config);
        assert!(result.is_ok());
    }

    #[rstest]
    fn errors_on_missing_equals() {
        let config = "mode";
        let result = parse_inline_config(config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expected key=value"));
    }

    #[rstest]
    fn errors_on_invalid_json5() {
        let config = "{ invalid json }";
        let result = parse_inline_config(config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON5"));
    }
}
