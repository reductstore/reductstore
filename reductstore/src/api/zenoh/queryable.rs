// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::zenoh::attachments::QueryAttachments;
use crate::api::Components;
use crate::cfg::io::IoConfig;
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::sync::AsyncRwLock;
use crate::core::weak::Weak;
use crate::storage::entry::RecordReader;
use crate::storage::query::QueryRx;
use log::{debug, info};
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{QueryEntry, QueryType};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Queryable pipeline for handling Zenoh queries against ReductStore.
///
/// All queries target a fixed bucket configured via `RS_ZENOH_BUCKET`.
/// The full Zenoh key expression becomes the entry name.
pub(crate) struct QueryablePipeline {
    components: Arc<Components>,
    bucket: String,
}

impl QueryablePipeline {
    pub(crate) fn new(config: ZenohApiConfig, components: Arc<Components>) -> Self {
        QueryablePipeline {
            components,
            bucket: config.bucket.clone(),
        }
    }

    pub(crate) async fn bootstrap(&self) -> Result<(), String> {
        let server_info = self
            .components
            .storage
            .info()
            .await
            .map_err(|err| err.to_string())?;

        info!(
            "Zenoh queryable ready (storage version {}): bucket='{}'",
            server_info.version, self.bucket
        );
        Ok(())
    }

    pub(crate) async fn check_api_request(&self) -> Result<(), ReductError> {
        self.components.limits.check_api_request().await
    }

    pub(crate) async fn check_egress(&self, bytes: u64) -> Result<(), ReductError> {
        self.components.limits.check_egress(bytes).await
    }

    /// Resolves a Zenoh selector and query parameters into ReductStore records.
    ///
    /// The full key expression is used as the entry name within the configured bucket.
    pub(crate) async fn handle_query(
        &self,
        key_expr: &str,
        params: &HashMap<String, String>,
        attachments: &QueryAttachments,
    ) -> Result<QueryResult, QueryError> {
        let entry_name = key_expr.trim_matches('/');

        debug!(
            "Handling Zenoh query: bucket={} entry={} when={:?}",
            self.bucket, entry_name, attachments.when
        );

        let bucket = self
            .components
            .storage
            .get_bucket(&self.bucket)
            .await?
            .upgrade()?;
        let entry = bucket.get_entry(&entry_name).await?.upgrade()?;

        if let Some(ts) = parse_timestamp(params)? {
            let reader = entry.begin_read(ts).await?;
            return Ok(QueryResult::Record(reader));
        }

        if parse_last(params)? {
            let info = entry.info().await?;
            if info.record_count == 0 {
                return Err(QueryError::Storage(reduct_base::not_found!(
                    "No records in entry {}",
                    entry_name
                )));
            }
            let reader = entry.begin_read(info.latest_record).await?;
            return Ok(QueryResult::Record(reader));
        }

        let query_entry = build_query_entry(params.clone(), attachments)?;
        let query_id = entry.query(query_entry).await?;
        let (receiver, io_config) = entry.get_query_receiver(query_id).await?;

        Ok(QueryResult::Stream {
            receiver,
            io_config,
        })
    }
}
pub(crate) enum QueryResult {
    Record(RecordReader),
    Stream {
        receiver: Weak<AsyncRwLock<QueryRx>>,
        io_config: IoConfig,
    },
}

#[derive(Debug)]
pub(crate) enum QueryError {
    Storage(ReductError),
    InvalidParameter(String),
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::Storage(err) => write!(f, "Storage error: {}", err),
            QueryError::InvalidParameter(param) => write!(f, "{}", param),
        }
    }
}

impl Error for QueryError {}

impl From<ReductError> for QueryError {
    fn from(value: ReductError) -> Self {
        QueryError::Storage(value)
    }
}

fn parse_timestamp(params: &HashMap<String, String>) -> Result<Option<u64>, QueryError> {
    match params.get("ts") {
        Some(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| QueryError::InvalidParameter("'ts' must be an unsigned integer".into())),
        None => Ok(None),
    }
}

fn parse_last(params: &HashMap<String, String>) -> Result<bool, QueryError> {
    match params.get("last") {
        Some(raw) => raw
            .parse::<bool>()
            .map_err(|_| QueryError::InvalidParameter("'last' must be a boolean value".into())),
        None => Ok(false),
    }
}

fn build_query_entry(
    params: HashMap<String, String>,
    attachments: &QueryAttachments,
) -> Result<QueryEntry, QueryError> {
    let (start, stop) = parse_time_range(&params)?;
    let strict = parse_strict(&params)?;
    Ok(QueryEntry {
        query_type: QueryType::Query,
        entries: None,
        start,
        stop,
        include: None,
        exclude: None,
        each_s: None,
        each_n: None,
        limit: None,
        continuous: None,
        ttl: None,
        only_metadata: None,
        when: attachments.when.clone(),
        strict,
        ext: None,
    })
}

fn parse_strict(params: &HashMap<String, String>) -> Result<Option<bool>, QueryError> {
    match params.get("strict") {
        Some(raw) => raw
            .parse::<bool>()
            .map(Some)
            .map_err(|_| QueryError::InvalidParameter("'strict' must be a boolean value".into())),
        None => Ok(None),
    }
}

fn parse_time_range(
    params: &HashMap<String, String>,
) -> Result<(Option<u64>, Option<u64>), QueryError> {
    let start = match params.get("start") {
        Some(raw) => Some(raw.parse::<u64>().map_err(|_| {
            QueryError::InvalidParameter("'start' must be an unsigned integer".into())
        })?),
        None => None,
    };

    let stop = match params.get("stop") {
        Some(raw) => Some(raw.parse::<u64>().map_err(|_| {
            QueryError::InvalidParameter("'stop' must be an unsigned integer".into())
        })?),
        None => None,
    };

    Ok((start, stop))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::components::StateKeeper;
    use crate::api::http::tests::{api_limited_keeper, egress_limited_keeper};
    use crate::cfg::zenoh::ZenohApiConfig;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use std::sync::Arc;

    #[test]
    fn parses_timestamp_param() {
        let params = HashMap::from_iter(vec![("ts".to_string(), "123".to_string())]);
        assert_eq!(parse_timestamp(&params).unwrap(), Some(123));
    }

    #[test]
    fn parses_last_param_true() {
        let params = HashMap::from_iter(vec![("last".to_string(), "true".to_string())]);
        assert_eq!(parse_last(&params).unwrap(), true);
    }

    #[test]
    fn parses_last_param_false() {
        let params = HashMap::from_iter(vec![("last".to_string(), "false".to_string())]);
        assert_eq!(parse_last(&params).unwrap(), false);
    }

    #[test]
    fn parses_last_param_missing() {
        let params = HashMap::new();
        assert_eq!(parse_last(&params).unwrap(), false);
    }

    #[test]
    fn rejects_invalid_last() {
        let params = HashMap::from_iter(vec![("last".to_string(), "abc".to_string())]);
        assert!(parse_last(&params).is_err());
    }

    #[test]
    fn build_query_entry_with_when_attachment() {
        use serde_json::json;

        let params = HashMap::from_iter(vec![
            ("start".to_string(), "100".to_string()),
            ("stop".to_string(), "200".to_string()),
            ("strict".to_string(), "true".to_string()),
        ]);
        let attachments = QueryAttachments {
            when: Some(json!({"$and": [{"&status": "ok"}, {"$limit": 10}]})),
        };

        let query = build_query_entry(params, &attachments).unwrap();

        assert_eq!(query.start, Some(100));
        assert_eq!(query.stop, Some(200));
        assert_eq!(
            query.when,
            Some(json!({"$and": [{"&status": "ok"}, {"$limit": 10}]}))
        );
        assert_eq!(query.strict, Some(true));
    }

    #[test]
    fn build_query_entry_without_attachments() {
        let params = HashMap::from_iter(vec![
            ("start".to_string(), "0".to_string()),
            ("stop".to_string(), "1000".to_string()),
        ]);
        let attachments = QueryAttachments::default();

        let query = build_query_entry(params, &attachments).unwrap();

        assert_eq!(query.start, Some(0));
        assert_eq!(query.stop, Some(1000));
        assert_eq!(query.limit, None);
        assert_eq!(query.only_metadata, None);
        assert_eq!(query.when, None);
        assert_eq!(query.strict, None);
        assert_eq!(query.ext, None);
    }

    #[rstest]
    #[tokio::test]
    async fn check_api_request_applies_rate_limit(#[future] api_limited_keeper: Arc<StateKeeper>) {
        let components = api_limited_keeper.await.get_anonymous().await.unwrap();
        let pipeline = QueryablePipeline::new(
            ZenohApiConfig {
                bucket: "bucket-1".to_string(),
                ..Default::default()
            },
            components,
        );

        assert!(pipeline.check_api_request().await.is_ok());
        let err = pipeline.check_api_request().await.err().unwrap();
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("api requests"));
    }

    #[rstest]
    #[tokio::test]
    async fn check_egress_applies_rate_limit(#[future] egress_limited_keeper: Arc<StateKeeper>) {
        let components = egress_limited_keeper.await.get_anonymous().await.unwrap();
        let pipeline = QueryablePipeline::new(
            ZenohApiConfig {
                bucket: "bucket-1".to_string(),
                ..Default::default()
            },
            components,
        );

        let err = pipeline.check_egress(6).await.err().unwrap();
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("egress bytes"));
    }
}
