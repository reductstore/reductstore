// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use super::sanitize_entry_name;
use crate::cfg::io::IoConfig;
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::Components;
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
/// In single-bucket mode, all queries target a fixed bucket configured via
/// `RS_ZENOH_BUCKET`. The full Zenoh key expression becomes the entry name.
pub(crate) struct QueryablePipeline {
    components: Arc<Components>,
    /// The fixed bucket name for all queries.
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

    /// Resolves a Zenoh selector and query parameters into ReductStore records.
    ///
    /// The full key expression is used as the entry name within the configured bucket.
    /// Slashes in the key expression are replaced with underscores since ReductStore
    /// entry names only allow alphanumeric characters, hyphens, and underscores.
    pub(crate) async fn handle_query(
        &self,
        key_expr: &str,
        params: &HashMap<String, String>,
    ) -> Result<QueryResult, QueryError> {
        // In single-bucket mode: entry = full Zenoh key (with slashes replaced)
        let entry_name = sanitize_entry_name(key_expr.trim_matches('/'));

        debug!(
            "Handling Zenoh query: bucket={} entry={}",
            self.bucket, entry_name
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

        let only_metadata = parse_only_metadata(params)?;
        let query_entry = build_query_entry(params.clone(), only_metadata)?;
        let query_id = entry.query(query_entry).await?;
        let (receiver, io_config) = entry.get_query_receiver(query_id).await?;

        Ok(QueryResult::Stream {
            receiver,
            io_config,
        })
    }
}

/// Result of resolving a Zenoh query selector.
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

fn parse_only_metadata(params: &HashMap<String, String>) -> Result<bool, QueryError> {
    match params.get("metadata") {
        Some(raw) => raw
            .parse::<bool>()
            .map_err(|_| QueryError::InvalidParameter("'metadata' must be a boolean value".into())),
        None => Ok(false),
    }
}

fn build_query_entry(
    params: HashMap<String, String>,
    only_metadata: bool,
) -> Result<QueryEntry, QueryError> {
    let continuous = parse_bool_flag(&params, "continuous")?;
    let ttl = parse_ttl(&params)?;
    let (start, stop) = parse_time_range(&params)?;
    let (include, exclude) = parse_include_exclude_filters(&params);
    let each_s = parse_each_s(&params)?;
    let each_n = parse_each_n(&params)?;
    let limit = parse_limit(&params)?;

    Ok(QueryEntry {
        query_type: QueryType::Query,
        entries: None,
        start,
        stop,
        include: Some(include),
        exclude: Some(exclude),
        each_s,
        each_n,
        limit,
        continuous,
        ttl,
        only_metadata: Some(only_metadata),
        when: None,
        strict: None,
        ext: None,
    })
}

fn parse_bool_flag(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<bool>, QueryError> {
    match params.get(key) {
        Some(raw) => raw.parse::<bool>().map(Some).map_err(|_| {
            QueryError::InvalidParameter(format!("'{}' must be a boolean value", key))
        }),
        None => Ok(None),
    }
}

fn parse_ttl(params: &HashMap<String, String>) -> Result<Option<u64>, QueryError> {
    match params.get("ttl") {
        Some(raw) => raw.parse::<u64>().map(Some).map_err(|_| {
            QueryError::InvalidParameter("'ttl' must be an unsigned integer in seconds".into())
        }),
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

fn parse_each_s(params: &HashMap<String, String>) -> Result<Option<f64>, QueryError> {
    match params.get("each_s") {
        Some(raw) => {
            let value = raw.parse::<f64>().map_err(|_| {
                QueryError::InvalidParameter("'each_s' must be a floating point value".into())
            })?;
            if value <= 0.0 {
                return Err(QueryError::InvalidParameter(
                    "'each_s' must be greater than 0".into(),
                ));
            }
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn parse_each_n(params: &HashMap<String, String>) -> Result<Option<u64>, QueryError> {
    match params.get("each_n") {
        Some(raw) => {
            let value = raw.parse::<u64>().map_err(|_| {
                QueryError::InvalidParameter("'each_n' must be an unsigned integer".into())
            })?;
            if value == 0 {
                return Err(QueryError::InvalidParameter(
                    "'each_n' must be greater than 0".into(),
                ));
            }
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn parse_limit(params: &HashMap<String, String>) -> Result<Option<u64>, QueryError> {
    match params.get("limit") {
        Some(raw) => raw.parse::<u64>().map(Some).map_err(|_| {
            QueryError::InvalidParameter("'limit' must be an unsigned integer".into())
        }),
        None => Ok(None),
    }
}

fn parse_include_exclude_filters(
    params: &HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut include = HashMap::new();
    let mut exclude = HashMap::new();

    for (key, value) in params.iter() {
        if let Some(label) = key.strip_prefix("include-") {
            include.insert(label.to_string(), value.to_string());
        } else if let Some(label) = key.strip_prefix("exclude-") {
            exclude.insert(label.to_string(), value.to_string());
        }
    }

    (include, exclude)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamp_param() {
        let params = HashMap::from_iter(vec![("ts".to_string(), "123".to_string())]);
        assert_eq!(parse_timestamp(&params).unwrap(), Some(123));
    }

    #[test]
    fn rejects_invalid_each_s() {
        let params = HashMap::from_iter(vec![("each_s".to_string(), "0".to_string())]);
        assert!(parse_each_s(&params).is_err());
    }

    #[test]
    fn include_exclude_filters() {
        let params = HashMap::from_iter(vec![
            ("include-scope".to_string(), "a".to_string()),
            ("exclude-tag".to_string(), "b".to_string()),
        ]);
        let (include, exclude) = parse_include_exclude_filters(&params);
        assert_eq!(include.get("scope"), Some(&"a".to_string()));
        assert_eq!(exclude.get("tag"), Some(&"b".to_string()));
    }
}
