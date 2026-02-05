// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::zenoh::subscriber::{KeyParseError, KeyPattern};
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

pub(crate) struct QueryablePipeline {
    components: Arc<Components>,
    key_pattern: KeyPattern,
}

impl QueryablePipeline {
    pub(crate) fn new(config: ZenohApiConfig, components: Arc<Components>) -> Self {
        QueryablePipeline {
            components,
            key_pattern: KeyPattern::new(config.key_prefix),
        }
    }

    pub(crate) async fn bootstrap(&self) -> Result<(), String> {
        let server_info = self
            .components
            .storage
            .info()
            .await
            .map_err(|err| err.to_string())?;

        let selector = QuerySelector::new(&self.key_pattern, "probe-bucket", "probe-entry", None);
        debug!(
            "Zenoh queryable probe target key={} labels={:?}",
            selector.key_expr(),
            selector.label_filter()
        );
        info!(
            "Zenoh query interface online for prefix {} ({} buckets)",
            self.key_pattern.prefix(),
            server_info.bucket_count
        );
        Ok(())
    }

    /// Resolves a Zenoh selector and query parameters into ReductStore records.
    pub(crate) async fn handle_query(
        &self,
        key_expr: &str,
        params: &HashMap<String, String>,
    ) -> Result<QueryResult, QueryError> {
        let route = self.key_pattern.parse(key_expr)?;

        let bucket = self
            .components
            .storage
            .get_bucket(&route.bucket)
            .await?
            .upgrade()?;
        let entry = bucket.get_entry(&route.entry).await?.upgrade()?;

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
    KeyParse(KeyParseError),
    Storage(ReductError),
    InvalidParameter(String),
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::KeyParse(err) => write!(f, "Invalid selector: {}", err),
            QueryError::Storage(err) => write!(f, "Storage error: {}", err),
            QueryError::InvalidParameter(param) => write!(f, "{}", param),
        }
    }
}

impl Error for QueryError {}

impl From<KeyParseError> for QueryError {
    fn from(value: KeyParseError) -> Self {
        QueryError::KeyParse(value)
    }
}

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

#[derive(Debug, Clone, PartialEq)]
struct QuerySelector {
    key_expr: String,
    label_filter: Option<String>,
}

impl QuerySelector {
    fn new(
        pattern: &KeyPattern,
        bucket: impl AsRef<str>,
        entry: impl AsRef<str>,
        label_filter: Option<String>,
    ) -> Self {
        QuerySelector {
            key_expr: pattern.format(bucket.as_ref(), entry.as_ref()),
            label_filter,
        }
    }

    fn key_expr(&self) -> &str {
        &self.key_expr
    }

    fn label_filter(&self) -> Option<&str> {
        self.label_filter.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_selector() {
        let selector = QuerySelector::new(
            &KeyPattern::new("reduct"),
            "bucket",
            "entry",
            Some("sensor=imu".into()),
        );
        assert_eq!(selector.key_expr(), "reduct/bucket/entry");
        assert_eq!(selector.label_filter(), Some("sensor=imu"));
    }

    #[test]
    fn trims_segments() {
        let selector =
            QuerySelector::new(&KeyPattern::new("/reduct/"), "/bucket/", "/entry/", None);
        assert_eq!(selector.key_expr(), "reduct/bucket/entry");
    }

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
