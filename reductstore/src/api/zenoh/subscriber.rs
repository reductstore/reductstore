use crate::api::zenoh::attachments;
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::Components;
use bytes::Bytes;
use log::{debug, info, warn};
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct SubscriberPipeline {
    components: Arc<Components>,
    key_pattern: KeyPattern,
}

impl SubscriberPipeline {
    pub(crate) fn new(config: ZenohApiConfig, components: Arc<Components>) -> Self {
        SubscriberPipeline {
            components,
            key_pattern: KeyPattern::new(config.key_prefix),
        }
    }

    /// Key expression used for the Zenoh subscription. Includes wildcard
    /// to cover all entries and nested paths under the configured prefix.
    pub(crate) fn subscription_key_expr(&self) -> String {
        format!("{}/**", self.key_pattern.prefix())
    }

    /// Handles a single Zenoh sample by writing it into storage.
    pub(crate) async fn handle_sample(
        &self,
        key_expr: &str,
        payload: Bytes,
        attachment: Option<Vec<u8>>,
        timestamp: Option<u64>,
    ) -> Result<(), IngestError> {
        let route = self.key_pattern.parse(key_expr)?;

        let labels = match attachment {
            Some(raw_labels) => match attachments::deserialize_labels(&raw_labels) {
                Ok(labels) => labels,
                Err(err) => {
                    warn!(
                        "Failed to decode labels for {}:{} ({}): {}",
                        route.bucket, route.entry, key_expr, err
                    );
                    Labels::new()
                }
            },
            None => Labels::new(),
        };

        let ts = timestamp.unwrap_or_else(|| current_time_us());
        let content_size = payload.len() as u64;

        debug!(
            "Ingesting Zenoh sample bucket={} entry={} timestamp={} bytes={}",
            route.bucket, route.entry, ts, content_size
        );

        let bucket = self
            .components
            .storage
            .get_bucket(&route.bucket)
            .await?
            .upgrade()?;

        let mut writer = bucket
            .begin_write(
                &route.entry,
                ts,
                content_size,
                "application/octet-stream".to_string(),
                labels,
            )
            .await?;

        writer.send(Ok(Some(payload))).await?;
        writer.send(Ok(None)).await?;

        Ok(())
    }

    pub(crate) async fn bootstrap(&self) -> Result<(), String> {
        let server_info = self
            .components
            .storage
            .info()
            .await
            .map_err(|err| err.to_string())?;

        let probe_key = self.key_pattern.format("probe-bucket", "probe-entry");
        let route = self
            .key_pattern
            .parse(&probe_key)
            .map_err(|err| err.to_string())?;

        debug!(
            "Zenoh subscriber probe for bucket={} entry={} tail={:?}",
            route.bucket, route.entry, route.tail
        );
        info!(
            "Zenoh subscriber wiring ready (storage version {}): {}",
            server_info.version,
            self.key_pattern.prefix()
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct KeyPattern {
    prefix: String,
}

impl KeyPattern {
    pub(crate) fn new(prefix: impl Into<String>) -> Self {
        KeyPattern {
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn format(&self, bucket: &str, entry: &str) -> String {
        format!("{}/{}/{}", self.prefix, bucket, entry)
    }

    pub(crate) fn parse(&self, expr: &str) -> Result<KeyRoute, KeyParseError> {
        let segments: Vec<&str> = expr.trim_matches('/').split('/').collect();
        if segments.is_empty() {
            return Err(KeyParseError::MissingPrefix);
        }
        if segments[0] != self.prefix {
            return Err(KeyParseError::PrefixMismatch {
                expected: self.prefix.clone(),
                found: segments[0].to_string(),
            });
        }
        if segments.len() < 2 {
            return Err(KeyParseError::MissingBucket);
        }
        if segments.len() < 3 {
            return Err(KeyParseError::MissingEntry);
        }

        Ok(KeyRoute {
            bucket: segments[1].to_string(),
            entry: segments[2].to_string(),
            tail: segments[3..].iter().map(|s| (*s).to_string()).collect(),
        })
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum KeyParseError {
    MissingPrefix,
    MissingBucket,
    MissingEntry,
    PrefixMismatch { expected: String, found: String },
}

impl Display for KeyParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyParseError::MissingPrefix => write!(f, "key expression must include a prefix"),
            KeyParseError::MissingBucket => write!(f, "key expression missing bucket segment"),
            KeyParseError::MissingEntry => write!(f, "key expression missing entry segment"),
            KeyParseError::PrefixMismatch { expected, found } => {
                write!(f, "expected prefix '{}' but found '{}'", expected, found)
            }
        }
    }
}

impl Error for KeyParseError {}

#[derive(Debug, PartialEq)]
pub(crate) struct KeyRoute {
    pub(crate) bucket: String,
    pub(crate) entry: String,
    pub(crate) tail: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum IngestError {
    KeyParse(KeyParseError),
    Storage(ReductError),
}

impl Display for IngestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::KeyParse(err) => write!(f, "Invalid key expression: {}", err),
            IngestError::Storage(err) => write!(f, "Storage error: {}", err),
        }
    }
}

impl Error for IngestError {}

impl From<KeyParseError> for IngestError {
    fn from(value: KeyParseError) -> Self {
        IngestError::KeyParse(value)
    }
}

impl From<ReductError> for IngestError {
    fn from(value: ReductError) -> Self {
        IngestError::Storage(value)
    }
}

fn current_time_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_key() {
        let pattern = KeyPattern::new("reduct");
        let route = pattern.parse("/reduct/demo/temp").unwrap();
        assert_eq!(route.bucket, "demo");
        assert_eq!(route.entry, "temp");
        assert!(route.tail.is_empty());
    }

    #[test]
    fn detects_prefix_mismatch() {
        let pattern = KeyPattern::new("reduct");
        let result = pattern.parse("/other/demo/temp");
        assert!(matches!(result, Err(KeyParseError::PrefixMismatch { .. })));
    }

    #[test]
    fn detects_short_path() {
        let pattern = KeyPattern::new("reduct");
        assert_eq!(
            pattern.parse("reduct").unwrap_err(),
            KeyParseError::MissingBucket
        );
        assert_eq!(
            pattern.parse("reduct/demo").unwrap_err(),
            KeyParseError::MissingEntry
        );
    }
}
