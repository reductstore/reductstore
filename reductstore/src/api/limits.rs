// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::sync::RwLock;
use async_trait::async_trait;
use reduct_base::error::{ErrorCode, ReductError};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) type BoxedLimits = Arc<dyn ManageLimits + Send + Sync>;

const DEFAULT_WINDOW: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LimitKind {
    ApiRequests(u64),
    IngressBytes(u64),
    EgressBytes(u64),
}

impl LimitKind {
    fn amount(&self) -> u64 {
        match self {
            LimitKind::ApiRequests(value)
            | LimitKind::IngressBytes(value)
            | LimitKind::EgressBytes(value) => *value,
        }
    }
}

impl Display for LimitKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitKind::ApiRequests(_) => write!(f, "api requests"),
            LimitKind::IngressBytes(_) => write!(f, "ingress bytes"),
            LimitKind::EgressBytes(_) => write!(f, "egress bytes"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LimitExceeded {
    pub kind: LimitKind,
    pub limit: u64,
    pub used: u64,
    pub retry_after: Duration,
}

impl Display for LimitExceeded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "instance-level rate limit for {} exceeded: used={} limit={} retry_after={}s",
            self.kind,
            self.used,
            self.limit,
            self.retry_after.as_secs()
        )
    }
}

impl From<LimitExceeded> for ReductError {
    fn from(value: LimitExceeded) -> Self {
        ReductError::new(ErrorCode::TooManyRequests, &value.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct LimitsConfig {
    pub api_requests_per_window: Option<u64>,
    pub ingress_bytes_per_window: Option<u64>,
    pub egress_bytes_per_window: Option<u64>,
}

#[async_trait]
pub(crate) trait ManageLimits {
    async fn consume(&self, kind: LimitKind) -> Result<(), ReductError>;

    async fn check_api_request(&self) -> Result<(), ReductError> {
        self.consume(LimitKind::ApiRequests(1)).await
    }

    async fn check_ingress(&self, bytes: u64) -> Result<(), ReductError> {
        self.consume(LimitKind::IngressBytes(bytes)).await
    }

    async fn check_egress(&self, bytes: u64) -> Result<(), ReductError> {
        self.consume(LimitKind::EgressBytes(bytes)).await
    }
}

pub(crate) struct LimitsBuilder {
    config: LimitsConfig,
    window: Duration,
}

impl Default for LimitsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LimitsBuilder {
    pub fn new() -> Self {
        Self {
            config: LimitsConfig::default(),
            window: DEFAULT_WINDOW,
        }
    }

    pub fn with_config(mut self, config: LimitsConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    pub fn build(self) -> BoxedLimits {
        if self.config == LimitsConfig::default() {
            Arc::new(NoopLimits)
        } else {
            Arc::new(RateLimits::new(self.config, self.window))
        }
    }
}

struct NoopLimits;

#[async_trait]
impl ManageLimits for NoopLimits {
    async fn consume(&self, _kind: LimitKind) -> Result<(), ReductError> {
        Ok(())
    }
}

struct RateLimits {
    api: RwLock<WindowCounter>,
    ingress: RwLock<WindowCounter>,
    egress: RwLock<WindowCounter>,
    window: Duration,
}

impl RateLimits {
    fn new(config: LimitsConfig, window: Duration) -> Self {
        Self {
            api: RwLock::new(WindowCounter::new(config.api_requests_per_window)),
            ingress: RwLock::new(WindowCounter::new(config.ingress_bytes_per_window)),
            egress: RwLock::new(WindowCounter::new(config.egress_bytes_per_window)),
            window: if window.is_zero() {
                Duration::from_secs(1)
            } else {
                window
            },
        }
    }
}

#[async_trait]
impl ManageLimits for RateLimits {
    async fn consume(&self, kind: LimitKind) -> Result<(), ReductError> {
        let now_secs = now_secs();
        match kind {
            LimitKind::ApiRequests(_) => self
                .api
                .write()?
                .consume(kind, now_secs, self.window)
                .map_err(ReductError::from),
            LimitKind::IngressBytes(_) => self
                .ingress
                .write()?
                .consume(kind, now_secs, self.window)
                .map_err(ReductError::from),
            LimitKind::EgressBytes(_) => self
                .egress
                .write()?
                .consume(kind, now_secs, self.window)
                .map_err(ReductError::from),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WindowCounter {
    limit: Option<u64>,
    used: u64,
    window_start_secs: Option<u64>,
}

impl WindowCounter {
    fn new(limit: Option<u64>) -> Self {
        Self {
            limit,
            used: 0,
            window_start_secs: None,
        }
    }

    fn consume(
        &mut self,
        kind: LimitKind,
        now_secs: u64,
        window: Duration,
    ) -> Result<(), LimitExceeded> {
        let amount = kind.amount();
        let Some(limit) = self.limit else {
            return Ok(());
        };

        let window_secs = window.as_secs().max(1);
        let window_start = match self.window_start_secs {
            Some(start) => {
                if now_secs.saturating_sub(start) >= window_secs {
                    self.used = 0;
                    self.window_start_secs = Some(now_secs);
                    now_secs
                } else {
                    start
                }
            }
            None => {
                self.window_start_secs = Some(now_secs);
                now_secs
            }
        };

        let used_after = self.used.saturating_add(amount);
        if used_after > limit {
            let elapsed = now_secs.saturating_sub(window_start);
            let retry_after_secs = window_secs.saturating_sub(elapsed).max(1);
            return Err(LimitExceeded {
                kind,
                limit,
                used: used_after,
                retry_after: Duration::from_secs(retry_after_secs),
            });
        }

        self.used = used_after;
        Ok(())
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    #[fixture]
    fn window() -> Duration {
        Duration::from_secs(10)
    }

    #[fixture]
    fn now() -> u64 {
        100
    }

    #[rstest]
    #[case(LimitKind::ApiRequests(7), 7)]
    #[case(LimitKind::IngressBytes(11), 11)]
    #[case(LimitKind::EgressBytes(13), 13)]
    fn amount_is_extracted_from_limit_kind(#[case] kind: LimitKind, #[case] expected: u64) {
        assert_eq!(kind.amount(), expected);
    }

    #[rstest]
    #[case(LimitKind::ApiRequests(1))]
    #[case(LimitKind::IngressBytes(1))]
    #[case(LimitKind::EgressBytes(1))]
    fn limit_exceeded_converts_to_too_many_requests(#[case] kind: LimitKind) {
        let err = LimitExceeded {
            kind,
            limit: 10,
            used: 11,
            retry_after: Duration::from_secs(5),
        };

        let reduct_err = ReductError::from(err);
        assert_eq!(reduct_err.status(), ErrorCode::TooManyRequests);
        assert!(reduct_err.message().contains("instance-level rate limit"));
    }

    #[rstest]
    fn window_counter_allows_unlimited(window: Duration, now: u64) {
        let mut counter = WindowCounter::new(None);
        assert!(counter
            .consume(LimitKind::ApiRequests(u64::MAX), now, window)
            .is_ok());
        assert!(counter
            .consume(LimitKind::IngressBytes(u64::MAX), now, window)
            .is_ok());
        assert!(counter
            .consume(LimitKind::EgressBytes(u64::MAX), now, window)
            .is_ok());
    }

    #[rstest]
    #[case(LimitKind::ApiRequests(2))]
    #[case(LimitKind::IngressBytes(2))]
    #[case(LimitKind::EgressBytes(2))]
    fn window_counter_rejects_when_exceeded(window: Duration, now: u64, #[case] kind: LimitKind) {
        let mut counter = WindowCounter::new(Some(2));
        counter
            .consume(LimitKind::ApiRequests(1), now, window)
            .expect("first consume must pass");

        let err = counter.consume(kind, now, window).err().unwrap();
        assert_eq!(err.limit, 2);
        assert_eq!(err.used, 3);
        assert!(err.retry_after.as_secs() >= 1);
    }

    #[rstest]
    fn window_counter_resets_after_window(window: Duration, now: u64) {
        let mut counter = WindowCounter::new(Some(2));
        assert!(counter
            .consume(LimitKind::IngressBytes(2), now, window)
            .is_ok());
        assert!(counter
            .consume(LimitKind::IngressBytes(1), now, window)
            .is_err());

        let later = now + window.as_secs() + 1;
        assert!(counter
            .consume(LimitKind::IngressBytes(2), later, window)
            .is_ok());
    }

    #[tokio::test]
    async fn noop_limits_accept_everything() {
        let limits = LimitsBuilder::new().build();
        assert!(limits.check_api_request().await.is_ok());
        assert!(limits.check_ingress(u64::MAX).await.is_ok());
        assert!(limits.check_egress(u64::MAX).await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn api_limit_blocks_when_exceeded() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(2),
                ..LimitsConfig::default()
            })
            .build();

        assert!(limits.check_api_request().await.is_ok());
        assert!(limits.check_api_request().await.is_ok());

        let err = limits.check_api_request().await.err().unwrap();
        assert_eq!(err.status(), ErrorCode::TooManyRequests);
        assert!(err.message().contains("api requests"));
    }

    #[rstest]
    #[tokio::test]
    async fn limits_builder_applies_independent_counters() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(1),
                ingress_bytes_per_window: Some(3),
                egress_bytes_per_window: Some(5),
            })
            .build();

        assert!(limits.check_api_request().await.is_ok());
        assert!(limits.check_ingress(3).await.is_ok());
        assert!(limits.check_egress(5).await.is_ok());

        assert!(limits.check_api_request().await.is_err());
        assert!(limits.check_ingress(1).await.is_err());
        assert!(limits.check_egress(1).await.is_err());
    }
}
