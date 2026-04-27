// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::sync::RwLock;
use async_trait::async_trait;
use bytesize::ByteSize;
use reduct_base::error::{ErrorCode, ReductError};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) type BoxedLimits = Arc<dyn ManageLimits + Send + Sync>;

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LimitScope {
    ClientIp(String),
    GlobalFallback,
}

impl Display for LimitScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitScope::ClientIp(ip) => write!(f, "client {}", ip),
            LimitScope::GlobalFallback => write!(f, "global"),
        }
    }
}

pub(crate) fn limit_scope_from_client_ip(client_ip: Option<&str>) -> LimitScope {
    match client_ip.map(str::trim) {
        Some(ip) if !ip.is_empty() => LimitScope::ClientIp(ip.to_string()),
        _ => LimitScope::GlobalFallback,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LimitExceeded {
    pub scope: LimitScope,
    pub kind: LimitKind,
    pub limit: u64,
    pub used: u64,
    pub retry_after: Duration,
}

impl Display for LimitExceeded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let used = format_amount(self.kind, self.used);
        let limit = format_amount(self.kind, self.limit);
        write!(
            f,
            "rate limit for {} ({}) exceeded: used={} limit={} retry_after={}s",
            self.kind,
            self.scope,
            used,
            limit,
            self.retry_after.as_secs()
        )
    }
}

fn format_amount(kind: LimitKind, amount: u64) -> String {
    match kind {
        LimitKind::ApiRequests(_) => amount.to_string(),
        LimitKind::IngressBytes(_) | LimitKind::EgressBytes(_) => {
            format!("{} ({})", ByteSize::b(amount), amount)
        }
    }
}

impl From<LimitExceeded> for ReductError {
    fn from(value: LimitExceeded) -> Self {
        ReductError::new(ErrorCode::TooManyRequests, &value.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct LimitsConfig {
    pub api_requests_per_window: Option<WindowLimit>,
    pub ingress_bytes_per_window: Option<WindowLimit>,
    pub egress_bytes_per_window: Option<WindowLimit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowLimit {
    pub amount: u64,
    pub window: Duration,
}

impl WindowLimit {
    pub fn new(amount: u64, window: Duration) -> Self {
        Self {
            amount,
            window: if window.is_zero() {
                Duration::from_secs(1)
            } else {
                window
            },
        }
    }
}

#[async_trait]
pub(crate) trait ManageLimits {
    async fn consume_scoped(&self, scope: LimitScope, kind: LimitKind) -> Result<(), ReductError>;

    async fn check_api_request_for(&self, scope: LimitScope) -> Result<(), ReductError> {
        self.consume_scoped(scope, LimitKind::ApiRequests(1)).await
    }

    async fn check_ingress_for(&self, scope: LimitScope, bytes: u64) -> Result<(), ReductError> {
        self.consume_scoped(scope, LimitKind::IngressBytes(bytes))
            .await
    }

    async fn check_egress_for(&self, scope: LimitScope, bytes: u64) -> Result<(), ReductError> {
        self.consume_scoped(scope, LimitKind::EgressBytes(bytes))
            .await
    }
}

pub(crate) struct LimitsBuilder {
    config: LimitsConfig,
    window: Option<Duration>,
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
            window: None,
        }
    }

    pub fn with_config(mut self, config: LimitsConfig) -> Self {
        self.config = config;
        self
    }

    #[cfg(test)]
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = Some(window);
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
    async fn consume_scoped(
        &self,
        _scope: LimitScope,
        _kind: LimitKind,
    ) -> Result<(), ReductError> {
        Ok(())
    }
}

struct RateLimits {
    api_limit: Option<WindowLimit>,
    ingress_limit: Option<WindowLimit>,
    egress_limit: Option<WindowLimit>,
    api: RwLock<HashMap<LimitScope, WindowCounter>>,
    ingress: RwLock<HashMap<LimitScope, WindowCounter>>,
    egress: RwLock<HashMap<LimitScope, WindowCounter>>,
}

impl RateLimits {
    fn new(config: LimitsConfig, window_override: Option<Duration>) -> Self {
        let resolve_window = |limit: Option<WindowLimit>| {
            limit.map(|limit| {
                WindowLimit::new(
                    limit.amount,
                    window_override.unwrap_or(limit.window.max(Duration::from_secs(1))),
                )
            })
        };

        Self {
            api_limit: resolve_window(config.api_requests_per_window),
            ingress_limit: resolve_window(config.ingress_bytes_per_window),
            egress_limit: resolve_window(config.egress_bytes_per_window),
            api: RwLock::new(HashMap::new()),
            ingress: RwLock::new(HashMap::new()),
            egress: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ManageLimits for RateLimits {
    async fn consume_scoped(&self, scope: LimitScope, kind: LimitKind) -> Result<(), ReductError> {
        let now_secs = now_secs();
        match kind {
            LimitKind::ApiRequests(_) => {
                consume_for_scope(&self.api, self.api_limit, scope, kind, now_secs)
            }
            LimitKind::IngressBytes(_) => {
                consume_for_scope(&self.ingress, self.ingress_limit, scope, kind, now_secs)
            }
            LimitKind::EgressBytes(_) => {
                consume_for_scope(&self.egress, self.egress_limit, scope, kind, now_secs)
            }
        }
    }
}

fn consume_for_scope(
    map: &RwLock<HashMap<LimitScope, WindowCounter>>,
    limit: Option<WindowLimit>,
    scope: LimitScope,
    kind: LimitKind,
    now_secs: u64,
) -> Result<(), ReductError> {
    let Some(limit) = limit else {
        return Ok(());
    };

    let mut counters = map.write()?;
    prune_stale_scopes(&mut counters, now_secs, limit.window);
    let counter = counters
        .entry(scope.clone())
        .or_insert_with(|| WindowCounter::new(Some(limit)));

    counter
        .consume(scope, kind, now_secs)
        .map_err(ReductError::from)
}

fn prune_stale_scopes(
    counters: &mut HashMap<LimitScope, WindowCounter>,
    now_secs: u64,
    window: Duration,
) {
    let expiry_secs = window.as_secs().max(1);
    counters.retain(|_, counter| {
        counter
            .window_start_secs
            .is_some_and(|start| now_secs.saturating_sub(start) <= expiry_secs)
    });
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WindowCounter {
    limit: Option<WindowLimit>,
    used: u64,
    window_start_secs: Option<u64>,
}

impl WindowCounter {
    fn new(limit: Option<WindowLimit>) -> Self {
        Self {
            limit,
            used: 0,
            window_start_secs: None,
        }
    }

    fn consume(
        &mut self,
        scope: LimitScope,
        kind: LimitKind,
        now_secs: u64,
    ) -> Result<(), LimitExceeded> {
        let amount = kind.amount();
        let Some(limit) = self.limit else {
            return Ok(());
        };

        let window_secs = limit.window.as_secs().max(1);
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
        if used_after > limit.amount {
            let elapsed = now_secs.saturating_sub(window_start);
            let retry_after_secs = window_secs.saturating_sub(elapsed).max(1);
            return Err(LimitExceeded {
                scope,
                kind,
                limit: limit.amount,
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
    use tokio::time::sleep;

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
            scope: LimitScope::GlobalFallback,
            kind,
            limit: 10,
            used: 11,
            retry_after: Duration::from_secs(5),
        };

        let reduct_err = ReductError::from(err);
        assert_eq!(reduct_err.status(), ErrorCode::TooManyRequests);
        assert!(reduct_err.message().contains("rate limit for"));
    }

    #[rstest]
    #[case(LimitKind::ApiRequests(1), "api requests")]
    #[case(LimitKind::IngressBytes(1), "ingress bytes")]
    #[case(LimitKind::EgressBytes(1), "egress bytes")]
    fn limit_kind_display_formats_name(#[case] kind: LimitKind, #[case] expected: &str) {
        assert_eq!(kind.to_string(), expected);
    }

    #[rstest]
    fn limit_exceeded_formats_human_readable_bytes() {
        let err = LimitExceeded {
            scope: LimitScope::ClientIp("127.0.0.1".to_string()),
            kind: LimitKind::IngressBytes(1),
            limit: 10_000_000,
            used: 10_005_949,
            retry_after: Duration::from_secs(38),
        };

        let msg = err.to_string();
        assert!(msg.contains("ingress bytes"));
        assert!(msg.contains("limit="));
        assert!(msg.contains("used="));
        assert!(msg.contains("(10000000)"));
        assert!(msg.contains("10005949"));
        assert!(msg.contains("retry_after=38s"));
    }

    #[rstest]
    fn limit_exceeded_formats_plain_api_amount() {
        let err = LimitExceeded {
            scope: LimitScope::ClientIp("127.0.0.1".to_string()),
            kind: LimitKind::ApiRequests(1),
            limit: 10,
            used: 11,
            retry_after: Duration::from_secs(1),
        };

        let msg = err.to_string();
        assert!(msg.contains("api requests"));
        assert!(msg.contains("used=11"));
        assert!(msg.contains("limit=10"));
        assert!(msg.contains("127.0.0.1"));
    }

    #[rstest]
    fn window_counter_allows_unlimited(now: u64) {
        let mut counter = WindowCounter::new(None);
        assert!(counter
            .consume(
                LimitScope::GlobalFallback,
                LimitKind::ApiRequests(u64::MAX),
                now
            )
            .is_ok());
        assert!(counter
            .consume(
                LimitScope::GlobalFallback,
                LimitKind::IngressBytes(u64::MAX),
                now
            )
            .is_ok());
        assert!(counter
            .consume(
                LimitScope::GlobalFallback,
                LimitKind::EgressBytes(u64::MAX),
                now
            )
            .is_ok());
    }

    #[rstest]
    #[case(LimitKind::ApiRequests(2))]
    #[case(LimitKind::IngressBytes(2))]
    #[case(LimitKind::EgressBytes(2))]
    fn window_counter_rejects_when_exceeded(window: Duration, now: u64, #[case] kind: LimitKind) {
        let mut counter = WindowCounter::new(Some(WindowLimit::new(2, window)));
        counter
            .consume(LimitScope::GlobalFallback, LimitKind::ApiRequests(1), now)
            .expect("first consume must pass");

        let err = counter
            .consume(LimitScope::GlobalFallback, kind, now)
            .err()
            .unwrap();
        assert_eq!(err.limit, 2);
        assert_eq!(err.used, 3);
        assert!(err.retry_after.as_secs() >= 1);
    }

    #[rstest]
    fn window_counter_resets_after_window(window: Duration, now: u64) {
        let mut counter = WindowCounter::new(Some(WindowLimit::new(2, window)));
        assert!(counter
            .consume(LimitScope::GlobalFallback, LimitKind::IngressBytes(2), now)
            .is_ok());
        assert!(counter
            .consume(LimitScope::GlobalFallback, LimitKind::IngressBytes(1), now)
            .is_err());

        let later = now + window.as_secs() + 1;
        assert!(counter
            .consume(
                LimitScope::GlobalFallback,
                LimitKind::IngressBytes(2),
                later
            )
            .is_ok());
    }

    #[rstest]
    fn window_limit_enforces_minimum_one_second() {
        let limit = WindowLimit::new(1, Duration::ZERO);
        assert_eq!(limit.window, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn noop_limits_accept_everything() {
        let limits = LimitsBuilder::new().build();
        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
        assert!(limits
            .check_ingress_for(LimitScope::GlobalFallback, u64::MAX)
            .await
            .is_ok());
        assert!(limits
            .check_egress_for(LimitScope::GlobalFallback, u64::MAX)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn limits_builder_default_matches_new() {
        let limits = LimitsBuilder::default().build();
        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
        assert!(limits
            .check_ingress_for(LimitScope::GlobalFallback, 1)
            .await
            .is_ok());
        assert!(limits
            .check_egress_for(LimitScope::GlobalFallback, 1)
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn api_limit_blocks_when_exceeded() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(2, Duration::from_secs(3600))),
                ..LimitsConfig::default()
            })
            .build();

        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());

        let err = limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), ErrorCode::TooManyRequests);
        assert!(err.message().contains("api requests"));
    }

    #[rstest]
    #[tokio::test]
    async fn limits_builder_applies_independent_counters() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(1, Duration::from_secs(3600))),
                ingress_bytes_per_window: Some(WindowLimit::new(3, Duration::from_secs(3600))),
                egress_bytes_per_window: Some(WindowLimit::new(5, Duration::from_secs(3600))),
                ..LimitsConfig::default()
            })
            .build();

        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
        assert!(limits
            .check_ingress_for(LimitScope::GlobalFallback, 3)
            .await
            .is_ok());
        assert!(limits
            .check_egress_for(LimitScope::GlobalFallback, 5)
            .await
            .is_ok());

        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_err());
        assert!(limits
            .check_ingress_for(LimitScope::GlobalFallback, 1)
            .await
            .is_err());
        assert!(limits
            .check_egress_for(LimitScope::GlobalFallback, 1)
            .await
            .is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn limits_builder_window_override_is_used() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(1, Duration::from_secs(3600))),
                ..LimitsConfig::default()
            })
            .with_window(Duration::from_secs(1))
            .build();

        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_err());

        sleep(Duration::from_millis(1_100)).await;
        assert!(limits
            .check_api_request_for(LimitScope::GlobalFallback)
            .await
            .is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn limits_are_tracked_per_scope() {
        let limits = LimitsBuilder::new()
            .with_config(LimitsConfig {
                api_requests_per_window: Some(WindowLimit::new(1, Duration::from_secs(3600))),
                ..LimitsConfig::default()
            })
            .build();

        let ip1 = LimitScope::ClientIp("10.0.0.1".to_string());
        let ip2 = LimitScope::ClientIp("10.0.0.2".to_string());

        assert!(limits.check_api_request_for(ip1.clone()).await.is_ok());
        assert!(limits.check_api_request_for(ip2).await.is_ok());
        assert!(limits.check_api_request_for(ip1).await.is_err());
    }
}
