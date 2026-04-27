// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::Cfg;
use reduct_base::error::{ErrorCode, ReductError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

#[derive(Clone, Default)]
pub(crate) struct InFlightIoLimiter {
    writer_slots: Option<Arc<Semaphore>>,
    acquire_timeout: Duration,
}

impl InFlightIoLimiter {
    pub(crate) fn from_cfg(cfg: &Cfg) -> Self {
        Self {
            writer_slots: cfg
                .io_conf
                .max_writers_in_flight
                .map(Semaphore::new)
                .map(Arc::new),
            acquire_timeout: cfg.io_conf.operation_timeout,
        }
    }

    pub(crate) async fn acquire_writer_slot(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, ReductError> {
        self.try_acquire_slot(
            &self.writer_slots,
            "in-flight writers limit exceeded: try again later",
        )
        .await
    }

    async fn try_acquire_slot(
        &self,
        semaphore: &Option<Arc<Semaphore>>,
        message: &str,
    ) -> Result<Option<OwnedSemaphorePermit>, ReductError> {
        let Some(semaphore) = semaphore else {
            return Ok(None);
        };

        timeout(self.acquire_timeout, semaphore.clone().acquire_owned())
            .await
            .map_err(|_| ReductError::new(ErrorCode::TooManyRequests, message))?
            .map(Some)
            .map_err(|_| ReductError::new(ErrorCode::TooManyRequests, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::io::IoConfig;

    fn cfg_with_limits(max_writers_in_flight: Option<usize>) -> Cfg {
        Cfg {
            io_conf: IoConfig {
                max_writers_in_flight,
                operation_timeout: Duration::from_millis(1),
                ..IoConfig::default()
            },
            ..Cfg::default()
        }
    }

    #[tokio::test]
    async fn disabled_limits_do_not_require_permits() {
        let limiter = InFlightIoLimiter::from_cfg(&cfg_with_limits(None));

        assert!(limiter.acquire_writer_slot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn writer_limit_rejects_when_all_slots_are_held() {
        let limiter = InFlightIoLimiter::from_cfg(&cfg_with_limits(Some(1)));
        let permit = limiter.acquire_writer_slot().await.unwrap();

        let err = limiter.acquire_writer_slot().await.err().unwrap();
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert_eq!(
            err.message,
            "in-flight writers limit exceeded: try again later"
        );

        drop(permit);
        assert!(limiter.acquire_writer_slot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn cloned_limiter_shares_slots() {
        let limiter = InFlightIoLimiter::from_cfg(&cfg_with_limits(Some(1)));
        let clone = limiter.clone();
        let _permit = limiter.acquire_writer_slot().await.unwrap();

        let err = clone.acquire_writer_slot().await.err().unwrap();

        assert_eq!(err.status, ErrorCode::TooManyRequests);
    }
}
