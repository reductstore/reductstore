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
    reader_slots: Option<Arc<Semaphore>>,
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
            reader_slots: cfg
                .io_conf
                .max_readers_in_flight
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

    pub(crate) async fn acquire_reader_slot(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, ReductError> {
        self.try_acquire_slot(
            &self.reader_slots,
            "in-flight readers limit exceeded: try again later",
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
