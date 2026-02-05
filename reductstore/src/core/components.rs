// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

//! Server-wide shared state used by all API layers (HTTP, Zenoh).

use crate::asset::asset_manager::ManageStaticAsset;
use crate::auth::policy::Policy;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::ManageTokens;
use crate::cfg::Cfg;
use crate::core::cache::Cache;
use crate::core::sync::AsyncRwLock;
use crate::ext::ext_repository::ManageExtensions;
use crate::lock_file::BoxedLockFile;
use crate::replication::ManageReplications;
use crate::storage::engine::StorageEngine;
use axum::http::HeaderMap;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;
use reduct_base::service_unavailable;
use serde::de::StdError;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

/// Core server components shared across all APIs.
pub struct Components {
    pub storage: Arc<StorageEngine>,
    pub(crate) auth: TokenAuthorization,
    pub(crate) token_repo: AsyncRwLock<Box<dyn ManageTokens + Send + Sync>>,
    pub(crate) console: Box<dyn ManageStaticAsset + Send + Sync>,
    pub(crate) replication_repo: AsyncRwLock<Box<dyn ManageReplications + Send + Sync>>,
    pub(crate) ext_repo: Box<dyn ManageExtensions + Send + Sync>,
    pub(crate) query_link_cache: AsyncRwLock<Cache<String, Arc<Mutex<BoxedReadRecord>>>>,

    pub(crate) cfg: Cfg,
}

/// Initialization and shared access to core server components.
///
/// Both the HTTP API and Zenoh API use this to wait for the server to be ready
/// and obtain references to the storage engine and other services.
pub struct StateKeeper {
    rx: AsyncRwLock<Receiver<Components>>,
    components: AsyncRwLock<Option<Arc<Components>>>,
    pub(crate) lock_file: Arc<BoxedLockFile>,
}

impl StateKeeper {
    pub fn new(lock_file: Arc<BoxedLockFile>, rx: Receiver<Components>) -> Self {
        StateKeeper {
            rx: AsyncRwLock::new(rx),
            components: AsyncRwLock::new(None),
            lock_file,
        }
    }

    pub async fn get_with_permissions<P>(
        &self,
        headers: &HeaderMap,
        policy: P,
    ) -> Result<Arc<Components>, ComponentError>
    where
        P: Policy,
    {
        let components = self.wait_components().await?;

        components
            .auth
            .check(
                headers
                    .get("Authorization")
                    .map(|header| header.to_str().unwrap_or("")),
                components.token_repo.write().await?.as_mut(),
                policy,
            )
            .await?;

        Ok(components)
    }

    async fn wait_components(&self) -> Result<Arc<Components>, ComponentError> {
        let locked =
            self.lock_file.is_locked().await.map_err(|err| {
                ComponentError::new(ErrorCode::InternalServerError, &err.to_string())
            })?;

        if !locked {
            return Err(ComponentError::from(service_unavailable!(
                "The server is starting up, please try again later"
            ))
            .with_log_hint(LogHint::SkipErrorLogging));
        }

        {
            let mut lock = self.components.write().await?;
            // it's important to check again after acquiring the lock and lock must be exclusive to avoid race conditions
            if lock.is_none() {
                // check if there are components in the channel
                if self.rx.read().await?.capacity() != 0 {
                    return Err(service_unavailable!(
                        "The server is starting up, please try again later"
                    )
                    .into());
                }

                let components = match self.rx.write().await?.recv().await {
                    Some(cmp) => cmp,
                    None => {
                        return Err(service_unavailable!(
                            "The server is starting up, please try again later"
                        )
                        .into())
                    }
                };
                // ensure background services (like replication) start after HTTP is ready to accept connections
                // however, in tests we want to control when these services start
                #[cfg(not(test))]
                components.replication_repo.write().await?.start();
                lock.replace(Arc::new(components));
            }
        }
        let components = self.components.read().await?;
        let components = components
            .as_ref()
            .cloned()
            .expect("Components must be initialized before use");
        Ok(components)
    }

    pub async fn get_anonymous(&self) -> Result<Arc<Components>, ComponentError> {
        self.wait_components().await
    }
}

#[derive(PartialEq, Clone, Copy, Debug, Eq)]
pub enum LogHint {
    Default,
    SkipErrorLogging,
}

/// Error type for component access failures.
#[derive(PartialEq, Clone)]
pub struct ComponentError {
    inner: ReductError,
    log_hint: LogHint,
}

impl ComponentError {
    pub fn new(status: ErrorCode, message: &str) -> Self {
        ComponentError {
            inner: ReductError::new(status, message),
            log_hint: LogHint::Default,
        }
    }

    pub fn with_log_hint(mut self, log_hint: LogHint) -> Self {
        self.log_hint = log_hint;
        self
    }

    pub fn status(&self) -> ErrorCode {
        self.inner.status
    }

    pub fn message(&self) -> &str {
        &self.inner.message
    }

    pub fn log_hint(&self) -> LogHint {
        self.log_hint
    }

    pub fn into_inner(self) -> ReductError {
        self.inner
    }

    pub fn inner(&self) -> &ReductError {
        &self.inner
    }
}

impl Debug for ComponentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Display for ComponentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl StdError for ComponentError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<ReductError> for ComponentError {
    fn from(st: ReductError) -> Self {
        Self {
            inner: st,
            log_hint: LogHint::Default,
        }
    }
}

impl From<ComponentError> for ReductError {
    fn from(err: ComponentError) -> ReductError {
        err.inner
    }
}
