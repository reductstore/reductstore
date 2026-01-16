// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod async_rw_lock;
pub mod rw_lock;

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::panic::Location;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

pub use async_rw_lock::AsyncRwLock;
pub use rw_lock::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RwLockFailureAction {
    Panic = 0,
    Error = 1,
}

impl RwLockFailureAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            RwLockFailureAction::Panic => "panic",
            RwLockFailureAction::Error => "error",
        }
    }
}

#[cfg(not(test))]
const DEFAULT_RWLOCK_TIMEOUT_MS: u64 = 60_000;

#[cfg(test)]
const DEFAULT_RWLOCK_TIMEOUT_MS: u64 = 5_000;

#[cfg(not(test))]
const DEFAULT_RWLOCK_FAILURE_ACTION: RwLockFailureAction = RwLockFailureAction::Error;

#[cfg(test)]
const DEFAULT_RWLOCK_FAILURE_ACTION: RwLockFailureAction = RwLockFailureAction::Error;

static RWLOCK_TIMEOUT_MS: AtomicU64 = AtomicU64::new(DEFAULT_RWLOCK_TIMEOUT_MS);
static RWLOCK_FAILURE_ACTION: AtomicU8 = AtomicU8::new(DEFAULT_RWLOCK_FAILURE_ACTION as u8);

pub fn set_rwlock_timeout(timeout: Duration) {
    let _ = RWLOCK_TIMEOUT_MS.swap(timeout.as_millis() as u64, Ordering::Relaxed);
}

pub fn set_rwlock_failure_action(action: RwLockFailureAction) {
    let _ = RWLOCK_FAILURE_ACTION.swap(action as u8, Ordering::Relaxed);
}

pub fn rwlock_timeout() -> Duration {
    Duration::from_millis(RWLOCK_TIMEOUT_MS.load(Ordering::Relaxed))
}

pub fn rwlock_failure_action() -> RwLockFailureAction {
    match RWLOCK_FAILURE_ACTION.load(Ordering::Relaxed) {
        0 => RwLockFailureAction::Panic,
        _ => RwLockFailureAction::Error,
    }
}

pub fn default_rwlock_timeout() -> Duration {
    Duration::from_millis(DEFAULT_RWLOCK_TIMEOUT_MS)
}

pub fn default_rwlock_failure_action() -> RwLockFailureAction {
    DEFAULT_RWLOCK_FAILURE_ACTION
}

#[allow(dead_code)]
#[track_caller]
fn lock_timeout_error(message: &str) -> ReductError {
    lock_timeout_error_at(message, Location::caller())
}

pub(crate) fn lock_timeout_error_at(
    message: &str,
    location: &'static Location<'static>,
) -> ReductError {
    let enriched = format!(
        "{} (caller: {}:{})",
        message,
        location.file(),
        location.line()
    );

    match rwlock_failure_action() {
        RwLockFailureAction::Panic => panic!("{enriched}"),
        RwLockFailureAction::Error => internal_server_error!(&enriched),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_lock_timeout_error_panics() {
        set_rwlock_failure_action(RwLockFailureAction::Panic);
        assert!(std::panic::catch_unwind(|| lock_timeout_error("boom")).is_err());
        reset_rwlock_config();
    }

    #[test]
    #[serial]
    fn test_test_timeout_value() {
        reset_rwlock_config();
        assert_eq!(rwlock_timeout(), Duration::from_secs(5));
        reset_rwlock_config();
    }

    #[test]
    #[serial]
    #[cfg(not(target_os = "windows"))]
    fn test_lock_timeout_error_returns_error() {
        reset_rwlock_config();
        set_rwlock_failure_action(RwLockFailureAction::Error);
        let err = lock_timeout_error("boom");
        assert_eq!(err.status, internal_server_error!("boom").status);
        assert!(err.message.contains("boom"));
        assert!(err.message.contains("core/sync.rs"));
        reset_rwlock_config();
    }

    #[test]
    #[serial]
    fn test_override_timeout() {
        reset_rwlock_config();
        set_rwlock_timeout(Duration::from_secs(5));
        assert_eq!(rwlock_timeout(), Duration::from_secs(5));
        reset_rwlock_config();
    }
}

#[cfg(test)]
pub(crate) fn reset_rwlock_config() {
    set_rwlock_timeout(default_rwlock_timeout());
    set_rwlock_failure_action(default_rwlock_failure_action());
}
