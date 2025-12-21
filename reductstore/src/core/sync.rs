// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub mod async_rw_lock;
pub mod rw_lock;

use reduct_base::error::ReductError;
use std::time::Duration;

pub use async_rw_lock::AsyncRwLock;
pub use rw_lock::RwLock;

#[cfg(test)]
fn lock_timeout_error(message: &str) -> ReductError {
    panic!("{message}");
}

#[cfg(not(test))]
fn lock_timeout_error(message: &str) -> ReductError {
    reduct_base::internal_server_error!(message)
}

#[cfg(not(test))]
pub const RWLOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
pub const RWLOCK_TIMEOUT: Duration = Duration::from_secs(1);
