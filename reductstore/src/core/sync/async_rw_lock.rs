// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::sync::{lock_timeout_error_at, rwlock_timeout};
use reduct_base::error::ReductError;
use std::future::Future;
use std::panic::Location;
use std::sync::Arc;
use tokio::time::timeout;

/// An async read-write lock with embedded timeouts.
pub struct AsyncRwLock<T> {
    inner: Arc<tokio::sync::RwLock<T>>,
}

impl<T> AsyncRwLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(data)),
        }
    }

    #[track_caller]
    pub fn read(
        &self,
    ) -> impl Future<Output = Result<tokio::sync::RwLockReadGuard<'_, T>, ReductError>> + '_ {
        let location = Location::caller();
        async move {
            timeout(rwlock_timeout(), self.inner.read())
                .await
                .map_err(|_| {
                    lock_timeout_error_at(
                        "Failed to acquire async read lock within timeout",
                        location,
                    )
                })
        }
    }

    #[track_caller]
    pub fn write(
        &self,
    ) -> impl Future<Output = Result<tokio::sync::RwLockWriteGuard<'_, T>, ReductError>> + '_ {
        let location = Location::caller();
        async move {
            timeout(rwlock_timeout(), self.inner.write())
                .await
                .map_err(|_| {
                    lock_timeout_error_at(
                        "Failed to acquire async write lock within timeout",
                        location,
                    )
                })
        }
    }

    pub fn try_read(&self) -> Option<tokio::sync::RwLockReadGuard<'_, T>> {
        self.inner.try_read().ok()
    }

    pub fn try_write(&self) -> Option<tokio::sync::RwLockWriteGuard<'_, T>> {
        self.inner.try_write().ok()
    }

    #[track_caller]
    pub fn write_owned(
        &self,
    ) -> impl Future<Output = Result<tokio::sync::OwnedRwLockWriteGuard<T>, ReductError>> + '_ {
        let location = Location::caller();
        async move {
            timeout(rwlock_timeout(), Arc::clone(&self.inner).write_owned())
                .await
                .map_err(|_| {
                    lock_timeout_error_at(
                        "Failed to acquire async owned write lock within timeout",
                        location,
                    )
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncRwLock;
    use crate::core::sync::{reset_rwlock_config, set_rwlock_timeout};
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_async_rwlock_read() {
        let lock = AsyncRwLock::new(5);
        let read_guard = lock.read().await.unwrap();
        assert_eq!(*read_guard, 5);
    }

    #[tokio::test]
    async fn test_async_rwlock_write() {
        let lock = Arc::new(AsyncRwLock::new(5));
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut write_guard = lock_clone.write().await.unwrap();
            *write_guard = 10;
            sleep(Duration::from_millis(50)).await;
        });
        handle.await.unwrap();
        let read_guard = lock.read().await.unwrap();
        assert_eq!(*read_guard, 10);
    }

    #[tokio::test]
    #[serial]
    async fn test_async_rwlock_timeout() {
        struct ResetGuard;
        impl Drop for ResetGuard {
            fn drop(&mut self) {
                reset_rwlock_config();
            }
        }
        let _reset = ResetGuard;
        set_rwlock_timeout(Duration::from_millis(100));
        let lock = Arc::new(AsyncRwLock::new(5));
        let _guard = lock.write().await.unwrap();
        let res = lock.read().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_async_rwlock_try_none() {
        let lock = AsyncRwLock::new(5);
        let _guard = lock.write().await.unwrap();
        assert!(lock.try_read().is_none());

        drop(_guard);
        let _read = lock.read().await.unwrap();
        assert!(lock.try_write().is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_async_rwlock_write_timeout() {
        struct ResetGuard;
        impl Drop for ResetGuard {
            fn drop(&mut self) {
                reset_rwlock_config();
            }
        }
        let _reset = ResetGuard;
        set_rwlock_timeout(Duration::from_millis(100));
        let lock = Arc::new(AsyncRwLock::new(5));
        let _guard = lock.read().await.unwrap();
        let res = lock.write().await;
        assert!(res.is_err());
    }
}
