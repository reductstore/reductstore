// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::fallback_runtime::FallbackRuntime;
use crate::core::sync::{lock_timeout_error, RWLOCK_TIMEOUT};
use reduct_base::error::ReductError;
use tokio::time::timeout;

/// An async read-write lock with embedded timeouts.
pub struct AsyncRwLock<T> {
    inner: tokio::sync::RwLock<T>,
    rt: FallbackRuntime,
}

impl<T> AsyncRwLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: tokio::sync::RwLock::new(data),
            rt: FallbackRuntime::new(),
        }
    }

    pub async fn read(&self) -> Result<tokio::sync::RwLockReadGuard<'_, T>, ReductError> {
        timeout(RWLOCK_TIMEOUT, self.inner.read())
            .await
            .map_err(|_| lock_timeout_error("Failed to acquire async read lock within timeout"))
    }

    pub async fn write(&self) -> Result<tokio::sync::RwLockWriteGuard<'_, T>, ReductError> {
        timeout(RWLOCK_TIMEOUT, self.inner.write())
            .await
            .map_err(|_| lock_timeout_error("Failed to acquire async write lock within timeout"))
    }

    pub fn try_read(&self) -> Option<tokio::sync::RwLockReadGuard<'_, T>> {
        self.inner.try_read().ok()
    }

    pub fn try_write(&self) -> Option<tokio::sync::RwLockWriteGuard<'_, T>> {
        self.inner.try_write().ok()
    }

    pub fn blocking_read(&self) -> Result<tokio::sync::RwLockReadGuard<'_, T>, ReductError> {
        Ok(self.rt.block_on(self.inner.read()))
    }

    pub fn blocking_write(&self) -> Result<tokio::sync::RwLockWriteGuard<'_, T>, ReductError> {
        Ok(self.rt.block_on(self.inner.write()))
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncRwLock;
    use crate::core::sync::RWLOCK_TIMEOUT;
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
    #[should_panic(expected = "Failed to acquire async read lock within timeout")]
    async fn test_async_rwlock_timeout() {
        let lock = Arc::new(AsyncRwLock::new(5));
        let lock_clone = lock.clone();
        let _guard = lock.write().await.unwrap();
        let handle = tokio::spawn(async move {
            sleep(RWLOCK_TIMEOUT + Duration::from_millis(50)).await;
            drop(lock_clone);
        });

        let res = lock.read().await;
        assert!(res.is_err());
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_rwlock_try_and_blocking() {
        let lock = AsyncRwLock::new(5);
        assert_eq!(*lock.try_read().unwrap(), 5);
        {
            let mut guard = lock.try_write().unwrap();
            *guard = 8;
        }

        let mut guard = lock.blocking_write().unwrap();
        *guard = 11;
        drop(guard);

        assert_eq!(*lock.blocking_read().unwrap(), 11);
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
}
