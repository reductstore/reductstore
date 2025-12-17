// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::time::Duration;

/// A read-write lock based on parking_lot with embedded timeouts.
pub struct RwLock<T> {
    inner: parking_lot::RwLock<T>,
}

#[cfg(not(test))]
pub const RWLOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
// Tests run in parallel and share global caches; give a bit more headroom to avoid
// flakes from transient contention on shared locks.
pub const RWLOCK_TIMEOUT: Duration = Duration::from_secs(15);

impl<T> RwLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: parking_lot::RwLock::new(data),
        }
    }

    pub fn read(&self) -> Result<parking_lot::RwLockReadGuard<'_, T>, ReductError> {
        self.inner
            .try_read_for(RWLOCK_TIMEOUT)
            .ok_or(internal_server_error!(
                "Failed to acquire read lock within timeout"
            ))
    }

    pub fn write(&self) -> Result<parking_lot::RwLockWriteGuard<'_, T>, ReductError> {
        self.inner
            .try_write_for(RWLOCK_TIMEOUT)
            .ok_or(internal_server_error!(
                "Failed to acquire write lock within timeout"
            ))
    }

    pub fn try_read(&self) -> Option<parking_lot::RwLockReadGuard<'_, T>> {
        self.inner.try_read()
    }

    pub fn try_write(&self) -> Option<parking_lot::RwLockWriteGuard<'_, T>> {
        self.inner.try_write()
    }

    pub fn read_blocking(&self) -> parking_lot::RwLockReadGuard<'_, T> {
        self.inner.read()
    }
    pub fn write_blocking(&self) -> parking_lot::RwLockWriteGuard<'_, T> {
        self.inner.write()
    }
}

#[cfg(test)]
mod tests {
    use super::RwLock;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rwlock_read() {
        let lock = Arc::new(RwLock::new(5));
        let lock_clone = Arc::clone(&lock);
        let handle = thread::spawn(move || {
            let read_guard = lock_clone.read().unwrap();
            assert_eq!(*read_guard, 5);
            thread::sleep(Duration::from_millis(100));
        });
        handle.join().unwrap();
    }

    #[test]
    fn test_rwlock_write() {
        let lock = Arc::new(RwLock::new(5));
        let lock_clone = Arc::clone(&lock);
        let handle = thread::spawn(move || {
            let mut write_guard = lock_clone.write().unwrap();
            *write_guard = 10;
            thread::sleep(Duration::from_millis(100));
        });
        handle.join().unwrap();
        let read_guard = lock.read().unwrap();
        assert_eq!(*read_guard, 10);
    }
}
