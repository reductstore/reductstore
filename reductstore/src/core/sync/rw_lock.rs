// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::sync::{lock_timeout_error_at, rwlock_timeout};
use reduct_base::error::ReductError;
use std::panic::Location;

/// A read-write lock based on parking_lot with embedded timeouts.
pub struct RwLock<T> {
    inner: parking_lot::RwLock<T>,
}

impl<T> RwLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: parking_lot::RwLock::new(data),
        }
    }

    #[track_caller]
    pub fn read(&self) -> Result<parking_lot::RwLockReadGuard<'_, T>, ReductError> {
        let location = Location::caller();
        self.inner.try_read_for(rwlock_timeout()).ok_or_else(|| {
            lock_timeout_error_at("Failed to acquire read lock within timeout", location)
        })
    }

    #[track_caller]
    pub fn write(&self) -> Result<parking_lot::RwLockWriteGuard<'_, T>, ReductError> {
        let location = Location::caller();
        self.inner.try_write_for(rwlock_timeout()).ok_or_else(|| {
            lock_timeout_error_at("Failed to acquire write lock within timeout", location)
        })
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

    #[test]
    fn test_rwlock_try_and_blocking() {
        let lock = RwLock::new(5);
        assert_eq!(*lock.try_read().unwrap(), 5);
        {
            let mut guard = lock.try_write().unwrap();
            *guard = 7;
        }

        assert_eq!(*lock.read_blocking(), 7);
        let mut guard = lock.write_blocking();
        *guard = 9;
        drop(guard);
        assert_eq!(*lock.read_blocking(), 9);
    }

    #[test]
    fn test_rwlock_timeout_panics() {
        let lock = Arc::new(RwLock::new(5));
        let write_guard = lock.write_blocking();

        let lock_clone = Arc::clone(&lock);
        let handle = thread::spawn(move || {
            let _ = lock_clone.read();
        });

        thread::sleep(Duration::from_millis(1200));
        drop(write_guard);
        assert!(handle.join().is_err());
    }
}
