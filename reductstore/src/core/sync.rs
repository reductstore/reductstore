// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::time::Duration;

/// A read-write lock based on parking_lot with embedded timeouts.
pub struct RwLock<T> {
    inner: parking_lot::RwLock<T>,
}

pub const RWLOCK_TIMEOUT: Duration = Duration::from_secs(5);

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
        match self.inner.try_write_for(RWLOCK_TIMEOUT) {
            Some(guard) => Ok(guard),
            None => {
                print!("");
                Err(internal_server_error!(
                    "Failed to acquire write lock within timeout"
                ))
            }
        }
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
