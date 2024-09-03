// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A counter to keep track of the number of times a block has been used.
pub(crate) struct UseCounter {
    counter: HashMap<u64, (usize, Instant)>,
    timeout: Duration,
}

impl UseCounter {
    /// Create a new `UseCounter`.
    pub fn new(timeout: Duration) -> Self {
        UseCounter {
            counter: HashMap::new(),
            timeout,
        }
    }

    /// Increment the use count for the given block.
    pub fn increment(&mut self, block_id: u64) {
        let entry = self.counter.entry(block_id).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
    }

    /// Get the use count for the given block.
    pub fn decrement(&mut self, block_id: u64) {
        if let Some(entry) = self.counter.get_mut(&block_id) {
            entry.0 -= 1;
            entry.1 = Instant::now();
        }
    }

    pub fn update(&mut self, block_id: u64) {
        if let Some(entry) = self.counter.get_mut(&block_id) {
            entry.1 = Instant::now();
        }
    }

    /// Clean stale entries and check if the block is still in use.
    /// Returns `true` if the block is no longer in use.
    pub fn clean_stale_and_check(&mut self, block_id: u64) -> bool {
        match self.counter.get(&block_id) {
            Some(count) => {
                if count.0 == 0 || count.1.elapsed() > self.timeout {
                    self.counter.remove(&block_id);
                    true
                } else {
                    false
                }
            }
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[rstest]
    fn test_use_counter(mut counter: UseCounter) {
        counter.increment(1);
        counter.increment(1);
        counter.increment(2);

        assert_eq!(counter.counter.get(&1).unwrap().0, 2);
        assert_eq!(counter.counter.get(&2).unwrap().0, 1);

        counter.decrement(1);
        counter.decrement(1);
        counter.decrement(2);

        assert_eq!(counter.counter.get(&1).unwrap().0, 0);
        assert_eq!(counter.counter.get(&2).unwrap().0, 0);

        assert!(counter.clean_stale_and_check(1));
        assert!(counter.clean_stale_and_check(2));

        assert!(counter.counter.is_empty());
    }

    #[rstest]
    fn test_use_counter_timeout(mut counter: UseCounter) {
        counter.increment(1);
        counter.increment(2);

        assert_eq!(counter.counter.get(&1).unwrap().0, 1);
        assert_eq!(counter.counter.get(&2).unwrap().0, 1);

        sleep(Duration::from_millis(200));

        assert!(counter.clean_stale_and_check(1));
        assert!(counter.clean_stale_and_check(2));
        assert!(counter.counter.is_empty());
    }

    #[fixture]
    fn counter() -> UseCounter {
        UseCounter::new(Duration::from_millis(100))
    }
}
