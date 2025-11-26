// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

struct ExpiringValue<V> {
    value: V,
    last_access: Instant,
}

type ExpiringStore<K, V> = HashMap<K, ExpiringValue<V>>;

/// A simple cache implementation that removes old entries after a given time.
///
/// The cache is limited in size and will remove the oldest entry if the limit is reached.
///
/// Only for references due to the `Clone` bound.
pub(crate) struct Cache<K, V> {
    store: ExpiringStore<K, V>,
    size: usize,
    ttl: Duration,
}

impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    pub fn new(size: usize, ttl: Duration) -> Cache<K, V> {
        Cache {
            store: HashMap::new(),
            size,
            ttl,
        }
    }

    /// Inserts a new value into the cache.
    ///
    /// If the cache is full or there are old entries, they will be removed.
    ///
    /// # Returns
    ///
    /// A vector of key-value pairs that were removed from the cache.
    pub fn insert(&mut self, key: K, value: V) -> Vec<(K, V)> {
        let value = ExpiringValue {
            value,
            last_access: Instant::now(),
        };
        self.store.insert(key, value);
        self.discard_old_descriptors()
    }

    /// Retrieves a reference to a value from the cache by key.
    ///
    /// This function updates the last access time of the value if it exists.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key of the value to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the value if it exists, or `None` if it does not.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key).map(|v| &*v)
    }

    /// Retrieves a mutable reference to a value from the cache by key.
    ///
    /// This function updates the last access time of the value if it exists.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key of the value to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option` containing a mutable reference to the value if it exists, or `None` if it does not.
    /// ```
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut value = self.store.get_mut(key);
        if let Some(ref mut value) = value {
            value.last_access = Instant::now();
        }

        if let Some(value) = value {
            Some(&mut value.value)
        } else {
            None
        }
    }

    /// Removes a value from the cache by key.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key of the value to remove.
    ///
    /// # Returns
    ///
    /// An `Option` containing the removed value if it existed, or `None` if it did not.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.store.remove(key).map(|v| v.value)
    }

    /// Returns the number of entries in the cache.
    ///
    /// # Returns
    ///
    /// The number of entries in the cache.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns a vector of references to all values in the cache.
    ///
    /// This function updates the last access time of all values.
    ///
    /// # Returns
    ///
    /// A vector of references to all values in the cache.
    ///
    pub fn values(&mut self) -> Vec<&V> {
        self.store
            .values_mut()
            .map(|v| {
                v.last_access = Instant::now();
                &v.value
            })
            .collect()
    }

    /// Returns a vector of references to all keys in the cache.
    ///
    /// # Returns
    ///
    /// A vector of references to all keys in the cache.
    ///
    pub fn keys(&self) -> Vec<&K> {
        self.store.keys().collect()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.store.iter_mut().map(|(k, v)| (k, &mut v.value))
    }

    fn discard_old_descriptors(&mut self) -> Vec<(K, V)> {
        // remove old descriptors
        let mut removed = Vec::new();
        removed.reserve(self.store.len());

        // need to collect keys to remove because we can't remove while iterating
        let mut keys_to_remove = Vec::new();
        for (key, value) in self.store.iter() {
            if value.last_access.elapsed() > self.ttl {
                keys_to_remove.push(key.clone());
            }
        }

        for key in keys_to_remove {
            if let Some(value) = self.remove(&key) {
                removed.push((key, value));
            }
        }

        // check if the cache is full and remove old
        if self.store.len() > self.size {
            let mut oldest: Option<(&K, &ExpiringValue<V>)> = None;

            for (key, value) in self.store.iter() {
                if let Some(oldest_value) = oldest {
                    if value.last_access < oldest_value.1.last_access {
                        oldest = Some((key, value));
                    }
                } else {
                    oldest = Some((key, value));
                }
            }

            let key = oldest.unwrap().0.clone();
            if let Some(value) = self.remove(&key) {
                removed.push((key, value));
            }
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_insert() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&3), Some(&3));
    }

    #[test]
    fn test_get() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);

        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&3), None);
    }

    #[test]
    fn test_get_mut() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);

        assert_eq!(cache.get_mut(&1), Some(&mut 1));
        assert_eq!(cache.get_mut(&2), Some(&mut 2));
        assert_eq!(cache.get_mut(&3), None);
    }

    #[test]
    fn test_remove() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);

        assert_eq!(cache.remove(&1), Some(1));
        assert_eq!(cache.remove(&1), None);
        assert_eq!(cache.remove(&2), Some(2));
        assert_eq!(cache.remove(&2), None);
    }

    #[test]
    fn test_values() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);

        let values = cache.values();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&&1));
        assert!(values.contains(&&2));
    }

    #[test]
    fn test_ttl() {
        let mut cache = Cache::new(2, Duration::from_millis(100));
        cache.insert(1, 1);

        sleep(Duration::from_millis(200));

        let discarded = cache.insert(2, 2);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&2), Some(&2));

        assert_eq!(discarded.len(), 1);
        assert_eq!(discarded[0].0, 1);
        assert_eq!(discarded[0].1, 1);
    }

    #[test]
    fn test_cache_max_size() {
        let mut cache = Cache::new(2, Duration::from_secs(1));
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&3), Some(&3));
    }
}
