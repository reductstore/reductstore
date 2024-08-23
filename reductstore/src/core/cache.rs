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

    pub fn insert(&mut self, key: K, value: V) {
        let value = ExpiringValue {
            value,
            last_access: Instant::now(),
        };
        self.store.insert(key, value);
        self.discard_old_descriptors();
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key).map(|v| &*v)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.discard_old_descriptors();

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

    pub fn remove(&mut self, key: &K) {
        self.store.remove(key);
    }

    pub fn clear(&mut self) {
        self.store.clear()
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.store.contains_key(key)
    }

    fn discard_old_descriptors(&mut self) {
        // remove old descriptors
        self.store
            .retain(|_, value| value.last_access.elapsed() < self.ttl);

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
            self.remove(&key);
        }
    }
}
