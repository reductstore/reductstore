// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

struct ExpiringValue<V> {
    value: V,
    last_access: Instant,
}

type ExpiringStore<V> = HashMap<String, ExpiringValue<V>>;

/// A simple cache implementation that removes old entries after a given time.
///
/// The cache is limited in size and will remove the oldest entry if the limit is reached.
///
/// Only for references due to the `Clone` bound.
pub(crate) struct Cache<V>
where
    V: Clone,
{
    store: Arc<RwLock<ExpiringStore<V>>>,
    size: usize,
    ttl: Duration,
}

impl<V: Clone> Cache<V> {
    pub fn new(size: usize, ttl: Duration) -> Cache<V> {
        Cache {
            store: Arc::new(RwLock::new(HashMap::new())),
            size,
            ttl,
        }
    }

    pub async fn insert(&mut self, key: String, value: V) {
        let value = ExpiringValue {
            value,
            last_access: Instant::now(),
        };

        let mut store = self.store.write().await;
        Self::discard_old_descriptors(self.ttl, self.size, &mut store);
        store.insert(key, value);
    }

    pub async fn get(&self, key: &str) -> Option<V> {
        let mut store = self.store.write().await;
        Self::discard_old_descriptors(self.ttl, self.size, &mut store);

        let mut value = store.get_mut(key);
        if let Some(ref mut value) = value {
            value.last_access = Instant::now();
        }

        value.map(|v| v.value.clone())
    }

    pub async fn remove(&mut self, key: &str) {
        self.store.write().await.remove(key);
    }

    pub async fn clear(&mut self) {
        self.store.write().await.clear();
    }

    fn discard_old_descriptors(ttl: Duration, max_size: usize, store: &mut ExpiringStore<V>) {
        // remove old descriptors
        store.retain(|_, value| value.last_access.elapsed() < ttl);

        // check if the cache is full and remove old
        if store.len() > max_size {
            let mut oldest: Option<(&String, &ExpiringValue<V>)> = None;

            for (key, value) in store.iter() {
                if let Some(oldest_value) = oldest {
                    if value.last_access < oldest_value.1.last_access {
                        oldest = Some((key, value));
                    }
                } else {
                    oldest = Some((key, value));
                }
            }

            let key = oldest.unwrap().0.clone();
            store.remove(&key);
        }
    }
}
