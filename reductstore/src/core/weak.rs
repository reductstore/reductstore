// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use std::sync::{Arc, Weak as StdWeak};

pub struct Weak<T> {
    weak: StdWeak<T>,
}

impl<T> Weak<T> {
    pub fn new(data: Arc<T>) -> Self {
        Self {
            weak: Arc::downgrade(&data),
        }
    }
    pub fn upgrade(&self) -> Result<Arc<T>, ReductError> {
        self.weak
            .upgrade()
            .ok_or(internal_server_error!("Weak reference is no longer valid"))
    }
}

impl<T> From<Arc<T>> for Weak<T> {
    fn from(data: Arc<T>) -> Self {
        Self::new(data)
    }
}
