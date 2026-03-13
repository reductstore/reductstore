// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

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

    pub fn upgrade_opt(&self) -> Option<Arc<T>> {
        self.weak.upgrade()
    }

    pub fn upgrade_and_unwrap(&self) -> Arc<T> {
        self.weak.upgrade().unwrap()
    }
}

impl<T> From<Arc<T>> for Weak<T> {
    fn from(data: Arc<T>) -> Self {
        Self::new(data)
    }
}
