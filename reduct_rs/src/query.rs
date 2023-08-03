// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::from_system_time;
use crate::Labels;
use chrono::Duration;
use futures::Stream;
use reduct_base::error::HttpError;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

pub struct Query {
    id: u64,
    start: Option<u64>,
    stop: Option<u64>,
    include: Option<Labels>,
    exclude: Option<Labels>,
    ttl: Option<Duration>,
    continuous: bool,
}

/// Builder for a query request.
pub struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    pub(crate) fn new() -> Self {
        Self {
            query: Query {
                id: 0,
                start: None,
                stop: None,
                include: None,
                exclude: None,
                ttl: None,
                continuous: false,
            },
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.query.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.query.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.query.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.query.stop = Some(time_us);
        self
    }

    /// Set the labels to include in the query.
    pub fn include(mut self, labels: Labels) -> Self {
        self.query.include = Some(labels);
        self
    }

    /// Set the labels to exclude from the query.
    pub fn exclude(mut self, labels: Labels) -> Self {
        self.query.exclude = Some(labels);
        self
    }

    /// Set TTL for the query.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.query.ttl = Some(ttl);
        self
    }

    /// Set the query to be continuous.
    pub fn continuous(mut self) -> Self {
        self.query.continuous = true;
        self
    }

    /// Set the query to be continuous.
    async fn query(mut self) -> Query {
        todo!()
    }
}

impl Stream for Query {
    type Item = Result<crate::Record, HttpError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        todo!()
    }
}
