// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;
use futures_util::Stream;
use reduct_base::io::{ReadRecord, RecordMeta};
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) fn make_headers_from_reader(meta: &RecordMeta) -> HeaderMap {
    let mut headers = HeaderMap::new();

    for (k, v) in meta.labels() {
        headers.insert(
            format!("x-reduct-label-{}", k)
                .parse::<HeaderName>()
                .unwrap(),
            v.parse().unwrap(),
        );
    }

    headers.insert(
        "content-type",
        HeaderValue::from_str(meta.content_type()).unwrap(),
    );
    headers.insert("content-length", HeaderValue::from(meta.content_length()));
    headers.insert("x-reduct-time", HeaderValue::from(meta.timestamp()));
    headers
}

pub(super) struct RecordStream {
    reader: Box<dyn ReadRecord + Send>,
    empty_body: bool,
}

impl RecordStream {
    pub fn new(reader: Box<dyn ReadRecord + Send>, empty_body: bool) -> Self {
        Self { reader, empty_body }
    }
}

/// A wrapper around a `RecordReader` that implements `Stream` with RwLock
impl Stream for RecordStream {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(
        mut self: Pin<&mut RecordStream>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.empty_body {
            return Poll::Ready(None);
        }

        match self.reader.read_chunk() {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(chunk))),
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
