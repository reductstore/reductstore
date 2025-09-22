// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::storage::storage::MAX_IO_BUFFER_SIZE;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;
use futures_util::Future;
use futures_util::Stream;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::io::{ReadRecord, RecordMeta};
use std::cmp::min;
use std::io::Read;
use std::pin::{pin, Pin};
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
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.empty_body {
            return Poll::Ready(None);
        }

        let mut buffer = vec![
            0u8;
            min(
                self.reader.meta().content_length() as usize,
                MAX_IO_BUFFER_SIZE
            )
        ];
        let read = self.reader.read(&mut buffer);

        match read {
            Ok(read) => {
                if read == 0 {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Bytes::from(buffer[..read].to_vec()))))
                }
            }
            Err(e) => Poll::Ready(Some(
                Err(internal_server_error!("Read error: {}", e).into()),
            )),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
