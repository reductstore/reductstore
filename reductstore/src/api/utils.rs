// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::storage::engine::MAX_IO_BUFFER_SIZE;
use axum::http::header::CONTENT_DISPOSITION;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum_extra::headers::{ContentLength, HeaderMapExt};
use bytes::Bytes;
use futures_util::Future;
use futures_util::Stream;
use hyper::header::CONTENT_TYPE;
use reduct_base::error::ReductError;
use reduct_base::io::{BoxedReadRecord, RecordMeta};
use reduct_base::{internal_server_error, unprocessable_entity};
use std::collections::Bound::Included;
use std::collections::{Bound, VecDeque};
use std::io::SeekFrom::Start;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Mutex;

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
        CONTENT_TYPE,
        HeaderValue::from_str(meta.content_type()).unwrap(),
    );
    headers.typed_insert(ContentLength(meta.content_length()));
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str("attachment").unwrap(),
    );
    headers.insert("x-reduct-time", HeaderValue::from(meta.timestamp()));
    headers
}

pub(super) struct RecordStream {
    reader: Arc<Mutex<BoxedReadRecord>>,
    empty_body: bool,
}

impl RecordStream {
    pub fn new(reader: Arc<Mutex<BoxedReadRecord>>, empty_body: bool) -> Self {
        Self { reader, empty_body }
    }
}

/// A wrapper around a `RecordReader` that implements `Stream` with RwLock
impl Stream for RecordStream {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(self: Pin<&mut RecordStream>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.empty_body {
            return Poll::Ready(None);
        }

        let mut pinned = std::pin::pin!(self.reader.lock());
        let Poll::Ready(mut lock) = pinned.as_mut().poll(cx) else {
            return Poll::Pending;
        };

        match lock.read_chunk() {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(chunk))),
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub(super) struct RangeRecordStream {
    reader: Arc<Mutex<BoxedReadRecord>>,
    ranges: VecDeque<(Bound<u64>, Bound<u64>)>,
    buffer_size: usize,
}

impl RangeRecordStream {
    pub fn new(
        reader: Arc<Mutex<BoxedReadRecord>>,
        ranges: VecDeque<(Bound<u64>, Bound<u64>)>,
    ) -> Self {
        Self {
            reader,
            ranges,
            buffer_size: MAX_IO_BUFFER_SIZE,
        }
    }
}

impl Stream for RangeRecordStream {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(
        mut self: Pin<&mut RangeRecordStream>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(range) = self.ranges.pop_front() else {
            return Poll::Ready(None);
        };

        let (ret, start) = {
            let mut pinned = std::pin::pin!(self.reader.lock());
            let Poll::Ready(mut lock) = pinned.as_mut().poll(cx) else {
                return Poll::Pending;
            };

            let (start, end) = match range {
                (Included(s), Included(e)) => (s, e + 1),
                (Included(s), Bound::Unbounded) => (s, lock.meta().content_length()),
                (Bound::Unbounded, Included(e)) => (0, e + 1),
                _ => return Poll::Ready(Some(Err(unprocessable_entity!("Invalid range").into()))),
            };

            let mut buffer_size = (end - start) as usize;
            let overwrite_buffer = if buffer_size > self.buffer_size {
                buffer_size = self.buffer_size;
                true
            } else {
                false
            };

            let mut buf = vec![0; buffer_size];
            match lock.deref_mut().seek(Start(start)) {
                Err(err) => {
                    return Poll::Ready(Some(Err(
                        internal_server_error!("Seek error: {}", err).into()
                    )))
                }
                _ => {}
            }

            let read = lock.read(&mut buf);
            let result = match read {
                Ok(0) => Poll::Ready(None),
                Ok(n) => Poll::Ready(Some(Ok(Bytes::from(buf[..n].to_vec())))),
                Err(e) => Poll::Ready(Some(
                    Err(internal_server_error!("Read error: {}", e).into()),
                )),
            };

            (
                result,
                if overwrite_buffer {
                    Some((start, end))
                } else {
                    None
                },
            )
        };

        if let Some((start, end)) = start {
            let start = start + self.buffer_size as u64;
            // If the range is larger than the buffer size, we read in chunks
            // and push the remaining range back to the front of the queue
            self.ranges.push_front((Included(start), Included(end - 1)));
        }

        ret
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    mod record_stream {}

    mod range_record_stream {
        use super::*;
        use futures_util::StreamExt;
        use reduct_base::io::records::{CursorRecord, ErroredReadRecord, ErroredSeekRecord};
        use std::collections::Bound::{Included, Unbounded};
        use std::io::Cursor;

        #[rstest]
        #[tokio::test]
        #[case(3)]
        #[case(10)]
        #[case(100)]
        async fn read_ranges(#[case] buffer_size: usize) {
            let ranges = VecDeque::from(vec![
                (Unbounded, Included(4)),
                (Included(10), Included(14)),
                (Included(20), Unbounded),
            ]);

            let record = from_content(b"xxxxx-----yyyyy-----zzzzzz".to_vec());
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(record));

            let mut stream = RangeRecordStream::new(reader, ranges);
            stream.buffer_size = buffer_size;
            let mut result = Vec::new();
            while let Some(chunk) = stream.next().await {
                result.extend_from_slice(&chunk.unwrap());
            }

            assert_eq!(
                String::from_utf8(result).unwrap(),
                "xxxxxyyyyyzzzzzz".to_string()
            );
        }

        #[rstest]
        #[tokio::test]
        async fn read_invalid_range() {
            let ranges = VecDeque::from(vec![(Unbounded, Unbounded)]);
            let record = from_content(b"xxxxx-----yyyyy-----zzzzzz".to_vec());
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(record));

            let mut stream = RangeRecordStream::new(reader, ranges);
            let chunk = stream.next().await;
            assert_eq!(
                chunk.unwrap().err().unwrap(),
                unprocessable_entity!("Invalid range").into()
            );
        }

        #[rstest]
        #[tokio::test]
        async fn read_zero_content() {
            let ranges = VecDeque::from(vec![(Included(0), Included(0))]);
            let record = from_content(b"".to_vec());
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(record));

            let mut stream = RangeRecordStream::new(reader, ranges);
            let chunk = stream.next().await;
            assert!(chunk.is_none());
        }

        #[rstest]
        #[tokio::test]
        async fn read_error() {
            let ranges = VecDeque::from(vec![(Included(1), Included(2))]);
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(
                ErroredReadRecord::boxed(RecordMeta::builder().build()),
            ));

            let mut stream = RangeRecordStream::new(reader, ranges);
            let chunk = stream.next().await;
            assert_eq!(
                chunk.unwrap().err().unwrap(),
                internal_server_error!("Read error: Read error").into()
            );
        }

        #[rstest]
        #[tokio::test]
        async fn seek_error() {
            let ranges = VecDeque::from(vec![(Included(1), Included(2))]);
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(
                ErroredSeekRecord::boxed(RecordMeta::builder().build()),
            ));

            let mut stream = RangeRecordStream::new(reader, ranges);
            let chunk = stream.next().await;
            assert_eq!(
                chunk.unwrap().err().unwrap(),
                internal_server_error!("Seek error: Seek error").into()
            );
        }

        fn from_content(content: Vec<u8>) -> BoxedReadRecord {
            let len = content.len();
            let meta = RecordMeta::builder()
                .timestamp(0)
                .content_length(len as u64)
                .content_type("application / octet - stream".to_string())
                .build();

            CursorRecord::boxed(Cursor::new(content), meta, 10)
        }
    }

    mod make_headers_from_reader {
        use super::*;
        use axum::http::header::CONTENT_LENGTH;
        use reduct_base::io::records::OneShotRecord;
        use reduct_base::Labels;

        #[rstest]
        #[tokio::test]
        async fn test_make_headers() {
            let meta = RecordMeta::builder()
                .timestamp(1625077800)
                .content_length(1234)
                .content_type("application/json".to_string())
                .labels(Labels::from_iter(vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ]))
                .build();

            let record = OneShotRecord::boxed("".into(), meta.clone());
            let headers = make_headers_from_reader(record.meta());

            assert_eq!(
                headers.get("x-reduct-label-key1").unwrap(),
                &HeaderValue::from_str("value1").unwrap()
            );
            assert_eq!(
                headers.get("x-reduct-label-key2").unwrap(),
                &HeaderValue::from_str("value2").unwrap()
            );
            assert_eq!(
                headers.get(CONTENT_TYPE).unwrap(),
                &HeaderValue::from_str("application/json").unwrap()
            );
            assert_eq!(
                headers.get(CONTENT_LENGTH).unwrap(),
                &HeaderValue::from_str("1234").unwrap()
            );
            assert_eq!(
                headers.get(CONTENT_DISPOSITION).unwrap(),
                &HeaderValue::from_str("attachment").unwrap()
            );
            assert_eq!(
                headers.get("x-reduct-time").unwrap(),
                &HeaderValue::from_str("1625077800").unwrap()
            );
        }
    }
}
