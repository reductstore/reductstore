// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::storage::storage::MAX_IO_BUFFER_SIZE;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;
use futures_util::Future;
use futures_util::Stream;
use reduct_base::error::ReductError;
use reduct_base::io::{BoxedReadRecord, RecordMeta};
use reduct_base::unprocessable_entity;
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
        "content-type",
        HeaderValue::from_str(meta.content_type()).unwrap(),
    );
    headers.insert("content-length", HeaderValue::from(meta.content_length()));
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
            lock.deref_mut().seek(Start(start)).unwrap();
            let read = lock.read(&mut buf);
            let result = match read {
                Ok(0) => Poll::Ready(None),
                Ok(n) => Poll::Ready(Some(Ok(Bytes::from(buf[..n].to_vec())))),
                Err(e) => Poll::Ready(Some(Err(unprocessable_entity!("Read error: {}", e).into()))),
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
        use std::collections::Bound::{Included, Unbounded};
        use std::io::{Read, Seek, SeekFrom};

        #[rstest]
        #[tokio::test]
        #[case(3)]
        #[case(10)]
        #[case(100)]
        async fn read_ranges(#[case] buffer_size: usize) {
            let ranges = VecDeque::from(vec![
                (Included(0), Included(4)),
                (Included(10), Included(14)),
                (Included(20), Unbounded),
            ]);

            let data = b"xxxxx-----yyyyy-----zzzzzz".to_vec();
            let record = InMemoryRecord::new(data.clone());
            let reader: Arc<Mutex<BoxedReadRecord>> = Arc::new(Mutex::new(Box::new(record)));

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

        struct InMemoryRecord {
            data: Vec<u8>,
            position: usize,
            meta: RecordMeta,
        }

        impl InMemoryRecord {
            fn new(data: Vec<u8>) -> Self {
                let len = data.len();

                Self {
                    data,
                    position: 0,
                    meta: RecordMeta::builder()
                        .timestamp(0)
                        .content_length(len as u64)
                        .content_type("application/octet-stream".to_string())
                        .build(),
                }
            }
        }

        impl Seek for InMemoryRecord {
            fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
                match pos {
                    Start(offset) => {
                        self.position = offset as usize;
                        Ok(self.position as u64)
                    }
                    _ => unimplemented!(),
                }
            }
        }

        impl Read for InMemoryRecord {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let remaining = &self.data[self.position..];
                let len = remaining.len().min(buf.len());
                buf[..len].copy_from_slice(&remaining[..len]);
                self.position += len;
                Ok(len)
            }
        }

        impl ReadRecord for InMemoryRecord {
            fn read_chunk(&mut self) -> Option<Result<Bytes, ReductError>> {
                unimplemented!()
            }

            fn meta(&self) -> &RecordMeta {
                &self.meta
            }

            fn meta_mut(&mut self) -> &mut RecordMeta {
                unimplemented!()
            }
        }
    }
}
