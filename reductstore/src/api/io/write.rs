// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::common::parse_content_length_from_header;
use crate::api::Components;
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::WriteAccessPolicy;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::bucket::Bucket;
use crate::storage::entry::RecordDrainer;
use axum::body::Body;
use axum::body::BodyDataStream;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum_extra::headers::{Expect, Header};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::batch::v2::{
    make_error_batched_header, parse_batched_headers, parse_entries,
    sort_headers_by_entry_and_time, EntryRecordHeader,
};
use reduct_base::error::ReductError;
use reduct_base::io::{RecordMeta, WriteRecord};
use reduct_base::{bad_request, internal_server_error, unprocessable_entity};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::timeout;

struct WriteContext {
    entry_name: String,
    time: u64,
    header: EntryRecordHeader,
    writer: Box<dyn WriteRecord + Sync + Send>,
}

type ErrorMap = std::collections::BTreeMap<(usize, u64), ReductError>;

#[derive(Clone)]
struct IndexedRecordHeader {
    entry_index: usize,
    time_delta: u64,
    record: EntryRecordHeader,
}

// POST /io/:bucket_name/write
pub(super) async fn write_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    body: Body,
) -> Result<impl IntoResponse, HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(&headers.clone(), WriteAccessPolicy { bucket })
        .await?;

    let parsed_headers = parse_and_index_headers(&headers)?;
    let content_length = check_and_get_content_length(&headers, &parsed_headers)?;
    let mut stream = body.into_data_stream();

    let process_stream = async {
        let (rx_writer, spawn_handler) =
            spawn_getting_writers(&components, bucket, parsed_headers)?;

        receive_body_and_write_records(
            bucket,
            components,
            content_length as i64,
            &mut stream,
            rx_writer,
        )
        .await?;

        Ok(spawn_handler
            .await
            .map_err(|e| internal_server_error!("Failed to complete write operation: {}", e))?)
    };

    match process_stream.await {
        Ok(error_map) => {
            let mut resp_headers = HeaderMap::new();
            error_map.iter().for_each(|((entry_idx, delta), err)| {
                let (name, value) = make_error_batched_header(*entry_idx, *delta, err);
                resp_headers.insert(name, value);
            });

            Ok(resp_headers.into())
        }
        Err(err) => {
            if !headers.contains_key(Expect::name()) {
                debug!("draining the stream");
                while let Some(_) = stream.next().await {}
            }
            Err::<HeaderMap, HttpError>(err)
        }
    }
}

fn parse_and_index_headers(headers: &HeaderMap) -> Result<Vec<IndexedRecordHeader>, ReductError> {
    let entries = parse_entries(headers)?;
    let records = parse_batched_headers(headers)?;
    let sorted_header_meta = sort_headers_by_entry_and_time(headers)?;

    if records.len() != sorted_header_meta.len() {
        return Err(unprocessable_entity!("Invalid batched headers"));
    }

    let mut indexed_headers = Vec::with_capacity(records.len());
    for (record, (entry_index, delta, _)) in records.into_iter().zip(sorted_header_meta.into_iter())
    {
        let expected_entry = entries.get(entry_index).ok_or_else(|| {
            unprocessable_entity!(
                "Invalid header 'x-reduct-{}-{}': entry index out of range",
                entry_index,
                delta
            )
        })?;
        if expected_entry != &record.entry {
            return Err(unprocessable_entity!("Invalid batched headers"));
        }

        indexed_headers.push(IndexedRecordHeader {
            entry_index,
            time_delta: delta,
            record,
        });
    }

    Ok(indexed_headers)
}

fn check_and_get_content_length(
    headers: &HeaderMap,
    records: &Vec<IndexedRecordHeader>,
) -> Result<u64, HttpError> {
    let total_content_length: u64 = records
        .iter()
        .map(|record| record.record.header.content_length)
        .sum();

    let header_content_length = parse_content_length_from_header(headers)?;

    if total_content_length != header_content_length {
        return Err(unprocessable_entity!(
            "content-length header does not match the sum of the content-lengths in the headers",
        )
        .into());
    }

    Ok(total_content_length)
}

async fn notify_replication_write(
    components: &Arc<Components>,
    bucket: &str,
    ctx: &WriteContext,
) -> Result<(), ReductError> {
    components
        .replication_repo
        .write()
        .await?
        .notify(TransactionNotification {
            bucket: bucket.to_string(),
            entry: ctx.entry_name.clone(),
            meta: RecordMeta::builder()
                .timestamp(ctx.time)
                .labels(ctx.header.header.labels.clone())
                .build(),
            event: Transaction::WriteRecord(ctx.time),
        })?;
    Ok(())
}

async fn receive_body_and_write_records(
    bucket: &String,
    components: Arc<Components>,
    mut total_content_len: i64,
    stream: &mut BodyDataStream,
    mut rx_writer: Receiver<WriteContext>,
) -> Result<(), ReductError> {
    let mut chunk = Bytes::new();

    let mut read_chunk = async || -> Result<Bytes, ReductError> {
        if total_content_len == 0 {
            // it makes the code simpler to handle the last empty chunk case and empty records
            return Ok(Bytes::new());
        }
        match timeout(components.cfg.io_conf.operation_timeout, stream.next())
            .await
            .map_err(|_| bad_request!("Timeout while receiving data"))?
        {
            Some(Ok(data_chunk)) => {
                total_content_len -= data_chunk.len() as i64;
                Ok(data_chunk)
            }
            Some(Err(e)) => Err(bad_request!("Error while receiving data chunk: {}", e)),
            None => Err(bad_request!(
                "Content is shorter than expected: no more data to read"
            )),
        }
    };

    while let Some(mut ctx) = rx_writer.recv().await {
        let mut written = 0;

        if chunk.is_empty() {
            chunk = read_chunk().await?
        }

        loop {
            match write_chunk(
                &mut ctx.writer,
                chunk,
                &mut written,
                ctx.header.header.content_length.clone(),
                components.cfg.io_conf.operation_timeout,
            )
            .await
            {
                Ok(None) => {
                    // chunk is written but record is not finished yet
                    chunk = read_chunk().await?;
                    continue;
                }
                Ok(Some(rest)) => {
                    // finish writing the current record and start a new one
                    // finished writing the current record
                    if let Err(err) = ctx
                        .writer
                        .send_timeout(Ok(None), components.cfg.io_conf.operation_timeout)
                        .await
                    {
                        debug!("Timeout while sending EOF: {}", err);
                    }

                    notify_replication_write(&components, bucket, &ctx).await?;
                    chunk = rest;
                    break;
                }
                Err(err) => return Err(err),
            }
        }
    }

    Ok(())
}

fn spawn_getting_writers(
    components: &Arc<Components>,
    bucket_name: &str,
    records: Vec<IndexedRecordHeader>,
) -> Result<(Receiver<WriteContext>, JoinHandle<ErrorMap>), ReductError> {
    let (tx_writer, rx_writer) = tokio::sync::mpsc::channel(64);

    let bucket = components
        .storage
        .get_bucket(&bucket_name)?
        .upgrade_and_unwrap();

    let spawn_handler = tokio::spawn(async move {
        let mut error_map: ErrorMap = ErrorMap::new();

        for record in records.into_iter() {
            let writer = start_writing(
                &record.record.entry,
                bucket.clone(),
                record.record.timestamp,
                &record.record.header,
                &mut error_map,
                record.entry_index,
                record.time_delta,
            )
            .await;

            tx_writer
                .send(WriteContext {
                    entry_name: record.record.entry.clone(),
                    time: record.record.timestamp,
                    header: record.record,
                    writer,
                })
                .await
                .map_err(|err| error!("Failed to send the writer: {}", err))
                .unwrap_or(());
        }
        error_map
    });

    Ok((rx_writer, spawn_handler))
}

async fn write_chunk(
    writer: &mut Box<dyn WriteRecord + Sync + Send>,
    chunk: Bytes,
    written: &mut usize,
    content_size: u64,
    io_timeout: std::time::Duration,
) -> Result<Option<Bytes>, ReductError> {
    let to_write = content_size - *written as u64;
    *written += chunk.len();
    let (chunk, rest) = if (chunk.len() as u64) < to_write {
        (chunk, None)
    } else {
        let chuck_to_write = chunk.slice(0..to_write as usize);
        (chuck_to_write, Some(chunk.slice(to_write as usize..)))
    };

    writer.send_timeout(Ok(Some(chunk)), io_timeout).await?;
    Ok(rest)
}

async fn start_writing(
    entry_name: &str,
    bucket: Arc<Bucket>,
    time: u64,
    record_header: &reduct_base::batch::RecordHeader,
    error_map: &mut ErrorMap,
    entry_index: usize,
    delta: u64,
) -> Box<dyn WriteRecord + Sync + Send> {
    let get_writer = async {
        bucket
            .begin_write(
                entry_name,
                time,
                record_header.content_length.clone(),
                record_header.content_type.clone(),
                record_header.labels.clone(),
            )
            .await
    };

    match get_writer.await {
        Ok(writer) => writer,
        Err(err) => {
            error_map.insert((entry_index, delta), err);
            // drain the stream
            Box::new(RecordDrainer::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_bucket_1};
    use axum::extract::Path;
    use axum::http::HeaderName;
    use axum_extra::headers::HeaderValue;
    use bytes::Bytes;
    use futures_util::stream;
    use reduct_base::batch::v2::encode_entry_name;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use reduct_base::io::WriteRecord;
    use reduct_base::Labels;
    use rstest::rstest;
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_multiple_entries(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("content-length", HeaderValue::from_static("10"));
        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_str(
                format!(
                    "{},{}",
                    encode_entry_name("entry-1"),
                    encode_entry_name("entry-2")
                )
                .as_str(),
            )
            .unwrap(),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("1000"));
        headers.insert(
            HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("4,text/plain,a=1"),
        );
        headers.insert(
            HeaderName::from_static("x-reduct-0-10"),
            HeaderValue::from_static("2,,a=2"),
        );
        headers.insert(
            HeaderName::from_static("x-reduct-1-0"),
            HeaderValue::from_static("3,text/csv,b=1"),
        );
        headers.insert(
            HeaderName::from_static("x-reduct-1-5"),
            HeaderValue::from_static("1,,b="),
        );

        let body = Body::from("aaaab bCCCd".replace(' ', "")); // 4 + 2 + 3 + 1 bytes

        write_batched_records(State(Arc::clone(&keeper)), headers, path_to_bucket_1, body)
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        {
            let mut reader = bucket.begin_read("entry-1", 1000).await.unwrap();
            assert_eq!(reader.meta().content_length(), 4);
            assert_eq!(reader.meta().content_type(), "text/plain");
            assert_eq!(reader.meta().labels().get("a").unwrap(), "1");
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("aaaa")));
        }
        {
            let mut reader = bucket.begin_read("entry-1", 1010).await.unwrap();
            assert_eq!(reader.meta().content_length(), 2);
            assert_eq!(reader.meta().labels().get("a").unwrap(), "2");
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("bb")));
        }
        {
            let mut reader = bucket.begin_read("entry-2", 1000).await.unwrap();
            assert_eq!(reader.meta().content_length(), 3);
            assert_eq!(reader.meta().content_type(), "text/csv");
            assert!(reader.meta().labels().get("b").is_some());
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("CCC")));
        }
        {
            let mut reader = bucket.begin_read("entry-2", 1005).await.unwrap();
            assert_eq!(reader.meta().content_length(), 1);
            assert!(!reader.meta().labels().contains_key("b"));
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("d")));
        }

        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("api-test")
            .unwrap();
        assert_eq!(info.info.pending_records, 4);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_content_length_mismatch(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        headers.insert("content-length", HeaderValue::from_static("15"));
        headers.insert("x-reduct-entries", HeaderValue::from_static("entry-1"));
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("0"));
        headers.insert(
            HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("5,text/plain"),
        );
        let err = write_batched_records(
            State(Arc::clone(&keeper.await)),
            headers,
            path_to_bucket_1,
            Body::from("12345"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "content-length header does not match the sum of the content-lengths in the headers",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_error_header(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_bucket_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("content-length", HeaderValue::from_static("6"));
        headers.insert(
            "x-reduct-entries",
            HeaderValue::from_static("entry-1,new-entry"),
        );
        headers.insert("x-reduct-start-ts", HeaderValue::from_static("0"));
        headers.insert(
            HeaderName::from_static("x-reduct-0-0"),
            HeaderValue::from_static("3,text/plain"),
        ); // conflicts with existing record at ts 0
        headers.insert(
            HeaderName::from_static("x-reduct-1-0"),
            HeaderValue::from_static("3,text/plain"),
        );

        let resp = write_batched_records(
            State(Arc::clone(&keeper)),
            headers,
            path_to_bucket_1,
            Body::from("abcdef"),
        )
        .await
        .unwrap()
        .into_response();

        let headers = resp.headers();
        assert_eq!(headers.len(), 1);
        let err_header = headers.get("x-reduct-error-0-0").unwrap();
        assert!(err_header.to_str().unwrap().starts_with("409,"));

        // The second record should be written despite the first failing.
        let mut reader = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap()
            .begin_read("new-entry", 0)
            .await
            .unwrap();
        assert_eq!(reader.meta().content_length(), 3);
        assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("def")));
    }

    struct TestWriter {
        chunks: StdArc<Mutex<Vec<Bytes>>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl WriteRecord for TestWriter {
        async fn send(&mut self, chunk: reduct_base::io::WriteChunk) -> Result<(), ReductError> {
            self.blocking_send(chunk)
        }

        fn blocking_send(&mut self, chunk: reduct_base::io::WriteChunk) -> Result<(), ReductError> {
            if self.fail {
                return Err(bad_request!("Simulated write error"));
            }
            if let Ok(Some(bytes)) = chunk {
                self.chunks.lock().unwrap().push(bytes);
            }
            Ok(())
        }

        async fn send_timeout(
            &mut self,
            chunk: reduct_base::io::WriteChunk,
            _timeout: Duration,
        ) -> Result<(), ReductError> {
            self.blocking_send(chunk)
        }
    }

    fn make_entry_header(content_length: u64) -> EntryRecordHeader {
        EntryRecordHeader {
            entry: "entry-1".to_string(),
            timestamp: 0,
            header: reduct_base::batch::RecordHeader {
                content_length,
                content_type: "text/plain".to_string(),
                labels: Labels::new(),
            },
        }
    }

    #[tokio::test]
    async fn test_write_chunk_splits_and_returns_rest() {
        let chunks = StdArc::new(Mutex::new(Vec::new()));
        let mut writer: Box<dyn WriteRecord + Sync + Send> = Box::new(TestWriter {
            chunks: StdArc::clone(&chunks),
            fail: false,
        });
        let mut written = 0;
        let rest = write_chunk(
            &mut writer,
            Bytes::from("abcdef"),
            &mut written,
            4,
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(rest, Bytes::from("ef"));
        assert_eq!(written, 6);
        assert_eq!(chunks.lock().unwrap().as_slice(), &[Bytes::from("abcd")]);
    }

    #[tokio::test]
    async fn test_write_chunk_partial_no_rest() {
        let chunks = StdArc::new(Mutex::new(Vec::new()));
        let mut writer: Box<dyn WriteRecord + Sync + Send> = Box::new(TestWriter {
            chunks: StdArc::clone(&chunks),
            fail: false,
        });
        let mut written = 0;
        let rest = write_chunk(
            &mut writer,
            Bytes::from("abcd"),
            &mut written,
            10,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(rest.is_none());
        assert_eq!(written, 4);
        assert_eq!(chunks.lock().unwrap().as_slice(), &[Bytes::from("abcd")]);
    }

    #[tokio::test]
    async fn test_write_chunk_error_propagation() {
        let chunks = StdArc::new(Mutex::new(Vec::new()));
        let mut writer: Box<dyn WriteRecord + Sync + Send> = Box::new(TestWriter {
            chunks: StdArc::clone(&chunks),
            fail: true,
        });
        let mut written = 0;
        let err = write_chunk(
            &mut writer,
            Bytes::from("abcd"),
            &mut written,
            4,
            Duration::from_secs(1),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err, bad_request!("Simulated write error"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_receive_body_and_write_records_short_body(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let writer: Box<dyn WriteRecord + Sync + Send> = Box::new(TestWriter {
            chunks: StdArc::new(Mutex::new(Vec::new())),
            fail: false,
        });

        tx.send(WriteContext {
            entry_name: "entry-1".to_string(),
            time: 0,
            header: make_entry_header(5),
            writer,
        })
        .await
        .unwrap();

        let body = Body::from("abc");
        let mut stream = body.into_data_stream();
        let err =
            receive_body_and_write_records(&"bucket-1".to_string(), components, 5, &mut stream, rx)
                .await
                .err()
                .unwrap();

        assert_eq!(
            err,
            bad_request!("Content is shorter than expected: no more data to read")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_receive_body_and_write_records_chunk_error(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let writer: Box<dyn WriteRecord + Sync + Send> = Box::new(TestWriter {
            chunks: StdArc::new(Mutex::new(Vec::new())),
            fail: false,
        });

        tx.send(WriteContext {
            entry_name: "entry-1".to_string(),
            time: 0,
            header: make_entry_header(1),
            writer,
        })
        .await
        .unwrap();

        let body = Body::from_stream(stream::iter(vec![Err::<Bytes, ReductError>(bad_request!(
            "Simulated chunk error"
        ))]));
        let mut stream = body.into_data_stream();
        let err =
            receive_body_and_write_records(&"bucket-1".to_string(), components, 1, &mut stream, rx)
                .await
                .err()
                .unwrap();

        assert_eq!(
            err,
            bad_request!("Error while receiving data chunk: [BadRequest] Simulated chunk error")
        );
    }
}
