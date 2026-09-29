// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod http;
mod local;

use crate::replication::remote_bucket::{ErrorRecordMap, RemoteBucketConfig};
use async_trait::async_trait;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::BoxedReadRecord;

// A client API of the destination instance: HTTP for a remote host or the storage engine for the same instance.
#[async_trait]
pub(super) trait ReductClientApi {
    async fn get_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError>;

    async fn create_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError>;

    async fn get_or_create_bucket(&self, bucket_name: &str) -> Result<BoxedBucketApi, ReductError> {
        match self.get_bucket(bucket_name).await {
            Ok(bucket) => Ok(bucket),
            Err(err) if err.status() == ErrorCode::NotFound => {
                match self.create_bucket(bucket_name).await {
                    Ok(bucket) => Ok(bucket),
                    Err(err) if err.status() == ErrorCode::Conflict => {
                        self.get_bucket(bucket_name).await
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Endpoint of the destination instance for logging (URL or `local://`)
    fn endpoint(&self) -> &str;
}

pub(super) type BoxedClientApi = Box<dyn ReductClientApi + Sync + Send>;

// A bucket API of the destination instance.
#[async_trait]
pub(super) trait ReductBucketApi {
    async fn write_batch(
        &self,
        entry: &str,
        records: Vec<BoxedReadRecord>,
    ) -> Result<ErrorRecordMap, ReductError>;

    async fn update_batch(
        &self,
        entry: &str,
        records: &Vec<BoxedReadRecord>,
    ) -> Result<ErrorRecordMap, ReductError>;

    /// Endpoint of the destination instance for logging (URL or `local://`)
    fn endpoint(&self) -> &str;

    fn name(&self) -> &str;
}

pub(super) type BoxedBucketApi = Box<dyn ReductBucketApi + Sync + Send>;

/// Create a client for the destination: the storage engine for a local destination, HTTP otherwise.
pub(super) fn create_client(config: &RemoteBucketConfig) -> Result<BoxedClientApi, ReductError> {
    match &config.local {
        Some(destination) => Ok(Box::new(local::LocalClient::new(destination.clone()))),
        None => http::create_client(config),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use crate::core::sync::RwLock;
    use crate::storage::proto::Record;
    use bytes::Bytes;
    use crossbeam_channel::Receiver;
    use reduct_base::error::ReductError;
    use reduct_base::io::{BoxedReadRecord, ReadChunk, ReadRecord, RecordMeta};
    use std::io::{Read, Seek, SeekFrom};

    pub struct MockRecordReader {
        meta: RecordMeta,
        rx: RwLock<Receiver<Result<Bytes, ReductError>>>,
    }

    impl MockRecordReader {
        pub(crate) fn form_record_with_rx(
            rx: Receiver<Result<Bytes, ReductError>>,
            record: Record,
        ) -> BoxedReadRecord {
            Box::new(Self {
                meta: record.into(),
                rx: RwLock::new(rx),
            })
        }
    }

    impl Read for MockRecordReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not implemented",
            ))
        }
    }

    impl Seek for MockRecordReader {
        fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "not implemented",
            ))
        }
    }

    impl ReadRecord for MockRecordReader {
        fn read_chunk(&mut self) -> ReadChunk {
            match self.rx.write().unwrap().recv() {
                Ok(chunk) => Some(chunk),
                Err(_) => None,
            }
        }

        fn meta(&self) -> &RecordMeta {
            &self.meta
        }

        fn meta_mut(&mut self) -> &mut RecordMeta {
            &mut self.meta
        }
    }
}
