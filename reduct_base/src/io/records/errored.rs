// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::internal_server_error;
use crate::io::ReductError;
use crate::io::{BoxedReadRecord, ReadChunk, ReadRecord, RecordMeta};
use std::io::{Error, Read, Seek, SeekFrom};

/// Dummy record that always returns an error on seek
pub struct ErroredSeekRecord {
    meta: RecordMeta,
}

impl ErroredSeekRecord {
    pub fn new(meta: RecordMeta) -> Self {
        Self { meta }
    }

    pub fn boxed(meta: RecordMeta) -> BoxedReadRecord {
        Box::new(Self::new(meta))
    }
}

impl Read for ErroredSeekRecord {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Seek for ErroredSeekRecord {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Seek error"))
    }
}

impl ReadRecord for ErroredSeekRecord {
    fn read_chunk(&mut self) -> ReadChunk {
        None
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}

/// Dummy record that always returns an error on read
pub struct ErroredReadRecord {
    meta: RecordMeta,
}

impl ErroredReadRecord {
    pub fn new(meta: RecordMeta) -> Self {
        Self { meta }
    }

    pub fn boxed(meta: RecordMeta) -> BoxedReadRecord {
        Box::new(Self::new(meta))
    }
}

impl Read for ErroredReadRecord {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Read error"))
    }
}

impl Seek for ErroredReadRecord {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        dummy_seek(pos)
    }
}

impl ReadRecord for ErroredReadRecord {
    fn read_chunk(&mut self) -> ReadChunk {
        None
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
    }
    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}

/// Dummy record that always returns an error on read_chunk
pub struct ErroredChunkRecord {
    meta: RecordMeta,
}

impl ErroredChunkRecord {
    #[allow(dead_code)]
    pub fn new(meta: RecordMeta) -> Self {
        Self { meta }
    }

    #[allow(dead_code)]
    pub fn boxed(meta: RecordMeta) -> BoxedReadRecord {
        Box::new(Self::new(meta))
    }
}

impl Read for ErroredChunkRecord {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Seek for ErroredChunkRecord {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        dummy_seek(pos)
    }
}

impl ReadRecord for ErroredChunkRecord {
    fn read_chunk(&mut self) -> ReadChunk {
        Some(Err(internal_server_error!("Chunk read error").into()))
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}

fn dummy_seek(_pos: SeekFrom) -> Result<u64, Error> {
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    mod errored_seek_record {
        use super::*;

        #[rstest]
        fn test_errored_seek_record(mut record: BoxedReadRecord) {
            // read_chunk should return None
            let chunk = record.read_chunk();
            assert!(chunk.is_none());

            let buf = &mut [0u8; 10];
            assert_eq!(record.read(buf).unwrap(), 0);

            // seek should return an error
            let seek_result = record.seek(SeekFrom::Start(0));
            assert!(seek_result.is_err());

            // just for coverage
            let _ = record.meta();
            let _ = record.meta_mut();
        }

        #[fixture]
        fn record() -> BoxedReadRecord {
            ErroredSeekRecord::boxed(RecordMeta::builder().build())
        }
    }

    mod errored_read_record {
        use super::*;

        #[rstest]
        fn test_errored_read_record(mut record: BoxedReadRecord) {
            // read_chunk should return None
            let chunk = record.read_chunk();
            assert!(chunk.is_none());

            let buf = &mut [0u8; 10];
            let read_result = record.read(buf);
            assert!(read_result.is_err());

            // just for coverage
            let _ = record.meta();
            let _ = record.meta_mut();
        }

        #[rstest]
        #[case(SeekFrom::Start(0), 0)]
        #[case(SeekFrom::Current(1), 0)]
        #[case(SeekFrom::End(-1), 0)]
        fn test_seek(
            mut record: BoxedReadRecord,
            #[case] seek_from: SeekFrom,
            #[case] expected_pos: u64,
        ) {
            let pos = record.seek(seek_from).unwrap();
            assert_eq!(pos, expected_pos);
        }

        #[fixture]
        fn record() -> BoxedReadRecord {
            ErroredReadRecord::boxed(RecordMeta::builder().build())
        }
    }

    mod errored_chunk_record {
        use super::*;

        #[rstest]
        fn test_errored_chunk_record(mut record: BoxedReadRecord) {
            // read_chunk should return an error
            let chunk = record.read_chunk();
            assert!(chunk.is_some());
            assert!(chunk.unwrap().is_err());

            let buf = &mut [0u8; 10];
            assert_eq!(record.read(buf).unwrap(), 0);

            // just for coverage
            let _ = record.meta();
            let _ = record.meta_mut();
        }

        #[rstest]
        #[case(SeekFrom::Start(0), 0)]
        #[case(SeekFrom::Current(1), 0)]
        #[case(SeekFrom::End(-1), 0)]
        fn test_seek(
            mut record: BoxedReadRecord,
            #[case] seek_from: SeekFrom,
            #[case] expected_pos: u64,
        ) {
            let pos = record.seek(seek_from).unwrap();
            assert_eq!(pos, expected_pos);
        }

        #[fixture]
        fn record() -> BoxedReadRecord {
            ErroredChunkRecord::boxed(RecordMeta::builder().build())
        }
    }
}
