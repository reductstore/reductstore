// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::io::RecordMeta;
use bytes::Bytes;
use std::io::{Read, Seek};

/// A record that reads from a `Read + Seek` source in chunks
/// The chunk size is configurable
pub struct CursorRecord<R: Read + Seek> {
    inner: R,
    meta: RecordMeta,
    chunk_size: usize,
    pos: u64,
}

impl<R: Read + Seek> CursorRecord<R> {
    pub fn new(inner: R, meta: RecordMeta, chunk_size: usize) -> Self {
        Self {
            inner,
            meta,
            chunk_size,
            pos: 0,
        }
    }

    pub fn boxed(inner: R, meta: RecordMeta, chunk_size: usize) -> Box<Self> {
        Box::new(Self::new(inner, meta, chunk_size))
    }
}

impl<R: Read + Seek> Read for CursorRecord<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: Read + Seek> Seek for CursorRecord<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let pos = self.inner.seek(pos)?;
        self.pos = pos;
        Ok(pos)
    }
}

impl<R: Read + Seek> crate::io::ReadRecord for CursorRecord<R> {
    fn read_chunk(&mut self) -> crate::io::ReadChunk {
        let mut buf = vec![0; self.chunk_size];
        match self.inner.read(&mut buf) {
            Ok(0) => None,
            Ok(n) => {
                self.pos += n as u64;
                buf.truncate(n);
                Some(Ok(Bytes::from(buf)))
            }
            Err(e) => Some(Err(e.into())),
        }
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::ReadRecord;
    use bytes::Bytes;
    use rstest::{fixture, rstest};
    use std::io::SeekFrom;

    #[rstest]
    fn test_read_chunk(mut record: CursorRecord<std::io::Cursor<Vec<u8>>>) {
        // First read_chunk should return the content
        let chunk = record.read_chunk();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().unwrap(), Bytes::from("Hello World"));

        // Second read_chunk should return None
        let chunk = record.read_chunk();
        assert!(chunk.is_none());
    }

    #[rstest]
    #[case(SeekFrom::Start(1), 1)]
    #[case(SeekFrom::Current(6), 6)]
    #[case(SeekFrom::End(-5), 6)]
    fn test_seek(
        mut record: CursorRecord<std::io::Cursor<Vec<u8>>>,
        #[case] seek_from: SeekFrom,
        #[case] expected_pos: u64,
    ) {
        let pos = record.seek(seek_from).unwrap();
        assert_eq!(pos, expected_pos);
        assert_eq!(record.pos, expected_pos);
    }

    #[rstest]
    fn test_meta(record: CursorRecord<std::io::Cursor<Vec<u8>>>) {
        assert_eq!(record.meta().timestamp(), 1234567890);
        assert_eq!(record.meta().content_type(), "text/plain");
        assert_eq!(record.meta().content_length(), 11);
    }

    #[rstest]
    fn test_meta_mut(mut record: CursorRecord<std::io::Cursor<Vec<u8>>>) {
        record.meta_mut().state = 1;
        assert_eq!(record.meta().state(), 1);
    }

    #[fixture]
    fn record() -> CursorRecord<std::io::Cursor<Vec<u8>>> {
        let data = b"Hello World".to_vec();
        let cursor = std::io::Cursor::new(data);
        let meta = RecordMeta::builder()
            .timestamp(1234567890)
            .content_length(11)
            .content_type("text/plain".to_string())
            .build();
        CursorRecord::new(cursor, meta, 1024)
    }
}
