// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::io::{BoxedReadRecord, ReadChunk, ReadRecord, RecordMeta};
use bytes::Bytes;
use std::io::{Read, Seek, SeekFrom};

/// A simple in-memory record that can be read in one chunk
#[derive(Debug)]
pub struct OneShotRecord {
    content: Option<Bytes>,
    meta: RecordMeta,
}

impl OneShotRecord {
    pub fn new(content: Bytes, meta: RecordMeta) -> Self {
        Self {
            content: Some(content),
            meta,
        }
    }

    pub fn boxed(content: Bytes, meta: RecordMeta) -> BoxedReadRecord {
        Box::new(Self::new(content, meta))
    }
}

impl Read for OneShotRecord {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!()
    }
}

impl Seek for OneShotRecord {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        unimplemented!()
    }
}

impl ReadRecord for OneShotRecord {
    fn read_chunk(&mut self) -> ReadChunk {
        match self.content.take() {
            Some(content) => Some(Ok(content)),
            None => None,
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
    use bytes::Bytes;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use std::io::{Seek, SeekFrom};

    #[rstest]
    fn test_read_chunk(mut record: BoxedReadRecord) {
        // First read_chunk should return the content
        let chunk = record.read_chunk();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().unwrap(), "Hello World");

        // Second read_chunk should return None
        let chunk = record.read_chunk();
        assert!(chunk.is_none());
    }

    #[rstest]
    fn test_meta(record: BoxedReadRecord, meta: RecordMeta) {
        assert_eq!(record.meta(), &meta);
    }

    #[rstest]
    fn test_meta_mut(mut record: BoxedReadRecord) {
        record.meta_mut().state = 1;
        assert_eq!(record.meta().state, 1);
    }

    #[rstest]
    #[should_panic(expected = "not implemented")]
    fn test_seek(mut record: BoxedReadRecord) {
        let _ = record.seek(SeekFrom::Start(5));
    }

    #[rstest]
    #[should_panic(expected = "not implemented")]
    fn test_read(mut record: BoxedReadRecord) {
        let mut buf = [0; 5];
        let _ = record.read(&mut buf);
    }

    #[fixture]
    fn meta() -> RecordMeta {
        RecordMeta {
            entry_name: "entry".to_string(),
            timestamp: 0,
            state: 0,
            labels: HashMap::new(),
            content_type: "application/octet-stream".to_string(),
            content_length: 11,
            computed_labels: HashMap::new(),
        }
    }

    #[fixture]
    fn record(meta: RecordMeta) -> BoxedReadRecord {
        let content = Bytes::from("Hello World");
        OneShotRecord::boxed(content, meta)
    }
}
