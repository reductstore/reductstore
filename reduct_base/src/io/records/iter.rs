use crate::error::ReductError;
use crate::io::{ReadRecord, RecordMeta};
use bytes::Bytes;
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};

/// A record that reads data from an iterator of byte chunks
/// Each item from the iterator is a Result<Bytes, ReductError>
#[derive(Debug)]
pub struct IterRecord {
    inner: VecDeque<Result<Bytes, ReductError>>,
    meta: RecordMeta,
}

impl IterRecord {
    pub fn new<R: Iterator<Item = Result<Bytes, ReductError>>>(inner: R, meta: RecordMeta) -> Self {
        Self {
            inner: inner.collect(),
            meta,
        }
    }
}
impl Read for IterRecord {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!()
    }
}

impl Seek for IterRecord {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        unimplemented!()
    }
}

impl ReadRecord for IterRecord {
    fn read_chunk(&mut self) -> Option<Result<Bytes, ReductError>> {
        if self.inner.is_empty() {
            None
        } else {
            Some(self.inner.pop_front().unwrap())
        }
    }

    fn meta(&self) -> &RecordMeta {
        &self.meta
    }

    fn meta_mut(&mut self) -> &mut RecordMeta {
        &mut self.meta
    }
}
