use crate::error::ReductError;
use crate::io::{BoxedReadRecord, ReadRecord, RecordMeta};
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

    pub fn boxed<R: Iterator<Item = Result<Bytes, ReductError>> + 'static>(
        inner: R,
        meta: RecordMeta,
    ) -> BoxedReadRecord {
        Box::new(Self::new(inner, meta))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::BoxedReadRecord;
    use rstest::*;

    #[rstest]
    fn test_iter_record(mut record: BoxedReadRecord) {
        assert_eq!(record.meta().timestamp(), 123);
        assert_eq!(record.meta().content_type(), "text/plain");
        assert_eq!(record.meta().content_length(), 11);

        let chunk1 = record.read_chunk();
        assert!(chunk1.is_some());
        assert_eq!(chunk1.unwrap().unwrap(), Bytes::from("Hello "));

        let chunk2 = record.read_chunk();
        assert!(chunk2.is_some());
        assert_eq!(chunk2.unwrap().unwrap(), Bytes::from("World"));

        let chunk3 = record.read_chunk();
        assert!(chunk3.is_none());
    }

    #[rstest]
    #[should_panic(expected = "not implemented")]
    fn test_seek(record: BoxedReadRecord) {
        let mut record = record;
        let _ = record.seek(SeekFrom::Start(0));
    }

    #[rstest]
    #[should_panic(expected = "not implemented")]
    fn test_read(record: BoxedReadRecord) {
        let mut record = record;
        let mut buf = [0; 10];
        let _ = record.read(&mut buf);
    }

    #[fixture]
    fn record() -> BoxedReadRecord {
        let chunks = vec![Ok(Bytes::from("Hello ")), Ok(Bytes::from("World"))];
        let meta = RecordMeta::builder()
            .timestamp(123)
            .state(1)
            .content_type("text/plain".to_string())
            .content_length(11)
            .build();
        IterRecord::boxed(chunks.into_iter(), meta)
    }
}
