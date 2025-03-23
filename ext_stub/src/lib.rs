use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, IoExtension, IoExtensionInfo, ProcessStatus};
use reduct_base::io::{ReadChunk, ReadRecord, RecordMeta};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::Labels;
use std::time::Duration;

#[no_mangle]
pub fn get_ext() -> *mut (dyn IoExtension + Send + Sync) {
    // Return a raw pointer to an instance of our plugin
    Box::into_raw(Box::new(TestExtension::new()))
}

struct TestExtension {
    info: IoExtensionInfo,
}

impl TestExtension {
    fn new() -> Self {
        Self {
            info: IoExtensionInfo::builder()
                .name("ext_stub")
                .version("0.1")
                .build(),
        }
    }
}

impl IoExtension for TestExtension {
    fn info(&self) -> &IoExtensionInfo {
        &self.info
    }

    fn register_query(
        &self,
        _query_id: u64,
        _bucket_name: &str,
        _entry_name: &str,
        _query: &QueryEntry,
    ) -> Result<(), ReductError> {
        Ok(())
    }

    fn next_processed_record(&self, _query_id: u64, reader: BoxedReadRecord) -> ProcessStatus {
        let labels = reader.labels().clone();
        let wrapper = Wrapper {
            reader,
            labels,
            computed_labels: Labels::from_iter(vec![("ext_label".to_string(), "true".to_string())]),
        };

        ProcessStatus::Ready(Ok(Box::new(wrapper)))
    }
}

struct Wrapper {
    reader: BoxedReadRecord,
    labels: Labels,
    computed_labels: Labels,
}

impl RecordMeta for Wrapper {
    fn timestamp(&self) -> u64 {
        self.reader.timestamp()
    }

    fn labels(&self) -> &Labels {
        &self.labels
    }
}

#[async_trait]
impl ReadRecord for Wrapper {
    async fn read(&mut self) -> ReadChunk {
        self.reader.read().await
    }

    async fn read_timeout(&mut self, timeout: Duration) -> ReadChunk {
        self.reader.read_timeout(timeout).await
    }

    fn blocking_read(&mut self) -> ReadChunk {
        self.reader.blocking_read()
    }

    fn last(&self) -> bool {
        self.reader.last()
    }
    fn computed_labels(&self) -> &Labels {
        &self.computed_labels
    }

    fn computed_labels_mut(&mut self) -> &mut Labels {
        &mut self.computed_labels
    }

    fn content_length(&self) -> u64 {
        self.reader.content_length()
    }

    fn content_type(&self) -> &str {
        self.reader.content_type()
    }
}
