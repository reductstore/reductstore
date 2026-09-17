// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::storage::engine::MAX_IO_BUFFER_SIZE;
use crate::storage::entry::RecordWriter;
use crate::storage::proto::Record;
use prost_wkt_types::Timestamp;
use rand::distr::Alphanumeric;
use rand::{rng, RngExt};
use reduct_base::io::WriteRecord;
use rstest::fixture;
use tempfile::tempdir;

pub(crate) async fn write_record(
    record_time: u64,
    record_size: usize,
    block_manager: &Arc<AsyncRwLock<BlockManager>>,
    block_ref: BlockRef,
) -> (Record, String) {
    let block_size = block_ref.read().await.unwrap().size();
    let record = Record {
        timestamp: Some(us_to_ts(&record_time)),
        begin: block_size,
        end: (record_size as u64 + block_size),
        state: 1,
        labels: vec![],
        content_type: "".to_string(),
    };
    block_ref
        .write()
        .await
        .unwrap()
        .insert_or_update_record(record.clone());

    let record_body: String = rng()
        .sample_iter(&Alphanumeric)
        .take(record_size)
        .map(char::from)
        .collect();

    let block_copy = block_ref.clone();
    let body_copy = record_body.clone();
    let bm_copy = Arc::clone(block_manager);
    let mut writer = RecordWriter::try_new(bm_copy, block_copy, record_time)
        .await
        .unwrap();
    writer.send(Ok(Some(Bytes::from(body_copy)))).await.unwrap();
    writer.send(Ok(None)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await; // wait for thread to finish
    block_manager
        .write()
        .await
        .unwrap()
        .save_meta_on_disk(block_ref.clone())
        .await
        .unwrap();
    (record, record_body)
}

#[fixture]
pub(crate) fn block_id() -> u64 {
    1
}

#[fixture]
pub(crate) async fn block(#[future] block_manager: BlockManager, block_id: u64) -> BlockRef {
    let mut block_manager = block_manager.await;
    block_manager.load_block(block_id).await.unwrap()
}

#[fixture]
pub(crate) async fn block_manager(block_id: u64) -> BlockManager {
    let path = tempdir().unwrap().keep().join("bucket").join("entry");

    let mut bm = BlockManager::build(
        path.clone(),
        BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
        "bucket".to_string(),
        "entry".to_string(),
        Cfg::default().into(),
        Default::default(),
    )
    .await
    .unwrap();
    let block_ref = bm.start_new_block(block_id, 1024).await.unwrap().clone();

    let mut block = block_ref.write().await.unwrap();
    block.insert_or_update_record(Record {
        timestamp: Some(Timestamp {
            seconds: 0,
            nanos: 0,
        }),
        begin: 0,
        end: (MAX_IO_BUFFER_SIZE + 1) as u64,
        state: 0,
        labels: vec![],
        content_type: "".to_string(),
    });

    let (file, offset) = bm.begin_write_record(&block, 0).unwrap();
    drop(block);

    FILE_CACHE
        .write_or_create(&file, SeekFrom::Start(offset))
        .await
        .unwrap()
        .write(&vec![0; MAX_IO_BUFFER_SIZE + 1])
        .unwrap();

    bm.finish_write_record(block_id, record::State::Finished, 0)
        .await
        .unwrap();
    bm.save_meta_on_disk(block_ref).await.unwrap();
    bm
}
