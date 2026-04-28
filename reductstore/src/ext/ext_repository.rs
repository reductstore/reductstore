// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

mod create;
mod load;

use crate::core::sync::AsyncRwLock;
use crate::storage::engine::StorageEngine;
use crate::storage::entry::{is_system_meta_entry, meta_entry_name};
use crate::storage::query::base::QueryOptions;
use crate::storage::query::condition::{Parser, Value};
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use dlopen2::wrapper::{Container, WrapperApi};
use futures_util::StreamExt;
use log::warn;
use reduct_base::error::ErrorCode::{NoContent, NotFound};
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedCommiter, BoxedProcessor, ExtSettings, IoExtension};
use reduct_base::io::ReadRecord;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{no_content, unprocessable_entity};
use serde_json::Map;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

type IoExtRef = Arc<AsyncRwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct ExtensionApi {
    get_ext: extern "Rust" fn(settings: ExtSettings) -> *mut (dyn IoExtension + Send + Sync),
}

#[async_trait]
pub(crate) trait ManageExtensions {
    async fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: QueryEntry,
    ) -> Result<(), ReductError>;

    /// Fetches and processes a record from the extension.
    ///
    /// This method is called for each record that is fetched from the storage engine.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query.
    /// * `query_rx` - The receiver for the query.
    ///
    /// # Returns
    ///
    ///  204 No Content if no records are available.
    async fn fetch_and_process_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> Option<Vec<Result<BoxedReadRecord, ReductError>>>;
}

pub type BoxedManageExtensions = Box<dyn ManageExtensions + Sync + Send>;

pub(crate) struct QueryContext {
    query: QueryOptions,
    last_access: Instant,
    steps: Vec<PipelineStep>,
}

pub(crate) struct PipelineStep {
    condition_filter: Box<dyn RecordFilter<BoxedReadRecord> + Send + Sync>,
    processor: BoxedProcessor,
    commiter: BoxedCommiter,
}

struct ExtRepository {
    extension_map: IoExtMap,
    query_map: AsyncRwLock<HashMap<u64, QueryContext>>,
    io_config: IoConfig,
    storage: Option<Arc<StorageEngine>>,

    #[allow(dead_code)]
    ext_wrappers: Vec<Container<ExtensionApi>>, // we need to keep the wrappers alive
}

#[async_trait]
impl ManageExtensions for ExtRepository {
    /// Register a query with the extension
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query
    /// * `bucket_name` - The name of the bucket
    /// * `entry_name` - The name of the entry
    /// * `query_request` - The query request
    ///
    /// # Errors
    ///
    /// * `ReductError::InternalServerError` - If the query is not valid
    async fn register_query(
        &self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        mut query_request: QueryEntry,
    ) -> Result<(), ReductError> {
        let mut query_map = self.query_map.write().await?;

        let ext_directive = {
            if let Some(when) = &query_request.when {
                let (_, directives) = Parser::new().parse(when.clone())?;
                if let Some(ext) = directives.get("#ext") {
                    Some(Self::decode_ext_directive(ext, query_id)?)
                } else {
                    None
                }
            } else {
                None
            }
        };

        // use ext parameter of the query if no #ext directive is found
        let mut ext_params = ext_directive.or(query_request.ext.clone());

        let controllers = {
            if let Some(ext_params) = &mut ext_params {
                let Some(mut ext_steps) = Self::decode_ext_steps(ext_params, query_id)? else {
                    return Err(unprocessable_entity!(
                        "Extension name is not found in query id={}",
                        query_id
                    ));
                };

                let mut pipeline = Vec::with_capacity(ext_steps.len());
                for (idx, ext_query) in ext_steps.iter_mut().enumerate() {
                    let condition_filter = if let Some(condition) = ext_query.remove("when") {
                        let (node, directives) = Parser::new().parse(condition)?;
                        Box::new(WhenFilter::try_new(
                            node,
                            directives,
                            self.io_config.clone(),
                            true,
                        )?)
                    } else {
                        DummyFilter::boxed()
                    };

                    if ext_query.is_empty() {
                        return Err(unprocessable_entity!(
                            "Extension name is not found in query id={}",
                            query_id
                        ));
                    }

                    if ext_query.iter().count() > 1 {
                        if ext_steps.len() == 1 {
                            return Err(unprocessable_entity!(
                                "Multiple extensions are not supported in query id={}",
                                query_id
                            ));
                        }

                        return Err(unprocessable_entity!(
                            "Each '#ext' pipeline step must contain exactly one extension in query id={}",
                            query_id
                        ));
                    }

                    let Some(name) = ext_query.keys().next().cloned() else {
                        return Err(unprocessable_entity!(
                            "Extension name is not found in query id={}",
                            query_id
                        ));
                    };

                    if let Some(attachments) = self
                        .get_ext_attachments(bucket_name, entry_name, &query_request, &name)
                        .await?
                    {
                        Self::attach_ext_attachments(ext_query, &name, attachments);
                    }

                    let Some(ext) = self.extension_map.get(&name) else {
                        if ext_steps.len() == 1 {
                            return Err(unprocessable_entity!(
                                "Unknown extension '{}' in query id={}",
                                name,
                                query_id
                            ));
                        }

                        return Err(unprocessable_entity!(
                            "Unknown extension '{}' in query id={} at step {}",
                            name,
                            query_id,
                            idx
                        ));
                    };

                    let mut ext_query_request = query_request.clone();
                    ext_query_request.ext = Some(serde_json::Value::Object(ext_query.clone()));
                    let (processor, commiter) =
                        ext.write()
                            .await?
                            .query(bucket_name, entry_name, &ext_query_request)?;
                    pipeline.push(PipelineStep {
                        condition_filter,
                        processor,
                        commiter,
                    });
                }

                query_request.ext = Some(if ext_steps.len() == 1 {
                    serde_json::Value::Object(ext_steps[0].clone())
                } else {
                    serde_json::Value::Array(
                        ext_steps
                            .iter()
                            .cloned()
                            .map(serde_json::Value::Object)
                            .collect(),
                    )
                });

                Some(pipeline)
            } else {
                None
            }
        };

        let query_options: QueryOptions = query_request.into();
        // remove expired queries
        let mut ids_to_remove = Vec::new();

        for (key, query) in query_map.iter() {
            if query.last_access.elapsed() > query.query.ttl {
                ids_to_remove.push(*key);
            }
        }

        for key in ids_to_remove {
            query_map.remove(&key);
        }

        if let Some(steps) = controllers {
            query_map.insert(query_id, {
                QueryContext {
                    query: query_options,
                    last_access: Instant::now(),
                    steps,
                }
            });
        }

        Ok(())
    }

    async fn fetch_and_process_record(
        &self,
        query_id: u64,
        query_rx: Arc<AsyncRwLock<QueryRx>>,
    ) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
        let mut lock = match self.query_map.write().await {
            Ok(lock) => lock,
            Err(err) => return Some(vec![Err(err)]),
        };
        let query = match lock.get_mut(&query_id) {
            Some(query) => query,
            None => {
                let result = match query_rx.write().await {
                    Ok(mut rx) => rx
                        .recv()
                        .await
                        .map(|record| record.map(|r| vec![Box::new(r) as BoxedReadRecord])),
                    Err(err) => return Some(vec![Err(err)]),
                };

                if result.is_none() {
                    // If no record is available, return a no content error to finish the query.
                    return Some(vec![Err(no_content!("No content"))]);
                }

                return result.map(|r| {
                    r.map_or_else(
                        |e| vec![Err(e)],
                        |records| records.into_iter().map(Ok).collect(),
                    )
                });
            }
        };

        query.last_access = Instant::now();

        let Some(record) = (match query_rx.write().await {
            Ok(mut rx) => rx.recv().await,
            Err(err) => return Some(vec![Err(err)]),
        }) else {
            return Some(vec![Err(no_content!("No content"))]);
        };

        let record = match record {
            Ok(record) => record,
            Err(e) => {
                return if e.status == NoContent {
                    if let Some(mut flushed_records) = self.flush_pipeline(query).await {
                        flushed_records.push(Err(e));
                        Some(flushed_records)
                    } else {
                        Some(vec![Err(e)])
                    }
                } else {
                    Some(vec![Err(e)])
                };
            }
        };

        self.process_pipeline_record(query, Box::new(record)).await
    }
}

impl ExtRepository {
    fn parse_directive_value(
        value: &Value,
        query_id: u64,
    ) -> Result<serde_json::Value, ReductError> {
        serde_json::from_str(&value.to_string()).map_err(|err| {
            unprocessable_entity!(
                "Directive '#ext' must be a JSON object or an array of JSON objects in query id={}: {}",
                query_id,
                err
            )
        })
    }

    fn decode_ext_directive(
        ext: &[Value],
        query_id: u64,
    ) -> Result<serde_json::Value, ReductError> {
        if ext.is_empty() {
            return Ok(serde_json::Value::Null);
        }

        if ext.len() == 1 {
            return Self::parse_directive_value(&ext[0], query_id);
        }

        let mut steps = Vec::with_capacity(ext.len());
        for step in ext {
            steps.push(Self::parse_directive_value(step, query_id)?);
        }

        Ok(serde_json::Value::Array(steps))
    }

    fn decode_ext_steps(
        ext_params: &serde_json::Value,
        query_id: u64,
    ) -> Result<Option<Vec<Map<String, serde_json::Value>>>, ReductError> {
        match ext_params {
            serde_json::Value::Object(ext_query) => Ok(Some(vec![ext_query.clone()])),
            serde_json::Value::Array(steps) => {
                if steps.is_empty() {
                    return Ok(None);
                }

                let mut ext_steps = Vec::with_capacity(steps.len());
                for step in steps {
                    let Some(ext_query) = step.as_object() else {
                        return Err(unprocessable_entity!(
                            "Each '#ext' pipeline step must be a JSON object in query id={}",
                            query_id
                        ));
                    };
                    ext_steps.push(ext_query.clone());
                }
                Ok(Some(ext_steps))
            }
            _ => Err(unprocessable_entity!(
                "Extension parameters must be a JSON object or an array of JSON objects in query id={}",
                query_id
            )),
        }
    }

    async fn process_pipeline_step(
        &self,
        query: &mut QueryContext,
        step_index: usize,
        inputs: Vec<BoxedReadRecord>,
    ) -> Result<Vec<BoxedReadRecord>, ReductError> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let mut outputs = Vec::new();
        for input in inputs {
            let stream = {
                let step = &mut query.steps[step_index];
                match step.processor.process_record(input).await {
                    Ok(stream) => stream,
                    Err(err) => return Err(err),
                }
            };

            let mut stream = Box::into_pin(stream);
            while let Some(item) = stream.next().await {
                let record = match item {
                    Ok(record) => record,
                    Err(err) => return Err(err),
                };

                let filtered = {
                    let step = &mut query.steps[step_index];
                    match step.condition_filter.filter(record) {
                        Ok(Some(records)) => records,
                        Ok(None) => continue,
                        Err(err) => return Err(err),
                    }
                };

                for record in filtered {
                    let inherited_computed = record.meta().computed_labels().clone();
                    let committed = {
                        let step = &mut query.steps[step_index];
                        step.commiter.commit_record(record).await
                    };
                    if let Some(committed) = committed {
                        match committed {
                            Ok(mut rec) => {
                                for (key, value) in inherited_computed {
                                    rec.meta_mut()
                                        .computed_labels_mut()
                                        .entry(key)
                                        .or_insert(value);
                                }

                                if rec.meta().entry_name().is_empty() {
                                    warn!("Extension commiter returned an invalid record with empty entry name, skipping it");
                                    continue;
                                }
                                outputs.push(rec);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
        }

        Ok(outputs)
    }

    async fn process_pipeline_record(
        &self,
        query: &mut QueryContext,
        record: BoxedReadRecord,
    ) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
        let mut inputs = vec![record];
        for idx in 0..query.steps.len() {
            let outputs = match self.process_pipeline_step(query, idx, inputs).await {
                Ok(outputs) => outputs,
                Err(err) => return Some(vec![Err(err)]),
            };
            inputs = outputs;
        }

        if inputs.is_empty() {
            None
        } else {
            Some(inputs.into_iter().map(Ok).collect())
        }
    }

    async fn flush_pipeline(
        &self,
        query: &mut QueryContext,
    ) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
        let mut flushed_records = Vec::new();

        for idx in 0..query.steps.len() {
            let flushed = {
                let step = &mut query.steps[idx];
                step.commiter.flush().await
            };

            let Some(flushed) = flushed else {
                continue;
            };

            let mut inputs = match flushed {
                Ok(rec) => vec![rec],
                Err(err) => return Some(vec![Err(err)]),
            };

            for downstream_idx in (idx + 1)..query.steps.len() {
                inputs = match self
                    .process_pipeline_step(query, downstream_idx, inputs)
                    .await
                {
                    Ok(outputs) => outputs,
                    Err(err) => return Some(vec![Err(err)]),
                };
            }

            for rec in inputs {
                flushed_records.push(Ok(rec));
            }
        }

        if flushed_records.is_empty() {
            None
        } else {
            Some(flushed_records)
        }
    }

    async fn get_ext_attachments(
        &self,
        bucket_name: &str,
        entry_name: &str,
        query_request: &QueryEntry,
        ext_name: &str,
    ) -> Result<Option<serde_json::Value>, ReductError> {
        let Some(storage) = &self.storage else {
            return Ok(None);
        };

        let bucket = match storage.get_bucket(bucket_name).await {
            Ok(bucket) => bucket.upgrade()?,
            Err(err) if err.status == NotFound => return Ok(None),
            Err(err) => return Err(err),
        };

        let patterns = if let Some(entries) = query_request.entries.clone() {
            entries
        } else if !entry_name.is_empty() {
            vec![entry_name.to_string()]
        } else {
            vec![]
        };

        if patterns.is_empty() {
            return Ok(None);
        }

        let has_all_wildcard = patterns.iter().any(|p| p == "*");
        let matches_pattern = |entry: &str| {
            if has_all_wildcard {
                return true;
            }

            patterns.iter().any(|pattern| {
                if let Some(prefix) = pattern.strip_suffix('*') {
                    entry.starts_with(prefix)
                } else {
                    entry == pattern
                }
            })
        };

        let full_info = bucket.clone().info().await?;
        let mut attachments = Map::new();
        for info in full_info.entries {
            let entry = info.name;
            if is_system_meta_entry(&entry) || !matches_pattern(&entry) {
                continue;
            }

            let meta_name = meta_entry_name(&entry);
            let meta_entry = match bucket.get_entry(&meta_name).await {
                Ok(entry) => entry.upgrade()?,
                Err(err) if err.status == NotFound => continue,
                Err(err) => return Err(err),
            };

            let query_id = meta_entry
                .query(QueryEntry {
                    include: Some(HashMap::from([("key".to_string(), format!("${ext_name}"))])),
                    limit: Some(1),
                    ..Default::default()
                })
                .await?;

            let (rx, _) = meta_entry.get_query_receiver(query_id).await?;
            let rx = rx.upgrade()?;

            let Some(result) = rx.write().await?.recv().await else {
                continue;
            };

            let mut record = match result {
                Ok(record) => record,
                Err(err) if err.status == NoContent => continue,
                Err(err) => return Err(err),
            };

            let mut data = Vec::new();
            while let Some(chunk) = record.read_chunk() {
                let chunk = chunk?;
                data.extend_from_slice(chunk.as_ref());
            }

            let parsed = serde_json::from_slice::<serde_json::Value>(&data).map_err(|err| {
                unprocessable_entity!(
                    "Meta attachment '${}' in '{}/{}' must be valid JSON: {}",
                    ext_name,
                    bucket_name,
                    meta_name,
                    err
                )
            })?;

            attachments.insert(entry, parsed);
        }

        if attachments.is_empty() {
            Ok(None)
        } else {
            Ok(Some(serde_json::Value::Object(attachments)))
        }
    }

    fn attach_ext_attachments(
        ext_query: &mut Map<String, serde_json::Value>,
        name: &str,
        attachments: serde_json::Value,
    ) {
        if !ext_query.contains_key(name) {
            ext_query.insert(name.to_string(), serde_json::json!({}));
        }

        if let Some(current) = ext_query
            .get_mut(name)
            .and_then(|value| value.as_object_mut())
        {
            current
                .entry("attachments".to_string())
                .or_insert(attachments);
        }
    }
}

use crate::cfg::io::IoConfig;
use crate::ext::filter::DummyFilter;
use crate::storage::query::filters::{RecordFilter, WhenFilter};
pub(crate) use create::create_ext_repository;
use reduct_base::io::BoxedReadRecord;

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::cfg::Cfg;
    use crate::storage::engine::StorageEngine;
    use crate::storage::entry::RecordReader;
    use crate::storage::proto::Record;
    use async_stream::stream;
    use bytes::Bytes;
    use futures_util::Stream;
    use mockall::predicate::eq;
    use mockall::{mock, predicate};
    use prost_wkt_types::Timestamp;
    use reduct_base::ext::{BoxedRecordStream, Commiter, IoExtensionInfo, Processor};
    use reduct_base::io::records::OneShotRecord;
    use reduct_base::io::RecordMeta;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::server_api::ServerInfo;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tempfile::tempdir;

    mod register_query {
        use super::*;
        use mockall::predicate::always;
        use mockall::Sequence;

        use reduct_base::not_found;
        use std::time::Duration;

        #[rstest]
        #[tokio::test]
        async fn test_no_ext_part(mock_ext: MockIoExtension) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", QueryEntry::default())
                .await
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().await.unwrap();
            assert_eq!(
                query_map.len(),
                0,
                "We don't need to register the query without 'ext' part"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_with_ext_part(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
        ) {
            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);

            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().await.unwrap();
            assert_eq!(
                query_map.len(),
                1,
                "We need to register the query with 'ext' part"
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn test_with_ext_params_from_meta_entry(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
        ) {
            let cfg = Cfg {
                data_path: tempdir().unwrap().keep(),
                ..Cfg::default()
            };
            let storage = Arc::new(
                StorageEngine::builder()
                    .with_data_path(cfg.data_path.clone())
                    .with_cfg(cfg)
                    .build()
                    .await,
            );
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            let bucket = storage
                .get_bucket("bucket")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            let meta_payload = br#"{"scale":100}"#;
            let mut writer = bucket
                .begin_write(
                    "entry/$meta",
                    1,
                    meta_payload.len() as u64,
                    "application/json".to_string(),
                    Labels::from_iter([("key".to_string(), "$test-ext".to_string())]),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from_static(meta_payload))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };
            let expected_query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {"attachments": {"entry": {"scale": 100}}},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(expected_query))
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            assert_eq!(
                mocked_ext_repo
                    .get_ext_attachments("bucket", "entry", &query, "test-ext")
                    .await
                    .unwrap(),
                Some(json!({"entry": {"scale": 100}}))
            );

            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_when_parsing(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
        ) {
            let query = QueryEntry {
                ext: Some(json!({
                    "test-ext": {},
                    "when": {"@label": {"$eq": "value"}},
                })),
                ..Default::default()
            };
            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);

            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .is_ok(),);

            // make sure we parsed condition correctly
            let mut query_map = mocked_ext_repo.query_map.write().await.unwrap();
            assert_eq!(query_map.len(), 1, "Query should be registered");
            let query_context = query_map.get_mut(&1).unwrap();
            assert_eq!(
                query_context.steps[0]
                    .condition_filter
                    .filter(record_with_labels("not-in-when", "val"))
                    .err()
                    .unwrap(),
                not_found!("Reference '@label' not found"),
                "Condition should be parsed and applied"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_with_pipeline_from_ext_directive_in_when(mut mock_ext: MockIoExtension) {
            let mut sequence = Sequence::new();
            mock_ext
                .expect_query()
                .withf(|_, _, q| {
                    q.ext == Some(json!({"test-ext": {"extract": {"as_label": {"z": "vector.z"}}}}))
                })
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| {
                    Ok((
                        Box::new(MockProcessor::new()),
                        Box::new(MockCommiter::new()),
                    ))
                });

            mock_ext
                .expect_query()
                .withf(|_, _, q| {
                    q.ext
                        == Some(
                            json!({"test-ext": {"select": {"json": {}, "columns": [{"name": "vector"}]}}}),
                        )
                })
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| {
                    Ok((Box::new(MockProcessor::new()), Box::new(MockCommiter::new())))
                });

            let query = QueryEntry {
                when: Some(json!({
                    "#ext": [
                        {
                            "test-ext": {"extract": {"as_label": {"z": "vector.z"}}},
                            "when": {"@z": {"$gte": 124}}
                        },
                        {
                            "test-ext": {"select": {"json": {}, "columns": [{"name": "vector"}]}}
                        }
                    ]
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .is_ok());

            let query_map = mocked_ext_repo.query_map.read().await.unwrap();
            assert_eq!(query_map.len(), 1);
            assert_eq!(query_map.get(&1).unwrap().steps.len(), 2);
        }

        #[rstest]
        #[tokio::test]
        async fn test_ttl(mut mock_ext: MockIoExtension) {
            let query = QueryEntry {
                ttl: Some(1),
                ext: Some(json!({
                    "test-ext": {},
                })),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .returning(|_, _, _| {
                    Ok((
                        Box::new(MockProcessor::new()),
                        Box::new(MockCommiter::new()),
                    ))
                })
                .times(3);

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert!(mocked_ext_repo
                .register_query(1, "bucket", "entry", query.clone())
                .await
                .is_ok());

            assert!(mocked_ext_repo
                .register_query(2, "bucket", "entry", query.clone())
                .await
                .is_ok());

            {
                let query_map = mocked_ext_repo.query_map.read().await.unwrap();
                assert_eq!(query_map.len(), 2);
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
            assert!(mocked_ext_repo
                .register_query(3, "bucket", "entry", query)
                .await
                .is_ok());
            {
                let query_map = mocked_ext_repo.query_map.read().await.unwrap();
                assert_eq!(query_map.len(), 1,);

                assert!(query_map.get(&1).is_none(), "Query 1 should be expired");
                assert!(query_map.get(&2).is_none(), "Query 2 should be expired");
                assert!(query_map.get(&3).is_some());
            }
        }

        #[rstest]
        #[case(json!({"test-ext": {}, "test-ext2": {}}),  unprocessable_entity!("Multiple extensions are not supported in query id=1")
        )]
        #[case(json!({"unknown-ext": {}}),  unprocessable_entity!("Unknown extension 'unknown-ext' in query id=1")
        )]
        #[case(json!({}),  unprocessable_entity!("Extension name is not found in query id=1"))]
        #[tokio::test]
        async fn test_error_handling(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
            #[case] ext_params: serde_json::Value,
            #[case] expected_error: ReductError,
        ) {
            let query = QueryEntry {
                ext: Some(ext_params),
                ..Default::default()
            };

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), eq(query.clone()))
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                expected_error
            );
        }

        #[rstest]
        #[case(
            json!([]),
            unprocessable_entity!("Extension name is not found in query id=1")
        )]
        #[case(
            json!(["test-ext"]),
            unprocessable_entity!("Each '#ext' pipeline step must be a JSON object in query id=1")
        )]
        #[case(
            json!([{"test-ext": {}, "test-ext2": {}}, {"test-ext": {}}]),
            unprocessable_entity!("Each '#ext' pipeline step must contain exactly one extension in query id=1")
        )]
        #[tokio::test]
        async fn test_pipeline_error_handling(
            mock_ext: MockIoExtension,
            #[case] ext_params: serde_json::Value,
            #[case] expected_error: ReductError,
        ) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            let query = QueryEntry {
                ext: Some(ext_params),
                ..Default::default()
            };

            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                expected_error
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_unknown_extension_in_pipeline_reports_step(
            mut mock_ext: MockIoExtension,
            processor: BoxedProcessor,
            commiter: BoxedCommiter,
        ) {
            mock_ext
                .expect_query()
                .withf(|_, _, q| q.ext == Some(json!({"test-ext": {}})))
                .times(1)
                .return_once(|_, _, _| Ok((processor, commiter)));

            let mocked_ext_repo = mocked_ext_repo("test-ext", mock_ext);
            let query = QueryEntry {
                ext: Some(json!([
                    {"test-ext": {}},
                    {"unknown-ext": {}}
                ])),
                ..Default::default()
            };

            assert_eq!(
                mocked_ext_repo
                    .register_query(1, "bucket", "entry", query)
                    .await
                    .err()
                    .unwrap(),
                unprocessable_entity!("Unknown extension 'unknown-ext' in query id=1 at step 1")
            );
        }

        #[test]
        fn test_parse_ext_directive_rejects_bad_json_syntax() {
            let err = ExtRepository::parse_directive_value(&Value::String("{bad".to_string()), 1)
                .err()
                .unwrap();

            assert_eq!(
                err.status(),
                reduct_base::error::ErrorCode::UnprocessableEntity
            );
            assert!(
                err.message().starts_with(
                    "Directive '#ext' must be a JSON object or an array of JSON objects in query id=1:"
                ),
                "{err}"
            );
        }

        #[test]
        fn test_decode_ext_directive_returns_null_for_empty_directive() {
            assert_eq!(
                ExtRepository::decode_ext_directive(&[], 1).unwrap(),
                serde_json::Value::Null
            );
        }

        #[test]
        fn test_decode_ext_directive_parses_single_element() {
            assert_eq!(
                ExtRepository::decode_ext_directive(
                    &[Value::String(r#"{"test-ext":{"scale":100}}"#.to_string())],
                    1
                )
                .unwrap(),
                json!({"test-ext": {"scale": 100}})
            );
        }
    }

    mod get_ext_attachments {
        use super::*;
        use reduct_base::error::ErrorCode;

        async fn create_storage() -> Arc<StorageEngine> {
            let cfg = Cfg {
                data_path: tempdir().unwrap().keep(),
                ..Cfg::default()
            };
            Arc::new(
                StorageEngine::builder()
                    .with_data_path(cfg.data_path.clone())
                    .with_cfg(cfg)
                    .build()
                    .await,
            )
        }

        async fn write_meta_record(
            storage: &Arc<StorageEngine>,
            bucket_name: &str,
            entry_name: &str,
            key: &str,
            payload: &'static [u8],
        ) {
            let bucket = storage
                .get_bucket(bucket_name)
                .await
                .unwrap()
                .upgrade_and_unwrap();

            let mut writer = bucket
                .begin_write(
                    entry_name,
                    1,
                    payload.len() as u64,
                    "application/json".to_string(),
                    Labels::from_iter([("key".to_string(), key.to_string())]),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from_static(payload))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        async fn write_record(storage: &Arc<StorageEngine>, bucket_name: &str, entry_name: &str) {
            let bucket = storage
                .get_bucket(bucket_name)
                .await
                .unwrap()
                .upgrade_and_unwrap();
            let mut writer = bucket
                .begin_write(
                    entry_name,
                    1,
                    2,
                    "application/json".to_string(),
                    Labels::new(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from_static(br#"{}"#))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn returns_none_for_empty_entry_name(mock_ext: MockIoExtension) {
            let repo = mocked_ext_repo("test-ext", mock_ext);
            assert_eq!(
                repo.get_ext_attachments("bucket", "", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test]
        async fn returns_none_without_storage(mock_ext: MockIoExtension) {
            let repo = mocked_ext_repo("test-ext", mock_ext);
            assert_eq!(
                repo.get_ext_attachments("bucket", "entry", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test]
        async fn returns_none_when_bucket_not_found(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            assert_eq!(
                repo.get_ext_attachments("missing", "entry", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn returns_none_when_request_has_no_entries_and_entry_name_empty(
            mock_ext: MockIoExtension,
        ) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            assert_eq!(
                repo.get_ext_attachments("bucket", "", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test]
        async fn returns_none_when_meta_entry_not_found(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            assert_eq!(
                repo.get_ext_attachments("bucket", "entry", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn returns_none_when_key_not_found(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            write_meta_record(
                &storage,
                "bucket",
                "entry/$meta",
                "$another-ext",
                br#"{"scale":100}"#,
            )
            .await;

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            assert_eq!(
                repo.get_ext_attachments("bucket", "entry", &QueryEntry::default(), "test-ext")
                    .await
                    .unwrap(),
                None
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn returns_error_for_malformed_json_payload(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            write_meta_record(&storage, "bucket", "entry/$meta", "$test-ext", b"not-json").await;

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            let err = repo
                .get_ext_attachments("bucket", "entry", &QueryEntry::default(), "test-ext")
                .await
                .err()
                .unwrap();

            assert_eq!(err.status, ErrorCode::UnprocessableEntity);
            assert!(err.message.contains("must be valid JSON"));
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn collects_attachments_for_all_wildcard(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            write_meta_record(
                &storage,
                "bucket",
                "entry-a/$meta",
                "$test-ext",
                br#"{"topic":"/a"}"#,
            )
            .await;
            write_meta_record(
                &storage,
                "bucket",
                "entry-b/$meta",
                "$test-ext",
                br#"{"topic":"/b"}"#,
            )
            .await;

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            let query = QueryEntry {
                entries: Some(vec!["*".to_string()]),
                ..Default::default()
            };
            assert_eq!(
                repo.get_ext_attachments("bucket", "", &query, "test-ext")
                    .await
                    .unwrap(),
                Some(json!({
                    "entry-a": {"topic": "/a"},
                    "entry-b": {"topic": "/b"}
                }))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn skips_non_matching_entries_and_entries_without_meta(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            write_meta_record(
                &storage,
                "bucket",
                "entry-matched/$meta",
                "$test-ext",
                br#"{"topic":"/matched"}"#,
            )
            .await;
            write_meta_record(
                &storage,
                "bucket",
                "other/$meta",
                "$test-ext",
                br#"{"topic":"/other"}"#,
            )
            .await;
            write_record(&storage, "bucket", "entry-no-meta").await;

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            let query = QueryEntry {
                entries: Some(vec!["entry-*".to_string()]),
                ..Default::default()
            };
            assert_eq!(
                repo.get_ext_attachments("bucket", "", &query, "test-ext")
                    .await
                    .unwrap(),
                Some(json!({
                    "entry-matched": {"topic": "/matched"}
                }))
            );
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn collects_attachments_for_wildcard_entries(mock_ext: MockIoExtension) {
            let storage = create_storage().await;
            storage
                .create_bucket("bucket", BucketSettings::default())
                .await
                .unwrap();

            write_meta_record(
                &storage,
                "bucket",
                "entry-a/$meta",
                "$test-ext",
                br#"{"topic":"/a"}"#,
            )
            .await;
            write_meta_record(
                &storage,
                "bucket",
                "entry-b/$meta",
                "$test-ext",
                br#"{"topic":"/b"}"#,
            )
            .await;

            let repo = mocked_ext_repo_with_storage("test-ext", mock_ext, Some(storage));
            let query = QueryEntry {
                entries: Some(vec!["entry-*".to_string()]),
                ..Default::default()
            };
            assert_eq!(
                repo.get_ext_attachments("bucket", "", &query, "test-ext")
                    .await
                    .unwrap(),
                Some(json!({
                    "entry-a": {"topic": "/a"},
                    "entry-b": {"topic": "/b"}
                }))
            );
        }
    }

    mod attach_ext_attachments {
        use super::*;

        #[test]
        fn creates_extension_entry_when_missing() {
            let mut ext_query = Map::new();
            ExtRepository::attach_ext_attachments(
                &mut ext_query,
                "test-ext",
                json!({"scale": 100}),
            );
            assert_eq!(
                ext_query.get("test-ext").cloned().unwrap(),
                json!({"attachments": {"scale": 100}})
            );
        }

        #[test]
        fn inserts_attachments_into_empty_extension_object() {
            let mut ext_query = Map::from_iter([("test-ext".to_string(), json!({}))]);
            ExtRepository::attach_ext_attachments(
                &mut ext_query,
                "test-ext",
                json!({"scale": 100}),
            );
            assert_eq!(
                ext_query.get("test-ext").cloned().unwrap(),
                json!({"attachments": {"scale": 100}})
            );
        }

        #[test]
        fn keeps_existing_attachments_unchanged() {
            let mut ext_query = Map::from_iter([(
                "test-ext".to_string(),
                json!({"attachments": {"keep": true}}),
            )]);
            ExtRepository::attach_ext_attachments(
                &mut ext_query,
                "test-ext",
                json!({"scale": 100}),
            );
            assert_eq!(
                ext_query.get("test-ext").cloned().unwrap(),
                json!({"attachments": {"keep": true}})
            );
        }

        #[test]
        fn ignores_non_object_extension_value() {
            let mut ext_query = Map::from_iter([("test-ext".to_string(), json!("bad"))]);
            ExtRepository::attach_ext_attachments(
                &mut ext_query,
                "test-ext",
                json!({"scale": 100}),
            );
            assert_eq!(ext_query.get("test-ext").cloned().unwrap(), json!("bad"));
        }
    }

    mod next_processed_record {
        use super::*;
        use crate::storage::entry::RecordReader;

        use mockall::predicate;
        use mockall::Sequence;
        use reduct_base::internal_server_error;
        use tokio::sync::mpsc;

        #[rstest]
        #[tokio::test]
        async fn test_empty_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = mpsc::channel(1);
            drop(tx);

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_eq!(
                *mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap(),
                no_content!("No content"),
                "Should return no content error when no records are available"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_error_query() {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let err = internal_server_error!("Test error!");
            tx.send(Err(err.clone())).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert_eq!(
                *mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap(),
                err
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_registered_query(record_reader: RecordReader) {
            let mocked_ext_repo = mocked_ext_repo("test-ext", MockIoExtension::new());
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .unwrap()[0]
                .as_ref()
                .is_ok(),);
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_not_ready(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor
                .expect_process_record()
                .return_once(|_| Ok(MockStream::boxed(Poll::Ready(None)) as BoxedRecordStream));
            commiter.expect_commit_record().never();

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(Ok(record_reader)).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .is_none());
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_pipeline_step_when_applies_before_next_step(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            let mut sequence = Sequence::new();
            let mut processor_2 = Box::new(MockProcessor::new());
            let mut commiter_2 = Box::new(MockCommiter::new());

            processor.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                    record_with_labels("z", "100"),
                )))))
            });
            commiter.expect_commit_record().never();
            commiter.expect_flush().return_once(|| None).times(1);

            processor_2.expect_process_record().never();
            commiter_2.expect_flush().return_once(|| None).times(1);

            mock_ext
                .expect_query()
                .withf(|_, _, q| q.ext == Some(json!({"test1": {"extract": {}}})))
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor, commiter)));
            mock_ext
                .expect_query()
                .withf(|_, _, q| {
                    q.ext
                        == Some(
                            json!({"test1": {"select": {"json": {}, "columns": [{"name": "vector"}]}}}),
                        )
                })
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor_2, commiter_2)));

            let query = QueryEntry {
                ext: Some(json!([
                    {
                        "test1": {"extract": {}},
                        "when": {"@z": {"$gte": 124}}
                    },
                    {
                        "test1": {"select": {"json": {}, "columns": [{"name": "vector"}]}}
                    }
                ])),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();
            let query_rx = Arc::new(AsyncRwLock::new(rx));

            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "step-local when should filter out the record before step 2"
            );

            assert_eq!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap()
                    .status(),
                NoContent
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_pipeline_propagates_computed_labels_to_next_step(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
        ) {
            let mut sequence = Sequence::new();
            let mut processor_1 = Box::new(MockProcessor::new());
            let mut commiter_1 = Box::new(MockCommiter::new());
            let mut processor_2 = Box::new(MockProcessor::new());
            let mut commiter_2 = Box::new(MockCommiter::new());

            processor_1.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                    record_with_computed_labels(&[("z", "130")]),
                )))))
            });
            commiter_1
                .expect_commit_record()
                .return_once(|_| Some(Ok(record_with_computed_labels(&[("z", "130")]))));

            processor_2
                .expect_process_record()
                .withf(|record| {
                    record
                        .meta()
                        .computed_labels()
                        .get("z")
                        .is_some_and(|v| v == "130")
                })
                .return_once(|_| {
                    Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                        record_with_computed_labels(&[("z", "130"), ("y", "1")]),
                    )))))
                });
            commiter_2
                .expect_commit_record()
                .return_once(|_| Some(Ok(record_with_computed_labels(&[("y", "1")]))));

            mock_ext
                .expect_query()
                .withf(|_, _, q| q.ext == Some(json!({"test1": {"extract": {}}})))
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor_1, commiter_1)));
            mock_ext
                .expect_query()
                .withf(|_, _, q| {
                    q.ext
                        == Some(
                            json!({"test1": {"select": {"json": {}, "columns": [{"name": "vector"}]}}}),
                        )
                })
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor_2, commiter_2)));

            let query = QueryEntry {
                ext: Some(json!([
                    {"test1": {"extract": {}}},
                    {"test1": {"select": {"json": {}, "columns": [{"name": "vector"}]}}}
                ])),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(Ok(record_reader)).await.unwrap();
            let query_rx = Arc::new(AsyncRwLock::new(rx));

            let mut results = mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .unwrap();
            let record = results.get_mut(0).unwrap().as_ref().unwrap();

            assert_eq!(
                record.meta().computed_labels().get("z"),
                Some(&"130".to_string())
            );
            assert_eq!(
                record.meta().computed_labels().get("y"),
                Some(&"1".to_string())
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_pipeline_processes_flushed_record_in_downstream_step(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
        ) {
            let mut sequence = Sequence::new();
            let mut processor_1 = Box::new(MockProcessor::new());
            let mut commiter_1 = Box::new(MockCommiter::new());
            let mut processor_2 = Box::new(MockProcessor::new());
            let mut commiter_2 = Box::new(MockCommiter::new());

            processor_1
                .expect_process_record()
                .return_once(|_| Ok(MockStream::boxed(Poll::Ready(None))));
            commiter_1.expect_commit_record().never();
            commiter_1
                .expect_flush()
                .return_once(|| Some(Ok(record_with_labels("z", "130"))))
                .times(1);

            processor_2
                .expect_process_record()
                .withf(|record| {
                    record
                        .meta()
                        .computed_labels()
                        .get("z")
                        .is_some_and(|value| value == "130")
                })
                .return_once(|_| {
                    Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                        record_with_labels("projected", "true"),
                    )))))
                });
            commiter_2
                .expect_commit_record()
                .return_once(|_| Some(Ok(record_with_labels("projected", "true"))));
            commiter_2.expect_flush().return_once(|| None).times(1);

            mock_ext
                .expect_query()
                .withf(|_, _, q| q.ext == Some(json!({"test1": {"extract": {}}})))
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor_1, commiter_1)));
            mock_ext
                .expect_query()
                .withf(|_, _, q| q.ext == Some(json!({"test1": {"select": {}}})))
                .times(1)
                .in_sequence(&mut sequence)
                .return_once(|_, _, _| Ok((processor_2, commiter_2)));

            let query = QueryEntry {
                ext: Some(json!([
                    {"test1": {"extract": {}}},
                    {"test1": {"select": {}}}
                ])),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();
            let query_rx = Arc::new(AsyncRwLock::new(rx));

            assert!(mocked_ext_repo
                .fetch_and_process_record(1, query_rx.clone())
                .await
                .is_none());

            let results = mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .unwrap();

            assert_eq!(results.len(), 2);
            let record = results[0].as_ref().unwrap();
            assert_eq!(
                record.meta().computed_labels().get("projected"),
                Some(&"true".to_string())
            );
            assert_eq!(results[1].as_ref().err().unwrap().status(), NoContent);
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_a_record(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                    record_with_labels("key", "val"),
                )))))
            });

            commiter
                .expect_commit_record()
                .return_once(|_| Some(Ok(record_with_labels("key", "val"))));
            commiter.expect_flush().return_once(|| None).times(1);

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));

            let mut records = mocked_ext_repo
                .fetch_and_process_record(1, query_rx.clone())
                .await
                .unwrap();

            assert_eq!(records.len(), 1, "Should return one record");

            let record = records.get_mut(0).unwrap().as_mut().unwrap();
            assert_eq!(record.read_chunk(), Some(Ok(Bytes::new())));

            assert_eq!(
                *mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap(),
                no_content!("")
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_a_record_empty_entry_name(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor.expect_process_record().return_once(|_| {
                Ok(MockStream::boxed(Poll::Ready(Some(Ok(
                    record_with_labels_empty_entry("key", "val"),
                )))))
            });

            commiter
                .expect_commit_record()
                .return_once(|_| Some(Ok(record_with_labels_empty_entry("key", "val"))));
            commiter.expect_flush().return_once(|| None).times(1);

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));

            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "Empty entry name should be skipped"
            );

            assert_eq!(
                *mocked_ext_repo
                    .fetch_and_process_record(1, query_rx)
                    .await
                    .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap(),
                no_content!("")
            );
        }

        #[rstest]
        #[tokio::test(flavor = "current_thread")]
        async fn test_process_flushed_record(
            record_reader: RecordReader,
            mut mock_ext: MockIoExtension,
            mut processor: Box<MockProcessor>,
            mut commiter: Box<MockCommiter>,
        ) {
            processor.expect_process_record().return_once(|_| {
                Ok(
                    MockStream::boxed(Poll::Ready(Some(Ok(record_with_labels("key", "val")))))
                        as BoxedRecordStream,
                )
            });

            commiter.expect_commit_record().return_once(|_| None);

            commiter
                .expect_flush()
                .return_once(|| Some(Ok(record_with_labels("key", "val"))))
                .times(1);

            mock_ext
                .expect_query()
                .with(eq("bucket"), eq("entry"), predicate::always())
                .return_once(|_, _, _| Ok((processor, commiter)));

            let query = QueryEntry {
                ext: Some(json!({
                    "test1": {},
                })),
                ..Default::default()
            };

            let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);
            mocked_ext_repo
                .register_query(1, "bucket", "entry", query)
                .await
                .unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tx.send(Ok(record_reader)).await.unwrap();
            tx.send(Err(no_content!(""))).await.unwrap();

            let query_rx = Arc::new(AsyncRwLock::new(rx));
            assert!(
                mocked_ext_repo
                    .fetch_and_process_record(1, query_rx.clone())
                    .await
                    .is_none(),
                "First run should be None (stupid implementation)"
            );

            let results = mocked_ext_repo
                .fetch_and_process_record(1, query_rx.clone())
                .await
                .unwrap();

            assert_eq!(
                results.len(),
                2,
                "Should return one record and non-content error"
            );
            assert!(
                results[0].as_ref().is_ok(),
                "we should get the record from flush"
            );
            assert_eq!(
                results[1].as_ref().err().unwrap().status(),
                NoContent,
                "we should get no content error"
            );
        }
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_process_a_record_limit(
        record_reader: RecordReader,
        mut mock_ext: MockIoExtension,
        mut processor: Box<MockProcessor>,
        mut commiter: Box<MockCommiter>,
    ) {
        processor.expect_process_record().return_once(|_| {
            let stream = stream! {
                yield Ok(record_with_labels("key", "val"));
                yield Ok(record_with_labels("key", "val"));
            };
            Ok(Box::new(stream) as BoxedRecordStream)
        });

        commiter
            .expect_commit_record()
            .return_once(|_| Some(Ok(record_with_labels("key", "val"))));
        commiter.expect_flush().return_once(|| None).times(1);

        mock_ext
            .expect_query()
            .with(eq("bucket"), eq("entry"), predicate::always())
            .return_once(|_, _, _| Ok((processor, commiter)));

        let query = QueryEntry {
            ext: Some(json!({
                "test1": {},
                "when": {"$limit": 1},
            })),
            ..Default::default()
        };

        let mocked_ext_repo = mocked_ext_repo("test1", mock_ext);

        mocked_ext_repo
            .register_query(1, "bucket", "entry", query)
            .await
            .unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(Ok(record_reader)).await.unwrap();
        tx.send(Err(no_content!(""))).await.unwrap();

        let query_rx = Arc::new(AsyncRwLock::new(rx));

        mocked_ext_repo
            .fetch_and_process_record(1, query_rx.clone())
            .await
            .unwrap()[0]
            .as_ref()
            .expect("Should return a record");

        assert_eq!(
            *mocked_ext_repo
                .fetch_and_process_record(1, query_rx)
                .await
                .unwrap()[0]
                .as_ref()
                .err()
                .unwrap(),
            no_content!("")
        );
    }

    #[fixture]
    fn mock_ext() -> MockIoExtension {
        MockIoExtension::new()
    }

    #[fixture]
    fn processor() -> Box<MockProcessor> {
        Box::new(MockProcessor::new())
    }

    #[fixture]
    fn commiter() -> Box<MockCommiter> {
        Box::new(MockCommiter::new())
    }

    #[fixture]
    fn record_reader() -> RecordReader {
        let record = Record {
            timestamp: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        RecordReader::form_record("entry", record)
    }

    #[fixture]
    pub fn mocked_record() -> BoxedReadRecord {
        record_with_labels("key1", "val1")
    }

    fn mocked_ext_repo(name: &str, mock_ext: MockIoExtension) -> ExtRepository {
        mocked_ext_repo_with_storage(name, mock_ext, None)
    }

    fn mocked_ext_repo_with_storage(
        name: &str,
        mock_ext: MockIoExtension,
        storage: Option<Arc<StorageEngine>>,
    ) -> ExtRepository {
        let ext_settings = ExtSettings::builder()
            .server_info(ServerInfo::default())
            .build();
        let mut ext_repo = ExtRepository::try_load(
            vec![tempdir().unwrap().keep()],
            vec![],
            ext_settings,
            IoConfig::default(),
            storage,
        )
        .unwrap();
        ext_repo.extension_map.insert(
            name.to_string(),
            Arc::new(AsyncRwLock::new(Box::new(mock_ext))),
        );
        ext_repo
    }

    mock! {
        IoExtension {}

        #[async_trait]
        impl IoExtension for IoExtension {
            fn info(&self) -> &IoExtensionInfo;


            fn query(
                &mut self,
                bucket_name: &str,
                entry_name: &str,
                query: &QueryEntry,
            ) -> Result<(BoxedProcessor, BoxedCommiter), ReductError>;
        }

    }

    mock! {
        Processor {}

        #[async_trait]
        impl Processor for Processor {
            async fn process_record(
                &mut self,
                record: BoxedReadRecord,
            ) -> Result<BoxedRecordStream, ReductError>;
        }
    }

    mock! {
        Commiter {}

        #[async_trait]
        impl Commiter for Commiter {
            async fn commit_record(&mut self, record: BoxedReadRecord) -> Option<Result<BoxedReadRecord, ReductError>>;
            async fn flush(&mut self) -> Option<Result<BoxedReadRecord, ReductError>>;
        }
    }

    struct MockStream {
        ret_value: Option<Poll<Option<Result<BoxedReadRecord, ReductError>>>>,
    }
    impl Stream for MockStream {
        type Item = Result<BoxedReadRecord, ReductError>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
            let Some(ret_value) = self.ret_value.take() else {
                return Poll::Ready(None);
            };

            ret_value
        }
    }

    impl MockStream {
        fn boxed(ret_value: Poll<Option<Result<BoxedReadRecord, ReductError>>>) -> Box<Self> {
            Box::new(MockStream {
                ret_value: Some(ret_value),
            })
        }
    }

    pub fn record_with_labels(key: &str, val: &str) -> BoxedReadRecord {
        let meta = RecordMeta::builder()
            .entry_name("entry")
            .timestamp(0)
            .computed_labels(Labels::from_iter(
                vec![(key.to_string(), val.to_string())].into_iter(),
            ))
            .build();
        OneShotRecord::boxed(Bytes::new(), meta)
    }

    pub fn record_with_labels_empty_entry(key: &str, val: &str) -> BoxedReadRecord {
        let meta = RecordMeta::builder()
            .timestamp(0)
            .computed_labels(Labels::from_iter(
                vec![(key.to_string(), val.to_string())].into_iter(),
            ))
            .build();
        OneShotRecord::boxed(Bytes::new(), meta)
    }

    pub fn record_with_computed_labels(labels: &[(&str, &str)]) -> BoxedReadRecord {
        let meta = RecordMeta::builder()
            .entry_name("entry")
            .timestamp(0)
            .computed_labels(Labels::from_iter(
                labels.iter().map(|(k, v)| (k.to_string(), v.to_string())),
            ))
            .build();
        OneShotRecord::boxed(Bytes::new(), meta)
    }
}
