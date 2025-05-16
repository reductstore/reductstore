// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod ext_info;
mod ext_settings;

use crate::error::ReductError;
use crate::io::ReadRecord;
use crate::msg::entry_api::QueryEntry;
use async_trait::async_trait;
pub use ext_info::{IoExtensionInfo, IoExtensionInfoBuilder};
use futures::stream::Stream;
use std::io::Seek;

pub use ext_settings::{ExtSettings, ExtSettingsBuilder};
pub type BoxedReadRecord = Box<dyn ReadRecord + Send + Sync>;
pub type BoxedRecordStream =
    Box<dyn Stream<Item = Result<BoxedReadRecord, ReductError>> + Send + Sync>;

pub const EXTENSION_API_VERSION: &str = "0.2";

/// The trait for the IO extension.
///
/// This trait is used to register queries and process records in a pipeline of extensions.
#[async_trait]
pub trait IoExtension {
    /// Returns details about the extension.
    fn info(&self) -> &IoExtensionInfo;

    /// Registers a query in the extension.
    ///
    /// This method is called before fetching records from the storage engine.
    /// All records that are fetched from the storage engine will be passed to this extension.
    ///
    /// A client can use "ext" field in the query to specify the extension to use and its options.
    ///
    /// The extension can use the query ID to identify the query and the bucket name and entry name to identify the data.
    /// It also can do some initialization based on the query options.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query.
    /// * `bucket_name` - The name of the bucket.
    /// * `entry_name` - The name of the entry.
    /// * `query` - The query options
    fn register_query(
        &mut self,
        query_id: u64,
        bucket_name: &str,
        entry_name: &str,
        query: &QueryEntry,
    ) -> Result<(), ReductError>;

    /// Unregisters a query in the extension.
    ///
    /// This method is called after fetching records from the storage engine.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query.
    ///
    /// # Returns
    ///
    /// The status of the unregistering of the query.
    fn unregister_query(&mut self, query_id: u64) -> Result<(), ReductError>;

    /// Processes a record in the extension.
    ///
    /// This method is called for each record that is fetched from the storage engine.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The ID of the query.
    /// * `record` - The record to process.
    ///
    /// # Returns
    ///
    ///  A stream of records that are processed by the extension. If the input represents data that has multiple entries,
    ///  the extension can return a stream of records that are processed by the extension for each entry.
    async fn process_record(&mut self, query_id: u64, record: BoxedReadRecord)
        -> BoxedRecordStream;

    /// Commit record after processing and filtering.
    ///
    /// This method is called after processing and filtering the record and
    /// can be used to rebatch records when they represent entries of some data format like CVS lines, or JSON objects.
    /// An extension can concatenate multiple records into one or split one record into multiple records depending on the query.
    async fn commit_record(
        &mut self,
        _query_id: u64,
        record: BoxedReadRecord,
    ) -> Option<Result<BoxedReadRecord, ReductError>> {
        Some(Ok(record))
    }
}
