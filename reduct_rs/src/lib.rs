// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod bucket;
mod client;
mod http_client;
mod record;

pub use bucket::Bucket;
pub use client::ReductClient;
pub use record::read_record::ReadRecordBuilder;
pub use record::write_batched_records::WriteBatchBuilder;
pub use record::write_record::WriteRecordBuilder;
pub use record::{Labels, Record, RecordBuilder, RecordStream};

// Re-export
pub use reduct_base::error::{ErrorCode, ReductError};
pub use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, QuotaType};
pub use reduct_base::msg::entry_api::EntryInfo;
pub use reduct_base::msg::token_api::{Permissions, Token};
