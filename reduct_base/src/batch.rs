// Copyright 2023-2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod v1;
pub use v1::{parse_batched_header, sort_headers_by_time, RecordHeader};

pub mod v2;
pub use v2::{
    decode_entry_name, encode_entry_name, make_batched_header_name, parse_batched_header_name,
    parse_batched_headers, sort_headers_by_entry_and_time, EntryRecordHeader,
};
