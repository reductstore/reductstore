// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use prost_wkt_types::Timestamp;
include!(concat!(env!("OUT_DIR"), "/reduct.proto.storage.rs"));

/// Converts a Timestamp to a u64.
pub fn ts_to_u64(ts: &Timestamp) -> u64 {
    (ts.seconds * 100000 + ts.nanos as i64 / 10000) as u64
}
