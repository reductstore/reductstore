// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use prost_wkt_types::Timestamp;
include!(concat!(env!("OUT_DIR"), "/reduct.proto.storage.rs"));

/// Converts a Timestamp to UNIX microseconds.
pub fn ts_to_us(ts: &Timestamp) -> u64 {
    (ts.seconds * 1000000 + ts.nanos as i64 / 1000) as u64
}

/// Converts a UNIX microseconds to a Timestamp.
pub fn us_to_ts(ts: u64) -> Timestamp {
    Timestamp {
        seconds: (ts / 1000000) as i64,
        nanos: ((ts % 1000000) * 1000) as i32,
        ..Default::default()
    }
}
