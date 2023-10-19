// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};

pub(crate) fn timestamp_to_iso(timestamp: u64, mask: bool) -> String {
    let dt = DateTime::<Utc>::from_timestamp(
        (timestamp / 1000_000) as i64,
        timestamp as u32 % 1000_000 * 1000,
    );
    if mask {
        "---".to_string()
    } else {
        dt.unwrap_or(DateTime::<Utc>::MIN_UTC)
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }
}
