// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use prost_wkt_types::Timestamp;
use reduct_base::io::RecordMeta;
use reduct_base::Labels;

include!(concat!(env!("OUT_DIR"), "/reduct.proto.storage.rs"));

/// Converts a Timestamp to UNIX microseconds.
pub fn ts_to_us(ts: &Timestamp) -> u64 {
    (ts.seconds * 1000000 + ts.nanos as i64 / 1000) as u64
}

/// Converts a UNIX microseconds to a Timestamp.
pub fn us_to_ts(ts: &u64) -> Timestamp {
    Timestamp {
        seconds: (ts / 1000000) as i64,
        nanos: ((ts % 1000000) * 1000) as i32,
        ..Default::default()
    }
}

impl Into<RecordMeta> for Record {
    fn into(self) -> RecordMeta {
        RecordMeta::builder()
            .timestamp(ts_to_us(self.timestamp.as_ref().unwrap()))
            .content_length(self.end - self.begin)
            .content_type(self.content_type)
            .state(self.state)
            .labels(Labels::from_iter(
                self.labels
                    .into_iter()
                    .map(|label| (label.name, label.value)),
            ))
            .build()
    }
}
