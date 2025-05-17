// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_base::error::ReductError;
use reduct_base::ext::BoxedReadRecord;
use std::fmt::Debug;

/// The status of the processing of a record.
///
/// The three possible states allow aggregating records on the extension side.
pub enum ProcessStatus {
    Ready(Result<BoxedReadRecord, ReductError>),
    NotReady,
    Stop,
}

impl Debug for ProcessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessStatus::Ready(_) => write!(f, "Ready(?)"),
            ProcessStatus::NotReady => write!(f, "NotReady"),
            ProcessStatus::Stop => write!(f, "Stop"),
        }
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use prost_wkt_types::Timestamp;

    use crate::storage::entry::RecordReader;
    use crate::storage::proto::Record;
    use rstest::rstest;

    #[rstest]
    fn test_debug() {
        let record: BoxedReadRecord = Box::new(RecordReader::form_record(
            Record {
                timestamp: Some(Timestamp::default()),
                ..Default::default()
            },
            true,
        ));
        let status = ProcessStatus::Ready(Ok(record));
        assert_eq!(format!("{:?}", status), "Ready(?)");

        let status = ProcessStatus::NotReady;
        assert_eq!(format!("{:?}", status), "NotReady");

        let status = ProcessStatus::Stop;
        assert_eq!(format!("{:?}", status), "Stop");
    }
}
