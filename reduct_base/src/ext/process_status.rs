// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::ReductError;
use crate::ext::BoxedReadRecord;
use std::fmt::Debug;

/// The status of the processing of a record.
///
/// The three possible states allow to aggregate records on the extension side.
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
    use crate::error::ReductError;
    use crate::io::tests::MockRecord;
    use crate::io::ReadRecord;
    use rstest::rstest;
    use std::collections::HashMap;

    #[rstest]
    fn test_debug() {
        let record: BoxedReadRecord = Box::new(MockRecord {});
        let status = ProcessStatus::Ready(Ok(record));
        assert_eq!(format!("{:?}", status), "Ready(?)");

        let status = ProcessStatus::NotReady;
        assert_eq!(format!("{:?}", status), "NotReady");

        let status = ProcessStatus::Stop;
        assert_eq!(format!("{:?}", status), "Stop");
    }
}
