// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context, Directives};
use crate::storage::query::filters::when::Padding::Records;
use crate::storage::query::filters::{FilterRecord, RecordFilter};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::internal_server_error;
use std::collections::VecDeque;

enum Padding {
    Records(usize),
}

/// A node representing a when filter with a condition.
pub struct WhenFilter<R> {
    condition: BoxedNode,
    strict: bool,
    buffer: VecDeque<R>,
    before: Padding,
}

impl<R> WhenFilter<R> {
    pub fn try_new(
        condition: BoxedNode,
        directives: Directives,
        strict: bool,
    ) -> Result<Self, ReductError> {
        let before = if let Some(before) = directives.get("#before") {
            let before = before.as_int()?;
            if before < 0 {
                return Err(ReductError::unprocessable_entity(
                    "Padding before must be non-negative",
                ));
            }
            Records(before as usize + 1)
        } else {
            Records(1) // Default to 0 records before
        };

        Ok(Self {
            condition,
            strict,
            buffer: VecDeque::new(),
            before,
        })
    }
}

impl<R: FilterRecord> RecordFilter<R> for WhenFilter<R> {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        self.buffer.push_back(record);
        match self.before {
            Padding::Records(n) => {
                if self.buffer.len() > n {
                    self.buffer.pop_front();
                }
            }
        }

        let record = self.buffer.back().ok_or(internal_server_error!(
            "Buffer is empty, but we expected at least one record to be present."
        ))?;
        let context = Context::new(
            record.timestamp(),
            record
                .labels()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            record
                .computed_labels()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
        );

        let result = match self.condition.apply(&context) {
            Ok(value) => value.as_bool()?,
            Err(err) => {
                if err.status == ErrorCode::Interrupt {
                    return Ok(None);
                }

                if self.strict {
                    // in strict mode, we return an error if a filter fails
                    return Err(err);
                }

                false
            }
        };

        if result {
            Ok(Some(self.buffer.drain(..).collect()))
        } else {
            Ok(Some(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::query::condition::Parser;
    use crate::storage::query::filters::tests::TestFilterRecord;
    use reduct_base::Labels;
    use rstest::rstest;

    #[rstest]
    fn filter() {
        let parser = Parser::new();
        let json = serde_json::from_str(r#"{"$and": [true, "&label"]}"#).unwrap();
        let (condition, directives) = parser.parse(json).unwrap();

        let mut filter = WhenFilter::try_new(condition, directives, true).unwrap();

        let record: TestFilterRecord = RecordMeta::builder()
            .timestamp(0)
            .labels(Labels::from_iter(vec![(
                "label".to_string(),
                "true".to_string(),
            )]))
            .build()
            .into();
        let result = filter.filter(record.clone()).unwrap();
        assert_eq!(result, Some(vec![record]));
    }
}
