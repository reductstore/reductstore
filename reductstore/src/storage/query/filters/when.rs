// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context};
use crate::storage::query::filters::{RecordFilter, RecordMeta};
use reduct_base::error::{ErrorCode, ReductError};

/// A node representing a when filter with a condition.
pub struct WhenFilter {
    condition: BoxedNode,
    strict: bool,
}

impl WhenFilter {
    pub fn new(condition: BoxedNode, strict: bool) -> Self {
        WhenFilter { condition, strict }
    }
}

impl<R: Into<RecordMeta> + Clone> RecordFilter<R> for WhenFilter {
    fn filter(&mut self, record: R) -> Result<Option<Vec<R>>, ReductError> {
        let meta = record.clone().into();
        let context = Context::new(
            meta.timestamp(),
            meta.labels()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            meta.computed_labels()
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
            Ok(Some(vec![record]))
        } else {
            Ok(Some(vec![]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::query::condition::Parser;
    use reduct_base::Labels;
    use rstest::rstest;

    #[rstest]
    fn filter() {
        let parser = Parser::new();
        let json = serde_json::from_str(r#"{"$and": [true, "&label"]}"#).unwrap();
        let condition = parser.parse(&json).unwrap();

        let mut filter = WhenFilter::new(condition);

        let meta = RecordMeta::builder()
            .timestamp(0)
            .labels(Labels::from_iter(vec![(
                "label".to_string(),
                "true".to_string(),
            )]))
            .build();
        let result = filter.filter(&meta).unwrap();
        assert_eq!(result, true);
    }
}
