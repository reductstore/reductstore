// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context};
use crate::storage::query::filters::{RecordFilter, RecordMeta};
use reduct_base::error::ReductError;

/// A node representing a when filter with a condition.
pub struct WhenFilter {
    condition: BoxedNode,
}

impl WhenFilter {
    pub fn new(condition: BoxedNode) -> Self {
        WhenFilter { condition }
    }
}

impl RecordFilter for WhenFilter {
    fn filter(&mut self, record: &RecordMeta) -> Result<bool, ReductError> {
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
        Ok(self.condition.apply(&context)?.as_bool()?)
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
