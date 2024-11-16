// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context};
use crate::storage::query::filters::{FilterPoint, RecordFilter};
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

impl<P> RecordFilter<P> for WhenFilter
where
    P: FilterPoint,
{
    fn filter(&mut self, record: &P) -> Result<bool, ReductError> {
        let context = Context::new(
            record
                .labels()
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect(),
        );
        Ok(self.condition.apply(&context)?.as_bool()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::proto::record::Label;
    use crate::storage::proto::Record;
    use crate::storage::query::condition::Parser;
    use rstest::rstest;

    #[rstest]
    fn filter() {
        let parser = Parser::new();
        let json = serde_json::from_str(r#"{"$and": [true, "&label"]}"#).unwrap();
        let condition = parser.parse(&json).unwrap();

        let mut filter = WhenFilter::new(condition);
        let record = Record {
            labels: vec![Label {
                name: "label".to_string(),
                value: "true".to_string(),
            }],
            ..Default::default()
        };
        let result = filter.filter(&record).unwrap();
        assert_eq!(result, true);
    }
}
