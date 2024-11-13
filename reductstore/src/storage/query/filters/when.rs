// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::{BoxedNode, Context, Node};
use crate::storage::query::filters::{FilterPoint, RecordFilter};
use reduct_base::error::ReductError;

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
