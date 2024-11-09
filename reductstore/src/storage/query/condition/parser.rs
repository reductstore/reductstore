// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::constant::Constant;
use crate::storage::query::condition::operators::GreaterThan;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::Node;

struct Parser {}

impl Parser {
    fn parse(&self, json: &serde_json::Value) -> Box<dyn Node> {
        match json {
            serde_json::Value::Object(map) => {
                let op = map.keys().next().unwrap();
                let operands = map.values().next().unwrap().as_array().unwrap();

                let left = self.parse(operands.get(0).unwrap());
                let right = self.parse(operands.get(1).unwrap());
                match op.as_str() {
                    "$gt" => GreaterThan::boxed(left, right),
                    _ => unimplemented!(),
                }
            }
            serde_json::Value::Bool(value) => Constant::boxed((*value).into()),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::Context;

    #[test]
    fn test_parser_array_syntax() {
        let json = serde_json::from_str(r#"{"$gt": [true, {"$gt": [true, true]}]}"#).unwrap();

        let parser = Parser {};
        let mut node = parser.parse(&json);
        let context = Context {};
        assert!(node.apply(&context).as_bool().unwrap());
    }

    // #[test]
    // fn test_parser_object_syntax() {
    //     let json = serde_json::from_str(r#"{"&label": {$gt: false}"#).unwrap();
    //
    //     let parser = Parser {};
    //     let node = parser.parse(&json);
    //     let context = Context {};
    //     assert_eq!(node.apply(&context), Value::Bool(true > false));
    // }
}
