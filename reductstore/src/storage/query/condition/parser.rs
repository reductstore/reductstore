// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::constant::Constant;
use crate::storage::query::condition::operators::arithmetic::{Add, Sub};
use crate::storage::query::condition::operators::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::storage::query::condition::operators::logical::{AllOf, AnyOf, NoneOf, OneOf};
use crate::storage::query::condition::reference::Reference;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use serde_json::{Map, Value as JsonValue};

/// Parses a JSON object into a condition tree.
pub(crate) struct Parser {}

impl Parser {
    pub fn parse(&self, json: &JsonValue) -> Result<BoxedNode, ReductError> {
        match json {
            JsonValue::Object(map) => {
                let mut expressions = vec![];
                for (key, value) in map.iter() {
                    if let JsonValue::Array(operand_list) = value {
                        // Parse array notation e.g. {"$and": [true, true]}
                        expressions.push(self.parse_array_syntax(key, operand_list)?);
                    } else if let JsonValue::Object(operator_right_operand) = value {
                        // Parse object notation e.g. {"&label": {"$and": true}}
                        expressions.push(self.parse_object_syntax(key, operator_right_operand)?);
                    } else {
                        return Err(unprocessable_entity!(
                            "A filed must contain array or object: {}",
                            json
                        ));
                    }
                }

                // We use AND operator to aggregate results from all expressions
                Ok(AllOf::boxed(expressions)?)
            }

            JsonValue::Bool(value) => Ok(Constant::boxed(Value::Bool(*value))),

            JsonValue::Number(value) => {
                if value.is_i64() || value.is_u64() {
                    Ok(Constant::boxed(Value::Int(value.as_i64().unwrap())))
                } else {
                    Ok(Constant::boxed(Value::Float(value.as_f64().unwrap())))
                }
            }
            JsonValue::String(value) => {
                if value.starts_with("&") {
                    Ok(Reference::boxed(value[1..].to_string()))
                } else {
                    Ok(Constant::boxed(Value::String(value.clone())))
                }
            }
            _ => Err(unprocessable_entity!("Invalid JSON value: {}", json)),
        }
    }

    fn parse_array_syntax(
        &self,
        operator: &str,
        json_operands: &Vec<JsonValue>,
    ) -> Result<BoxedNode, ReductError> {
        let mut operands = vec![];
        for operand in json_operands {
            operands.push(self.parse(operand)?);
        }
        Self::parse_operator(operator, operands)
    }

    fn parse_object_syntax(
        &self,
        left_operand: &str,
        op_right_operand: &Map<String, JsonValue>,
    ) -> Result<BoxedNode, ReductError> {
        let left_operand = self.parse(&JsonValue::String(left_operand.to_string()))?;
        if op_right_operand.len() != 1 {
            return Err(unprocessable_entity!(
                "Object notation must have exactly one operator"
            ));
        }

        let (operator, operand) = op_right_operand.iter().next().unwrap();
        let right_operand = self.parse(operand)?;
        let operands = vec![left_operand, right_operand];
        Self::parse_operator(operator, operands)
    }

    fn parse_operator(operator: &str, operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        match operator {
            // Arithmetic operators
            "$add" => Add::boxed(operands),
            "$sub" => Sub::boxed(operands),

            // Logical operators
            "$and" => AllOf::boxed(operands),
            "$all_of" => AllOf::boxed(operands),
            "$or" => AnyOf::boxed(operands),
            "$any_of" => AnyOf::boxed(operands),
            "$not" => NoneOf::boxed(operands),
            "$none_of" => NoneOf::boxed(operands),
            "$xor" => OneOf::boxed(operands),
            "$one_of" => OneOf::boxed(operands),

            // comparison operators
            "$eq" => Eq::boxed(operands),
            "$gt" => Gt::boxed(operands),
            "$gte" => Gte::boxed(operands),
            "$lt" => Lt::boxed(operands),
            "$lte" => Lte::boxed(operands),
            "$ne" => Ne::boxed(operands),

            _ => Err(unprocessable_entity!(
                "Operator '{}' not supported",
                operator
            )),
        }
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::Context;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[rstest]
    fn test_parser_array_syntax(parser: Parser, context: Context) {
        let json = serde_json::from_str(r#"{"$and": [true, {"$gt": [20, 10]}]}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_object_syntax(parser: Parser) {
        let json = serde_json::from_str(r#"{"&label": {"$gt": 10}}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        let context = Context::new(HashMap::from_iter(vec![("label", "20")]));
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_int(parser: Parser, context: Context) {
        let json = serde_json::from_str(r#"{"$and": [1, -2]}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_float(parser: Parser, context: Context) {
        let json = serde_json::from_str(r#"{"$and": [1.1, -2.2]}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_string(parser: Parser, context: Context) {
        let json = serde_json::from_str(r#"{"$and": ["a", "b"]}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_multiline(parser: Parser) {
        let json =
            serde_json::from_str(r#"{"&label": {"$and": true}, "$and": [true, true]}"#).unwrap();
        let node = parser.parse(&json).unwrap();
        let context = Context::new(HashMap::from_iter(vec![("label", "true")]));
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_invalid_json(parser: Parser) {
        let json = serde_json::from_str(r#"{"$and": [true, true], "invalid": "json"}"#).unwrap();
        let result = parser.parse(&json);
        assert_eq!(result.err().unwrap().to_string(), "[UnprocessableEntity] A filed must contain array or object: {\"$and\":[true,true],\"invalid\":\"json\"}");
    }

    #[rstest]
    fn test_parser_invalid_operator(parser: Parser) {
        let json = serde_json::from_str(r#"{"$xx": [true, true]}"#).unwrap();
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Operator '$xx' not supported"
        );
    }

    #[rstest]
    fn test_parser_invalid_object_notation(parser: Parser) {
        let json = serde_json::from_str(r#"{"&ref": {"$and": true, "x": "y"}}"#).unwrap();
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Object notation must have exactly one operator"
        );
    }

    #[rstest]
    fn test_parser_invalid_value(parser: Parser) {
        let json = serde_json::from_str(r#"{"&ref": {"$and": []}}"#).unwrap();
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Invalid JSON value: []"
        );
    }

    mod parse_operators {
        use super::*;
        #[rstest]
        // Arithmetic operators
        #[case("$add", vec![true, false], Value::Int(1))]
        #[case("$sub", vec![true, true], Value::Int(0))]
        // Logical operators
        #[case("$and", vec![true, false], Value::Bool(false))]
        #[case("$all_of", vec![true, false], Value::Bool(false))]
        #[case("$or", vec![true, false], Value::Bool(true))]
        #[case("$any_of", vec![true, false], Value::Bool(true))]
        #[case("$not", vec![true], Value::Bool(false))]
        #[case("$none_of", vec![true, true], Value::Bool(false))]
        #[case("$xor", vec![true, true], Value::Bool(false))]
        #[case("$one_of", vec![true, true], Value::Bool(false))]
        // Comparison operators
        #[case("$eq", vec![true, true], Value::Bool(true))]
        #[case("$gt", vec![true, false], Value::Bool(true))]
        #[case("$gte", vec![true, false], Value::Bool(true))]
        #[case("$lt", vec![true, false], Value::Bool(false))]
        #[case("$lte", vec![true, false], Value::Bool(false))]
        #[case("$ne", vec![true, false], Value::Bool(true))]
        fn test_parse_operator(
            parser: Parser,
            context: Context,
            #[case] operator: &str,
            #[case] operands: Vec<bool>,
            #[case] expected: Value,
        ) {
            let json =
                serde_json::from_str(&format!(r#"{{"{}": {:?}}}"#, operator, operands)).unwrap();
            let node = parser.parse(&json).unwrap();
            assert_eq!(node.apply(&context).unwrap(), expected);
        }
    }

    #[fixture]
    fn parser() -> Parser {
        Parser::new()
    }

    #[fixture]
    fn context() -> Context<'static> {
        Context::default()
    }
}
