// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::constant::Constant;
use crate::storage::query::condition::operators::And;
use crate::storage::query::condition::reference::Reference;
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::BoxedNode;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use serde_json::{Map, Number, Value as JsonValue};

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

                Ok(And::boxed(expressions))
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
                    Err(unprocessable_entity!("Invalid JSON value: {}", json))
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
        let operands = vec![right_operand, left_operand];
        Self::parse_operator(operator, operands)
    }

    fn parse_operator(operator: &str, operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        match operator {
            "$and" => Ok(And::boxed(operands)),
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
    use std::collections::HashMap;

    #[test]
    fn test_parser_array_syntax() {
        let json = serde_json::from_str(r#"{"$and": [true, {"$and": [true, true]}]}"#).unwrap();

        let parser = Parser {};
        let node = parser.parse(&json).unwrap();
        let context = Context::default();
        assert!(node.apply(&context).unwrap().as_bool());
    }

    #[test]
    fn test_parser_object_syntax() {
        let json = serde_json::from_str(r#"{"&label": {"$and": true}}"#).unwrap();

        let parser = Parser {};
        let node = parser.parse(&json).unwrap();
        let context = Context::new(HashMap::from_iter(vec![("label", "true")]));
        assert!(node.apply(&context).unwrap().as_bool());
    }

    #[test]
    fn test_parse_int() {
        let json = serde_json::from_str(r#"{"$and": [1, -2]}"#).unwrap();

        let parser = Parser {};
        let node = parser.parse(&json).unwrap();
        let context = Context::default();
        assert!(node.apply(&context).unwrap().as_bool());
    }

    #[test]
    fn test_parse_float() {
        let json = serde_json::from_str(r#"{"$and": [1.1, -2.2]}"#).unwrap();

        let parser = Parser {};
        let node = parser.parse(&json).unwrap();
        let context = Context::default();
        assert!(node.apply(&context).unwrap().as_bool());
    }

    #[test]
    fn test_parser_multiline() {
        let json =
            serde_json::from_str(r#"{"&label": {"$and": true}, "$and": [true, true]}"#).unwrap();

        let parser = Parser {};
        let node = parser.parse(&json).unwrap();
        let context = Context::new(HashMap::from_iter(vec![("label", "true")]));
        assert!(node.apply(&context).unwrap().as_bool());
    }

    #[test]
    fn test_parser_invalid_json() {
        let json = serde_json::from_str(r#"{"$and": [true, true], "invalid": "json"}"#).unwrap();

        let parser = Parser {};
        let result = parser.parse(&json);
        assert_eq!(result.err().unwrap().to_string(), "[UnprocessableEntity] A filed must contain array or object: {\"$and\":[true,true],\"invalid\":\"json\"}");
    }

    #[test]
    fn test_parser_invalid_operator() {
        let json = serde_json::from_str(r#"{"$xx": [true, true]}"#).unwrap();

        let parser = Parser {};
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Operator '$xx' not supported"
        );
    }

    #[test]
    fn test_parser_invalid_object_notation() {
        let json = serde_json::from_str(r#"{"&ref": {"$and": true, "x": "y"}}"#).unwrap();

        let parser = Parser {};
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Object notation must have exactly one operator"
        );
    }
}
