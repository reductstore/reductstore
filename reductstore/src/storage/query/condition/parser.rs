// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::constant::Constant;
use crate::storage::query::condition::operators::arithmetic::{
    Abs, Add, Div, DivNum, Mult, Rem, Sub,
};
use crate::storage::query::condition::operators::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::storage::query::condition::operators::logical::{AllOf, AnyOf, NoneOf, OneOf};
use crate::storage::query::condition::operators::string::{Contains, EndsWith, StartsWith};
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
        let expressions = self.parse_intern(json)?;
        Ok(AllOf::boxed(expressions)?)
    }

    fn parse_intern(&self, json: &JsonValue) -> Result<Vec<BoxedNode>, ReductError> {
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
                Ok(expressions)
            }

            JsonValue::Bool(value) => Ok(vec![Constant::boxed(Value::Bool(*value))]),

            JsonValue::Number(value) => {
                if value.is_i64() || value.is_u64() {
                    Ok(vec![Constant::boxed(Value::Int(value.as_i64().unwrap()))])
                } else {
                    Ok(vec![Constant::boxed(Value::Float(value.as_f64().unwrap()))])
                }
            }
            JsonValue::String(value) => {
                if value.starts_with("&") {
                    Ok(vec![Reference::boxed(value[1..].to_string())])
                } else {
                    Ok(vec![Constant::boxed(Value::String(value.clone()))])
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
            operands.extend(self.parse_intern(operand)?);
        }
        Self::parse_operator(operator, operands)
    }

    fn parse_object_syntax(
        &self,
        left_operand: &str,
        op_right_operand: &Map<String, JsonValue>,
    ) -> Result<BoxedNode, ReductError> {
        let mut left_operand = self.parse_intern(&JsonValue::String(left_operand.to_string()))?;
        if op_right_operand.len() != 1 {
            return Err(unprocessable_entity!(
                "Object notation must have exactly one operator"
            ));
        }

        let (operator, operand) = op_right_operand.iter().next().unwrap();
        let right_operand = self.parse_intern(operand)?;
        left_operand.extend(right_operand);
        Self::parse_operator(operator, left_operand)
    }

    fn parse_operator(operator: &str, operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        match operator {
            // Arithmetic operators
            "$add" => Add::boxed(operands),
            "$sub" => Sub::boxed(operands),
            "$mult" => Mult::boxed(operands),
            "$div" => Div::boxed(operands),
            "$div_num" => DivNum::boxed(operands),
            "$rem" => Rem::boxed(operands),
            "$abs" => Abs::boxed(operands),

            // Logical operators
            "$and" => AllOf::boxed(operands),
            "$all_of" => AllOf::boxed(operands),
            "$or" => AnyOf::boxed(operands),
            "$any_of" => AnyOf::boxed(operands),
            "$not" => NoneOf::boxed(operands),
            "$none_of" => NoneOf::boxed(operands),
            "$xor" => OneOf::boxed(operands),
            "$one_of" => OneOf::boxed(operands),

            // Comparison operators
            "$eq" => Eq::boxed(operands),
            "$gt" => Gt::boxed(operands),
            "$gte" => Gte::boxed(operands),
            "$lt" => Lt::boxed(operands),
            "$lte" => Lte::boxed(operands),
            "$ne" => Ne::boxed(operands),

            // String operators
            "$contains" => Contains::boxed(operands),
            "$starts_with" => StartsWith::boxed(operands),
            "$ends_with" => EndsWith::boxed(operands),

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
        #[case("$add", "[1, 2.0]", Value::Float(3.0))]
        #[case("$sub", "[1, 2]", Value::Int(-1))]
        #[case("$mult", "[2, 3]", Value::Int(6))]
        #[case("$div", "[3, 2]", Value::Float(1.5))]
        #[case("$div_num", "[3, 2]", Value::Int(1))]
        #[case("$rem", "[-10, 6]", Value::Int(-4))]
        #[case("$abs", "[-10]", Value::Int(10))]
        // Logical operators
        #[case("$and", "[true, false]", Value::Bool(false))]
        #[case("$all_of", "[true, false]", Value::Bool(false))]
        #[case("$or", "[true, false]", Value::Bool(true))]
        #[case("$any_of", "[true, false]", Value::Bool(true))]
        #[case("$not", "[true]", Value::Bool(false))]
        #[case("$none_of", "[true, true]", Value::Bool(false))]
        #[case("$xor", "[true, true]", Value::Bool(false))]
        #[case("$one_of", "[true, true]", Value::Bool(false))]
        // Comparison operators
        #[case("$eq", "[10, 10]", Value::Bool(true))]
        #[case("$gt", "[20, 10]", Value::Bool(true))]
        #[case("$gte", "[20, 10]", Value::Bool(true))]
        #[case("$lt", "[20, 10]", Value::Bool(false))]
        #[case("$lte", "[20, 10]", Value::Bool(false))]
        #[case("$ne", "[-10, 10]", Value::Bool(true))]
        // String operators
        #[case("$contains", "[\"abc\", \"b\"]", Value::Bool(true))]
        #[case("$starts_with", "[\"abc\", \"ab\"]", Value::Bool(true))]
        #[case("$ends_with", "[\"abc\", \"bc\"]", Value::Bool(true))]
        fn test_parse_operator(
            parser: Parser,
            context: Context,
            #[case] operator: &str,
            #[case] operands: &str,
            #[case] expected: Value,
        ) {
            let json = serde_json::from_str(&format!(
                r#"{{"$eq":[{}, {{"{}": {} }}] }}"#,
                expected.as_string().unwrap(),
                operator,
                operands
            ))
            .unwrap();
            let node = parser.parse(&json).unwrap();
            assert!(
                node.apply(&context).unwrap().as_bool().unwrap(),
                "{}",
                node.print()
            );
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
