// Copyright 2024-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::query::condition::computed_reference::ComputedReference;
use crate::storage::query::condition::constant::Constant;
use crate::storage::query::condition::operators::aggregation::{EachN, EachT, Limit};
use crate::storage::query::condition::operators::arithmetic::{
    Abs, Add, Div, DivNum, Mult, Rem, Sub,
};
use crate::storage::query::condition::operators::comparison::{Eq, Gt, Gte, Lt, Lte, Ne};
use crate::storage::query::condition::operators::logical::{AllOf, AnyOf, In, Nin, NoneOf, OneOf};
use crate::storage::query::condition::operators::misc::{Cast, Exists, Ref, Timestamp};
use crate::storage::query::condition::operators::string::{Contains, EndsWith, StartsWith};
use crate::storage::query::condition::reference::Reference;
use crate::storage::query::condition::value::{parse_duration, Value};
use crate::storage::query::condition::{Boxed, BoxedNode};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use serde_json::{Map, Number, Value as JsonValue};
use std::collections::HashMap;

/// Parses a JSON object into a condition tree.
pub(crate) struct Parser {}

pub(crate) type Directives = HashMap<String, Vec<Value>>;
static DIRECTIVES: [&str; 3] = ["#ctx_before", "#ctx_after", "#select_labels"];

impl Parser {
    /// Parses a JSON object into a condition tree.
    ///
    /// # Arguments
    ///
    /// * `json` - A JSON value representing the condition.
    ///
    /// # Returns
    ///
    /// A boxed node representing the condition tree.
    /// The root node is a `StagedAllOf` that aggregates all expressions
    pub fn parse(&self, mut json: JsonValue) -> Result<(BoxedNode, Directives), ReductError> {
        // parse directives if any
        let directives = Self::parse_directives(&mut json)?;
        let expressions = Self::parse_recursively(&json)?;
        Ok((AllOf::boxed(expressions)?, directives))
    }

    fn parse_directives(json: &mut JsonValue) -> Result<Directives, ReductError> {
        let mut directives = Directives::new();
        let mut keys_to_remove = vec![];
        for (key, value) in json.as_object().unwrap_or(&Map::new()).iter() {
            if key.starts_with("#") {
                if !DIRECTIVES.contains(&key.as_str()) {
                    return Err(unprocessable_entity!(
                        "Directive '{}' is not supported",
                        key
                    ));
                }

                let values = if key == "#select_labels" {
                    match value {
                        JsonValue::Array(arr) => {
                            if arr.is_empty() {
                                return Err(unprocessable_entity!(
                                    "Directive '{}' cannot be an empty array",
                                    key
                                ));
                            }

                            arr.iter()
                                .map(|v| match v {
                                    JsonValue::String(s) => Ok(Value::String(s.clone())),
                                    _ => Err(unprocessable_entity!(
                                        "Directive '{}' must contain only strings",
                                        key
                                    )),
                                })
                                .collect::<Result<Vec<Value>, _>>()?
                        }
                        _ => {
                            return Err(unprocessable_entity!(
                                "Directive '{}' must be an array of strings",
                                key
                            ));
                        }
                    }
                } else {
                    let parsed = match value {
                        JsonValue::Bool(value) => Value::Bool(*value),
                        JsonValue::Number(value) => {
                            if value.is_i64() || value.is_u64() {
                                Value::Int(value.as_i64().unwrap())
                            } else {
                                Value::Float(value.as_f64().unwrap())
                            }
                        }
                        JsonValue::String(value) => {
                            if let Ok(duration) = parse_duration(value) {
                                duration
                            } else {
                                Value::String(value.to_string())
                            }
                        }
                        _ => {
                            return Err(unprocessable_entity!(
                                "Directive '{}' must be a primitive value",
                                key
                            ));
                        }
                    };
                    vec![parsed]
                };

                directives.insert(key.to_string(), values);
                keys_to_remove.push(key.to_string());
            }
        }

        // Remove directives from the original JSON object
        for key in keys_to_remove {
            json.as_object_mut().unwrap().shift_remove(&key);
        }
        Ok(directives)
    }

    fn parse_recursively(json: &JsonValue) -> Result<Vec<BoxedNode>, ReductError> {
        match json {
            JsonValue::Object(map) => Self::parse_object(map),
            JsonValue::Bool(value) => Self::parse_bool(value),
            JsonValue::Number(value) => Self::parse_number(value),
            JsonValue::String(value) => Self::parse_string(value),
            JsonValue::Array(_) => Err(unprocessable_entity!(
                "Array type is not supported: {}",
                json
            )),
            JsonValue::Null => Err(unprocessable_entity!(
                "Null type is not supported: {}",
                json
            )),
        }
    }

    fn parse_object(map: &Map<String, serde_json::Value>) -> Result<Vec<BoxedNode>, ReductError> {
        let mut expressions = vec![];
        for (key, value) in map.iter() {
            if let JsonValue::Array(operand_list) = value {
                // Parse array notation e.g. {"$and": [true, true]}
                expressions.push(Self::parse_array_syntax(key, operand_list)?);
            } else if let JsonValue::Object(operator_right_operand) = value {
                // Parse object notation e.g. {"&label": {"$and": true}}
                expressions.push(Self::parse_object_syntax(key, operator_right_operand)?);
            } else {
                // For unary operators, we need to parse the value
                let operands = Self::parse_recursively(value)?;
                let operator = Self::parse_operator(key, operands)?;
                expressions.push(operator);
            }
        }

        // We use AND operator to aggregate results from all expressions
        Ok(expressions)
    }

    fn parse_bool(value: &bool) -> Result<Vec<BoxedNode>, ReductError> {
        Ok(vec![Constant::boxed(Value::Bool(*value))])
    }

    fn parse_number(value: &Number) -> Result<Vec<BoxedNode>, ReductError> {
        if value.is_i64() || value.is_u64() {
            Ok(vec![Constant::boxed(Value::Int(value.as_i64().unwrap()))])
        } else {
            Ok(vec![Constant::boxed(Value::Float(value.as_f64().unwrap()))])
        }
    }

    fn parse_string(value: &str) -> Result<Vec<BoxedNode>, ReductError> {
        if value.starts_with("&") {
            Ok(vec![Reference::boxed(value[1..].to_string())])
        } else if value.starts_with("@") {
            Ok(vec![ComputedReference::boxed(value[1..].to_string())])
        } else if value.starts_with("$") {
            Ok(vec![Self::parse_operator(value, vec![])?])
        } else if let Ok(duration) = parse_duration(value) {
            Ok(vec![Constant::boxed(duration)])
        } else {
            Ok(vec![Constant::boxed(Value::String(value.to_string()))])
        }
    }

    fn parse_array_syntax(
        operator: &str,
        json_operands: &Vec<JsonValue>,
    ) -> Result<BoxedNode, ReductError> {
        let mut operands = vec![];
        for operand in json_operands {
            operands.extend(Self::parse_recursively(operand)?);
        }
        Self::parse_operator(operator, operands)
    }

    fn parse_object_syntax(
        left_operand: &str,
        op_right_operand: &Map<String, JsonValue>,
    ) -> Result<BoxedNode, ReductError> {
        let mut left_operand =
            Self::parse_recursively(&JsonValue::String(left_operand.to_string()))?;
        if op_right_operand.len() != 1 {
            return Err(unprocessable_entity!(
                "Object notation must have exactly one operator"
            ));
        }

        let (operator, operand) = op_right_operand.iter().next().unwrap();
        let right_operand = Self::parse_recursively(operand)?;
        left_operand.extend(right_operand);
        Self::parse_operator(operator, left_operand)
    }

    fn parse_operator(operator: &str, operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        if !operator.starts_with("$") {
            return Err(unprocessable_entity!(
                "Operator '{}' must start with '$'",
                operator
            ));
        }

        match operator {
            // Aggregation operators
            "$each_n" => EachN::boxed(operands),
            "$each_t" => EachT::boxed(operands),
            "$limit" => Limit::boxed(operands),
            // Arithmetic operators
            "$add" => Add::boxed(operands),
            "$sub" => Sub::boxed(operands),
            "$mult" => Mult::boxed(operands),
            "$div" => Div::boxed(operands),
            "$div_num" => DivNum::boxed(operands),
            "$rem" => Rem::boxed(operands),
            "$abs" => Abs::boxed(operands),

            // Logical operators
            "$and" | "$all_of" => AllOf::boxed(operands),
            "$or" | "$any_of" => AnyOf::boxed(operands),
            "$not" | "$none_of" => NoneOf::boxed(operands),
            "$xor" | "$one_of" => OneOf::boxed(operands),
            "$in" => In::boxed(operands),
            "$nin" => Nin::boxed(operands),

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

            // Misc
            "$exists" | "$has" => Exists::boxed(operands),
            "$cast" => Cast::boxed(operands),
            "$ref" => Ref::boxed(operands),
            "$timestamp" | "$id" => Timestamp::boxed(operands),

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
    use serde_json::json;
    use std::collections::HashMap;

    #[rstest]
    fn test_parser_array_syntax(parser: Parser, context: Context) {
        let json = json!({
        "$and": [true, {"$gt": [20, 10]}]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_object_syntax(parser: Parser) {
        let json = json!({
            "&label": {"$gt": 10}
        });
        let (mut node, _) = parser.parse(json).unwrap();
        let context = Context::new(0, HashMap::from_iter(vec![("label", "20")]), HashMap::new());
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_int(parser: Parser, context: Context) {
        let json = json!({
            "$and": [1, -2]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_float(parser: Parser, context: Context) {
        let json = json!({
            "$and": [1.1, -2.2]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_string(parser: Parser, context: Context) {
        let json = json!({
            "$and": ["a","b"]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_duration(parser: Parser, context: Context) {
        let json = json!({
            "$eq": ["1h", 3600_000_000u64]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_multiline(parser: Parser) {
        let json = json!({
            "$and": [
                {"&label": {"$and": true}},
                true
            ]
        }        );

        let (mut node, _) = parser.parse(json).unwrap();
        let context = Context::new(
            0,
            HashMap::from_iter(vec![("label", "true")]),
            HashMap::new(),
        );
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_nullary_operator(parser: Parser, context: Context) {
        let json = json!({
            "$add": [
                "$timestamp",
                1
            ]
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert_eq!(node.apply(&context).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn test_parse_unary_operator(parser: Parser, context: Context) {
        let json = json!({
        "$limit": 100
        });
        let (mut node, _) = parser.parse(json).unwrap();
        assert_eq!(node.apply(&context).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn test_parser_invalid_operator(parser: Parser) {
        let json = json!( {
            "$xx": [true, true]
        });
        let result = parser.parse(json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Operator '$xx' not supported"
        );
    }

    #[rstest]
    fn test_parser_invalid_object_notation(parser: Parser) {
        let json = json!({
                "&ref": {
                    "$and": true,
                    "x": "y"
                }
            }        );
        let result = parser.parse(json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Object notation must have exactly one operator"
        );
    }

    #[rstest]
    fn test_parser_invalid_array_type(parser: Parser) {
        let json = json!({
            "&label": {
                "$in": [10, 20]
            }
        });
        let result = parser.parse(json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Array type is not supported: [10,20]"
        );
    }

    #[rstest]
    fn test_parser_invalid_null(parser: Parser) {
        let json = json!({
            "&ref": {
                "$and": null
            }
        });
        let result = parser.parse(json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Null type is not supported: null"
        );
    }

    #[rstest]
    fn test_parser_invalid_operator_without_dollar(parser: Parser) {
        let json = json!({
            "and": [true, true]
        });
        let result = parser.parse(json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Operator 'and' must start with '$'"
        );
    }

    mod parse_directives {
        use super::*;
        use rstest::rstest;
        use serde::Serialize;

        #[rstest]
        fn test_parse_directives(parser: Parser) {
            let json = json!({
                "#ctx_before": "1h",
                "#ctx_after": "2h",
                "#select_labels": ["label1", "label2"]
            });
            let (_, directives) = parser.parse(json).unwrap();
            assert_eq!(directives.len(), 3);
            assert_eq!(
                directives["#ctx_before"],
                vec![Value::Duration(3600_000_000)]
            );
            assert_eq!(
                directives["#ctx_after"],
                vec![Value::Duration(7200_000_000)]
            );
            assert_eq!(
                directives["#select_labels"],
                vec![
                    Value::String("label1".to_string()),
                    Value::String("label2".to_string())
                ]
            );
        }

        #[rstest]
        #[case(true, Value::Bool(true))]
        #[case(123, Value::Int(123))]
        #[case(123.45, Value::Float(123.45))]
        #[case("test", Value::String("test".to_string()))]
        fn test_parse_directives_primitive_values<T: Serialize>(
            parser: Parser,
            #[case] value: T,
            #[case] expected: Value,
        ) {
            let json = json!({
                "#ctx_before": value,
            });
            let (_, directives) = parser.parse(json).unwrap();
            assert_eq!(directives["#ctx_before"], vec![expected]);
        }

        #[rstest]
        fn test_parse_array_directives(parser: Parser) {
            let json = json!({
                "#select_labels": ["label1", "label2"]
            });
            let (_, directives) = parser.parse(json).unwrap();
            assert_eq!(
                directives["#select_labels"],
                vec![
                    Value::String("label1".to_string()),
                    Value::String("label2".to_string())
                ]
            );
        }

        #[rstest]
        fn test_parse_directives_empty_array(parser: Parser) {
            let json = json!({
                "#select_labels": []
            });
            let result = parser.parse(json);
            assert_eq!(
                result.err().unwrap().to_string(),
                "[UnprocessableEntity] Directive '#select_labels' cannot be an empty array"
            );
        }

        #[rstest]
        fn test_parse_directives_invalid_value(parser: Parser) {
            let json = json!({
                "#ctx_before": [1, 2, 3]
            });
            let result = parser.parse(json);
            assert_eq!(
                result.err().unwrap().to_string(),
                "[UnprocessableEntity] Directive '#ctx_before' must be a primitive value"
            );
        }

        #[rstest]
        fn test_parse_invalid_directive(parser: Parser) {
            let json = json!({
                "#invalid_directive": "value"
            });
            let result = parser.parse(json);
            assert_eq!(
                result.err().unwrap().to_string(),
                "[UnprocessableEntity] Directive '#invalid_directive' is not supported"
            );
        }

        #[rstest]
        fn test_parse_select_labels_with_non_string(parser: Parser) {
            let json = json!({
                "#select_labels": ["label1", 123]
            });
            let result = parser.parse(json);
            assert_eq!(
                result.err().unwrap().to_string(),
                "[UnprocessableEntity] Directive '#select_labels' must contain only strings"
            );
        }

        #[rstest]
        fn test_parse_select_labels_not_array(parser: Parser) {
            let json = json!({
                "#select_labels": "not_an_array"
            });
            let result = parser.parse(json);
            assert_eq!(
                result.err().unwrap().to_string(),
                "[UnprocessableEntity] Directive '#select_labels' must be an array of strings"
            );
        }
    }

    mod parse_operators {
        use super::*;
        #[rstest]
        // Aggregation operators
        #[case("$each_n", "[1]", Value::Bool(true))]
        #[case("$each_t", "[1]", Value::Bool(false))]
        #[case("$limit", "[1]", Value::Bool(true))]
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
        #[case("$in", "[\"a\", \"a\", \"b\"]", Value::Bool(true))]
        #[case("$nin", "[\"a\", \"a\", \"b\"]", Value::Bool(false))]
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
        // Misc
        #[case("$exists", "[\"label\"]", Value::Bool(true))]
        #[case("$has", "[\"label\"]", Value::Bool(true))]
        #[case("$cast", "[10.0, \"int\"]", Value::Int(10))]
        #[case("$ref", "[\"label\"]", Value::Int(10))]
        #[case("$timestamp", "[]", Value::Int(0))]
        #[case("$id", "[]", Value::Int(0))]
        fn test_parse_operator(
            parser: Parser,
            context: Context,
            #[case] operator: &str,
            #[case] operands: &str,
            #[case] expected: Value,
        ) {
            let json = serde_json::from_str(&format!(
                r#"{{"$eq":[{}, {{"{}": {} }}] }}"#,
                expected.to_string(),
                operator,
                operands
            ))
            .unwrap();
            let (mut node, _) = parser.parse(json).unwrap();
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
        let mut context = Context::default();
        context.labels.insert("label", "10");
        context
    }
}
