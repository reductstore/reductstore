// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

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
use crate::storage::query::condition::value::Value;
use crate::storage::query::condition::{Boxed, BoxedNode, Context, EvaluationStage, Node};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use serde_json::{Map, Value as JsonValue};

/// Parses a JSON object into a condition tree.
pub(crate) struct Parser {}

/// A node in a condition tree.
///
/// It evaluates the node in the given context on different stages.
struct StagedAllOff {
    operands: Vec<BoxedNode>,
}

impl Node for StagedAllOff {
    fn apply(&mut self, context: &Context) -> Result<Value, ReductError> {
        for operand in self.operands.iter_mut() {
            // Filter out operands that are not in the current stage
            if operand.stage() == &context.stage {
                let value = operand.apply(context)?;
                if !value.as_bool()? {
                    return Ok(Value::Bool(false));
                }
            }
        }

        Ok(Value::Bool(true))
    }

    fn operands(&self) -> &Vec<BoxedNode> {
        &self.operands
    }

    fn print(&self) -> String {
        format!("AllOf({:?})", self.operands)
    }
}

impl Boxed for StagedAllOff {
    fn boxed(operands: Vec<BoxedNode>) -> Result<BoxedNode, ReductError> {
        Ok(Box::new(Self { operands }))
    }
}

impl Parser {
    pub fn parse(&self, json: &JsonValue) -> Result<BoxedNode, ReductError> {
        let expressions = self.parse_intern(json)?;
        Ok(StagedAllOff::boxed(expressions)?)
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
                        // For unary operators, we need to parse the value
                        let operands = self.parse_intern(value)?;
                        let operator = Self::parse_operator(key, operands)?;
                        expressions.push(operator);
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
                    Ok(vec![Reference::boxed(
                        value[1..].to_string(),
                        EvaluationStage::Retrieve,
                    )])
                } else if value.starts_with("@") {
                    Ok(vec![Reference::boxed(
                        value[1..].to_string(),
                        EvaluationStage::Compute,
                    )])
                } else if value.starts_with("$") {
                    // operator without operands (nullary)
                    Ok(vec![Self::parse_operator(value, vec![])?])
                } else {
                    Ok(vec![Constant::boxed(Value::String(value.clone()))])
                }
            }

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
        let mut node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parser_object_syntax(parser: Parser) {
        let json = json!({
            "&label": {"$gt": 10}
        });
        let mut node = parser.parse(&json).unwrap();
        let context = Context::new(
            0,
            HashMap::from_iter(vec![("label", "20")]),
            EvaluationStage::Retrieve,
        );
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_int(parser: Parser, context: Context) {
        let json = json!({
            "$and": [1, -2]
        });
        let mut node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_float(parser: Parser, context: Context) {
        let json = json!({
            "$and": [1.1, -2.2]
        });
        let mut node = parser.parse(&json).unwrap();
        assert!(node.apply(&context).unwrap().as_bool().unwrap());
    }

    #[rstest]
    fn test_parse_string(parser: Parser, context: Context) {
        let json = json!({
            "$and": [                "a","b"]
        });
        let mut node = parser.parse(&json).unwrap();
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

        let mut node = parser.parse(&json).unwrap();
        let context = Context::new(
            0,
            HashMap::from_iter(vec![("label", "true")]),
            EvaluationStage::Retrieve,
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
        let mut node = parser.parse(&json).unwrap();
        assert_eq!(node.apply(&context).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn test_parse_unary_operator(parser: Parser, context: Context) {
        let json = json!({
        "$limit": 100
        });
        let mut node = parser.parse(&json).unwrap();
        assert_eq!(node.apply(&context).unwrap(), Value::Int(1));
    }

    #[rstest]
    fn test_parser_invalid_operator(parser: Parser) {
        let json = json!( {
            "$xx": [true, true]
        });
        let result = parser.parse(&json);
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
        let result = parser.parse(&json);
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
        let result = parser.parse(&json);
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
        let result = parser.parse(&json);
        assert_eq!(
            result.err().unwrap().to_string(),
            "[UnprocessableEntity] Null type is not supported: null"
        );
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
                expected.as_string().unwrap(),
                operator,
                operands
            ))
            .unwrap();
            let mut node = parser.parse(&json).unwrap();
            assert!(
                node.apply(&context).unwrap().as_bool().unwrap(),
                "{}",
                node.print()
            );
        }
    }

    mod staged_all_of {
        use super::*;
        use rstest::rstest;

        #[rstest]
        fn test_staged_all_of() {
            let operands: Vec<BoxedNode> = vec![
                Constant::boxed(Value::Bool(true)),
                Constant::boxed(Value::Bool(false)),
            ];
            let staged_all_of = StagedAllOff::boxed(operands);
            assert_eq!(
                staged_all_of.unwrap().print(),
                "AllOf([Bool(true), Bool(false)])"
            );
        }

        #[rstest]
        fn test_run_only_staged_all_of() {
            let operands: Vec<BoxedNode> = vec![
                Constant::boxed(Value::Bool(false)), // ignored because not in stage
            ];

            let mut staged_all_of = StagedAllOff::boxed(operands).unwrap();
            let context = Context::new(0, HashMap::new(), EvaluationStage::Compute);
            assert_eq!(staged_all_of.apply(&context).unwrap(), Value::Bool(true));
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
