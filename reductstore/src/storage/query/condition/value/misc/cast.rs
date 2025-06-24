use crate::storage::query::condition::value::Value;
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;

/// A trait for adding two values.
pub(crate) trait Cast {
    /// Casts a value to a different type.
    ///
    /// # Arguments
    ///
    /// * `type_name` - The name of the type to cast to.
    ///
    /// # Returns
    ///
    /// The value cast to the specified type or an error if the value cannot be cast.
    fn cast(self, type_name: &str) -> Result<Self, ReductError>
    where
        Self: Sized;
}

impl Cast for Value {
    fn cast(self, type_name: &str) -> Result<Self, ReductError>
    where
        Self: Sized,
    {
        match type_name {
            "bool" => self.as_bool().map(Value::Bool),
            "int" => self.as_int().map(Value::Int),
            "float" => self.as_float().map(Value::Float),
            "string" => Ok(Value::String(self.to_string())),
            "duration" => self.as_float().map(Value::Duration),
            _ => Err(unprocessable_entity!("Unknown type '{}'", type_name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::query::condition::value::Value;
    use rstest::rstest;

    #[rstest]
    #[case("bool", Value::Bool(true), Ok(Value::Bool(true)))]
    #[case("bool", Value::Int(1), Ok(Value::Bool(true)))]
    #[case("bool", Value::Float(1.0), Ok(Value::Bool(true)))]
    #[case("bool", Value::String("true".to_string()), Ok(Value::Bool(true)))]
    #[case("bool", Value::Duration(1.0), Ok(Value::Bool(true)))]
    #[case("int", Value::Bool(true), Ok(Value::Int(1)))]
    #[case("int", Value::Int(1), Ok(Value::Int(1)))]
    #[case("int", Value::Float(1.0), Ok(Value::Int(1)))]
    #[case("int", Value::String("1".to_string()), Ok(Value::Int(1)))]
    #[case("int", Value::String("xx".to_string()), Err(unprocessable_entity!("Value 'xx' could not be parsed as integer")))]
    #[case("int", Value::Duration(1.0), Ok(Value::Int(1)))]
    #[case("float", Value::Bool(true), Ok(Value::Float(1.0)))]
    #[case("float", Value::Int(1), Ok(Value::Float(1.0)))]
    #[case("float", Value::Float(1.0), Ok(Value::Float(1.0)))]
    #[case("float", Value::String("1.0".to_string()), Ok(Value::Float(1.0)))]
    #[case("float", Value::String("xx".to_string()), Err(unprocessable_entity!("Value 'xx' could not be parsed as float")))]
    #[case("float", Value::Duration(1.0), Ok(Value::Float(1.0)))]
    #[case("string", Value::Bool(true), Ok(Value::String("true".to_string())))]
    #[case("string", Value::Int(1), Ok(Value::String("1".to_string())))]
    #[case("string", Value::Float(1.0), Ok(Value::String("1".to_string())))]
    #[case("string", Value::String("1".to_string()), Ok(Value::String("1".to_string())))]
    #[case("string", Value::Duration(1.0), Ok(Value::String("1s".to_string())))]
    #[case("duration", Value::Bool(true), Ok(Value::Duration(1.0)))]
    #[case("duration", Value::Int(1), Ok(Value::Duration(1.0)))]
    #[case("duration", Value::Float(1.0), Ok(Value::Duration(1.0)))]
    #[case("duration", Value::String("1".to_string()), Ok(Value::Duration(1.0)))]
    #[case("duration", Value::String("xx".to_string()), Err(unprocessable_entity!("Value 'xx' could not be parsed as float")))]
    #[case("unknown", Value::Bool(true), Err(unprocessable_entity!("Unknown type 'unknown'")))]
    fn test_cast(
        #[case] type_name: &str,
        #[case] value: Value,
        #[case] expected: Result<Value, ReductError>,
    ) {
        let result = value.cast(type_name);
        assert_eq!(result, expected);
    }
}
