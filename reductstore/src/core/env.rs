// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use regex::Regex;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;

/// A helper class to read environment variables.
pub struct Env {
    message: String,
}

/// Create a new environment in a box for C++ integration.
pub fn new_env() -> Box<Env> {
    Box::new(Env::new())
}

impl Env {
    /// Create a new environment.
    pub fn new() -> Env {
        Env {
            message: String::new(),
        }
    }

    /// Get a value from the environment.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to get.
    /// * `default_value` - The default value to return if the key is not found.
    /// * `masked` - Whether or not to mask the value in the log.
    ///
    /// # Returns
    ///
    /// The value of the environment variable.
    pub fn get<T: FromStr + Display + Default + PartialEq>(
        &mut self,
        key: &str,
        default_value: T,
        masked: bool,
    ) -> T {
        let mut additional = String::new();
        let value: T = match std::env::var(key) {
            Ok(value) => match value.parse() {
                Ok(value) => value,
                Err(_) => {
                    additional.push_str("(invalid)");
                    default_value
                }
            },
            Err(_) => {
                additional.push_str("(default)");
                default_value
            }
        };

        if value != T::default() {
            // Add to the message
            if masked {
                self.message.push_str(&format!(
                    "\t{} = {} {}\n",
                    key,
                    "*".repeat(value.to_string().len()),
                    additional
                ));
            } else {
                self.message
                    .push_str(&format!("\t{} = {} {}\n", key, value, additional));
            }
        }
        return value;
    }

    pub fn matches<T: FromStr + Display + Default + PartialEq>(
        &mut self,
        pattern: &str,
        masked: bool,
    ) -> BTreeMap<String, T> {
        let mut matches = BTreeMap::new();
        let pattern = Regex::new(pattern).unwrap();
        for (key, _) in std::env::vars() {
            if let Some(result) = pattern.captures(&key) {
                matches.insert(
                    result.get(1).unwrap().as_str().to_string(),
                    self.get(&key, T::default(), masked),
                );
            }
        }

        matches
    }
    /// Get pretty printed message.
    pub fn message(&self) -> &String {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use crate::core::env::{new_env, Env};
    use rstest::{fixture, rstest};

    #[rstest]
    fn make_env(mut env: Env) {
        let env = new_env();
        assert_eq!(env.message(), "");
    }

    #[rstest]
    fn default_values(mut env: Env) {
        let value = env.get("TEST__", String::from("default"), false);
        assert_eq!(value, "default");
        assert_eq!(env.message(), "\tTEST__ = default (default)\n");
    }

    #[rstest]
    fn masked_values(mut env: Env) {
        std::env::set_var("TEST", "123");

        let value = env.get("TEST", String::from("default"), true);
        assert_eq!(value, "123");
        assert_eq!(env.message(), "\tTEST = *** \n");
    }

    #[rstest]
    fn test_matches(mut env: Env) {
        std::env::set_var("RS_BUCKET_TEST_QUOTA_TYPE", "test");
        std::env::set_var("RS_BUCKET_TEST2_QUOTA_TYPE", "test2");

        let matches = env.matches::<String>("RS_BUCKET_(.+)_QUOTA_TYPE", false);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches.get("TEST").unwrap(), "test");
        assert_eq!(matches.get("TEST2").unwrap(), "test2");
    }

    #[fixture]
    fn env() -> Env {
        std::env::remove_var("TEST");
        Env::new()
    }
}
