// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use regex::Regex;
use std::collections::BTreeMap;
use std::env::VarError;
use std::fmt::Display;
use std::str::FromStr;

/// A trait to get environment variables.
/// This is used to mock the environment in tests.
pub trait GetEnv {
    fn get(&self, key: &str) -> Result<String, VarError>;
    fn all(&self) -> BTreeMap<String, String>;
}

/// The default implementation of GetEnv.
/// This uses the standard library.
#[derive(Default)]
pub struct StdEnvGetter {}

impl GetEnv for StdEnvGetter {
    fn get(&self, key: &str) -> Result<String, VarError> {
        std::env::var(key)
    }

    fn all(&self) -> BTreeMap<String, String> {
        std::env::vars().into_iter().collect()
    }
}

/// A helper class to read environment variables.
pub struct Env<EnvGetter: GetEnv = StdEnvGetter> {
    getter: EnvGetter,
    message: String,
}

pub trait EnvValue: FromStr + Display + Default + PartialEq {}

impl<T: FromStr + Display + Default + PartialEq> EnvValue for T {}

impl Default for Env {
    fn default() -> Self {
        Env::new(StdEnvGetter::default())
    }
}

impl<EnvGetter: GetEnv> Env<EnvGetter> {
    /// Create a new environment.
    pub fn new(getter: EnvGetter) -> Env<EnvGetter> {
        Env::<EnvGetter> {
            getter,
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
    pub(crate) fn get<T: EnvValue>(&mut self, key: &str, default_value: T) -> T {
        self.get_impl(key, default_value, false)
    }

    /// Get a value from the environment.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to get.
    /// * `default_value` - The default value to return if the key is not found.
    ///
    /// # Returns
    ///
    /// The value of the environment variable.
    pub(crate) fn get_masked<T: EnvValue>(&mut self, key: &str, default_value: T) -> T {
        self.get_impl(key, default_value, true)
    }

    /// Get a value from the environment. without default value
    pub(crate) fn get_optional<T: EnvValue>(&mut self, key: &str) -> Option<T> {
        let mut additional = String::new();
        let value = match self.getter.get(key) {
            Ok(value) => match T::from_str(&value) {
                Ok(value) => Ok(value),
                Err(_) => {
                    additional.push_str("(invalid)");
                    Err(value)
                }
            },
            Err(_) => Err(String::new()),
        };

        let print_value = match &value {
            Ok(value) => value.to_string(),
            Err(e) => e.clone(),
        };

        if !print_value.is_empty() {
            // Add to the message
            self.message
                .push_str(&format!("\t{} = {} {}\n", key, print_value, additional));
        }

        value.ok()
    }

    fn get_impl<T: EnvValue>(&mut self, key: &str, default_value: T, masked: bool) -> T {
        let mut additional = String::new();
        let value: T = match self.getter.get(key) {
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
    ) -> BTreeMap<String, T> {
        let mut matches = BTreeMap::new();
        let pattern = Regex::new(pattern).unwrap();
        for (key, _) in self.getter.all() {
            if let Some(result) = pattern.captures(&key) {
                matches.insert(
                    result.get(1).unwrap().as_str().to_string(),
                    self.get(&key, T::default()),
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
    use crate::core::env::Env;
    use rstest::{fixture, rstest};

    #[rstest]
    fn make_env(_env: Env) {
        let env = Env::default();
        assert_eq!(env.message(), "");
    }

    #[rstest]
    fn default_values(mut env: Env) {
        let value = env.get("TEST__", String::from("default"));
        assert_eq!(value, "default");
        assert_eq!(env.message(), "\tTEST__ = default (default)\n");
    }

    #[rstest]
    fn masked_values(mut env: Env) {
        std::env::set_var("TEST", "123");

        let value = env.get_masked("TEST", String::from("default"));
        assert_eq!(value, "123");
        assert_eq!(env.message(), "\tTEST = *** \n");
    }

    #[rstest]
    fn test_matches(mut env: Env) {
        std::env::set_var("RS_BUCKET_TEST_QUOTA_TYPE", "test");
        std::env::set_var("RS_BUCKET_TEST2_QUOTA_TYPE", "test2");

        let matches = env.matches::<String>("RS_BUCKET_(.+)_QUOTA_TYPE");
        assert_eq!(matches.len(), 2);
        assert_eq!(matches.get("TEST").unwrap(), "test");
        assert_eq!(matches.get("TEST2").unwrap(), "test2");
    }

    #[fixture]
    fn env() -> Env {
        std::env::remove_var("TEST");
        Env::default()
    }
}
