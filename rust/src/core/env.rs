// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
    fn new() -> Env {
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
    fn get<T: FromStr + Display + Default + PartialEq>(
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

    /// Get a string from the environment. (See `get` for details)
    pub fn get_string(&mut self, key: &str, default_value: &str, masked: bool) -> String {
        self.get(key, String::from(default_value), masked)
    }

    /// Get an int from the environment. (See `get` for details)
    pub fn get_int(&mut self, key: &str, default_value: i32, masked: bool) -> i32 {
        self.get(key, default_value, masked)
    }

    /// Get pretty printed message.
    pub fn message(&self) -> &String {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use crate::core::env::{new_env, Env};

    #[test]
    fn make_env() {
        let env = new_env();
        assert_eq!(env.message(), "");
    }

    #[test]
    fn default_values() {
        let mut env = setup();

        let value = env.get("TEST", String::from("default"), false);
        assert_eq!(value, "default");
        assert_eq!(env.message(), "\tTEST = default (default)\n");
    }

    #[test]
    fn masked_values() {
        let mut env = setup();

        std::env::set_var("TEST", "123");

        let value = env.get("TEST", String::from("default"), true);
        assert_eq!(value, "123");
        assert_eq!(env.message(), "\tTEST = *** \n");
    }

    fn setup() -> Env {
        std::env::remove_var("TEST");
        Env::new()
    }
}
