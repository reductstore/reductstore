pub mod core;
use crate::core::{Env, new_env};

#[cxx::bridge(namespace = "reduct::core")]
mod ffi {
    extern "Rust" {
        type Env;
        fn new_env() -> Box<Env>;
        fn get_string(&mut self, key: &str, default_value: &str, masked: bool) -> String;
        fn get_int(&mut self, key: &str, default_value: i32, masked: bool) -> i32;
        fn message(&self) -> &str;
    }
}
