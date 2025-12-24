// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use std::cmp::max;
use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::thread::available_parallelism;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ThreadPoolConfig {
    pub min_idle_threads: usize,
    pub worker_task_timeout: Duration,
    pub scale_down_cooldown: Duration,
    pub scale_step: usize,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            #[cfg(not(test))]
            min_idle_threads: 4,
            #[cfg(test)]
            min_idle_threads: 8, // More threads for tests to avoid deadlock when tasks wait for other tasks
            #[cfg(not(test))]
            worker_task_timeout: Duration::from_secs(1),
            #[cfg(test)]
            worker_task_timeout: Duration::from_millis(100),
            scale_down_cooldown: Duration::from_secs(5),
            scale_step: 1,
        }
    }
}

static THREAD_POOL_CONFIG: OnceLock<ThreadPoolConfig> = OnceLock::new();

pub(crate) fn configure_thread_pool(config: ThreadPoolConfig) {
    let _ = THREAD_POOL_CONFIG.set(config);
}

pub fn thread_pool_config() -> ThreadPoolConfig {
    THREAD_POOL_CONFIG
        .get()
        .cloned()
        .unwrap_or_else(ThreadPoolConfig::default)
}

impl<EnvGetter: GetEnv> CfgParser<EnvGetter> {
    pub fn parse_thread_pool_config(env: &mut Env<EnvGetter>) -> ThreadPoolConfig {
        let default = ThreadPoolConfig::default();

        let min_idle_threads = env
            .get_optional::<usize>("RS_THREAD_POOL_MIN_IDLE_THREADS")
            .map(|value| value.max(1))
            .unwrap_or(default.min_idle_threads);

        let worker_task_timeout = env
            .get_optional::<u64>("RS_THREAD_POOL_WORKER_TIMEOUT_US")
            .map(Duration::from_micros)
            .unwrap_or(default.worker_task_timeout);

        let scale_down_cooldown = env
            .get_optional::<u64>("RS_THREAD_POOL_SCALE_DOWN_COOLDOWN_SECS")
            .map(Duration::from_secs)
            .unwrap_or(default.scale_down_cooldown);

        ThreadPoolConfig {
            min_idle_threads,
            worker_task_timeout,
            scale_down_cooldown,
            ..default
        }
    }
}
