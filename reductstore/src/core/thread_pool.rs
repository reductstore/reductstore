// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod scaling;
mod task_handle;

use crate::core::sync::RwLock;
use crate::core::thread_pool::scaling::WorkerManager;
use crossbeam_channel::{unbounded, Sender};
use log::{error, trace};
use std::cmp::max;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::thread::{available_parallelism, JoinHandle};
use std::time::Duration;
pub(crate) use task_handle::TaskHandle;

#[derive(Clone, Debug)]
pub struct ThreadPoolConfig {
    pub min_threads: usize,
    pub worker_task_timeout: Duration,
    pub scale_down_cooldown: Duration,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            min_threads: default_min_threads(),
            worker_task_timeout: default_worker_task_timeout(),
            scale_down_cooldown: Duration::from_secs(5),
        }
    }
}

static THREAD_POOL_CONFIG: OnceLock<ThreadPoolConfig> = OnceLock::new();

pub(crate) fn configure_thread_pool(config: ThreadPoolConfig) {
    let _ = THREAD_POOL_CONFIG.set(config);
}

fn thread_pool_config() -> ThreadPoolConfig {
    THREAD_POOL_CONFIG
        .get()
        .cloned()
        .unwrap_or_else(ThreadPoolConfig::default)
}

fn default_min_threads() -> usize {
    let threads = available_parallelism().unwrap_or(NonZeroUsize::new(2).unwrap());
    max(threads.get() / 2, 2)
}

#[cfg(unix)]
fn default_worker_task_timeout() -> Duration {
    Duration::from_micros(250)
}

#[cfg(windows)]
fn default_worker_task_timeout() -> Duration {
    Duration::from_micros(1000)
}

#[cfg(not(any(unix, windows)))]
fn default_worker_task_timeout() -> Duration {
    Duration::from_micros(500)
}

#[derive(PartialEq)]
pub(crate) enum ThreadPoolState {
    Running,
    Stopped,
}

/// Spawn a task in the thread pool.
pub(crate) fn spawn<T>(
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.spawn(description, task)
}

/// Boxed function to wrap a function for exicution in thread pool.
type BoxedFunc = Box<dyn FnOnce() + Send>;

struct Task {
    description: String,
    func: BoxedFunc,
}

impl Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task({})", self.description,)
    }
}

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let config = thread_pool_config();
    ThreadPool::new(config)
});

/// A thread pool for executing tasks.
struct ThreadPool {
    task_queue: Sender<Task>,
    queued_tasks: Arc<AtomicUsize>,
    state: Arc<RwLock<ThreadPoolState>>,
    supervisor: Option<JoinHandle<()>>,
}

impl ThreadPool {
    /// Create a new thread pool with a given configuration.
    pub fn new(config: ThreadPoolConfig) -> Self {
        let (task_queue, task_queue_rx) = unbounded::<Task>();
        let state = Arc::new(RwLock::new(ThreadPoolState::Running));
        let queued_tasks = Arc::new(AtomicUsize::new(0));
        let worker_manager = Arc::new(
            WorkerManager::builder()
                .state(Arc::clone(&state))
                .min_threads(config.min_threads)
                .scale_down_cooldown(config.scale_down_cooldown)
                .worker_task_timeout(config.worker_task_timeout)
                .build(),
        );

        let supervisor = worker_manager.start(
            task_queue_rx.clone(),
            queued_tasks.clone(),
            config.min_threads,
        );

        Self {
            task_queue,
            queued_tasks,
            state,
            supervisor: Some(supervisor),
        }
    }

    pub fn spawn<T: Send + 'static>(
        &self,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn task '{}'", description);
        let description = description.to_string();

        let (task, task_handle) = Self::build_task(description, task);

        self.queued_tasks.fetch_add(1, Ordering::SeqCst);
        self.task_queue.send(task).unwrap_or(());

        task_handle
    }

    fn build_task<T: Send + 'static>(
        description: String,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> (Task, TaskHandle<T>) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let (tx_start, rx_start) = crossbeam_channel::bounded(1);

        let copy_description = description.clone();
        let box_task = Box::new(move || {
            tx_start.send(()).unwrap_or(());
            let result = task();
            tx.send(result).unwrap_or(());
        });

        let task = Task {
            description: copy_description,
            func: box_task,
        };

        (task, TaskHandle::new(rx, rx_start))
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        *self.state.write().unwrap() = ThreadPoolState::Stopped;
        if let Some(handle) = self.supervisor.take() {
            if let Err(err) = handle.join() {
                error!("Thread pool exited with error: {:?}", err);
            }
        }
    }
}
