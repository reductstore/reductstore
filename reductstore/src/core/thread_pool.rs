// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod scaling;
mod task_handle;

use crate::cfg::thread_pool::thread_pool_config;
use crate::cfg::thread_pool::ThreadPoolConfig;
use crate::core::sync::RwLock;
use crate::core::thread_pool::scaling::WorkerManager;
use crossbeam_channel::{unbounded, Sender};
use log::{error, trace};
use std::cmp::max;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};
use std::thread::{available_parallelism, JoinHandle};
use std::time::Duration;
pub(crate) use task_handle::TaskHandle;

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
    id: usize,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task {{ id: {}, description: '{}' }}",
            self.id, self.description
        )
    }
}

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let config = thread_pool_config();
    ThreadPool::new(config)
});

/// A thread pool for executing tasks.
struct ThreadPool {
    task_queue: Sender<Task>,
    state: Arc<RwLock<ThreadPoolState>>,
    supervisor: Option<JoinHandle<()>>,
}

pub(super) type StateRef = Arc<RwLock<ThreadPoolState>>;

impl ThreadPool {
    /// Create a new thread pool with a given configuration.
    pub fn new(config: ThreadPoolConfig) -> Self {
        let (task_queue, task_queue_rx) = unbounded::<Task>();
        let state = Arc::new(RwLock::new(ThreadPoolState::Running));
        let worker_manager = Arc::new(WorkerManager::new(config.clone(), state.clone()));
        let supervisor = worker_manager.start(task_queue_rx.clone());

        Self {
            task_queue,
            state,
            supervisor: Some(supervisor),
        }
    }

    pub fn spawn<T: Send + 'static>(
        &self,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        let (task, task_handle) = Self::build_task(description.to_string(), task);

        trace!("Spawn {:?}", task);
        self.task_queue.send(task).unwrap_or(());

        task_handle
    }

    fn build_task<T: Send + 'static>(
        description: String,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> (Task, TaskHandle<T>) {
        static TASK_ID_COUNTER: LazyLock<RwLock<usize>> = LazyLock::new(|| RwLock::new(0));

        let (tx, rx) = crossbeam_channel::bounded(1);
        let (tx_start, rx_start) = crossbeam_channel::bounded(1);

        let copy_description = description.clone();
        let box_task = Box::new(move || {
            tx_start.send(()).unwrap_or(());
            let result = task();
            tx.send(result).unwrap_or(());
        });

        let id = {
            let mut counter = TASK_ID_COUNTER.write().unwrap();
            *counter += 1;
            *counter
        };

        let task = Task {
            description: copy_description,
            func: box_task,
            id,
        };

        (task, TaskHandle::new(rx, rx_start, id))
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
