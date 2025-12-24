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
use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
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
}

impl Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task({})", self.description,)
    }
}

static THREAD_POOL_TASK_TIMEOUT: Duration = Duration::from_millis(1);

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let thread_pool_size = max(
        available_parallelism()
            .unwrap_or(NonZero::new(1).unwrap())
            .get()
            / 2,
        2,
    );
    ThreadPool::new(thread_pool_size)
});

/// A thread pool for executing tasks.
struct ThreadPool {
    task_queue: Sender<Task>,
    queued_tasks: Arc<AtomicUsize>,
    state: Arc<RwLock<ThreadPoolState>>,
    supervisor: Option<JoinHandle<()>>,
}

impl ThreadPool {
    /// Create a new thread pool with a given size.
    pub fn new(size: usize) -> Self {
        let (task_queue, task_queue_rx) = unbounded::<Task>();
        let state = Arc::new(RwLock::new(ThreadPoolState::Running));
        let queued_tasks = Arc::new(AtomicUsize::new(0));
        let worker_manager = Arc::new(
            WorkerManager::builder()
                .state(Arc::clone(&state))
                .min_threads(size)
                .scale_down_cooldown(Duration::from_secs(1))
                .worker_task_timeout(Duration::from_millis(5))
                .build(),
        );

        worker_manager.spawn_initial(&task_queue_rx, queued_tasks.clone());

        let state_clone = state.clone();

        let supervisor = worker_manager.clone().start_supervisor(
            task_queue_rx.clone(),
            queued_tasks.clone(),
            size,
        );

        Self {
            task_queue,
            queued_tasks,
            state: state_clone,
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
