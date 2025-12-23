// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod task_handle;

use crate::core::sync::RwLock;
use crossbeam_channel::{unbounded, Sender};
use log::trace;
use std::cmp::max;
use std::fmt::Display;
use std::num::NonZero;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use std::thread::{available_parallelism, JoinHandle};
use std::time::Duration;
pub(crate) use task_handle::TaskHandle;

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

#[derive(PartialEq)]
enum ThreadPoolState {
    Running,
    Stopped,
}

static THREAD_POOL_TASK_TIMEOUT: Duration = Duration::from_millis(1);

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let thread_pool_size = max(
        available_parallelism()
            .unwrap_or(NonZero::new(1).unwrap())
            .get()
            / 2,
        4,
    );
    ThreadPool::new(thread_pool_size)
});

/// A thread pool for executing tasks.
struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    task_queue: Sender<Task>,
    state: Arc<RwLock<ThreadPoolState>>,
}

impl ThreadPool {
    /// Create a new thread pool with a given size.
    pub fn new(size: usize) -> Self {
        let mut threads = Vec::with_capacity(size);
        let (task_queue, task_queue_rc) = unbounded::<Task>();
        let state = Arc::new(RwLock::new(ThreadPoolState::Running));

        for _ in 0..size {
            let task_rx = task_queue_rc.clone();
            let pool_state = state.clone();

            let thread = std::thread::spawn(move || loop {
                match pool_state.read().unwrap().deref() {
                    ThreadPoolState::Running => {}
                    ThreadPoolState::Stopped => {
                        break;
                    }
                }
                let task = task_rx.recv_timeout(THREAD_POOL_TASK_TIMEOUT);
                if task.is_err() {
                    continue;
                }

                let task = task.unwrap();
                let Task { description, func } = task;
                let start = std::time::Instant::now();
                func();
                trace!("Executed Task({}) in {:?}", description, start.elapsed());
            });

            threads.push(thread);
        }
        Self {
            threads,
            task_queue,
            state,
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

        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
