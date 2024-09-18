// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod task_group;
mod task_handle;

use crossbeam_channel::internal::SelectHandle;
use crossbeam_channel::{unbounded, Sender};
use futures_util::{FutureExt, StreamExt};
use log::{error, trace};
use std::num::NonZero;
use std::ops::Deref;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{available_parallelism, sleep, JoinHandle};
use std::time::Duration;

/// Spawn a unique task for a task group.
///
/// The task will wait until the task group  and all parent groups are unlocked and no other shared
/// tasks are running for same group including parent groups.
///
/// # Arguments
///
/// * `group_path` - A vector of strings that represents the path to the task group.
/// * `task` - A closure that returns the result of the task.
///
/// # Returns
///
/// A handle to the task that can be used to wait for the result.
pub fn unique<T, Str, const N: usize>(
    group_path: [Str; N],
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    Str: ToString + Sized,
    T: Send + 'static,
{
    THREAD_POOL.unique(
        &group_path
            .iter()
            .map(|el| el.to_string())
            .reduce(|a, b| a + "/" + &b)
            .unwrap(),
        task,
    )
}

/// Spawn a unique task for a task group.
///
/// The task will wait until the task group is unlocked and no unique tasks are running for the same
/// group. It will not wait for parent groups to be unlocked.
///
/// Use this method for tasks  which is spawned from a unique task:
///
/// ```rust
/// use crate::reductstore::core::thread_pool::{unique, unique_child};
///
/// unique(["group1"], || {
///     unique_child(["group1", "group2"], || {
///        // shared task
///    });
/// });
///
pub fn unique_child<T, Str, const N: usize>(
    group_path: [Str; N],
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    Str: ToString + Sized,
    T: Send + 'static,
{
    THREAD_POOL.unique_child(
        &group_path
            .iter()
            .map(|el| el.to_string())
            .reduce(|a, b| a + "/" + &b)
            .unwrap(),
        task,
    )
}

/// Spawn a shared task for a task group.
///
/// The task will wait until the task group  and all parent groups, but it will not wait for other shared tasks
/// to finish.
///
/// # Arguments
///
/// * `group_path` - A vector of strings that represents the path to the task group.
/// * `task` - A closure that returns the result of the task.
///
/// # Returns
///
/// A handle to the task that can be used to wait for the result.

pub fn shared<T, Str, const N: usize>(
    group_path: [Str; N],
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    Str: ToString + Sized,
    T: Send + 'static,
{
    THREAD_POOL.shared(
        &group_path
            .iter()
            .map(|el| el.to_string())
            .reduce(|a, b| a + "/" + &b)
            .unwrap(),
        task,
    )
}

/// Spawn a shared task for a task group.
///
/// The task will wait until the task group is unlocked and no unique tasks are running for the same
/// group. It will not wait for parent groups to be unlocked.
///
/// Use this method for tasks  which is spawned from a shared task:
///
/// ```rust
/// use crate::reductstore::core::thread_pool::{unique, shared_child};
///
/// unique(["group1"], || {
///    shared_child(["group1", "group2"], || {
///       // shared task
///     });
/// });
pub fn shared_child<T, Str, const N: usize>(
    group_path: [Str; N],
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    Str: ToString + Sized,
    T: Send + 'static,
{
    THREAD_POOL.shared_child(
        &group_path
            .iter()
            .map(|el| el.to_string())
            .reduce(|a, b| a + "/" + &b)
            .unwrap(),
        task,
    )
}

type Func = Box<dyn FnOnce() + Send>;

enum Task {
    Unique(String, Func),
    Shared(String, Func),
    ChildUnique(String, Func),
    ChildShared(String, Func),
}

use crate::core::thread_pool::task_group::TaskGroup;
pub(crate) use task_handle::TaskHandle;

#[derive(PartialEq)]
enum ThreadPoolState {
    Running,
    Stopped,
}

static THREAD_POOL_TICK: Duration = Duration::from_micros(10);

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPool::new(
        available_parallelism()
            .unwrap_or(NonZero::new(4).unwrap())
            .get(),
    )
});

struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    task_queue: Sender<Task>,
    state: Arc<Mutex<ThreadPoolState>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let mut threads = Vec::with_capacity(size);
        let (task_queue, task_queue_rc) = unbounded::<Task>();
        let task_group_global = Arc::new(Mutex::new(TaskGroup::new("".to_string())));
        let state = Arc::new(Mutex::new(ThreadPoolState::Running));

        for _ in 0..size {
            let task_group = task_group_global.clone();
            let task_rx = task_queue_rc.clone();
            let pool_state = state.clone();

            let thread = std::thread::spawn(move || loop {
                match pool_state.lock().unwrap().deref() {
                    ThreadPoolState::Running => {}
                    ThreadPoolState::Stopped => {
                        break;
                    }
                }

                let task = task_rx.try_recv();
                if task.is_err() {
                    sleep(THREAD_POOL_TICK);
                    continue;
                }

                let (name, func, unique, child) = match task {
                    Ok(Task::Unique(name, func)) => (name, func, true, false),
                    Ok(Task::Shared(name, func)) => (name, func, false, false),
                    Ok(Task::ChildUnique(name, func)) => (name, func, true, true),
                    Ok(Task::ChildShared(name, func)) => (name, func, false, true),
                    Err(err) => {
                        error!("Thread pool receive error: {}", err);
                        break;
                    }
                };

                let group_path = name.split('/').collect();

                loop {
                    let ready = {
                        let lock = task_group.lock().unwrap();
                        (child && lock.is_ready_current(&group_path, unique))
                            || (!child && lock.is_ready(&group_path, unique))
                    };

                    if !ready {
                        sleep(THREAD_POOL_TICK);
                        continue;
                    }
                    break;
                }

                {
                    let mut lock = task_group.lock().unwrap();
                    lock.lock(&group_path, unique);
                }

                func();

                {
                    let mut lock = task_group.lock().unwrap();
                    lock.unlock(&group_path, unique);
                }
            });

            threads.push(thread);
        }
        Self {
            threads,
            task_queue,
            state,
        }
    }

    pub fn unique<T: Send + 'static>(
        &self,
        group: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        self.check_current_thread();

        trace!("Spawn unique task: {}", group);

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.task_queue
            .send(Task::Unique(
                group.to_string(),
                Box::new(move || {
                    let result = task();
                    tx.send(result).unwrap_or(());
                }),
            ))
            .unwrap();

        TaskHandle::new(rx)
    }

    pub fn unique_child<T: Send + 'static>(
        &self,
        group: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn unique child task: {}", group);

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.task_queue
            .send(Task::ChildUnique(
                group.to_string(),
                Box::new(move || {
                    let result = task();
                    tx.send(result).unwrap_or(());
                }),
            ))
            .unwrap();

        TaskHandle::new(rx)
    }

    pub fn shared<T: Send + 'static>(
        &self,
        group: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        self.check_current_thread();

        trace!("Spawn shared task: {}", group);

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.task_queue
            .send(Task::Shared(
                group.to_string(),
                Box::new(move || {
                    let result = task();
                    tx.send(result).unwrap_or(());
                }),
            ))
            .unwrap();

        TaskHandle::new(rx)
    }

    pub fn shared_child<T: Send + 'static>(
        &self,
        group: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn shared child task: {}", group);

        let (tx, rx) = crossbeam_channel::bounded(1);
        self.task_queue
            .send(Task::ChildShared(
                group.to_string(),
                Box::new(move || {
                    let result = task();
                    tx.send(result).unwrap_or(());
                }),
            ))
            .unwrap();

        TaskHandle::new(rx)
    }

    fn check_current_thread(&self) {
        // check if the method is called from the current thread
        if self
            .threads
            .iter()
            .any(|thread| thread.thread().id() == std::thread::current().id())
        {
            panic!("Thread pool must be called from the current thread");
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        while !self.task_queue.is_empty() {
            sleep(THREAD_POOL_TICK);
        }

        *self.state.lock().unwrap() = ThreadPoolState::Stopped;

        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
