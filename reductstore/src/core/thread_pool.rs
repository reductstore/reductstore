// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod task_group;
mod task_handle;

use crossbeam_channel::internal::SelectHandle;
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures_util::{FutureExt, StreamExt};
use log::trace;
use std::cmp::max;
use std::fmt::Display;
use std::num::NonZero;
use std::ops::Deref;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{available_parallelism, sleep, JoinHandle};
use std::time::Duration;
pub(crate) use task_group::TaskGroup;
pub(crate) use task_handle::TaskHandle;

/// Spawn a unique task for a task group.
///
/// The task will wait until the task group  and all parent groups are unlocked and no other shared
/// tasks are running for same group including parent groups.
pub(crate) fn unique<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.unique(group_path, description, task)
}

/// Spawn a unique task for a task group.
///
/// The task will wait until the task group is unlocked and no unique tasks are running for the same
/// group. It will not wait for parent groups to be unlocked.
///
/// Use this method for tasks  which is spawned from a unique task
pub(crate) fn unique_child<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.unique_child(group_path, description, task)
}

/// Spawn a shared task for a task group.
///
/// The task will wait until the task group  and all parent groups, but it will not wait for other shared tasks
/// to finish.
pub(crate) fn shared<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.shared(group_path, description, task)
}

/// Spawn a shared task for a task group.
///
/// The task will wait until the task group is unlocked and no unique tasks are running for the same
/// group. It will not wait for parent groups to be unlocked.
///
/// Use this method for tasks  which is spawned from a shared task:
pub(crate) fn shared_child<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.shared_child(group_path, description, task)
}

pub(crate) fn try_unique<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> Option<TaskHandle<T>>
where
    T: Send + 'static,
{
    let path = group_path.split('/').collect::<Vec<&str>>();
    if THREAD_POOL.task_group.lock().unwrap().is_ready(&path, true) {
        Some(unique(group_path, description, task))
    } else {
        None
    }
}

/// Spawn a shared task as a isolated task outside of thread pool.
///
/// Use it for loops or other blocking operations.
///
/// The task will wait until the task group  and all parent groups, but it will not wait for other shared tasks
/// to finish.
pub(crate) fn shared_isolated<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.shared_isolated(group_path, description, task)
}

/// Spawn a shared task as a isolated task outside of thread pool.
///
/// Use it for loops or other blocking operations.
///
/// The task will wait until the task group is unlocked and no unique tasks are running for the same
/// group. It will not wait for parent groups to be unlocked.
pub(crate) fn shared_child_isolated<T>(
    group_path: &str,
    description: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    THREAD_POOL.shared_child_isolated(group_path, description, task)
}

/// Find a task group by path.
pub(crate) fn find_task_group(group_path: &str) -> Option<TaskGroup> {
    let group = THREAD_POOL.task_group.lock().unwrap();
    group
        .find(&group_path.to_string().split("/").collect())
        .cloned()
}

type BoxedFunc = Box<dyn FnOnce() + Send>;

#[derive(PartialEq, Debug)]
enum TaskMode {
    Unique,
    Shared,
}

struct Task {
    task_group: String,
    func: BoxedFunc,
    mode: TaskMode,
    child: bool,    // Child task, check only current group and children
    isolated: bool, // Isolated task, run in separate thread
}

impl Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task({}) mode={:?} child={} isolated={}",
            self.task_group, self.mode, self.child, self.isolated
        )
    }
}

#[derive(PartialEq)]
enum ThreadPoolState {
    Running,
    Stopped,
}

static THREAD_POOL_TICK: Duration = Duration::from_micros(5);

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let thread_pool_size = max(
        available_parallelism()
            .unwrap_or(NonZero::new(2).unwrap())
            .get()
            / 2,
        2,
    );
    ThreadPool::new(thread_pool_size)
});

struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    task_queue: Sender<Task>,
    state: Arc<Mutex<ThreadPoolState>>,
    task_group: Arc<Mutex<TaskGroup>>,
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
            let task_tx = task_queue.clone();
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

                let task = task.unwrap();
                let group_path = task.task_group.split('/').collect();
                let unique = task.mode == TaskMode::Unique;

                let ready = {
                    let lock = task_group.lock().unwrap();
                    (task.child && lock.is_ready_current(&group_path, unique))
                        || (!task.child && lock.is_ready(&group_path, unique))
                };

                if !ready {
                    trace!("Group '{}' is not ready for {}", task.task_group, task);
                    sleep(THREAD_POOL_TICK);
                    task_tx.send(task).unwrap_or(());
                    continue;
                }

                {
                    let mut lock = task_group.lock().unwrap();
                    lock.lock(&group_path, unique);
                }

                if !task.isolated {
                    (task.func)();
                    let mut lock = task_group.lock().unwrap();
                    lock.unlock(&group_path, unique);
                } else {
                    let task_group = task_group.clone();
                    let group_path = group_path
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    std::thread::spawn(move || {
                        (task.func)();
                        let mut lock = task_group.lock().unwrap();
                        lock.unlock(
                            &group_path.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                            unique,
                        );
                    });
                }
            });

            threads.push(thread);
        }
        Self {
            threads,
            task_queue,
            state,
            task_group: task_group_global,
        }
    }

    pub fn unique<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        self.check_current_thread();

        trace!("Spawn unique task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Unique;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    pub fn unique_child<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn unique child task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Unique;
        task.child = true;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    pub fn shared<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        self.check_current_thread();

        trace!("Spawn shared task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Shared;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    pub fn shared_child<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn shared child task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Shared;
        task.child = true;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    pub fn shared_isolated<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn isolated task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Shared;
        task.isolated = true;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    pub fn shared_child_isolated<T: Send + 'static>(
        &self,
        group: &str,
        description: &str,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> TaskHandle<T> {
        trace!("Spawn isolated child task '{}: {}", group, description);
        let group = group.to_string();
        let description = description.to_string();

        let (mut task, rx) = Self::build_task(group, description, task);
        task.mode = TaskMode::Shared;
        task.child = true;
        task.isolated = true;

        self.task_queue.send(task).unwrap_or(());
        TaskHandle::new(rx)
    }

    fn build_task<T: Send + 'static>(
        group: String,
        description: String,
        task: impl FnOnce() -> T + Send + 'static,
    ) -> (Task, Receiver<T>) {
        let (tx, rx) = crossbeam_channel::bounded(1);

        let copy_group = group.clone();
        let box_task = Box::new(move || {
            trace!("Task '{}' started: {}", group, description);

            let result = task();
            tx.send(result).unwrap_or(());

            trace!("Task '{}' completed: {}", group, description);
        });

        let task = Task {
            task_group: copy_group,
            func: box_task,
            mode: TaskMode::Shared,
            child: false,
            isolated: false,
        };

        (task, rx)
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
