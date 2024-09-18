// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crossbeam_channel::internal::SelectHandle;
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures_util::FutureExt;
use log::{error, trace};
use reduct_base::error::ReductError;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZero;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::thread::{available_parallelism, sleep, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot::Receiver as AsyncReceiver;

type Func = Box<dyn FnOnce() + Send>;

enum Task {
    Unique(String, Func),
    Shared(String, Func),
    ChildUnique(String, Func),
    ChildShared(String, Func),
}

struct TaskGroup {
    name: String,
    unique_lock: bool,
    use_count: i32,
    children: HashMap<String, TaskGroup>,
}

pub(crate) struct TaskHandle<T> {
    rx: Receiver<T>,
}

impl<T> TaskHandle<T> {
    pub fn wait(self) -> T {
        self.rx.recv().unwrap()
    }

    pub fn is_finished(&self) -> bool {
        self.rx.is_ready()
    }
}

impl<T> From<T> for TaskHandle<T> {
    fn from(data: T) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.send(data).unwrap();
        Self { rx }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.rx.try_recv() {
            Ok(result) => std::task::Poll::Ready(result),
            Err(_) => {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}

impl<T> From<ReductError> for TaskHandle<Result<T, ReductError>> {
    fn from(err: ReductError) -> Self {
        TaskHandle::from(Err(err))
    }
}

impl TaskGroup {
    fn new(name: String) -> Self {
        Self {
            name,
            unique_lock: false,
            use_count: 0,
            children: HashMap::new(),
        }
    }

    /// Lock a task group for a unique execution.
    fn lock(&mut self, path: &Vec<&str>, unique: bool) {
        let group = self.find_or_create(path);
        group.unique_lock = unique;
        group.use_count += 1;
    }

    /// Check if a task group is ready for an execution
    ///
    /// The method checks unique_lock and use_count for parent groups.
    fn is_ready(&self, path: &Vec<&str>, unique: bool) -> bool {
        for (i, name) in path.iter().enumerate() {
            if let Some(group) = self.find(&path[..i].to_vec()) {
                if group.unique_lock || (group.use_count > 0 && unique) {
                    trace!(
                        "Task group {} is locked: unique_lock: {}, use_count: {}",
                        path[..i].join("/"),
                        group.unique_lock,
                        group.use_count
                    );
                    return false;
                }
            }
        }
        true
    }

    /// Check only the current group if it is ready for an execution
    fn is_ready_current(&self, path: &Vec<&str>, unique: bool) -> bool {
        let group = self.find(path);
        if let Some(group) = group {
            if group.unique_lock || (group.use_count > 0 && unique) {
                trace!(
                    "Task child group {} is locked: unique_lock: {}, use_count: {}",
                    path.join("/"),
                    group.unique_lock,
                    group.use_count
                );
                return false;
            }
        }
        true
    }

    fn unlock(&mut self, path: &Vec<&str>, unique: bool) {
        let mut group = self.find_mut(path).unwrap();
        if unique {
            group.unique_lock = false;
        }

        group.use_count -= 1;
        if group.use_count < 0 {
            panic!("Task group use count is negative");
        }

        // remove empty groups
        group.children.retain(|_, group| {
            if group.use_count == 0 && !group.unique_lock && group.children.is_empty() {
                return false;
            }
            true
        });
    }

    fn find_or_create(&mut self, path: &Vec<&str>) -> &mut TaskGroup {
        if path.is_empty() {
            return self;
        }

        let name = path[0];
        let path = path[1..].to_vec();

        match self.children.entry(name.to_string()) {
            Entry::Occupied(entry) => entry.into_mut().find_or_create(&path),
            Entry::Vacant(entry) => entry
                .insert(TaskGroup::new(name.to_string()))
                .find_or_create(&path),
        }
    }

    fn find(&self, path: &Vec<&str>) -> Option<&TaskGroup> {
        if path.is_empty() {
            return Some(self);
        }

        let name = path[0];
        let path = path[1..].to_vec();

        self.children.get(name).and_then(|group| group.find(&path))
    }

    fn find_mut(&mut self, path: &Vec<&str>) -> Option<&mut TaskGroup> {
        if path.is_empty() {
            return Some(self);
        }

        let name = path[0];
        let path = path[1..].to_vec();

        self.children
            .get_mut(name)
            .and_then(|group| group.find_mut(&path))
    }
}

#[derive(PartialEq)]
enum ThreadPoolState {
    Running,
    Stopped,
}

static THREAD_POOL_TICK: Duration = Duration::from_micros(10);

pub(crate) static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPool::new(
        available_parallelism()
            .unwrap_or(NonZero::new(4).unwrap())
            .get(),
    )
});
pub(crate) struct ThreadPool {
    threads: Vec<JoinHandle<()>>,
    task_queue: Sender<Task>,
    state: Arc<Mutex<ThreadPoolState>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let mut threads = Vec::with_capacity(size);
        let (task_queue, mut task_queue_rc) = unbounded::<Task>();
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

        TaskHandle { rx }
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

        TaskHandle { rx }
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

        TaskHandle { rx }
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

        TaskHandle { rx }
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
