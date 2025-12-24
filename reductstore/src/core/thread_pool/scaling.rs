// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{Task, ThreadPoolState};
use log::{debug, error, trace};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::core::sync::RwLock;
use crossbeam_channel::{Receiver, Sender};

#[derive(Clone)]
pub(crate) struct ScalingConfig {
    pub(crate) min_threads: usize,
    pub(crate) scale_down_cooldown: Duration,
    pub(crate) worker_task_timeout: Duration,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_threads: 1,
            scale_down_cooldown: Duration::from_millis(100),
            worker_task_timeout: Duration::from_millis(5),
        }
    }
}

/// Manages worker lifecycle with basic auto-scaling.
pub(crate) struct WorkerManager {
    workers: Arc<RwLock<HashMap<u64, JoinHandle<()>>>>,
    config: ScalingConfig,
    state: Arc<RwLock<ThreadPoolState>>,
    stop_tx: Sender<()>,
    stop_rx: Receiver<()>,
    last_scale_down: Arc<RwLock<Instant>>,
}

pub(crate) struct WorkerManagerBuilder {
    config: ScalingConfig,
    state: Option<Arc<RwLock<ThreadPoolState>>>,
}

impl Default for WorkerManagerBuilder {
    fn default() -> Self {
        Self {
            config: ScalingConfig::default(),
            state: None,
        }
    }
}

impl WorkerManager {
    pub(crate) fn builder() -> WorkerManagerBuilder {
        WorkerManagerBuilder::default()
    }

    pub(crate) fn spawn_initial(
        &self,
        task_rx: &Receiver<Task>,
        queued_task_counter: Arc<AtomicUsize>,
    ) {
        for _ in 0..self.config.min_threads {
            self.spawn_worker(task_rx, queued_task_counter.clone());
        }
    }

    pub(crate) fn spawn_worker(
        &self,
        task_rx: &Receiver<Task>,
        queued_task_counter: Arc<AtomicUsize>,
    ) {
        static WORKER_ID: AtomicUsize = AtomicUsize::new(0);
        let id = WORKER_ID.fetch_add(1, Ordering::Relaxed) as u64;

        let task_rx = task_rx.clone();
        let state = self.state.clone();
        let stop_rx = self.stop_rx.clone();

        let worker_task_timeout = self.config.worker_task_timeout;
        let handle = std::thread::spawn(move || loop {
            match state.read().unwrap().deref() {
                ThreadPoolState::Running => {}
                ThreadPoolState::Stopped => break,
            }

            crossbeam_channel::select! {
                recv(stop_rx) -> _ => {
                    debug!("Worker {} received stop signal", id);
                    break;
                }
                recv(task_rx) -> msg => {
                    match msg {
                        Ok(Task { description, func }) => {
                            let start = Instant::now();
                            queued_task_counter.fetch_sub(1, Ordering::SeqCst);
                            func();
                            trace!("Executed Task({}) in {:?}", description, start.elapsed());
                        }
                        Err(_) => break,
                    }
                }
                recv(crossbeam_channel::after(worker_task_timeout)) -> _ => {}
            }
        });

        self.workers.write().unwrap().insert(id, handle);
    }

    pub(crate) fn scale_down(&self) {
        self.cleanup_finished_workers();

        let worker_count = self.worker_count();
        if worker_count <= self.config.min_threads {
            return;
        }

        if Instant::now().duration_since(*self.last_scale_down.read().unwrap())
            < self.config.scale_down_cooldown
        {
            return;
        }

        if self.stop_tx.try_send(()).is_ok() {
            *self.last_scale_down.write().unwrap() = Instant::now();
            self.cleanup_finished_workers();
        }
    }

    fn cleanup_finished_workers(&self) {
        let mut workers = self.workers.write().unwrap();
        let finished: Vec<u64> = workers
            .iter()
            .filter_map(|(id, handle)| handle.is_finished().then_some(*id))
            .collect();

        for id in finished {
            if let Some(handle) = workers.remove(&id) {
                if let Err(err) = handle.join() {
                    error!("Failed to join worker {id}: {:?}", err);
                }
            }
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    pub(crate) fn join_all(&self) {
        for (id, handle) in self.workers.write().unwrap().drain() {
            trace!("Joining worker thread {}", id);
            if let Err(err) = handle.join() {
                error!("Failed to join worker {id}: {:?}", err);
            }
        }
    }

    pub(crate) fn start_supervisor(
        self: Arc<Self>,
        task_queue_rx: Receiver<Task>,
        queued_task_counter: Arc<AtomicUsize>,
        pool_size: usize,
    ) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let mut idle_since = Instant::now();
            let mut busy_since = Instant::now();
            let worker_task_timeout = self.config.worker_task_timeout;
            let timeout = worker_task_timeout * 50;

            loop {
                if self.state.read().unwrap().deref() == &ThreadPoolState::Stopped {
                    self.join_all();
                    break;
                }

                let current_threads = self.worker_count();

                let task_count = queued_task_counter.load(Ordering::SeqCst);
                if task_count == 0 {
                    busy_since = Instant::now();
                } else {
                    idle_since = Instant::now();
                }

                if busy_since.elapsed() > timeout {
                    self.spawn_worker(&task_queue_rx, queued_task_counter.clone());
                    debug!("Scaling up thread pool to {} threads", self.worker_count());
                    busy_since = Instant::now();
                }

                if idle_since.elapsed() > timeout && current_threads > pool_size {
                    self.scale_down();
                    debug!(
                        "Scaling down thread pool to {} threads",
                        self.worker_count()
                    );
                    idle_since = Instant::now();
                }

                std::thread::sleep(worker_task_timeout);
            }
        })
    }
}

impl WorkerManagerBuilder {
    pub(crate) fn state(mut self, state: Arc<RwLock<ThreadPoolState>>) -> Self {
        self.state = Some(state);
        self
    }

    pub(crate) fn min_threads(mut self, min_threads: usize) -> Self {
        self.config.min_threads = min_threads;
        self
    }

    pub(crate) fn scale_down_cooldown(mut self, cooldown: Duration) -> Self {
        self.config.scale_down_cooldown = cooldown;
        self
    }

    pub(crate) fn worker_task_timeout(mut self, timeout: Duration) -> Self {
        self.config.worker_task_timeout = timeout;
        self
    }

    pub(crate) fn build(self) -> WorkerManager {
        let (stop_tx, stop_rx) = crossbeam_channel::unbounded();
        WorkerManager {
            workers: Arc::new(RwLock::new(HashMap::new())),
            config: self.config,
            state: self
                .state
                .unwrap_or_else(|| Arc::new(RwLock::new(ThreadPoolState::Running))),
            stop_tx,
            stop_rx,
            last_scale_down: Arc::new(RwLock::new(Instant::now())),
        }
    }
}
