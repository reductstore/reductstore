// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::thread_pool::{StateRef, Task, ThreadPoolState};
use log::{debug, error, trace};
use std::collections::HashMap;
use std::ops::Deref;
use std::panic::{self, AssertUnwindSafe};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use crate::cfg::thread_pool::ThreadPoolConfig;
use crate::core::sync::RwLock;
use crossbeam_channel::{Receiver, Sender};

/// Manages worker lifecycle with basic auto-scaling.
pub(crate) struct WorkerManager {
    workers: Arc<RwLock<HashMap<u64, JoinHandle<()>>>>,
    config: ThreadPoolConfig,
    state: Arc<RwLock<ThreadPoolState>>,
    stop_tx: Sender<()>,
    stop_rx: Receiver<()>,
    idle_workers: Arc<AtomicUsize>,
    last_scale_down: Arc<RwLock<Instant>>,
}

impl WorkerManager {
    pub fn new(config: ThreadPoolConfig, state: StateRef) -> Self {
        let (stop_tx, stop_rx) = crossbeam_channel::unbounded();
        WorkerManager {
            workers: Arc::new(RwLock::new(HashMap::new())),
            config,
            state,
            stop_tx,
            stop_rx,
            idle_workers: Arc::new(AtomicUsize::new(0)),
            last_scale_down: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn start(self: Arc<Self>, task_queue_rx: Receiver<Task>) -> JoinHandle<()> {
        self.spawn_initial(&task_queue_rx);

        std::thread::spawn(move || {
            let mut idle_since = Instant::now();
            let worker_task_timeout = self.config.worker_task_timeout * 5;
            loop {
                if self.state.read().unwrap().deref() == &ThreadPoolState::Stopped {
                    self.join_all();
                    break;
                }

                let idlers = self.idle_workers.load(Ordering::SeqCst);

                // Scale up immediately if no idle workers (prevents deadlock from nested tasks)
                if idlers == 0 {
                    for _ in 0..self.config.scale_step {
                        self.spawn_worker(&task_queue_rx);
                    }
                    idle_since = Instant::now();
                } else if idlers > self.config.min_idle_threads {
                    // Scale down if we have more idle workers than needed for a while
                    if idle_since.elapsed() > worker_task_timeout {
                        self.scale_down();
                        idle_since = Instant::now();
                    }
                } else {
                    idle_since = Instant::now();
                }

                std::thread::sleep(self.config.worker_task_timeout);
            }
        })
    }

    fn spawn_initial(&self, task_rx: &Receiver<Task>) {
        for _ in 0..self.config.min_idle_threads {
            self.spawn_worker(task_rx);
        }
    }

    fn spawn_worker(&self, task_rx: &Receiver<Task>) {
        static WORKER_ID: AtomicUsize = AtomicUsize::new(0);
        let id = WORKER_ID.fetch_add(1, Ordering::Relaxed) as u64;

        let task_rx = task_rx.clone();
        let state = self.state.clone();
        let stop_rx = self.stop_rx.clone();
        let idle_counter = self.idle_workers.clone();

        let worker_task_timeout = self.config.worker_task_timeout;

        let handle = std::thread::spawn(move || loop {
            match state.read().unwrap().deref() {
                ThreadPoolState::Running => {}
                ThreadPoolState::Stopped => break,
            }

            idle_counter.fetch_add(1, Ordering::SeqCst);

            crossbeam_channel::select! {
                recv(stop_rx) -> _ => {
                    debug!("Worker {} received stop signal", id);
                    idle_counter.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
                recv(task_rx) -> msg => {
                    idle_counter.fetch_sub(1, Ordering::SeqCst);
                    match msg {
                        Ok(task) => {
                            let print = format!("{:?}", task);
                            let start = Instant::now();

                            let result =
                                panic::catch_unwind(AssertUnwindSafe(|| (task.func)()));
                            if let Err(err) = result {
                                error!("Worker {id} panicked running {print:?}: {:?}", err);
                                #[cfg(not(test))]
                                process::abort();
                            }
                            trace!("Executed {} at worker={} in {:?}", print, id, start.elapsed());
                        }
                        Err(_) => break,
                    }
                }
                recv(crossbeam_channel::after(worker_task_timeout)) -> _ => {
                    idle_counter.fetch_sub(1, Ordering::SeqCst);

                }
            }
        });
        self.workers.write().unwrap().insert(id, handle);
        debug!(
            "Scaling up thread pool to {} ({} idle) workers",
            self.worker_count(),
            self.idle_workers.load(Ordering::SeqCst)
        );
    }

    fn scale_down(&self) {
        self.cleanup_finished_workers();

        if Instant::now().duration_since(*self.last_scale_down.read().unwrap())
            < self.config.scale_down_cooldown
        {
            return;
        }

        if self.stop_tx.try_send(()).is_ok() {
            *self.last_scale_down.write().unwrap() = Instant::now();
            self.cleanup_finished_workers();
        }

        debug!(
            "Scaling down thread pool to {} ({} idle) workers",
            self.worker_count(),
            self.idle_workers.load(Ordering::SeqCst)
        );
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

    fn worker_count(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    fn join_all(&self) {
        for (id, handle) in self.workers.write().unwrap().drain() {
            trace!("Joining worker thread {}", id);
            if let Err(err) = handle.join() {
                error!("Failed to join worker {id}: {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    const WAIT_TIMEOUT: Duration = Duration::from_millis(200);

    fn make_manager(min_threads: usize) -> Arc<WorkerManager> {
        Arc::new(WorkerManager::new(
            ThreadPoolConfig {
                min_idle_threads: min_threads,
                worker_task_timeout: Duration::from_millis(10),
                scale_down_cooldown: Duration::from_millis(5),
                scale_step: 1,
            },
            Arc::new(RwLock::new(ThreadPoolState::Running)),
        ))
    }

    fn make_task(action: impl FnOnce() + Send + 'static) -> Task {
        Task {
            description: "test-task".into(),
            func: Box::new(action),
            id: 0,
        }
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        predicate()
    }

    fn wait_for_workers(manager: &WorkerManager, expected: usize) -> bool {
        wait_until(|| {
            manager.cleanup_finished_workers();
            manager.worker_count() == expected
        })
    }

    fn shutdown(manager: &Arc<WorkerManager>) {
        *manager.state.write().unwrap() = ThreadPoolState::Stopped;
        manager.join_all();
    }

    #[test]
    fn builder_applies_overrides() {
        let state = Arc::new(RwLock::new(ThreadPoolState::Stopped));
        let cooldown = Duration::from_millis(7);
        let timeout = Duration::from_millis(2);
        let manager = WorkerManager::new(
            ThreadPoolConfig {
                min_idle_threads: 3,
                scale_down_cooldown: cooldown,
                worker_task_timeout: timeout,
                scale_step: 1,
            },
            state.clone(),
        );

        assert_eq!(manager.config.min_idle_threads, 3);
        assert_eq!(manager.config.scale_down_cooldown, cooldown);
        assert_eq!(manager.config.worker_task_timeout, timeout);
        assert!(Arc::ptr_eq(&manager.state, &state));
    }

    #[test]
    fn spawn_initial_launches_minimum_workers() {
        let manager = make_manager(2);
        let (tx, rx) = unbounded::<Task>();
        manager.spawn_initial(&rx);
        assert!(wait_for_workers(&manager, 2));
        drop(tx);
        shutdown(&manager);
    }

    #[test]
    fn spawn_worker_executes_task_and_updates_queue_counter() {
        let manager = make_manager(1);
        let (tx, rx) = unbounded::<Task>();
        manager.spawn_worker(&rx);
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        tx.send(make_task(move || {
            flag_clone.store(true, Ordering::SeqCst);
        }))
        .unwrap();
        assert!(wait_until(|| flag.load(Ordering::SeqCst)));
        drop(tx);
        shutdown(&manager);
    }

    #[test]
    fn start_supervisor_stops_when_state_changes() {
        let manager = make_manager(1);
        let (tx, rx) = unbounded::<Task>();
        let supervisor = Arc::clone(&manager).start(rx);
        assert!(wait_for_workers(&manager, 1));

        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();
        tx.send(make_task(move || {
            flag_clone.store(true, Ordering::SeqCst);
        }))
        .unwrap();
        assert!(wait_until(|| flag.load(Ordering::SeqCst)));

        drop(tx);
        *manager.state.write().unwrap() = ThreadPoolState::Stopped;
        supervisor.join().unwrap();
        manager.join_all();
    }

    #[test]
    fn scale_down_stops_extra_workers() {
        let manager = make_manager(1);
        let (tx, rx) = unbounded::<Task>();
        manager.spawn_initial(&rx);
        manager.spawn_worker(&rx);
        assert!(wait_for_workers(&manager, 2));

        *manager.last_scale_down.write().unwrap() = Instant::now() - Duration::from_millis(10);
        manager
            .idle_workers
            .store(manager.worker_count(), Ordering::SeqCst);
        manager.scale_down();
        assert!(wait_for_workers(&manager, 1));
        drop(tx);
        shutdown(&manager);
    }

    #[test]
    fn scale_down_respects_cooldown_window() {
        let manager = make_manager(1);
        let (tx, rx) = unbounded::<Task>();
        manager.spawn_initial(&rx);
        manager.spawn_worker(&rx);
        assert!(wait_for_workers(&manager, 2));

        *manager.last_scale_down.write().unwrap() = Instant::now();
        manager
            .idle_workers
            .store(manager.worker_count(), Ordering::SeqCst);
        manager.scale_down();
        thread::sleep(Duration::from_millis(5));
        manager.cleanup_finished_workers();
        assert_eq!(manager.worker_count(), 2);
        drop(tx);
        shutdown(&manager);
    }

    #[test]
    fn cleanup_finished_workers_removes_completed_handles() {
        let manager = make_manager(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let handle = std::thread::spawn(move || {
            done_tx.send(()).ok();
        });
        done_rx.recv().unwrap();
        manager.workers.write().unwrap().insert(42, handle);
        manager.cleanup_finished_workers();
        assert_eq!(manager.worker_count(), 0);
    }

    #[test]
    fn worker_count_matches_internal_registry() {
        let manager = make_manager(1);
        assert_eq!(manager.worker_count(), 0);
        let handle = std::thread::spawn(|| {});
        manager.workers.write().unwrap().insert(7, handle);
        assert_eq!(manager.worker_count(), 1);
        manager.join_all();
    }

    #[test]
    fn join_all_drains_workers() {
        let manager = make_manager(1);
        for id in 0..2 {
            let handle = std::thread::spawn(|| {});
            manager.workers.write().unwrap().insert(id, handle);
        }
        manager.join_all();
        assert_eq!(manager.worker_count(), 0);
    }
}
