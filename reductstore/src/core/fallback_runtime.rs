// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Minimal executor wrapper that prefers the current Tokio runtime handle.
/// If no runtime is available, it falls back to an owned current-thread runtime.
#[derive(Clone)]
pub(crate) enum FallbackRuntime {
    Handle(Handle),
    Owned(Arc<tokio::runtime::Runtime>),
}

impl FallbackRuntime {
    pub fn new() -> Self {
        if let Ok(handle) = Handle::try_current() {
            FallbackRuntime::Handle(handle)
        } else {
            FallbackRuntime::Owned(Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            ))
        }
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            FallbackRuntime::Handle(handle) => {
                // Avoid block_on inside runtime; offload to a blocking section and wait via channel.
                tokio::task::block_in_place(|| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    handle.spawn(async move {
                        let _ = tx.send(future.await);
                    });
                    rx.recv().expect("FallbackRuntime task panicked")
                })
            }
            FallbackRuntime::Owned(rt) => rt.block_on(future),
        }
    }
}

impl Drop for FallbackRuntime {
    fn drop(&mut self) {
        if let FallbackRuntime::Owned(rt) = self {
            // Dropping a runtime inside an async context can panic; hand it off to a thread
            // when this is the last reference and we're currently on a Tokio runtime.
            if Arc::strong_count(rt) == 1 && Handle::try_current().is_ok() {
                let rt = rt.clone();
                std::thread::spawn(move || drop(rt));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FallbackRuntime;
    use rstest::rstest;
    #[rstest]
    fn test_fallback_runtime_outside_tokio() {
        let rt = FallbackRuntime::new();
        let result = rt.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fallback_runtime_inside_tokio() {
        let rt = FallbackRuntime::new();
        let result = rt.block_on(async { 42 });
        assert_eq!(result, 42);
    }
}
