// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crossbeam_channel::internal::SelectHandle;
use crossbeam_channel::Receiver;
use reduct_base::error::ReductError;
use std::future::Future;

pub(crate) struct TaskHandle<T> {
    rx: Receiver<T>,
}

impl<T> TaskHandle<T> {
    pub(super) fn new(rx: Receiver<T>) -> Self {
        Self { rx }
    }

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
