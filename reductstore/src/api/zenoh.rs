use crate::api::components::StateKeeper;
use crate::cfg::zenoh::ZenohApiConfig;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(crate) mod attachments;
pub(crate) mod queryable;
mod session;
pub(crate) mod subscriber;

pub struct ZenohRuntimeHandle {
    task: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl ZenohRuntimeHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

pub fn spawn_runtime(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
) -> Option<ZenohRuntimeHandle> {
    if !config.enabled {
        return None;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        if let Err(err) = session::run_session(config, state_keeper, shutdown_rx).await {
            log::error!("Zenoh API runtime terminated: {}", err);
        }
    });

    Some(ZenohRuntimeHandle { task, shutdown_tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::keeper;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_spawn_runtime_disabled(#[future] keeper: Arc<StateKeeper>) {
        let keeper = keeper.await;
        let config = ZenohApiConfig::default(); // enabled=false by default
        let handle = spawn_runtime(config, keeper);
        assert!(handle.is_none());
    }
}
