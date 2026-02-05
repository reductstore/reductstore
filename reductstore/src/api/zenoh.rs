use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::StateKeeper;
use std::sync::Arc;

#[cfg(feature = "zenoh-api")]
pub(crate) mod attachments;
#[cfg(feature = "zenoh-api")]
pub(crate) mod queryable;
#[cfg(feature = "zenoh-api")]
mod session;
#[cfg(feature = "zenoh-api")]
pub(crate) mod subscriber;
#[cfg(feature = "zenoh-api")]
use tokio::sync::watch;
#[cfg(feature = "zenoh-api")]
use tokio::task::JoinHandle;

/// Handle to the Zenoh runtime, allows graceful shutdown.
#[cfg(feature = "zenoh-api")]
pub struct ZenohRuntimeHandle {
    task: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

#[cfg(feature = "zenoh-api")]
impl ZenohRuntimeHandle {
    /// Signal the Zenoh runtime to shut down and wait for it to complete.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

#[cfg(not(feature = "zenoh-api"))]
pub struct ZenohRuntimeHandle;

#[cfg(not(feature = "zenoh-api"))]
impl ZenohRuntimeHandle {
    pub async fn shutdown(self) {}
}

pub fn spawn_runtime(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
) -> Option<ZenohRuntimeHandle> {
    #[cfg(feature = "zenoh-api")]
    {
        if !config.enabled {
            return None;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = tokio::spawn(async move {
            if let Err(err) = session::run_session(config, state_keeper, shutdown_rx).await {
                log::error!("Zenoh API runtime terminated: {}", err);
            }
        });

        return Some(ZenohRuntimeHandle { task, shutdown_tx });
    }

    #[cfg(not(feature = "zenoh-api"))]
    {
        let _ = (config, state_keeper);
        None
    }
}
