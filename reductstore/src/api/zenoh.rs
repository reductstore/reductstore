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
use tokio::task::JoinHandle;

#[cfg(feature = "zenoh-api")]
pub type ZenohRuntimeHandle = JoinHandle<()>;
#[cfg(not(feature = "zenoh-api"))]
pub type ZenohRuntimeHandle = ();

pub fn spawn_runtime(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
) -> Option<ZenohRuntimeHandle> {
    #[cfg(feature = "zenoh-api")]
    {
        if !config.enabled {
            return None;
        }

        let handle = tokio::spawn(async move {
            if let Err(err) = session::run_session(config, state_keeper).await {
                log::error!("Zenoh API runtime terminated: {}", err);
            }
        });

        return Some(handle);
    }

    #[cfg(not(feature = "zenoh-api"))]
    {
        let _ = (config, state_keeper);
        None
    }
}
