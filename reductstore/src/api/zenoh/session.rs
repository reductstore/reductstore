use crate::api::zenoh::{
    attachments, queryable::QueryablePipeline, subscriber::SubscriberPipeline,
};
use crate::cfg::zenoh::ZenohApiConfig;
use crate::core::components::{ComponentError, StateKeeper};
use log::info;
use reduct_base::Labels;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub(crate) async fn run_session(
    config: ZenohApiConfig,
    state_keeper: Arc<StateKeeper>,
) -> Result<(), SessionError> {
    info!(
        "Starting Zenoh API runtime: prefix='{}', listen={:?}, connect={:?}",
        config.key_prefix, config.listen_endpoints, config.connect_endpoints
    );

    validate_label_codec()?;

    let components = state_keeper
        .get_anonymous()
        .await
        .map_err(SessionError::Component)?;

    let subscriber = SubscriberPipeline::new(config.clone(), Arc::clone(&components));
    subscriber
        .bootstrap()
        .await
        .map_err(SessionError::Subscriber)?;

    let queryable = QueryablePipeline::new(config, components);
    queryable
        .bootstrap()
        .await
        .map_err(SessionError::Queryable)?;

    Ok(())
}

fn validate_label_codec() -> Result<(), SessionError> {
    let mut labels = Labels::new();
    labels.insert("codec".into(), "probe".into());

    let encoded = attachments::serialize_labels(&labels)?;
    let decoded = attachments::deserialize_labels(&encoded)?;

    if decoded != labels {
        return Err(SessionError::AttachmentCodecMismatch);
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) enum SessionError {
    Component(ComponentError),
    Subscriber(String),
    Queryable(String),
    AttachmentCodec(serde_json::Error),
    AttachmentCodecMismatch,
}

impl Display for SessionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Component(err) => write!(f, "{}", err),
            SessionError::Subscriber(err) => write!(f, "Subscriber pipeline error: {}", err),
            SessionError::Queryable(err) => write!(f, "Queryable pipeline error: {}", err),
            SessionError::AttachmentCodec(err) => {
                write!(f, "Failed to serialize labels: {}", err)
            }
            SessionError::AttachmentCodecMismatch => {
                write!(f, "Label codec roundtrip produced mismatched values")
            }
        }
    }
}

impl Error for SessionError {}

impl From<serde_json::Error> for SessionError {
    fn from(value: serde_json::Error) -> Self {
        SessionError::AttachmentCodec(value)
    }
}
