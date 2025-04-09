use super::{ClientContext, DefaultClientContext, NativeClient};
use crate::{
    RDKafkaLogLevel,
    config::{ClientConfig, NativeClientConfig},
    error::Result,
};
use rdkafka2_sys::RDKafkaType;
use std::sync::Arc;

#[derive(Debug)]
pub struct AdminClient<C = DefaultClientContext> {
    inner: Arc<NativeClient<C>>,
}

impl<C> AdminClient<C>
where
    C: ClientContext,
{
    pub(crate) fn try_from_parts(
        config: ClientConfig,
        native_config: NativeClientConfig,
        context: Arc<C>,
        log_level: Option<RDKafkaLogLevel>,
    ) -> Result<Self> {
        Ok(Self {
            inner: NativeClient::builder()
                .config(config)
                .with_log_level(log_level)
                .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
                .context(context)
                .try_build()?
                .into(),
        })
    }
}
