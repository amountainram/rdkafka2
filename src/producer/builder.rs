use super::{BoxProducer, DefaultProducerContext, Producer, ProducerContext};
use crate::{config::ClientConfig, error::Result, log::RDKafkaLogLevel};
use std::{marker::PhantomData, sync::Arc};
use typed_builder::Optional;

pub struct ProducerBuilder<C = DefaultProducerContext, F = ((), (), ())> {
    fields: F,
    _marker: PhantomData<C>,
}

impl<C> Producer<C> {
    pub fn builder() -> ProducerBuilder<C> {
        ProducerBuilder {
            fields: ((), (), ()),
            _marker: PhantomData,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __context, __log_level> ProducerBuilder<C, ((), __context, __log_level)> {
    #[must_use]
    #[doc(hidden)]
    pub fn config<K, V, I>(
        self,
        config: I,
    ) -> ProducerBuilder<C, ((ClientConfig,), __context, __log_level)>
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let ProducerBuilder {
            fields: ((), context, log_level),
            _marker,
        } = self;
        ProducerBuilder {
            fields: ((ClientConfig::from_iter(config),), context, log_level),
            _marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __config, __log_level> ProducerBuilder<C, (__config, (), __log_level)> {
    #[must_use]
    #[doc(hidden)]
    pub fn context(
        self,
        context: Arc<C>,
    ) -> ProducerBuilder<C, (__config, (Arc<C>,), __log_level)> {
        let ProducerBuilder {
            fields: (config, (), log_level),
            _marker,
        } = self;
        ProducerBuilder {
            fields: (config, (context,), log_level),
            _marker,
        }
    }
}

impl ProducerBuilder {
    pub fn try_build(self) -> Result<BoxProducer> {
        let client_config = ClientConfig::default();
        BoxProducer::try_from_parts(
            client_config,
            DefaultProducerContext::default().into(),
            None,
        )
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl ProducerBuilder<DefaultProducerContext, ((ClientConfig,), (), ())> {
    pub fn try_build(self) -> Result<BoxProducer> {
        let ProducerBuilder {
            fields: ((client_config,), (), ()),
            ..
        } = self;
        BoxProducer::try_from_parts(
            client_config,
            DefaultProducerContext::default().into(),
            None,
        )
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __log_level: Optional<Option<RDKafkaLogLevel>>>
    ProducerBuilder<C, ((ClientConfig,), (Arc<C>,), __log_level)>
where
    C: ProducerContext,
{
    pub fn try_build(self) -> Result<BoxProducer<C>> {
        let ProducerBuilder {
            fields: ((client_config,), (context,), log_level),
            ..
        } = self;
        let log_level = Optional::into_value(log_level, || None);
        BoxProducer::try_from_parts(client_config, context, log_level)
    }
}
