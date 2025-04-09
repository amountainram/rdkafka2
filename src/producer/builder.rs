use super::{
    ProducerContext,
    boxed::{DefaultProducerContext, Producer},
};
use crate::{config::ClientConfig, error::Result, log::RDKafkaLogLevel};
use futures::{FutureExt, future::BoxFuture};
use std::{marker::PhantomData, sync::Arc};
use typed_builder::Optional;

pub struct ProducerBuilder<
    C = DefaultProducerContext,
    S = BoxFuture<'static, ()>,
    F = ((), (), (), ()),
> {
    fields: F,
    _marker: PhantomData<(C, S)>,
}

impl<C> Producer<C> {
    pub fn builder() -> ProducerBuilder<C> {
        ProducerBuilder {
            fields: ((), (), (), ()),
            _marker: PhantomData,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, S, __context, __log_level, __shutdown>
    ProducerBuilder<C, S, ((), __context, __log_level, __shutdown)>
{
    #[must_use]
    #[doc(hidden)]
    pub fn config<K, V, I>(
        self,
        config: I,
    ) -> ProducerBuilder<C, S, ((Option<ClientConfig>,), __context, __log_level, __shutdown)>
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let ProducerBuilder {
            fields: ((), context, log_level, shutdown),
            _marker,
        } = self;
        ProducerBuilder {
            fields: (
                (ClientConfig::from_iter(config).into(),),
                context,
                log_level,
                shutdown,
            ),
            _marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, S, __config, __log_level, __shutdown>
    ProducerBuilder<C, S, (__config, (), __log_level, __shutdown)>
{
    #[must_use]
    #[doc(hidden)]
    pub fn context(
        self,
        context: C,
    ) -> ProducerBuilder<C, S, (__config, (C,), __log_level, __shutdown)> {
        let ProducerBuilder {
            fields: (config, (), log_level, shutdown),
            _marker,
        } = self;
        ProducerBuilder {
            fields: (config, (context,), log_level, shutdown),
            _marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, S, __config, __context, __shutdown>
    ProducerBuilder<C, S, (__config, __context, (), __shutdown)>
{
    #[must_use]
    #[doc(hidden)]
    pub fn log_level(
        self,
        log_level: RDKafkaLogLevel,
    ) -> ProducerBuilder<C, S, (__config, __context, (Option<RDKafkaLogLevel>,), __shutdown)> {
        let ProducerBuilder {
            fields: (config, context, (), shutdown),
            _marker,
        } = self;
        ProducerBuilder {
            fields: (config, context, (Some(log_level),), shutdown),
            _marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, S, __config, __context, __log_level>
    ProducerBuilder<C, S, (__config, __context, __log_level, ())>
{
    #[must_use]
    #[doc(hidden)]
    pub fn shutdown(
        self,
        shutdown: S,
    ) -> ProducerBuilder<C, S, (__config, __context, __log_level, (Option<S>,))> {
        let ProducerBuilder {
            fields: (config, context, log_level, ()),
            _marker,
        } = self;
        ProducerBuilder {
            fields: (config, context, log_level, (Some(shutdown),)),
            _marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<
    C,
    S,
    __config: Optional<Option<ClientConfig>>,
    __log_level: Optional<Option<RDKafkaLogLevel>>,
    __shutdown: Optional<Option<S>>,
> ProducerBuilder<C, S, (__config, (C,), __log_level, __shutdown)>
where
    C: ProducerContext + Send + 'static,
    S: Future + Unpin + Send + 'static,
{
    pub fn try_build(self) -> Result<Producer<C>> {
        let ProducerBuilder {
            fields: (config, (context,), log_level, shutdown),
            ..
        } = self;
        let config = Optional::into_value(config, || None).unwrap_or_default();
        let log_level = Optional::into_value(log_level, || None);
        let shutdown = Optional::into_value(shutdown, || None)
            .map(|s| s.map(|_| ()).boxed())
            .unwrap_or(futures::future::pending().boxed());
        Producer::try_from_parts(config, Arc::new(context), log_level, shutdown)
    }
}
