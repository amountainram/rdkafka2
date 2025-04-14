use super::{ClientContext, admin::AdminClient, native::NativeClient};
use crate::{
    RDKafkaLogLevel,
    config::{ClientConfig, NativeClientConfig},
    error::{KafkaError, Result},
};
use rdkafka2_sys::RDKafkaType;
use std::{marker::PhantomData, sync::Arc};
use typed_builder::Optional;

#[doc(hidden)]
pub trait ResultOptional<T, E> {
    fn into_value<F: FnOnce() -> Result<T, E>>(self, default: F) -> Result<T, E>;
}

impl<T, E> ResultOptional<T, E> for () {
    fn into_value<F: FnOnce() -> Result<T, E>>(self, default: F) -> Result<T, E> {
        default()
    }
}

impl<T, E> ResultOptional<T, E> for (T,) {
    fn into_value<F: FnOnce() -> Result<T, E>>(self, _: F) -> Result<T, E> {
        Ok(self.0)
    }
}

impl<T, E> ResultOptional<T, E> for (Option<T>,) {
    fn into_value<F: FnOnce() -> Result<T, E>>(self, default: F) -> Result<T, E> {
        match self.0 {
            Some(v) => Ok(v),
            None => default(),
        }
    }
}

#[must_use]
#[doc(hidden)]
pub struct NativeClientBuilder<C, F = ((), (), (), (), ())> {
    fields: F,
    _marker: PhantomData<C>,
}

impl<C> NativeClient<C> {
    #[allow(dead_code)]
    pub fn builder() -> NativeClientBuilder<C, ((), (), (), (), ())> {
        NativeClientBuilder {
            fields: ((), (), (), (), ()),
            _marker: PhantomData,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __config, __native_config, __context, __log_level>
    NativeClientBuilder<C, ((), __config, __native_config, __context, __log_level)>
{
    #[allow(clippy::used_underscore_binding, clippy::no_effect_underscore_binding)]
    pub fn rd_type(
        self,
        rd_type: RDKafkaType,
    ) -> NativeClientBuilder<
        C,
        (
            (RDKafkaType,),
            __config,
            __native_config,
            __context,
            __log_level,
        ),
    > {
        let rd_type = (rd_type,);
        let NativeClientBuilder {
            fields: ((), config, native_config, context, log_level),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __rd_type, __native_config, __context, __log_level>
    NativeClientBuilder<C, (__rd_type, (), __native_config, __context, __log_level)>
{
    #[allow(clippy::used_underscore_binding, clippy::no_effect_underscore_binding)]
    pub fn config<K, V, I>(
        self,
        config: I,
    ) -> NativeClientBuilder<
        C,
        (
            __rd_type,
            (ClientConfig,),
            __native_config,
            __context,
            __log_level,
        ),
    >
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let config = (ClientConfig::from_iter(config),);
        let NativeClientBuilder {
            fields: (rd_type, (), native_config, context, log_level),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __rd_type, __config, __context, __log_level>
    NativeClientBuilder<C, (__rd_type, __config, (), __context, __log_level)>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn native_config(
        self,
        native_config: NativeClientConfig,
    ) -> NativeClientBuilder<
        C,
        (
            __rd_type,
            __config,
            (Option<NativeClientConfig>,),
            __context,
            __log_level,
        ),
    > {
        let native_config = (Some(native_config),);
        let NativeClientBuilder {
            fields: (rd_type, config, (), context, log_level),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __rd_type, __config, __native_config, __log_level>
    NativeClientBuilder<C, (__rd_type, __config, __native_config, (), __log_level)>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn context(
        self,
        context: Arc<C>,
    ) -> NativeClientBuilder<C, (__rd_type, __config, __native_config, (Arc<C>,), __log_level)>
    {
        let context = (context,);
        let NativeClientBuilder {
            fields: (rd_type, config, native_config, (), log_level),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __rd_type, __config, __native_config, __context>
    NativeClientBuilder<C, (__rd_type, __config, __native_config, __context, ())>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn log_level(
        self,
        log_level: RDKafkaLogLevel,
    ) -> NativeClientBuilder<
        C,
        (
            __rd_type,
            __config,
            __native_config,
            __context,
            (Option<RDKafkaLogLevel>,),
        ),
    > {
        let log_level = (Some(log_level),);
        let NativeClientBuilder {
            fields: (rd_type, config, native_config, context, ()),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __rd_type, __config, __native_config, __context>
    NativeClientBuilder<C, (__rd_type, __config, __native_config, __context, ())>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub(crate) fn with_log_level(
        self,
        log_level: Option<RDKafkaLogLevel>,
    ) -> NativeClientBuilder<
        C,
        (
            __rd_type,
            __config,
            __native_config,
            __context,
            (Option<RDKafkaLogLevel>,),
        ),
    > {
        let NativeClientBuilder {
            fields: (rd_type, config, native_config, context, ()),
            _marker,
        } = self;
        NativeClientBuilder {
            fields: (rd_type, config, native_config, context, (log_level,)),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<
    C,
    __rd_type: Optional<RDKafkaType>,
    __config: Optional<ClientConfig>,
    __native_config: ResultOptional<NativeClientConfig, KafkaError>,
    __log_level: Optional<Option<RDKafkaLogLevel>>,
> NativeClientBuilder<C, (__rd_type, __config, __native_config, (Arc<C>,), __log_level)>
where
    C: ClientContext,
{
    pub fn try_build(self) -> Result<NativeClient<C>> {
        let NativeClientBuilder {
            fields: (rd_type, config, native_config, (context,), log_level),
            ..
        } = self;
        let rd_type = Optional::into_value(rd_type, || RDKafkaType::Producer);
        let config = Optional::into_value(config, Default::default);
        let log_level = Optional::into_value(log_level, || None);
        let native_config = ResultOptional::into_value(native_config, || {
            NativeClientConfig::try_from(config.clone())
        })?;
        NativeClient::try_from_parts(rd_type, config, native_config, context, log_level)
    }
}

#[must_use]
#[doc(hidden)]
pub struct AdminClientBuilder<C, F = ((), (), (), ())> {
    fields: F,
    _marker: PhantomData<C>,
}

impl<C> AdminClient<C> {
    #[allow(dead_code)]
    pub fn builder() -> AdminClientBuilder<C, ((), (), (), ())> {
        AdminClientBuilder {
            fields: ((), (), (), ()),
            _marker: PhantomData,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __native_config, __context, __log_level>
    AdminClientBuilder<C, ((), __native_config, __context, __log_level)>
{
    #[allow(clippy::used_underscore_binding, clippy::no_effect_underscore_binding)]
    pub fn config<K, V, I>(
        self,
        config: I,
    ) -> AdminClientBuilder<C, ((ClientConfig,), __native_config, __context, __log_level)>
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let config = (ClientConfig::from_iter(config),);
        let AdminClientBuilder {
            fields: ((), native_config, context, log_level),
            _marker,
        } = self;
        AdminClientBuilder {
            fields: (config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __config, __context, __log_level>
    AdminClientBuilder<C, (__config, (), __context, __log_level)>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn native_config(
        self,
        native_config: NativeClientConfig,
    ) -> AdminClientBuilder<
        C,
        (
            __config,
            (Option<NativeClientConfig>,),
            __context,
            __log_level,
        ),
    > {
        let native_config = (Some(native_config),);
        let AdminClientBuilder {
            fields: (config, (), context, log_level),
            _marker,
        } = self;
        AdminClientBuilder {
            fields: (config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __config, __native_config, __log_level>
    AdminClientBuilder<C, (__config, __native_config, (), __log_level)>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn context(
        self,
        context: Arc<C>,
    ) -> AdminClientBuilder<C, (__config, __native_config, (Arc<C>,), __log_level)> {
        let context = (context,);
        let AdminClientBuilder {
            fields: (config, native_config, (), log_level),
            _marker,
        } = self;
        AdminClientBuilder {
            fields: (config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<C, __config, __native_config, __context>
    AdminClientBuilder<C, (__config, __native_config, __context, ())>
{
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn log_level(
        self,
        log_level: RDKafkaLogLevel,
    ) -> AdminClientBuilder<
        C,
        (
            __config,
            __native_config,
            __context,
            (Option<RDKafkaLogLevel>,),
        ),
    > {
        let log_level = (Some(log_level),);
        let AdminClientBuilder {
            fields: (config, native_config, context, ()),
            _marker,
        } = self;
        AdminClientBuilder {
            fields: (config, native_config, context, log_level),
            _marker,
        }
    }
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
impl<
    C,
    __config: Optional<ClientConfig>,
    __native_config: ResultOptional<NativeClientConfig, KafkaError>,
    __log_level: Optional<Option<RDKafkaLogLevel>>,
> AdminClientBuilder<C, (__config, __native_config, (Arc<C>,), __log_level)>
where
    C: ClientContext,
{
    pub fn try_build(self) -> Result<AdminClient<C>> {
        let AdminClientBuilder {
            fields: (config, native_config, (context,), log_level),
            ..
        } = self;
        let config = Optional::into_value(config, Default::default);
        let log_level = Optional::into_value(log_level, || None);
        let native_config = ResultOptional::into_value(native_config, || {
            NativeClientConfig::try_from(config.clone())
        })?;
        AdminClient::try_from_parts(config, native_config, context, log_level)
    }
}
