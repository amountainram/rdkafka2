use crate::{
    config::{ClientConfig, NativeClientConfig},
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::ErrBuf,
};
use log::trace;
use rdkafka2_sys::{RDKafka, RDKafkaType};
use std::{marker::PhantomData, mem::ManuallyDrop, sync::Arc};

/// Wrapper of the native rdkafka2-sys client.
/// Librdkafka is completely thread-safe (unless otherwise noted in the API documentation).
/// Any API, short of the destructor functions, may be called at any time from any thread.
/// The common restrictions of object destruction still applies
/// (e.g., you must not call `rd_kafka_destroy()` while another thread is calling `rd_kafka_poll()` or similar).
pub(crate) struct NativeClient {
    // FIXME: add a polling list to prevent undefined behavior on rd_kafka_destroy?
    ptr: NativePtr<RDKafka>,
}

/// SAFETY: [cft. official wiki](https://github.com/confluentinc/librdkafka/wiki/FAQ#is-the-library-thread-safe)
unsafe impl Sync for NativeClient {}
/// SAFETY: [cft. official wiki](https://github.com/confluentinc/librdkafka/wiki/FAQ#is-the-library-thread-safe)
unsafe impl Send for NativeClient {}

impl NativeClient {
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    /// UNSAFE: the caller must
    /// ensure that the pointer is not null otherwise undefined behavior
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafka) -> NativeClient {
        unsafe {
            NativeClient {
                ptr: NativePtr::from_ptr(ptr),
            }
        }
    }
}

impl NativeClient {
    fn try_from(rd_type: RDKafkaType, config: NativeClientConfig) -> Result<Self> {
        let mut err_buf = ErrBuf::new();
        //unsafe {
        //    // TODO: what does this do???
        //    rdkafka2_sys::rd_kafka_conf_set_opaque(
        //        config.ptr(),
        //        Arc::as_ptr(&context) as *mut c_void,
        //    )
        //};
        //native_config.set("log.queue", "true")?;

        let mut native_config = ManuallyDrop::new(config);
        let client_ptr = unsafe {
            rdkafka2_sys::rd_kafka_new(
                rd_type,
                native_config.ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        trace!("Create new librdkafka client {:p}", client_ptr);

        if client_ptr.is_null() {
            // SAFETY: Documentation of librdkafka states that if rd_kafka_new
            // fails returning NULL it also DOES NOT take ownership of
            // the configuration
            unsafe {
                ManuallyDrop::drop(&mut native_config);
            }
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        //let ret = unsafe {
        //    rdkafka2_sys::rd_kafka_set_log_queue(
        //        client_ptr,
        //        rdsys::rd_kafka_queue_get_main(client_ptr),
        //    )
        //};
        //if ret.is_error() {
        //    return Err(KafkaError::Global(ret.into()));
        //}
        //unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

        unsafe { Ok(NativeClient::from_ptr(client_ptr)) }
    }
}

#[derive(Clone)]
pub struct Client<C> {
    client: Arc<NativeClient>,
    context: Arc<C>,
}

pub struct ClientBuilder<C, Fields = ((), ())> {
    fields: Fields,
    #[allow(unused)]
    marker: PhantomData<C>,
}

impl<C> Client<C> {
    pub fn builder() -> ClientBuilder<C, ((), (), ())> {
        ClientBuilder {
            fields: ((), (), ()),
            marker: Default::default(),
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __rd_type, __context> ClientBuilder<C, ((), __rd_type, __context)> {
    #[must_use]
    #[doc(hidden)]
    fn config<K, V>(
        self,
        config: ClientConfig<K, V>,
    ) -> ClientBuilder<C, ((ClientConfig<K, V>,), __rd_type, __context)> {
        let ClientBuilder {
            fields: ((), rd_type, context),
            marker,
        } = self;
        ClientBuilder {
            fields: ((config,), rd_type, context),
            marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __config, __context> ClientBuilder<C, (__config, (), __context)> {
    #[must_use]
    #[doc(hidden)]
    pub fn client_type<T>(
        self,
        rd_type: T,
    ) -> ClientBuilder<C, (__config, (RDKafkaType,), __context)>
    where
        T: Into<RDKafkaType>,
    {
        let ClientBuilder {
            fields: (config, (), context),
            marker,
        } = self;
        ClientBuilder {
            fields: (config, (rd_type.into(),), context),
            marker,
        }
    }
}

#[allow(non_camel_case_types, clippy::complexity)]
impl<C, __client, __rd_type> ClientBuilder<C, (__client, __rd_type, ())> {
    #[must_use]
    #[doc(hidden)]
    pub fn context(self, context: C) -> ClientBuilder<C, (__client, __rd_type, (C,))> {
        let ClientBuilder {
            fields: (client, rd_type, ()),
            marker,
        } = self;
        ClientBuilder {
            fields: (client, rd_type, (context,)),
            marker,
        }
    }
}

#[allow(missing_docs, clippy::complexity)]
impl<C, K, V> ClientBuilder<C, ((ClientConfig<K, V>,), (RDKafkaType,), (C,))>
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    pub fn try_build(self) -> Result<Client<C>> {
        let ClientBuilder {
            fields: ((config,), (rd_type,), (context,)),
            ..
        } = self;
        let native_config: NativeClientConfig = config.try_into()?;
        let native_client = NativeClient::try_from(rd_type, native_config)?;

        Ok(Client {
            client: native_client.into(),
            context: context.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    #[test]
    fn from_client_config() {
        let cfg = ClientConfig::from_iter([("bootstrap.servers", "localhost:9092")]);
        Client::builder()
            .config(cfg)
            .client_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .context(())
            .try_build()
            .unwrap();
    }
}
