use super::{ClientContext, DefaultClientContext};
use crate::{
    RDKafkaLogLevel, Timeout,
    config::{ClientConfig, NativeClientConfig, get_conf_default_value},
    error::{KafkaError, Result},
    log::RDKafkaSyslogLogLevel,
    ptr::NativePtr,
    util::ErrBuf,
};
use rdkafka2_sys::{RDKafka, RDKafkaErrorCode, RDKafkaType};
use std::{borrow::Borrow, hash::Hash, mem::ManuallyDrop, sync::Arc};

/// Wrapper of the native rdkafka2-sys client.
/// Librdkafka is completely thread-safe (unless otherwise noted in the API documentation).
/// Any API, short of the destructor functions, may be called at any time from any thread.
/// The common restrictions of object destruction still applies
/// (e.g., you must not call `rd_kafka_destroy()` while another thread is calling `rd_kafka_poll()` or similar).
#[derive(Debug)]
pub struct NativeClient<C = DefaultClientContext> {
    rd_type: RDKafkaType,
    inner: Arc<NativePtr<RDKafka>>,
    config: Arc<ClientConfig>,
    context: Arc<C>,
}

impl<C> Clone for NativeClient<C> {
    fn clone(&self) -> Self {
        Self {
            rd_type: self.rd_type,
            inner: self.inner.clone(),
            config: self.config.clone(),
            context: self.context.clone(),
        }
    }
}

/// # Safety
///
/// The [official documentation](https://github.com/confluentinc/librdkafka/wiki/FAQ#is-the-library-thread-safe) states that `librdkafka` clients are thread safe.
unsafe impl<C> Sync for NativeClient<C> where C: Sync {}

/// # Safety
///
/// The [official documentation](https://github.com/confluentinc/librdkafka/wiki/FAQ#is-the-library-thread-safe) states that `librdkafka` clients are thread safe.
unsafe impl<C> Send for NativeClient<C> where C: Send {}

impl<C> NativeClient<C>
where
    C: ClientContext,
{
    // in this function, originally, the log level was set
    // but now it is [deprecated](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#acadeefced6bb60acd27e7a0dad553aa4) in favor of
    //  - `log_level` config property
    //  - `debug` config property
    pub(crate) fn try_from_parts(
        rd_type: RDKafkaType,
        config: ClientConfig,
        native_config: NativeClientConfig,
        context: Arc<C>,
        log_level: Option<RDKafkaLogLevel>,
    ) -> Result<Self> {
        if native_config.get("log.queue")?.as_str() == "false" {
            if let Some(log_level) = log_level {
                let syslog_level: RDKafkaSyslogLogLevel = log_level.into();
                let syslog_level_str: &str = syslog_level.into();
                println!("log_level: {syslog_level_str}");
                native_config.set("log_level", syslog_level_str)?;
            }

            unsafe {
                rdkafka2_sys::rd_kafka_conf_set_log_cb(native_config.ptr(), Some(C::log_cb));
            }
        }

        let mut err_buf = ErrBuf::new();
        let mut native_config = ManuallyDrop::new(native_config);
        let client_ptr = unsafe {
            rdkafka2_sys::rd_kafka_new(
                rd_type,
                native_config.ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        if client_ptr.is_null() {
            // # Safety
            //
            // Documentation of librdkafka states that if rd_kafka_new
            // fails returning NULL it also DOES NOT take ownership of
            // the configuration
            unsafe {
                ManuallyDrop::drop(&mut native_config);
            }
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        unsafe {
            Ok(NativeClient {
                rd_type,
                inner: NativePtr::from_ptr(client_ptr).into(),
                config: config.into(),
                context,
            })
        }
    }
}

impl<C> NativeClient<C> {
    pub fn get_config_prop_or_default<Q>(&self, k: &Q) -> Option<String>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + AsRef<str> + ?Sized,
    {
        let key_str = k.as_ref();
        self.config
            .as_ref()
            .get(k)
            .map(String::from)
            .or_else(|| get_conf_default_value(key_str).ok())
    }

    pub fn rd_type(&self) -> RDKafkaType {
        self.rd_type
    }

    pub fn context(&self) -> &C {
        self.context.as_ref()
    }

    pub fn native_ptr(&self) -> *mut RDKafka {
        self.inner.ptr()
    }

    pub fn poll<T>(&self, timeout: T) -> u64
    where
        T: Into<Timeout>,
    {
        unsafe { rdkafka2_sys::rd_kafka_poll(self.inner.ptr(), timeout.into().as_millis()) as u64 }
    }

    pub fn flush<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        unsafe { rdkafka2_sys::rd_kafka_flush(self.inner.ptr(), timeout.into().as_millis()).into() }
    }

    pub fn purge<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        unsafe { rdkafka2_sys::rd_kafka_purge(self.inner.ptr(), timeout.into().as_millis()).into() }
    }
}
