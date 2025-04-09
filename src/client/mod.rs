use crate::{
    config::{ClientConfig, NativeClientConfig, get_conf_default_value},
    error::{KafkaError, Result},
    log::{RDKafkaLogLevel, RDKafkaSyslogLogLevel, debug, error, info, trace, warn},
    ptr::NativePtr,
    util::{ErrBuf, Timeout, cstr_to_owned},
};
pub use builder::*;
use rdkafka2_sys::{RDKafka, RDKafkaErrorCode, RDKafkaType};
use std::{
    borrow::Borrow,
    ffi::{c_char, c_int},
    hash::Hash,
    mem::ManuallyDrop,
    sync::Arc,
};

mod builder;

pub trait ClientContext {
    /// Callback to log messages from librdkafka
    ///
    /// # Safety
    ///
    /// This function is called from C (via FFI) as a logging callback for `librdkafka`.
    /// All pointers passed (`rk`, `fac`, `buf`) must be valid for reading and properly null-terminated C strings,
    /// as expected by `rd_kafka_log_print`. `level` must be a valid log level supported by `librdkafka`.
    ///
    /// This function **must not panic** or unwind across the FFI boundary, and should not assume anything
    /// about thread-local context, since it may be called from internal threads of `librdkafka`. According to [documentation](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a06ade2ca41f32eb82c6f7e3d4acbe19f), log levels are [POSIX syslog(3) levels](https://www.man7.org/linux/man-pages/man3/syslog.3.html#DESCRIPTION).
    ///
    /// - `rk`: A valid (non-null) pointer to an `rd_kafka_t` instance.
    /// - `fac`: A valid C string (null-terminated), representing the log facility.
    /// - `buf`: A valid C string (null-terminated), representing the log message.
    /// - `level`: An integer representing the log level (usually in the range defined by `librdkafka`).
    ///
    /// A native implementation may perform no modification and only forward arguments to [rd_kafka_log_print](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a3d37a5cef2d6aa0077fdfd23e3020ca7),
    /// which is assumed safe for these arguments.
    ///
    /// ```
    /// unsafe extern "C" fn log_cb(
    ///     _rk: *const rdkafka2_sys::rd_kafka_t,
    ///     level: c_int,
    ///     fac: *const c_char,
    ///     buf: *const c_char,
    /// ) {
    ///     unsafe {
    ///         rdkafka2_sys::rd_kafka_log_print(rk, level, fac, buf);
    ///     }
    /// }
    /// ```
    unsafe extern "C" fn log_cb(
        _rk: *const rdkafka2_sys::rd_kafka_t,
        level: c_int,
        fac: *const c_char,
        buf: *const c_char,
    ) {
        let fac = unsafe { cstr_to_owned(fac) };
        let buf = unsafe { cstr_to_owned(buf) };
        match level {
            libc::LOG_EMERG..=libc::LOG_ERR => error!("{fac}: {buf}"),
            libc::LOG_WARNING..=libc::LOG_NOTICE => warn!("{fac}: {buf}"),
            libc::LOG_INFO => info!("{fac}: {buf}"),
            libc::LOG_DEBUG => debug!("{fac}: {buf}"),
            _ => trace!("{fac}: {buf}"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    use rdkafka2_sys::RDKafkaType;
    use rstest::{fixture, rstest};

    #[fixture]
    #[once]
    fn log_init() {
        env_logger::builder().is_test(true).init();
    }

    #[rstest]
    #[tokio::test]
    async fn test_log(#[allow(unused)] log_init: ()) {
        let client = NativeClient::builder()
            .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .config([("log_level", "7"), ("debug", "all")])
            .context(DefaultClientContext.into())
            .log_level(RDKafkaLogLevel::Info)
            .try_build()
            .expect("client to be built");

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        drop(client);
    }

    #[rstest]
    fn client_conf_property(#[allow(unused)] log_init: ()) {
        let mut conf = ClientConfig::default();
        conf.set("bootstrap.servers", "localhost:9092");

        let client = NativeClient::builder()
            .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .config(conf)
            .context(DefaultClientContext.into())
            .try_build()
            .expect("a valid client");

        assert_eq!(
            client
                .get_config_prop_or_default("allow.auto.create.topics")
                .as_deref(),
            Some("false")
        );
        assert_eq!(
            client.config.get("bootstrap.servers").map(|s| s.as_str()),
            Some("localhost:9092")
        );
    }
}
