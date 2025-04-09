use crate::{
    log::{debug, error, info, trace, warn},
    util::cstr_to_owned,
};
pub use admin::AdminClient;
pub use builders::*;
pub use native::NativeClient;
use std::ffi::{c_char, c_int};

mod admin;
mod builders;
mod native;

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

//#[cfg(test)]
//mod tests {
//    use super::*;
//    use crate::config::clientconfig;
//    use rdkafka2_sys::rdkafkatype;
//    use rstest::{fixture, rstest};
//
//    #[fixture]
//    #[once]
//    fn log_init() {
//        env_logger::builder().is_test(true).init();
//    }
//
//    #[rstest]
//    #[tokio::test]
//    async fn test_log(#[allow(unused)] log_init: ()) {
//        let client = nativeclient::builder()
//            .rd_type(rdkafkatype::rd_kafka_producer)
//            .config([("log_level", "7"), ("debug", "all")])
//            .context(defaultclientcontext.into())
//            .log_level(rdkafkaloglevel::info)
//            .try_build()
//            .expect("client to be built");
//
//        tokio::time::sleep(std::time::duration::from_secs(2)).await;
//
//        drop(client);
//    }
//
//    #[rstest]
//    fn client_conf_property(#[allow(unused)] log_init: ()) {
//        let mut conf = clientconfig::default();
//        conf.set("bootstrap.servers", "localhost:9092");
//
//        let client = nativeclient::builder()
//            .rd_type(rdkafkatype::rd_kafka_producer)
//            .config(conf)
//            .context(defaultclientcontext.into())
//            .try_build()
//            .expect("a valid client");
//
//        assert_eq!(
//            client
//                .get_config_prop_or_default("allow.auto.create.topics")
//                .as_deref(),
//            some("false")
//        );
//        assert_eq!(
//            client.config.get("bootstrap.servers").map(|s| s.as_str()),
//            some("localhost:9092")
//        );
//    }
//}
