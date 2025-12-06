use super::{ClientContext, DefaultClientContext};
use crate::{
    IntoOpaque, RDKafkaLogLevel, Timeout,
    config::{ClientConfig, NativeClientConfig, get_conf_default_value},
    error::{KafkaError, Result},
    log::RDKafkaSyslogLogLevel,
    ptr::NativePtr,
    topic::{NativeTopic, NativeTopicConf, Partitioner, PartitionerSelector, TopicSettings},
    util::{ErrBuf, cstr_to_owned},
};
use once_cell::sync::Lazy;
use rdkafka2_sys::{
    RDKafkaConfErrorCode, RDKafkaErrorCode, RDKafkaType, rd_kafka_t, rd_kafka_topic_conf_t,
};
use std::{
    borrow::Borrow,
    collections::{HashMap, hash_map::IntoIter},
    ffi::{CStr, CString, c_char, c_void},
    hash::Hash,
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr::null_mut,
    sync::{Arc, Mutex},
};

#[repr(transparent)]
struct DefaultNativeTopicConfig(NativePtr<rd_kafka_topic_conf_t>);

unsafe impl Send for DefaultNativeTopicConfig {}

unsafe impl Sync for DefaultNativeTopicConfig {}

static DEFAULT_RDKAFKA_TOPIC_CONFIG: Lazy<DefaultNativeTopicConfig> = Lazy::new(|| unsafe {
    DefaultNativeTopicConfig(NativePtr::from_ptr(rdkafka2_sys::rd_kafka_topic_conf_new()))
});

fn get_topic_conf_property(conf: *mut rd_kafka_topic_conf_t, key: &str) -> Result<String> {
    let make_err = |res| KafkaError::ClientConfig(res, key.into());
    let key_c = CString::new(key.to_string())?;

    // Call with a `NULL` buffer to determine the size of the string.
    let mut size = 0_usize;
    let res = unsafe {
        rdkafka2_sys::rd_kafka_topic_conf_get(conf, key_c.as_ptr(), null_mut(), &mut size)
    };
    if let Some(err) = RDKafkaConfErrorCode::from(res).error() {
        return Err(make_err(*err));
    }

    // Allocate a buffer of that size and call again to get the actual
    // string.
    let mut buf = vec![0_u8; size];
    let res = unsafe {
        rdkafka2_sys::rd_kafka_topic_conf_get(
            conf,
            key_c.as_ptr(),
            buf.as_mut_ptr() as *mut c_char,
            &mut size,
        )
    };
    if let Some(err) = RDKafkaConfErrorCode::from(res).error() {
        return Err(make_err(*err));
    }

    unsafe { Ok(cstr_to_owned(buf.as_ptr() as *const c_char)) }
}

fn get_topic_conf_default_value(key: &str) -> Result<String> {
    get_topic_conf_property(DEFAULT_RDKAFKA_TOPIC_CONFIG.0.ptr(), key)
}

/// Generic Kafka topic configuration.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct TopicConfig {
    inner: HashMap<String, String>,
}

impl TopicConfig {
    pub fn set<Q, R>(&mut self, key: Q, value: R) -> &mut Self
    where
        Q: Into<String>,
        R: Into<String>,
    {
        self.inner.insert(key.into(), value.into());
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.inner.iter()
    }

    pub fn get_config_prop_or_default<Q>(&self, k: &Q) -> Option<String>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + AsRef<str> + ?Sized,
    {
        let key_str = k.as_ref();
        self.inner
            .get(k)
            .map(String::from)
            .or_else(|| get_topic_conf_default_value(key_str).ok())
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            inner: HashMap::with_capacity(0),
        }
    }
}

impl<K, V> FromIterator<(K, V)> for TopicConfig
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I>(iter: I) -> TopicConfig
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut config = TopicConfig::default();
        config
            .inner
            .extend(iter.into_iter().map(|(k, v)| (k.into(), v.into())));
        config
    }
}

impl<K, V> Extend<(K, V)> for TopicConfig
where
    K: Into<String>,
    V: Into<String>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.inner
            .extend(iter.into_iter().map(|(k, v)| (k.into(), v.into())))
    }
}

impl IntoIterator for TopicConfig {
    type Item = (String, String);

    type IntoIter = IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[derive(Debug)]
struct TopicRegistration<D = ()> {
    name: String,
    native_topic: NativeTopic,
    configuration: TopicConfig,
    partitioner: Option<*mut c_void>,
    _marker: PhantomData<D>,
}

/// # Safety
///
/// Topic registration is opaque to the library clients.
/// [`NativeTopic`] is thread-safe as per the documentation of `librdkafka`.
/// The partitioner is placed in the heap and is owned by [`TopicRegistration`]
/// despite being a raw pointer (due to the fact it must be passed to the `librdkafka` C API).
/// The partitioner is only used immutably and is dropped only when the registration is dropped
/// by the hashmap hold by the [`NativeClient`].
///
/// `librdkafka` documentation states that native topics are created behing a
/// counted reference, so when asking for a topic already registered, a reference is
/// returned to the previous instance.
///
/// See <https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#ab1dcba74a35e8f3bfe3270ff600581d8> for more details.
unsafe impl Send for TopicRegistration {}

/// # Safety
///
/// The partitioner is only used immutably.
unsafe impl Sync for TopicRegistration {}

impl<D> Drop for TopicRegistration<D> {
    fn drop(&mut self) {
        if let Some(partitioner) = self.partitioner.take() {
            let _: Box<Box<dyn Partitioner<D>>> = unsafe { Box::from_raw(partitioner as *mut _) };
        }
    }
}

type TopicMap = HashMap<String, Arc<TopicRegistration>>;

/// Wrapper of the native rdkafka2-sys client.
/// Librdkafka is completely thread-safe (unless otherwise noted in the API documentation).
/// Any API, short of the destructor functions, may be called at any time from any thread.
/// The common restrictions of object destruction still applies
/// (e.g., you must not call `rd_kafka_destroy()` while another thread is calling `rd_kafka_poll()` or similar).
#[derive(Debug)]
pub struct NativeClient<C = DefaultClientContext> {
    rd_type: RDKafkaType,
    topics: Arc<Mutex<TopicMap>>,
    inner: Arc<NativePtr<rd_kafka_t>>,
    config: Arc<ClientConfig>,
    context: Arc<C>,
}

impl<C> Clone for NativeClient<C> {
    fn clone(&self) -> Self {
        Self {
            rd_type: self.rd_type,
            topics: self.topics.clone(),
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
                rd_type.into(),
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
                topics: Default::default(),
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

    pub fn native_ptr(&self) -> *mut rd_kafka_t {
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

    pub fn partition_available(&self, topic: &Topic, partition: i32) -> bool {
        unsafe {
            rdkafka2_sys::rd_kafka_topic_partition_available(topic.native_topic().ptr(), partition)
                != 0
        }
    }
}

/// The handler of a Kafka topic registered successfully
/// in a client.
///
/// It wraps with reference counting the native topic
/// and the topic configuration.
#[derive(Debug)]
pub struct Topic<D = ()> {
    inner: Arc<TopicRegistration>,
    _marker: PhantomData<D>,
}

impl<D> PartialEq for Topic<D> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<D> Topic<D> {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn native_topic(&self) -> &NativeTopic {
        &self.inner.native_topic
    }

    pub fn configuration(&self) -> &TopicConfig {
        &self.inner.configuration
    }
}

impl<D> Clone for Topic<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

unsafe extern "C" fn custom_partitioner<D>(
    _rkt: *const rdkafka2_sys::rd_kafka_topic_t,
    keydata: *const c_void,
    keylen: usize,
    partition_cnt: i32,
    rkt_opaque: *mut c_void,
    msg_opaque: *mut c_void,
) -> i32
where
    D: IntoOpaque,
{
    let key = unsafe { std::slice::from_raw_parts(keydata as *mut u8, keylen) };
    let partitioner = unsafe { &*(rkt_opaque as *const Box<dyn Partitioner<D>>) };
    partitioner(key, partition_cnt, unsafe { D::from_ptr(msg_opaque) })
}

impl<C> NativeClient<C> {
    pub(crate) unsafe fn create_topic(
        &self,
        topic_name: &CStr,
        native_conf: Option<NativeTopicConf>,
    ) -> Result<NativePtr<rdkafka2_sys::rd_kafka_topic_t>> {
        unsafe {
            let topic_ptr = rdkafka2_sys::rd_kafka_topic_new(
                self.native_ptr(),
                topic_name.as_ptr(),
                native_conf.as_ref().map(|p| p.ptr()).unwrap_or(null_mut()),
            );
            if let Some(native_conf) = native_conf {
                std::mem::forget(native_conf);
            }
            (!topic_ptr.is_null())
                .then(|| NativePtr::from_ptr(topic_ptr))
                .ok_or(KafkaError::TopicCreation(RDKafkaErrorCode::from(
                    rdkafka2_sys::rd_kafka_errno2err(
                        #[cfg(target_os = "linux")]
                        *libc::__errno_location(),
                    ),
                )))
        }
    }

    pub(crate) fn register_topic<D, F, T>(&self, topic: T) -> Result<Topic<D>>
    where
        D: IntoOpaque,
        F: Partitioner<D> + 'static,
        T: Into<TopicSettings<D, F>>,
    {
        let mut topic_map = self.topics.lock().map_err(|_| KafkaError::TopicMapLock)?;

        let TopicSettings {
            name,
            configuration,
            mut partitioner,
            ..
        } = topic.into();
        topic_map
            .remove(name.as_str())
            // if filter out, the hashmap is the only holder of the topic
            // so it is safe to drop it and allow a new registration
            // with the same name
            .filter(|c| Arc::strong_count(c) > 1)
            .map(|t| t.name.to_string())
            .map(KafkaError::TopicAlreadyRegistered)
            .map(Err)
            .unwrap_or(Ok(()))?;

        let conf = unsafe { NativeTopicConf::from_ptr(rdkafka2_sys::rd_kafka_topic_conf_new()) };
        let conf = configuration
            .iter()
            .map(Ok::<_, KafkaError>)
            .try_fold(conf, |conf, next| {
                let (k, v) = next?;
                conf.set(k.as_str(), v.as_str())?;

                Ok::<_, KafkaError>(conf)
            })?;

        let partitioner = partitioner.take().map(|partitioner_selector| {
            let raw_partitioner = match partitioner_selector {
                PartitionerSelector::Random => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_random as *mut c_void
                }
                PartitionerSelector::Consistent => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_consistent as *mut c_void
                }
                PartitionerSelector::ConsistentRandom => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_consistent_random as *mut c_void
                }
                PartitionerSelector::Murmur2 => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_murmur2 as *mut c_void
                }
                PartitionerSelector::Murmur2Random => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_murmur2_random as *mut c_void
                }
                PartitionerSelector::Fnv1a => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_fnv1a as *mut c_void
                }
                PartitionerSelector::Fnv1aRandom => {
                    rdkafka2_sys::rd_kafka_msg_partitioner_fnv1a_random as *mut c_void
                }
                PartitionerSelector::Custom(partitioner) => {
                    let partitioner: Box<Box<dyn Partitioner<D>>> = Box::new(Box::new(partitioner));
                    Box::into_raw(partitioner) as *mut c_void
                }
            };

            unsafe {
                rdkafka2_sys::rd_kafka_topic_conf_set_opaque(conf.ptr(), raw_partitioner);
                rdkafka2_sys::rd_kafka_topic_conf_set_partitioner_cb(
                    conf.ptr(),
                    Some(custom_partitioner::<D>),
                );
            }

            raw_partitioner
        });

        let topic_name = CString::new(name.as_str()).map_err(KafkaError::Nul)?;
        let topic = unsafe { self.create_topic(&topic_name, Some(conf)) }?;
        let native_topic = NativeTopic::from_ptr(topic);
        let topic_registration = Arc::new(TopicRegistration {
            name: name.to_string(),
            native_topic,
            configuration,
            partitioner,
            _marker: PhantomData,
        });

        topic_map.insert(name, topic_registration.clone());

        Ok(Topic {
            inner: topic_registration,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchecked_topic_conf_property() {
        let mut topic_settings: TopicSettings = "topic_name".into();

        topic_settings
            .configuration
            .set("compression.type", "snappy");

        assert_eq!(
            topic_settings
                .configuration
                .get_config_prop_or_default("request.required.acks")
                .unwrap(),
            "-1"
        );
        assert_eq!(
            topic_settings
                .configuration
                .get_config_prop_or_default("compression.type")
                .unwrap(),
            "snappy"
        );
    }
}
