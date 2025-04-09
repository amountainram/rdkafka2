use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::{ErrBuf, cstr_to_owned},
};
use once_cell::sync::Lazy;
use rdkafka2_sys::{RDKafkaConf, RDKafkaConfErrorCode};
use std::{
    borrow::Borrow,
    collections::{HashMap, hash_map::IntoIter},
    ffi::{CString, c_char},
    hash::Hash,
    pin::Pin,
    ptr::null_mut,
};

#[repr(transparent)]
struct DefaultNativeClientConfig(NativePtr<RDKafkaConf>);

unsafe impl Send for DefaultNativeClientConfig {}

unsafe impl Sync for DefaultNativeClientConfig {}

static DEFAULT_RDKAFKA_CONFIG: Lazy<Pin<Box<DefaultNativeClientConfig>>> = Lazy::new(|| unsafe {
    Box::pin(DefaultNativeClientConfig(NativePtr::from_ptr(
        rdkafka2_sys::rd_kafka_conf_new(),
    )))
});

fn get_conf_property(conf: *mut RDKafkaConf, key: &str) -> Result<String> {
    let make_err = |res| KafkaError::ClientConfig(res, key.into());
    let key_c = CString::new(key.to_string())?;

    // Call with a `NULL` buffer to determine the size of the string.
    let mut size = 0_usize;
    let res =
        unsafe { rdkafka2_sys::rd_kafka_conf_get(conf, key_c.as_ptr(), null_mut(), &mut size) };
    if let Some(err) = RDKafkaConfErrorCode::from(res).error() {
        return Err(make_err(*err));
    }

    // Allocate a buffer of that size and call again to get the actual
    // string.
    let mut buf = vec![0_u8; size];
    let res = unsafe {
        rdkafka2_sys::rd_kafka_conf_get(
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

pub(crate) fn get_conf_default_value(key: &str) -> Result<String> {
    get_conf_property(DEFAULT_RDKAFKA_CONFIG.0.ptr(), key)
}

/// A native rdkafka-sys client config.
pub struct NativeClientConfig {
    ptr: NativePtr<RDKafkaConf>,
}

impl NativeClientConfig {
    /// Wraps a pointer to an `RDKafkaConfig` object and returns a new `NativeClientConfig`.
    /// UNSAFE: the caller must
    /// ensure that the pointer is not null otherwise undefined behavior
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafkaConf) -> NativeClientConfig {
        unsafe {
            NativeClientConfig {
                ptr: NativePtr::from_ptr(ptr),
            }
        }
    }

    pub fn ptr(&self) -> *mut RDKafkaConf {
        self.ptr.ptr()
    }

    pub fn get(&self, key: &str) -> Result<String> {
        get_conf_property(self.ptr(), key)
    }

    pub(crate) fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut err_buf = ErrBuf::new();
        let key_c = CString::new(key)?;
        let value_c = CString::new(value)?;
        let ret = unsafe {
            rdkafka2_sys::rd_kafka_conf_set(
                self.ptr(),
                key_c.as_ptr(),
                value_c.as_ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        if let Some(err) = RDKafkaConfErrorCode::from(ret).error() {
            return Err(KafkaError::ClientConfig(*err, err_buf.to_string()));
        }
        Ok(())
    }

    // Set internal opaque pointer to the context needed in callbacks
    //pub(crate) unsafe fn enable_background_callback_triggering<C>(
    //    &mut self,
    //    //background_event_cb: fn(&NativeClient, &mut NativeEvent, &Arc<C>),
    //    background_event_cb: unsafe extern "C" fn(*mut RDKafka, *mut RDKafkaEvent, *mut c_void),
    //    opaque: &Arc<C>,
    //) where
    //    C: Sized,
    //{
    //    unsafe {
    //        // C: Sized and this check should ensure that
    //        // we are not passing a fat pointer here
    //        assert_eq!(size_of::<*const C>(), size_of::<*const c_void>());
    //        // Set a pointer to the application context
    //        // that can be used by internal callbacks
    //        rdkafka2_sys::rd_kafka_conf_set_opaque(self.ptr(), Arc::as_ptr(opaque) as *mut c_void);
    //        rdkafka2_sys::rd_kafka_conf_set_background_event_cb(
    //            self.ptr(),
    //            Some(std::mem::transmute(background_event_cb)),
    //        );
    //    };
    //}
}

/// Generic Kafka client configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    inner: HashMap<String, String>,
}

impl ClientConfig {
    pub fn get<Q>(&self, k: &Q) -> Option<&String>
    where
        String: Eq + Hash + Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(k)
    }

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
}

impl TryFrom<ClientConfig> for NativeClientConfig {
    type Error = KafkaError;

    fn try_from(value: ClientConfig) -> Result<Self> {
        let ClientConfig { inner } = value;
        let conf = unsafe { NativeClientConfig::from_ptr(rdkafka2_sys::rd_kafka_conf_new()) };
        for (key, value) in inner.iter() {
            conf.set(key.as_ref(), value.as_ref())?;
        }
        Ok(conf)
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            inner: HashMap::with_capacity(0),
        }
    }
}

impl<K, V> FromIterator<(K, V)> for ClientConfig
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I>(iter: I) -> ClientConfig
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut config = ClientConfig::default();
        config
            .inner
            .extend(iter.into_iter().map(|(k, v)| (k.into(), v.into())));
        config
    }
}

impl<K, V> Extend<(K, V)> for ClientConfig
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

impl IntoIterator for ClientConfig {
    type Item = (String, String);

    type IntoIter = IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchecked_conf_property() {
        let conf = unsafe {
            let cfg_handler = rdkafka2_sys::rd_kafka_conf_new();
            NativeClientConfig::from_ptr(cfg_handler)
        };

        conf.set("bootstrap.servers", "localhost:9092")
            .expect("prop to be set");

        assert_eq!(conf.get("allow.auto.create.topics").unwrap(), "false");
        assert_eq!(conf.get("bootstrap.servers").unwrap(), "localhost:9092");
    }
}
