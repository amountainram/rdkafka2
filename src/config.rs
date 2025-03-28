use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::{ErrBuf, cstr_to_owned},
};
use rdkafka2_sys::{RDKafkaConf, RDKafkaConfErrorCode};
use std::{
    borrow::Borrow,
    collections::HashMap,
    ffi::{CString, c_char},
    hash::Hash,
    ptr::null_mut,
};

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

    /// Returns the pointer to the librdkafka RDKafkaConf structure.
    pub fn ptr(&self) -> *mut RDKafkaConf {
        self.ptr.ptr()
    }

    /// Gets the value of a parameter in the configuration.
    ///
    /// This method reflects librdkafka's view of the current value of the
    /// parameter. If the parameter was overridden by the user, it returns the
    /// user-specified value. Otherwise, it returns librdkafka's default value
    /// for the parameter.
    pub fn get(&self, key: &str) -> Result<String> {
        let make_err = |res| KafkaError::ClientConfig(res, key.into());
        let key_c = CString::new(key.to_string())?;

        // Call with a `NULL` buffer to determine the size of the string.
        let mut size = 0_usize;
        let res = unsafe {
            rdkafka2_sys::rd_kafka_conf_get(self.ptr(), key_c.as_ptr(), null_mut(), &mut size)
        };
        if let Some(err) = RDKafkaConfErrorCode::from(res).error() {
            return Err(make_err(*err));
        }

        // Allocate a buffer of that size and call again to get the actual
        // string.
        let mut buf = vec![0_u8; size];
        let res = unsafe {
            rdkafka2_sys::rd_kafka_conf_get(
                self.ptr(),
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
}

/// Generic Kafka client configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig<K, V> {
    inner: HashMap<K, V>,
    ///// The librdkafka logging level. Refer to [`RDKafkaLogLevel`] for the list
    ///// of available levels.
    //pub log_level: RDKafkaLogLevel,
}

impl<K, V> ClientConfig<K, V> {
    /// Gets the value of a parameter in the configuration.
    ///
    /// Returns the current value set for `key`, or `None` if no value for `key`
    /// exists.
    ///
    /// Note that this method will only ever return values that were installed
    /// by a call to [`ClientConfig::set`]. To retrieve librdkafka's default
    /// value for a parameter, build a [`NativeClientConfig`] and then call
    /// [`NativeClientConfig::get`] on the resulting object.
    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Eq + Hash + Borrow<Q>,
        Q: Hash + Eq,
    {
        self.inner.get(k)
    }

    /// Sets a parameter in the configuration.
    ///
    /// If there is an existing value for `key` in the configuration, it is
    /// overridden with the new `value`.
    pub fn set<Q, R>(&mut self, key: Q, value: R) -> &mut Self
    where
        Q: Into<K>,
        R: Into<V>,
        K: Eq + Hash,
    {
        self.inner.insert(key.into(), value.into());
        self
    }
}

impl<K, V> TryFrom<ClientConfig<K, V>> for NativeClientConfig
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    type Error = KafkaError;

    fn try_from(value: ClientConfig<K, V>) -> Result<Self> {
        let ClientConfig { inner } = value;
        let conf = unsafe { NativeClientConfig::from_ptr(rdkafka2_sys::rd_kafka_conf_new()) };
        for (key, value) in inner.iter() {
            conf.set(key.as_ref(), value.as_ref())?;
        }
        Ok(conf)
    }
}

impl<K, V> Default for ClientConfig<K, V> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K, V> FromIterator<(K, V)> for ClientConfig<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<I>(iter: I) -> ClientConfig<K, V>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut config = ClientConfig::default();
        config.inner.extend(iter);
        config
    }
}

impl<K, V> Extend<(K, V)> for ClientConfig<K, V>
where
    K: Eq + Hash,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.inner.extend(iter)
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
