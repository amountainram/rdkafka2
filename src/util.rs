use futures::FutureExt;
use rdkafka2_sys::{RDKafkaErrorCode, rd_kafka_resp_err_t};
use std::{
    ffi::{CStr, c_char, c_void},
    fmt::{self, Display},
    slice,
    time::Duration,
};

use crate::error::{KafkaError, Result};

pub(crate) struct ErrBuf {
    buf: [u8; ErrBuf::MAX_ERR_LEN],
}

impl ErrBuf {
    const MAX_ERR_LEN: usize = 512;

    pub fn new() -> ErrBuf {
        ErrBuf {
            buf: [0; Self::MAX_ERR_LEN],
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut c_char {
        self.buf.as_mut_ptr() as *mut c_char
    }

    pub fn filled(&self) -> &[u8] {
        let i = self.buf.iter().position(|c| *c == 0).unwrap();
        &self.buf[..i + 1]
    }

    pub fn len(&self) -> usize {
        self.filled().len()
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn clear(&mut self) {
        self.buf = [0; Self::MAX_ERR_LEN];
    }
}

impl Display for ErrBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", unsafe {
            cstr_to_owned(self.buf.as_ptr() as *const c_char)
        })
    }
}

/// Converts a C string into a [`String`].
///
/// # Safety
///
/// `cstr` must point to a valid, null-terminated C string.
pub(crate) unsafe fn cstr_to_owned(cstr: *const c_char) -> String {
    unsafe {
        CStr::from_ptr(cstr as *const c_char)
            .to_string_lossy()
            .into_owned()
    }
}

/// Converts a pointer to an array to an optional slice. If the pointer is null,
/// returns `None`.
pub(crate) unsafe fn ptr_to_opt_slice<'a, T>(ptr: *const c_void, size: usize) -> Option<&'a [T]> {
    if ptr.is_null() {
        None
    } else {
        unsafe { Some(slice::from_raw_parts::<T>(ptr as *const T, size)) }
    }
}

pub(crate) fn check_rdkafka_invalid_arg(
    res: rd_kafka_resp_err_t,
    context: &str,
    err_buf: &ErrBuf,
) -> Result<()> {
    match res.into() {
        RDKafkaErrorCode::NoError => Ok(()),
        RDKafkaErrorCode::InvalidArgument => {
            let msg = if err_buf.len() == 0 {
                "invalid argument".into()
            } else {
                err_buf.to_string()
            };
            Err(KafkaError::AdminOpCreation(msg))
        }
        res => Err(KafkaError::AdminOpCreation(format!(
            "setting {context} options returned unexpected error code {res}"
        ))),
    }
}

/// Returns a tuple representing the version of `librdkafka` in hexadecimal and
/// string format.
pub fn rdkafka_version() -> (i32, String) {
    let version_number = unsafe { rdkafka2_sys::rd_kafka_version() };
    let version = unsafe { cstr_to_owned(rdkafka2_sys::rd_kafka_version_str()) };

    (version_number, version)
}

pub trait Shutdown {
    fn subscribe(&self) -> impl Future<Output = ()> + Send + 'static;
}

impl<T> Shutdown for futures::future::Pending<T>
where
    T: Send + 'static,
{
    fn subscribe(&self) -> impl Future<Output = ()> + Send + 'static {
        self.clone().map(|_| ())
    }
}

#[cfg(feature = "tokio")]
impl Shutdown for tokio::sync::broadcast::Sender<()> {
    fn subscribe(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.subscribe();
        async move {
            let _ = rx.recv().await;
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Timeout {
    /// Time out after the specified duration elapses.
    After(Duration),
    /// Time out after the specified duration elapses.
    NonBlock,
    /// Block forever.
    Never,
}

impl Timeout {
    /// Converts a timeout to Kafka's expected representation.
    pub(crate) fn as_millis(&self) -> i32 {
        match self {
            Timeout::After(d) => d.as_millis() as i32,
            Timeout::NonBlock => 0,
            Timeout::Never => -1,
        }
    }
}

impl From<Duration> for Timeout {
    fn from(value: Duration) -> Self {
        if value.is_zero() {
            Timeout::NonBlock
        } else {
            Timeout::After(value)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayOfResults<T> {
    pub(crate) inner: Vec<Result<T>>,
}

impl<T> ArrayOfResults<T> {
    pub fn into_inner(self) -> Vec<Result<T>> {
        let ArrayOfResults { inner } = self;
        inner
    }

    pub fn flatten(self) -> Result<Vec<T>> {
        let ArrayOfResults { inner } = self;
        inner.into_iter().collect()
    }
}

impl<T> ArrayOfResults<T> {
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> ArrayOfResults<ArrayOfResults<T>> {
    pub fn deep_flatten(self) -> Result<Vec<T>> {
        Ok(self
            .flatten()?
            .into_iter()
            .flat_map(|x| x.flatten())
            .flatten()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version() {
        assert_eq!(rdkafka_version(), (34078975, "2.8.0".into()));
    }
}
