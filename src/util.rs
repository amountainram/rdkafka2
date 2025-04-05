use std::{
    ffi::{CStr, c_char},
    fmt::{self, Display},
};

pub(crate) struct ErrBuf {
    buf: [u8; ErrBuf::MAX_ERR_LEN],
}

impl ErrBuf {
    const MAX_ERR_LEN: usize = 512;

    pub fn new() -> ErrBuf {
        ErrBuf {
            buf: [0; ErrBuf::MAX_ERR_LEN],
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut c_char {
        self.buf.as_mut_ptr() as *mut c_char
    }

    //pub fn filled(&self) -> &[u8] {
    //    let i = self.buf.iter().position(|c| *c == 0).unwrap();
    //    &self.buf[..i + 1]
    //}
    //
    //pub fn len(&self) -> usize {
    //    self.filled().len()
    //}

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
}

impl Display for ErrBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", unsafe {
            cstr_to_owned(self.buf.as_ptr() as *const c_char)
        })
    }
}

/// Returns a tuple representing the version of `librdkafka` in hexadecimal and
/// string format.
pub fn rdkafka_version() -> (i32, String) {
    let version_number = unsafe { rdkafka2_sys::rd_kafka_version() };
    let version = unsafe { cstr_to_owned(rdkafka2_sys::rd_kafka_version_str()) };

    (version_number, version)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version() {
        assert_eq!(rdkafka_version(), (34078975, "2.8.0".into()));
    }
}
