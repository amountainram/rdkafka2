use log::trace;
use std::{
    ffi::c_void,
    marker::PhantomPinned,
    ptr::{self, NonNull},
    sync::Arc,
};

#[allow(clippy::missing_safety_doc)]
pub(crate) unsafe trait KafkaDrop {
    const TYPE: &'static str;
    const DROP: unsafe extern "C" fn(*mut Self);
}

unsafe impl KafkaDrop for rdkafka2_sys::RDKafka {
    const TYPE: &'static str = "rd_kafka_t";
    const DROP: unsafe extern "C" fn(*mut Self) = rdkafka2_sys::rd_kafka_destroy;
}

unsafe impl KafkaDrop for rdkafka2_sys::RDKafkaConf {
    const TYPE: &'static str = "rd_kafka_conf_t";
    const DROP: unsafe extern "C" fn(*mut Self) = rdkafka2_sys::rd_kafka_conf_destroy;
}

unsafe impl KafkaDrop for rdkafka2_sys::RDKafkaHeaders {
    const TYPE: &'static str = "rd_kafka_headers_t";
    const DROP: unsafe extern "C" fn(*mut Self) = rdkafka2_sys::rd_kafka_headers_destroy;
}

unsafe extern "C" fn no_op(_: *mut rdkafka2_sys::RDKafkaMessage) {}

unsafe impl KafkaDrop for rdkafka2_sys::RDKafkaMessage {
    const TYPE: &'static str = "rd_kafka_message_t";
    const DROP: unsafe extern "C" fn(*mut Self) = no_op;
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub(crate) struct NativePtr<T>
where
    T: KafkaDrop,
{
    ptr: NonNull<T>,
    _pin: PhantomPinned,
}

impl<T> NativePtr<T>
where
    T: KafkaDrop,
{
    /// Native structure from raw pointer.
    /// UNSAFE: the caller must
    /// ensure that the pointer is not null otherwise undefined behavior
    pub(crate) unsafe fn from_ptr(ptr: *mut T) -> Self {
        Self {
            ptr: NonNull::new(ptr).unwrap(),
            _pin: PhantomPinned,
        }
    }

    pub(crate) fn ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }
}

impl<T> Drop for NativePtr<T>
where
    T: KafkaDrop,
{
    fn drop(&mut self) {
        trace!("Destroying {}: {:?}", T::TYPE, self.ptr);
        unsafe { T::DROP(self.ptr.as_ptr()) }
        trace!("Destroyed {}: {:?}", T::TYPE, self.ptr);
    }
}

/// Converts Rust data to and from raw pointers.
///
/// This conversion is used to pass opaque objects to the C library and vice
/// versa.
pub trait IntoOpaque: Send + Sync + Sized {
    /// Converts the object into a raw pointer.
    fn into_ptr(self) -> *mut c_void;

    /// Converts the raw pointer back to the original Rust object.
    ///
    /// # Safety
    ///
    /// The pointer must be created with [into_ptr](IntoOpaque::into_ptr).
    ///
    /// Care must be taken to not call more than once if it would result
    /// in an aliasing violation (e.g. [Box]).
    unsafe fn from_ptr(_: *mut c_void) -> Self;
}

impl IntoOpaque for () {
    fn into_ptr(self) -> *mut c_void {
        ptr::null_mut()
    }

    unsafe fn from_ptr(_: *mut c_void) -> Self {}
}

impl<T> IntoOpaque for Box<T>
where
    T: Send + Sync,
{
    fn into_ptr(self) -> *mut c_void {
        Box::into_raw(self) as *mut c_void
    }

    unsafe fn from_ptr(ptr: *mut c_void) -> Self {
        unsafe { Box::from_raw(ptr as *mut T) }
    }
}

impl<T> IntoOpaque for Arc<T>
where
    T: Send + Sync,
{
    fn into_ptr(self) -> *mut c_void {
        Arc::into_raw(self) as *mut c_void
    }

    unsafe fn from_ptr(ptr: *mut c_void) -> Self {
        unsafe { Arc::from_raw(ptr as *const T) }
    }
}

macro_rules! impl_into_opaque_for {
    ($ty:tt) => {
        impl IntoOpaque for $ty {
            fn into_ptr(self) -> *mut c_void {
                // equivalent to C cast to uintptr_t
                // which is portable and avoid compiler-specific behavior
                self as usize as *mut c_void
            }

            unsafe fn from_ptr(ptr: *mut c_void) -> Self {
                ptr as usize as $ty
            }
        }
    };
}

impl_into_opaque_for!(usize);

impl_into_opaque_for!(u64);

impl_into_opaque_for!(u32);

impl_into_opaque_for!(i64);

impl_into_opaque_for!(i32);
