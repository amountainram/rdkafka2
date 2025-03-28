use log::trace;
use std::ptr::NonNull;

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

pub(crate) struct NativePtr<T>
where
    T: KafkaDrop,
{
    ptr: NonNull<T>,
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
