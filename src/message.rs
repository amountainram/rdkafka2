use crate::{IntoOpaque, error::KafkaError, ptr::NativePtr, util};
use rdkafka2_sys::{RDKafkaErrorCode, RDKafkaHeaders, RDKafkaMessage};
use std::{ffi::CStr, marker::PhantomData, str::Utf8Error};
use typed_builder::TypedBuilder;

pub type DeliveryResult<'a> = Result<BorrowedMessage<'a>, (KafkaError, BorrowedMessage<'a>)>;

/// Timestamp of a Kafka message.
#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Clone, Copy)]
pub enum Timestamp {
    /// Timestamp not available.
    NotAvailable,
    /// Message creation time.
    CreateTime(i64),
    /// Log append time.
    LogAppendTime(i64),
}

/// A zero-copy collection of Kafka message headers.
///
/// Provides a read-only access to headers owned by a Kafka consumer or producer
/// or by an [`OwnedHeaders`] struct.
pub struct BorrowedHeaders;

impl BorrowedHeaders {
    unsafe fn from_native_ptr<T>(
        _owner: &T,
        headers_ptr: *mut rdkafka2_sys::rd_kafka_headers_t,
    ) -> &BorrowedHeaders {
        unsafe { &*(headers_ptr as *mut BorrowedHeaders) }
    }

    fn as_native_ptr(&self) -> *const RDKafkaHeaders {
        self as *const BorrowedHeaders as *const RDKafkaHeaders
    }

    /// Clones the content of `BorrowedHeaders` and returns an [`OwnedHeaders`]
    /// that can outlive the consumer.
    ///
    /// This operation requires memory allocation and can be expensive.
    pub fn detach(&self) -> OwnedHeaders {
        OwnedHeaders(unsafe {
            NativePtr::from_ptr(rdkafka2_sys::rd_kafka_headers_copy(self.as_native_ptr()))
        })
    }
}

/// A collection of Kafka message headers that owns its backing data.
///
/// Kafka supports associating an array of key-value pairs to every message,
/// called message headers. The `OwnedHeaders` can be used to create the desired
/// headers and to pass them to the producer. See also [`BorrowedHeaders`].
#[derive(Debug)]
#[repr(transparent)]
pub struct OwnedHeaders(NativePtr<RDKafkaHeaders>);

unsafe impl Send for OwnedHeaders {}

unsafe impl Sync for OwnedHeaders {}

impl OwnedHeaders {
    pub(crate) fn ptr(&self) -> *mut RDKafkaHeaders {
        self.0.ptr()
    }
}

impl Clone for OwnedHeaders {
    fn clone(&self) -> Self {
        unsafe {
            OwnedHeaders(NativePtr::from_ptr(rdkafka2_sys::rd_kafka_headers_copy(
                self.0.ptr(),
            )))
        }
    }
}

#[repr(transparent)]
pub struct BorrowedMessage<'a> {
    ptr: NativePtr<RDKafkaMessage>,
    _marker: PhantomData<&'a u8>,
}

impl<'a> BorrowedMessage<'a> {
    /// Creates a new `BorrowedMessage` that wraps the native Kafka message
    /// pointer returned via the delivery report event. The lifetime of
    /// the message will be bound to the lifetime of the client passed as
    /// parameter.
    pub(crate) unsafe fn from_dr_event(ptr: *mut RDKafkaMessage) -> DeliveryResult<'a> {
        let borrowed_message = unsafe {
            BorrowedMessage {
                ptr: NativePtr::from_ptr(ptr),
                _marker: PhantomData,
            }
        };
        let error: RDKafkaErrorCode = unsafe { (*ptr).err.into() };
        if let Some(err) = error.error() {
            Err((KafkaError::MessageProduction(err), borrowed_message))
        } else {
            Ok(borrowed_message)
        }
    }

    fn key(&self) -> Option<&[u8]> {
        unsafe {
            let rkmessage = *self.ptr.ptr();
            util::ptr_to_opt_slice(rkmessage.key, rkmessage.key_len)
        }
    }

    fn payload(&self) -> Option<&[u8]> {
        unsafe {
            let rkmessage = *self.ptr.ptr();
            util::ptr_to_opt_slice(rkmessage.payload, rkmessage.len)
        }
    }

    fn topic(&self) -> Result<&str, Utf8Error> {
        unsafe {
            let rkmessage = *self.ptr.ptr();
            CStr::from_ptr(rdkafka2_sys::rd_kafka_topic_name(rkmessage.rkt)).to_str()
        }
    }

    fn timestamp(&self) -> Timestamp {
        let mut timestamp_type =
            rdkafka2_sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let timestamp = unsafe {
            rdkafka2_sys::rd_kafka_message_timestamp(self.ptr.ptr(), &mut timestamp_type)
        };
        if timestamp == -1 {
            Timestamp::NotAvailable
        } else {
            match timestamp_type {
                rdkafka2_sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => {
                    Timestamp::NotAvailable
                }
                rdkafka2_sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => {
                    Timestamp::CreateTime(timestamp)
                }
                rdkafka2_sys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                    Timestamp::LogAppendTime(timestamp)
                }
            }
        }
    }

    fn partition(&self) -> i32 {
        let rkmessage = unsafe { *self.ptr.ptr() };
        rkmessage.partition
    }

    fn offset(&self) -> i64 {
        let rkmessage = unsafe { *self.ptr.ptr() };
        rkmessage.offset
    }

    fn headers(&self) -> Option<&BorrowedHeaders> {
        let mut native_headers_ptr = std::ptr::null_mut();
        let rkmessage = self.ptr.ptr();
        unsafe {
            let err = rdkafka2_sys::rd_kafka_message_headers(rkmessage, &mut native_headers_ptr);
            match err.into() {
                RDKafkaErrorCode::NoError => {
                    Some(BorrowedHeaders::from_native_ptr(self, native_headers_ptr))
                }
                RDKafkaErrorCode::NoEnt => None,
                _ => None,
            }
        }
    }

    /// Clones the content of the `BorrowedMessage` and returns an
    /// [`OwnedMessage`] that can outlive the consumer.
    ///
    /// This operation requires memory allocation and can be expensive.
    pub fn try_detach(&self) -> Result<OwnedMessage, Utf8Error> {
        Ok(OwnedMessage {
            key: self.key().map(|k| k.to_vec()),
            payload: self.payload().map(|p| p.to_vec()),
            topic: self.topic()?.to_owned(),
            timestamp: self.timestamp(),
            partition: self.partition(),
            offset: self.offset(),
            headers: self.headers().map(BorrowedHeaders::detach),
        })
    }
}

#[derive(Debug, TypedBuilder)]
pub struct BaseRecord<'a, D: IntoOpaque + Default = ()> {
    /// Required destination topic.
    pub topic: &'a str,
    /// Optional destination partition.
    #[builder(default = rdkafka2_sys::RD_KAFKA_PARTITION_UA)]
    pub partition: i32,
    /// Optional payload.
    #[builder(default, setter(strip_option, into))]
    pub payload: Option<&'a [u8]>,
    /// Optional key.
    #[builder(default, setter(strip_option, into))]
    pub key: Option<&'a [u8]>,
    /// Optional timestamp.
    ///
    /// Note that Kafka represents timestamps as the number of milliseconds
    /// since the Unix epoch.
    #[builder(default, setter(strip_option, into))]
    pub timestamp: Option<i64>,
    /// Optional message headers.
    #[builder(default, setter(strip_option))]
    pub headers: Option<OwnedHeaders>,
    /// Required delivery opaque (defaults to `()` if not required).
    #[builder(default)]
    pub delivery_opaque: D,
}

/// A Kafka message that owns its backing data.
///
/// An `OwnedMessage` can be created from a [`BorrowedMessage`] using the
/// [`BorrowedMessage::detach`] method. `OwnedMessage`s don't hold any reference
/// to the consumer and don't use any memory inside the consumer buffer.
#[derive(Debug, Clone, TypedBuilder)]
pub struct OwnedMessage {
    #[builder(default)]
    payload: Option<Vec<u8>>,
    #[builder(default)]
    key: Option<Vec<u8>>,
    topic: String,
    timestamp: Timestamp,
    #[builder(default = rdkafka2_sys::RD_KAFKA_PARTITION_UA)]
    partition: i32,
    offset: i64,
    #[builder(default)]
    headers: Option<OwnedHeaders>,
}

impl OwnedMessage {
    /// Creates a new message with the specified content.
    ///
    /// This function is mainly useful in tests of `rust-rdkafka` itself.
    pub fn new(
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        topic: String,
        timestamp: Timestamp,
        partition: i32,
        offset: i64,
        headers: Option<OwnedHeaders>,
    ) -> OwnedMessage {
        OwnedMessage {
            payload,
            key,
            topic,
            timestamp,
            partition,
            offset,
            headers,
        }
    }

    /// Detaches the [`OwnedHeaders`] from this `OwnedMessage`.
    pub fn detach_headers(&mut self) -> Option<OwnedHeaders> {
        self.headers.take()
    }

    /// Replaces the [`OwnedHeaders`] on this `OwnedMessage`.
    pub fn replace_headers(mut self, headers: Option<OwnedHeaders>) -> Self {
        if let Some(headers) = headers {
            self.headers.replace(headers);
        } else {
            self.headers = None;
        }
        self
    }

    /// Sets the payload for this `OwnedMessage`.
    pub fn set_payload(mut self, payload: Option<Vec<u8>>) -> Self {
        if let Some(payload) = payload {
            self.payload.replace(payload);
        } else {
            self.payload = None;
        }
        self
    }

    /// Sets the key for this `OwnedMessage`.
    pub fn set_key(mut self, key: Option<Vec<u8>>) -> Self {
        if let Some(key) = key {
            self.key.replace(key);
        } else {
            self.key = None;
        }
        self
    }

    /// Sets the topic for this `OwnedMessage`.
    pub fn set_topic(mut self, topic: String) -> Self {
        self.topic = topic;
        self
    }

    /// Sets the timestamp for this `OwnedMessage`.
    pub fn set_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Sets the partition for this `OwnedMessage`.
    pub fn set_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Sets the offset for this `OwnedMessage`.
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }
}
