use super::ProducerContext;
use crate::{
    KafkaError,
    client::{ClientContext, DefaultClientContext, NativeClient},
    config::{ClientConfig, NativeClientConfig},
    error::Result,
    log::RDKafkaLogLevel,
    message::{BaseRecord, BorrowedMessage, DeliveryResult, OwnedHeaders},
    ptr::IntoOpaque,
    util::Timeout,
};
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaMessage, RDKafkaType, rd_kafka_conf_set_dr_msg_cb,
    rd_kafka_conf_set_opaque, rd_kafka_message_t, rd_kafka_t,
};
use std::{
    ffi::{CString, c_void},
    pin::Pin,
    ptr,
    sync::Arc,
};

#[derive(Default, Clone)]
pub struct DefaultProducerContext(DefaultClientContext);

impl ClientContext for DefaultProducerContext {}

impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery_message_callback(&self, _: DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

#[derive(Debug)]
struct ProducerClient<C = DefaultProducerContext> {
    client: NativeClient<C>,
    #[cfg(feature = "tokio")]
    _poll_task_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl<C> ClientContext for ProducerClient<C> where C: ProducerContext {}

impl<C> ProducerContext for ProducerClient<C>
where
    C: ProducerContext,
{
    type DeliveryOpaque = C::DeliveryOpaque;

    fn delivery_message_callback(&self, msg: DeliveryResult<'_>, opaque: Self::DeliveryOpaque) {
        self.client.context().delivery_message_callback(msg, opaque);
    }
}

#[derive(Debug)]
pub struct Producer<C = DefaultProducerContext> {
    producer: Arc<Pin<Box<ProducerClient<C>>>>,
    _ptr: Arc<Pin<Box<*mut ProducerClient<C>>>>,
}

impl<C> Clone for Producer<C> {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            _ptr: self._ptr.clone(),
        }
    }
}

impl<C> Producer<C>
where
    C: ProducerContext,
{
    unsafe extern "C" fn dr_msg_cb(
        _rk: *mut rd_kafka_t,
        rkmessage: *const rd_kafka_message_t,
        opaque: *mut c_void,
    ) {
        let producer = unsafe { &**(opaque as *mut *mut ProducerClient<C>) };
        let delivery_opaque = unsafe { C::DeliveryOpaque::from_ptr((*rkmessage)._private) };
        let result = unsafe { BorrowedMessage::from_dr_event(rkmessage as *mut RDKafkaMessage) };

        producer.delivery_message_callback(result, delivery_opaque);
    }

    pub fn produce<'a>(
        &self,
        mut record: BaseRecord<'a, C::DeliveryOpaque>,
    ) -> Result<(), (KafkaError, BaseRecord<'a, C::DeliveryOpaque>)> {
        fn as_bytes(opt: Option<&[u8]>) -> (*mut c_void, usize) {
            match opt {
                None => (ptr::null_mut(), 0),
                Some(p) => (p.as_ptr() as *mut c_void, p.len()),
            }
        }

        let topic_cstr = CString::new(record.topic).unwrap();
        let (key_ptr, key_len) = as_bytes(record.key);
        let (payload_ptr, payload_len) = as_bytes(record.payload);
        let opaque_ptr = record.delivery_opaque.into_ptr();
        let ret: RDKafkaErrorCode = unsafe {
            rdkafka2_sys::rd_kafka_producev(
                self.producer.client.native_ptr(),
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_TOPIC,
                topic_cstr.as_ptr(),
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_PARTITION,
                record.partition,
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_MSGFLAGS,
                rdkafka2_sys::RD_KAFKA_MSG_F_COPY,
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_KEY,
                key_ptr,
                key_len,
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_VALUE,
                payload_ptr,
                payload_len,
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_OPAQUE,
                opaque_ptr,
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_TIMESTAMP,
                record.timestamp.unwrap_or(0),
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_HEADERS,
                record
                    .headers
                    .as_ref()
                    .map_or(ptr::null_mut(), OwnedHeaders::ptr),
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_END,
            )
            .into()
        };

        if let Some(error) = ret.error() {
            record.delivery_opaque = unsafe { C::DeliveryOpaque::from_ptr(opaque_ptr) };
            Err((KafkaError::MessageProduction(error), record))
        } else {
            std::mem::forget(record.headers);
            Ok(())
        }
    }

    pub fn send<'a>(
        &self,
        record: BaseRecord<'a, C::DeliveryOpaque>,
    ) -> Result<u64, (KafkaError, BaseRecord<'a, C::DeliveryOpaque>)> {
        self.produce(record)?;
        Ok(self.poll(Timeout::NonBlock))
    }

    pub fn poll<T>(&self, timeout: T) -> u64
    where
        T: Into<Timeout>,
    {
        self.producer.client.poll(timeout)
    }

    pub fn flush<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        self.producer.client.flush(timeout)
    }

    pub fn purge<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        self.producer.client.purge(timeout)
    }
}

impl<C> Producer<C>
where
    C: ProducerContext + Send + 'static,
{
    pub(super) fn try_from_parts<S>(
        config: ClientConfig,
        context: Arc<C>,
        log_level: Option<RDKafkaLogLevel>,
        #[cfg_attr(not(feature = "tokio"), allow(unused))] shutdown: S,
    ) -> Result<Self>
    where
        S: Future + Send + 'static,
    {
        let native_config = NativeClientConfig::try_from(config.clone())?;
        // This allocates a pointer onto the heap (8 bytes).
        // After that a refernce to it is used as conf opaque.
        // `ptr` is then assigned to the [`Self`] instance which
        // will drop it when done.
        //
        // MUST NOT use Box::clone since it allocates again a heap pointer
        let mut ptr: Box<*mut ProducerClient<C>> = Box::new(ptr::null_mut());
        let delivery_opaque_ptr = (&mut (*ptr)) as *mut *mut _;

        unsafe {
            // extern "C" copies the value of the [`delivery_opaque_ptr`]
            rd_kafka_conf_set_opaque(native_config.ptr(), delivery_opaque_ptr as *mut c_void);
            rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(Self::dr_msg_cb));
        }

        let native_client = NativeClient::builder()
            .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .config(config)
            .native_config(native_config)
            .context(context)
            .log_level(log_level.unwrap_or(RDKafkaLogLevel::Info))
            .try_build()?;

        #[cfg(feature = "tokio")]
        let poll_handle = {
            use std::time::Duration;

            let native_client = native_client.clone();
            tokio::spawn(async move {
                tokio::pin!(shutdown);

                let mut clock = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        _ = clock.tick() => {
                            native_client.poll(Timeout::NonBlock);
                        }
                        _ = &mut shutdown => {
                            break;
                        }
                    }
                }
            })
        };

        let mut producer = Box::pin(ProducerClient {
            client: native_client,
            #[cfg(feature = "tokio")]
            _poll_task_handle: poll_handle.into(),
        });

        unsafe {
            *delivery_opaque_ptr = Pin::as_mut(&mut producer).get_unchecked_mut();
        }

        let box_producer = Self {
            producer: Arc::new(producer),
            _ptr: Arc::new(Pin::new(ptr)),
        };
        Ok(box_producer.clone())
    }
}
