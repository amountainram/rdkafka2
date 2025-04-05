use crate::{
    IntoOpaque,
    client::{ClientContext, DefaultClientContext, NativeClient},
    config::{ClientConfig, NativeClientConfig},
    error::Result,
    log::RDKafkaLogLevel,
    time::Timeout,
};
use ::std::ffi::c_void;
pub use builder::*;
use rdkafka2_sys::{RDKafkaType, *};
use std::{pin::Pin, ptr, sync::Arc};

mod builder;

pub trait ProducerContext: ClientContext {
    //    type ConfOpaque: IntoOpaque;

    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque;

    fn delivery_message_callback(&self);
}

#[derive(Default)]
pub struct DefaultProducerContext(DefaultClientContext);

impl ClientContext for DefaultProducerContext {}

impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery_message_callback(&self) {
        // no-op
    }
}

pub struct Producer<C = DefaultProducerContext> {
    client: Arc<NativeClient<C>>,
}

impl<C> ClientContext for Producer<C> where C: ProducerContext {}

impl<C> ProducerContext for Producer<C>
where
    C: ProducerContext,
{
    type DeliveryOpaque = C::DeliveryOpaque;

    fn delivery_message_callback(&self) {
        self.client.context().delivery_message_callback();
    }
}

pub struct BoxProducer<C = DefaultProducerContext>(Pin<Box<Producer<C>>>);

impl<C> BoxProducer<C>
where
    C: ProducerContext,
{
    unsafe extern "C" fn dr_msg_cb(
        _rk: *mut rd_kafka_t,
        _rkmessage: *const rd_kafka_message_t,
        opaque: *mut c_void,
    ) {
        let opaque = opaque as *mut *mut Producer<C>;
        let producer = unsafe { &**opaque };

        producer.delivery_message_callback();
    }

    fn try_from_parts(
        config: ClientConfig,
        context: Arc<C>,
        log_level: Option<RDKafkaLogLevel>,
    ) -> Result<Self> {
        let native_config = NativeClientConfig::try_from(config.clone())?;
        let ptr: *mut *mut Producer<C> = Box::into_raw(Box::new(ptr::null_mut()));

        unsafe {
            rd_kafka_conf_set_opaque(native_config.ptr(), ptr as *mut c_void);
            rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(Self::dr_msg_cb));
        }

        let native_client = NativeClient::builder()
            .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .config(config)
            .native_config(native_config)
            .context(context)
            .log_level(log_level.unwrap_or(RDKafkaLogLevel::Info))
            .try_build()?;
        let mut producer = Box::pin(Producer {
            client: native_client.into(),
        });

        unsafe {
            *ptr = Pin::as_mut(&mut producer).get_unchecked_mut();
        }

        Ok(Self(producer))
    }
}

impl<C> Producer<C>
where
    C: ProducerContext,
{
    pub fn poll<T>(&self, timeout: T)
    where
        T: Into<Timeout>,
    {
        self.client.poll(timeout)
    }

    pub fn produce(&self) {
        todo!()
    }

    pub fn produce_batch(&self) {
        todo!()
    }

    pub fn send(&self) {
        todo!()
    }

    pub fn send_batch(&self) {
        todo!()
    }

    //#[allow(clippy::result_large_err)]
    //pub fn send<'a, K, P>(
    //    &self,
    //    mut record: BaseRecord<'a, K, P, C::DeliveryOpaque>,
    //) -> Result<(), (KafkaError, BaseRecord<'a, K, P, C::DeliveryOpaque>)>
    //where
    //    K: ToBytes + ?Sized,
    //    P: ToBytes + ?Sized,
    //{
    //    unsafe {
    //        rdkafka2_sys::rd_kafka_producev(
    //            self.client.native_ptr(),
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_TOPIC,
    //            // ptr to topic
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_PARTITION,
    //            // record.partition.unwrap_or(-1), OR RD_KAFKA_PARTITION_UA
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_MSGFLAGS,
    //            // => ??????? rdsys::RD_KAFKA_MSG_F_COPY,
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_KEY,
    //            // key_ptr,
    //            // key_len,
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_VALUE,
    //            // payload_ptr,
    //            // payload_len,
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_OPAQUE,
    //            // opaque_ptr,
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_TIMESTAMP,
    //            // record.timestamp.unwrap_or(0),
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_HEADERS,
    //            //record
    //            //    .headers
    //            //    .as_ref()
    //            //    .map_or(ptr::null_mut(), OwnedHeaders::ptr),
    //            rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_END,
    //        )
    //    }
    //}
}

#[cfg(test)]
mod tests {
    use super::Producer;

    #[test]
    fn deref_producer() {
        Producer::builder().try_build().unwrap();
    }
}
