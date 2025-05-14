use crate::{
    client::{ClientContext, DefaultClientContext, NativeClient}, config::{ClientConfig, NativeClientConfig}, error::{KafkaError, Result}, message::{BaseRecord, BorrowedMessage, DeliveryResult, OwnedHeaders}, topic::{self, NativeTopic, NativeTopicConf, Partitioner, Topic, TopicRegistration}, IntoOpaque, RDKafkaLogLevel, Timeout
};
pub use builder::ProducerBuilder;
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaType,
    bindings::{rd_kafka_message_t, rd_kafka_t},
};
use std::{
    collections::HashMap, ffi::{c_void, CString}, marker::PhantomData, pin::Pin, ptr, sync::{
        atomic::{AtomicUsize, Ordering}, Arc, Mutex
    }, time::Duration
};

mod builder;

static DEFAULT_PRODUCER_POLL_INTERVAL_MS: u64 = 100;

pub trait ProducerContext: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque + Default;

    fn poll_interval() -> Duration {
        Duration::from_millis(DEFAULT_PRODUCER_POLL_INTERVAL_MS)
    }

    fn delivery_message_callback(&self, msg: DeliveryResult<'_>, opaque: Self::DeliveryOpaque);
}

#[derive(Default, Clone)]
pub struct DefaultProducerContext(DefaultClientContext);

impl ClientContext for DefaultProducerContext {}

impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery_message_callback(&self, _: DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

struct ProducerClient<C = DefaultProducerContext> {
    client: NativeClient<C>,
    topics: TopicMap,
    #[cfg(feature = "tokio")]
    _poll_task_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl<C> std::fmt::Debug for ProducerClient<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerClient").finish()
    }
}

impl<C> ProducerClient<C> {}

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

impl<C> Producer<C> {
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

    pub fn context(&self) -> &C {
        self.producer.client.context()
    }
}

#[derive(Debug, Clone)]
pub struct TopicRegistration<'a>
{
    name: &'a str,
    native_topic: NativeTopic,
}

impl TopicRegistration<'_> {
    pub fn name(&self) -> &str {
        self.name
    }
}

impl<C> Producer<C> {
    pub fn register_topic<'a, D, F>(&'a self, topic: &'a mut Topic<D, F>) -> Result<TopicRegistration<'a>> where D: IntoOpaque, F: Partitioner<D> + 'static {
        let mut topics = self
            .producer
            .topics
            .lock()
            .map_err(|_| KafkaError::TopicMapLock)?;
        let native_topic = native_topic(&mut topics, &self.producer.client, topic)?;
        let name = topic.name();
        Ok(TopicRegistration { name, native_topic })
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
        let result =
            unsafe { BorrowedMessage::from_dr_event(rkmessage as *mut rd_kafka_message_t) };

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

        let (key_ptr, key_len) = as_bytes(record.key);
        let (payload_ptr, payload_len) = as_bytes(record.payload);
        let opaque_ptr = record.delivery_opaque.into_ptr();

        let ret: RDKafkaErrorCode = unsafe {
            rdkafka2_sys::rd_kafka_producev(
                self.producer.client.native_ptr(),
                rdkafka2_sys::rd_kafka_vtype_t::RD_KAFKA_VTYPE_RKT,
                record.topic.native_topic.ptr(),
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
        S::Output: Send,
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
            rdkafka2_sys::rd_kafka_conf_set_opaque(
                native_config.ptr(),
                delivery_opaque_ptr as *mut c_void,
            );
            rdkafka2_sys::rd_kafka_conf_set_dr_msg_cb(native_config.ptr(), Some(Self::dr_msg_cb));
        }

        let native_client = NativeClient::builder()
            .rd_type(RDKafkaType::Producer)
            .config(config)
            .native_config(native_config)
            .context(context)
            .log_level(log_level.unwrap_or(RDKafkaLogLevel::Info))
            .try_build()?;

        #[cfg(feature = "tokio")]
        let poll_handle = {
            use futures::StreamExt;
            use tokio::{spawn, time::interval};
            use tokio_stream::wrappers::IntervalStream;

            let poll_interval = C::poll_interval();
            let native_client = native_client.clone();
            spawn(
                IntervalStream::new(interval(poll_interval))
                    .take_until(shutdown)
                    .map(move |_| {
                        native_client.poll(Timeout::NonBlock);
                    })
                    .collect::<()>(),
            )
        };

        let mut producer = Box::pin(ProducerClient {
            client: native_client,
            topics: Default::default(),
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

impl<C> Clone for Producer<C> {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            _ptr: self._ptr.clone(),
        }
    }
}

type TopicMeta = (
    Arc<AtomicUsize>,
    Option<*mut c_void>,
    Arc<HashMap<String, String>>,
);

type TopicMap = Arc<Mutex<HashMap<String, TopicMeta>>>;

unsafe extern "C" fn custom_partitioner(
    _rkt: *const rdkafka2_sys::rd_kafka_topic_t,
    keydata: *const c_void,
    keylen: usize,
    partition_cnt: i32,
    rkt_opaque: *mut c_void,
    _msg_opaque: *mut c_void,
) -> i32 {
    let key = unsafe { std::slice::from_raw_parts(keydata as *mut u8, keylen) };
    let partitioner = unsafe { &*(rkt_opaque as *mut Box<dyn Fn(&[u8], i32) -> i32>) };
    partitioner(key, partition_cnt)
}

enum TopicEntity<F> {
    New {
        partitioner: Option<F>,
        config: Arc<HashMap<String, String>>,
        native_conf: NativeTopicConf,
    },
    Referenced {
        counter: Arc<AtomicUsize>,
    },
}

pub(crate) fn native_topic<C, D, F>(
    topic_map: &mut HashMap<String, TopicMeta>,
    client: &NativeClient<C>,
    topic: &mut Topic<D, F>,
) -> Result<NativeTopic>
where
    D: IntoOpaque,
    F: Partitioner<D> + 'static,
{
    let Topic {
        name,
        config,
        partitioner,
        ..
    } = topic;
    let topic_name = CString::new(name.as_str()).map_err(KafkaError::Nul)?;
    let topic_entity = topic_map
        .remove(name.as_str())
        .filter(|(c, ..)| c.load(Ordering::Acquire) != 0)
        .map(|(counter, ..)| TopicEntity::Referenced { counter })
        .map(Ok::<_, KafkaError>)
        .unwrap_or_else(|| {
            let conf =
                unsafe { NativeTopicConf::from_ptr(rdkafka2_sys::rd_kafka_topic_conf_new()) };
            let conf = config
                .iter()
                .map(Ok::<_, KafkaError>)
                .try_fold(conf, |conf, next| {
                    let (k, v) = next?;
                    conf.set(k.as_str(), v.as_str())?;

                    Ok::<_, KafkaError>(conf)
                })?;
            let partitioner = if let Some(partitioner) = partitioner.take() {
                let mut partitioner: Box<Box<dyn Partitioner<D>>> =
                    Box::new(Box::new(partitioner));
                let partitioner = Box::into_raw(partitioner) as *mut c_void;
                unsafe {
                    rdkafka2_sys::rd_kafka_topic_conf_set_opaque(conf.ptr(), partitioner.clone());
                    rdkafka2_sys::rd_kafka_topic_conf_set_partitioner_cb(
                        conf.ptr(),
                        Some(custom_partitioner),
                    );
                }
                Some(partitioner)
            } else {
                None
            };

            Ok(TopicEntity::New {
                partitioner,
                config: config.clone(),
                native_conf: conf,
            })
        })?;

    match topic_entity {
        TopicEntity::Referenced { counter } => {
            let prev_count = counter.fetch_add(1, Ordering::Release);
            if prev_count > 0 && (!config.is_empty() || partitioner.is_some()) {
                //  FIXME: attempt to reinitialize a topic?
            }
            let topic = unsafe { topic::create_topic(client, &topic_name, None) }?;
            Ok(NativeTopic::from_parts(topic, counter))
        }
        TopicEntity::New {
            partitioner: next_partitioner,
            config,
            native_conf,
        } => {
            unsafe {
                next_partitioner.map(|p| {
                let prev_partitioner: Box<Box<dyn Partitioner<D>>> =
                    Box::from_raw(next_partitioner)
                })
            }
            let counter = Arc::new(AtomicUsize::new(1));
            let topic_ptr = unsafe { topic::create_topic(client, &topic_name, Some(native_conf)) }?;
            topic_map.insert(name.to_string(), (counter.clone(), partitioner, config));
            Ok(NativeTopic::from_parts(topic_ptr, counter))
        }
    }
}
