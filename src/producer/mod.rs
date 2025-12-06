use crate::{
    IntoOpaque, RDKafkaLogLevel, Timeout,
    client::{ClientContext, DefaultClientContext, NativeClient, Topic},
    config::{ClientConfig, NativeClientConfig},
    error::{KafkaError, Result},
    message::{BaseRecord, BorrowedMessage, DeliveryResult, OwnedHeaders},
    topic::{Partitioner, TopicSettings},
};
pub use builder::ProducerBuilder;
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaType,
    bindings::{rd_kafka_message_t, rd_kafka_t},
};
use std::{ffi::c_void, pin::Pin, ptr, sync::Arc, time::Duration};

mod builder;

static DEFAULT_PRODUCER_POLL_INTERVAL_MS: u64 = 100;

/// A context trait for the Kafka producer.
///
/// It holds callbacks for asynchronous events that happen inside the
/// producer. Most notably the delivery callback that is called when a message
/// is successfully delivered or permanently failed.
///
/// The default implementation is [`DefaultProducerContext`] which does nothing
/// beside polling the internal queue at a regular interval of 100 milliseconds.
///
/// A future based implementation can be obtained using a queue implementing this
/// trait:
///
/// ```
/// use rdkafka2::{
///     client::ClientContext,
///     message::{DeliveryResult, OwnedMessage},
///     producer::{ProducerContext},
///     KafkaError,
/// };
/// use tokio::sync::mpsc;
///
/// type OwnedDeliveryResult = Result<OwnedMessage, (KafkaError, OwnedMessage)>;
///
/// struct DeliveryStreamContext {
///     tx: mpsc::UnboundedSender<OwnedDeliveryResult>,
/// }
///
/// impl ClientContext for DeliveryStreamContext {}
///
/// impl ProducerContext for DeliveryStreamContext {
///     type DeliveryOpaque = ();
///     
///     fn delivery_message_callback(&self, dr: DeliveryResult<'_>, _: Self::DeliveryOpaque) {
///         let report = match dr {
///             Ok(m) => m.try_detach().map(Ok),
///             Err((err, msg)) => msg.try_detach().map(|msg| Err((err, msg))),
///         }
///         .expect("no UTF8 errors");
///         self.tx.send(report).expect("msg to be sent");
///     }
/// }
/// ```
pub trait ProducerContext: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque + Default;

    /// The interval at which the internal producer queue is polled to trigger
    /// delivery of enqueued messages to the Kafka broker.
    ///
    /// It defaults to 100 milliseconds poll intervals.
    fn poll_interval() -> Duration {
        Duration::from_millis(DEFAULT_PRODUCER_POLL_INTERVAL_MS)
    }

    /// The callback that is called when a message has been delivered
    /// successfully or permanently failed.
    fn delivery_message_callback(&self, msg: DeliveryResult<'_>, opaque: Self::DeliveryOpaque);
}

/// The default producer context that does nothing.
#[derive(Default, Clone)]
pub struct DefaultProducerContext(DefaultClientContext);

impl ClientContext for DefaultProducerContext {}

impl ProducerContext for DefaultProducerContext {
    type DeliveryOpaque = ();

    fn delivery_message_callback(&self, _: DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}

struct ProducerClient<C = DefaultProducerContext> {
    client: NativeClient<C>,
    #[cfg(all(feature = "producer-polling", feature = "tokio"))]
    _poll_task_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl<C> std::fmt::Debug for ProducerClient<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerClient").finish()
    }
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

/// A Kafka producer.
///
/// The producer is used to send messages to Kafka topics. It is created using
/// the [`Producer::builder`] method.
///
/// To create a producer four ingredients are needed:
/// 1. A type that implements the [`ProducerContext`] trait.
/// 2. An optional [`ClientConfig`] with the necessary configuration properties.
/// 3. An optional log level ([`RDKafkaLogLevel`]) to control the verbosity of the logging.
/// 4. An optional shutdown signal to gracefully stop the internal polling loop.
///
/// The role of the `ProducerContext` is to set callbacks inside the producer.
/// Most notably the delivery callback that is called when a message is
/// successfully delivered or permanently failed.
///
/// On message [`Producer::produce`] or [`Producer::send`], the message is
/// enqueued in an internal buffer and the function returns immediately.
/// The actual delivery to the Kafka broker happens asynchronously in the
/// background. The delivery status is reported through the delivery callback
/// set in the `ProducerContext`.
///
/// ```rust,no_run
/// use rdkafka2::{
///     config::ClientConfig,
///     message::BaseRecord,
///     producer::{
///         DefaultProducerContext,
///         Producer,
///     }
/// };
///
/// let config = ClientConfig::from_iter([
///     ("bootstrap.servers", "localhost:9092"),
///     ("allow.auto.create.topics", "false"),
/// ]);
/// let producer = Producer::builder()
///     .config(config)
///     .context(DefaultProducerContext::default())
///     .try_build()
///     .unwrap();
///
/// let topic = producer
///     .register_topic("my_topic")
///     .expect("topic to be registered");
///
/// let record = BaseRecord::builder()
///     .key(r#"{"id":"1"}"#.as_bytes())
///     .payload(r#"{"id":"2"}"#.as_bytes())
///     .topic(&topic)
///     .build();
/// producer.send(record).expect("message to be produced");
/// producer.flush(std::time::Duration::from_secs(1));
/// ```
#[derive(Debug)]
pub struct Producer<C = DefaultProducerContext> {
    producer: Arc<Pin<Box<ProducerClient<C>>>>,
    _ptr: Arc<Pin<Box<*mut ProducerClient<C>>>>,
}

impl<C> Producer<C> {
    /// Polls the internal producer queue to trigger delivery of enqueued messages
    /// to the Kafka broker.
    pub fn poll<T>(&self, timeout: T) -> u64
    where
        T: Into<Timeout>,
    {
        self.producer.client.poll(timeout)
    }

    /// Blocks at most for `timeout` or until all messages in the internal
    /// producer queue are delivered to the Kafka broker.
    pub fn flush<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        self.producer.client.flush(timeout)
    }

    /// Purges any outstanding messages in the internal producer queue.
    /// This will drop any undelivered messages.
    pub fn purge<T>(&self, timeout: T) -> RDKafkaErrorCode
    where
        T: Into<Timeout>,
    {
        self.producer.client.purge(timeout)
    }

    /// Returns a reference to the producer context.
    pub fn context(&self) -> &C {
        self.producer.client.context()
    }

    /// Registers a topic with the producer.
    ///
    /// This registration is needed due to the underlying `librdkafka` API.
    /// When a topic is registered by name, with optional settings, any
    /// usage of its name will carry along those settings.
    ///
    /// Register a topic is a locking operation.
    pub fn register_topic<D, F, T>(&self, topic: T) -> Result<Topic<D>>
    where
        D: IntoOpaque,
        F: Partitioner<D> + 'static,
        T: Into<TopicSettings<D, F>>,
    {
        self.producer.client.register_topic(topic.into())
    }

    /// Checks if a partition is available for producing messages.
    pub fn partition_available(&self, topic: &Topic, partition: i32) -> bool {
        self.producer.client.partition_available(topic, partition)
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

    /// Enqueue a message for delivery to the Kafka broker.
    /// The message is enqueued in an internal buffer and the function
    /// returns immediately.
    ///
    /// The production queue is NOT polled by this method. Use [`Producer::poll`]
    /// to poll the queue and trigger delivery of enqueued messages or combine
    /// both with a single call to [`Producer::send`].
    ///
    /// This method is useful when producing a batch of messages
    /// and then polling the queue once to trigger delivery of all
    /// enqueued messages.
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
                record.topic.native_topic().ptr(),
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

    /// Enqueue a message for delivery to the Kafka broker and immediately
    /// poll the internal queue to trigger delivery of enqueued messages.
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

        #[cfg(all(feature = "producer-polling", feature = "tokio"))]
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
            #[cfg(all(feature = "producer-polling", feature = "tokio"))]
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
