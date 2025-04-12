use super::{ClientContext, DefaultClientContext, NativeClient, native::Topic};
use crate::{
    KafkaError, RDKafkaLogLevel, Timeout,
    config::{ClientConfig, NativeClientConfig},
    error::Result,
    ptr::{KafkaDrop, NativePtr},
    util::ErrBuf,
};
pub use cluster::*;
use futures::{FutureExt, future::BoxFuture};
use log::error;
use rdkafka2_sys::{
    RDKafka, RDKafkaAdminOp, RDKafkaAdminOptions, RDKafkaErrorCode, RDKafkaEvent, RDKafkaEventType,
    RDKafkaMetadata, RDKafkaQueue, RDKafkaType,
};
use std::{ffi::c_void, sync::Arc};
use tokio::sync::oneshot;
use topics::{NewTopic, TopicResult, check_rdkafka_invalid_arg};

mod cluster;
mod topics;

type NativeQueue = NativePtr<RDKafkaQueue>;

unsafe impl Send for NativeQueue {}
unsafe impl Sync for NativeQueue {}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct NativeAdminOptions(NativePtr<RDKafkaAdminOptions>);

/// Options for an admin API request.
#[derive(Default)]
pub struct AdminOptions {
    request_timeout: Option<Timeout>,
    operation_timeout: Option<Timeout>,
    validate_only: bool,
    broker_id: Option<i32>,
}

impl AdminOptions {
    fn to_native(
        &self,
        op: RDKafkaAdminOp,
        client: *mut RDKafka,
        err_buf: &mut ErrBuf,
    ) -> Result<(NativeAdminOptions, oneshot::Receiver<NativeEvent>)> {
        let native_opts = unsafe {
            NativeAdminOptions(NativePtr::from_ptr(
                rdkafka2_sys::rd_kafka_AdminOptions_new(client, op),
            ))
        };

        if let Some(timeout) = self.request_timeout {
            let res = unsafe {
                rdkafka2_sys::rd_kafka_AdminOptions_set_request_timeout(
                    native_opts.0.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, "admin request timeout", err_buf)?;
        }

        if let Some(timeout) = self.operation_timeout {
            let res = unsafe {
                rdkafka2_sys::rd_kafka_AdminOptions_set_operation_timeout(
                    native_opts.0.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, "admin operation timeout", err_buf)?;
        }

        if self.validate_only {
            let res = unsafe {
                rdkafka2_sys::rd_kafka_AdminOptions_set_validate_only(
                    native_opts.0.ptr(),
                    1, // true
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, "admin validation flag", err_buf)?;
        }

        if let Some(broker_id) = self.broker_id {
            let res = unsafe {
                rdkafka2_sys::rd_kafka_AdminOptions_set_broker(
                    native_opts.0.ptr(),
                    broker_id,
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, "admin broker", err_buf)?;
        }

        let (tx, rx) = oneshot::channel();
        let tx = Box::into_raw(Box::new(tx)) as *mut c_void;
        unsafe { rdkafka2_sys::rd_kafka_AdminOptions_set_opaque(native_opts.0.ptr(), tx) };

        Ok((native_opts, rx))
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct NativeEvent(NativePtr<RDKafkaEvent>);

impl NativeEvent {
    unsafe fn from_ptr(ptr: *mut RDKafkaEvent) -> Self {
        Self(unsafe { NativePtr::from_ptr(ptr) })
    }
}

unsafe impl Send for NativeEvent {}

#[derive(Debug)]
pub struct AdminClient<C = DefaultClientContext> {
    inner: Arc<NativeClient<C>>,
    queue: Arc<NativeQueue>,
}

impl<C> AdminClient<C>
where
    C: ClientContext,
{
    unsafe extern "C" fn admin_event_cb(
        _rk: *mut RDKafka,
        rkev: *mut RDKafkaEvent,
        _opaque: *mut c_void,
    ) {
        let r#type = RDKafkaEventType::try_from(unsafe { rdkafka2_sys::rd_kafka_event_type(rkev) })
            .inspect_err(|err| {
                error!(
                    "background queue received an unknown event with number {}",
                    err.number
                );
            });

        if let Ok(ret) = r#type {
            let tx = unsafe {
                let opaque = rdkafka2_sys::rd_kafka_event_opaque(rkev);
                Box::from_raw(opaque as *mut oneshot::Sender<NativeEvent>)
            };

            match ret {
                RDKafkaEventType::CreateTopicsResult | RDKafkaEventType::DescribeClusterResult => {
                    let _ = tx.send(unsafe { NativeEvent::from_ptr(rkev) });
                }
                _ => unimplemented!(),
            }
        }
    }

    pub(crate) fn try_from_parts(
        config: ClientConfig,
        native_config: NativeClientConfig,
        context: Arc<C>,
        log_level: Option<RDKafkaLogLevel>,
    ) -> Result<Self> {
        unsafe {
            // extern "C" copies the value of the [`delivery_opaque_ptr`]
            rdkafka2_sys::rd_kafka_conf_set_background_event_cb(
                native_config.ptr(),
                Some(Self::admin_event_cb),
            );
        }

        let native_client = NativeClient::builder()
            .config(config)
            .native_config(native_config)
            .with_log_level(log_level)
            .rd_type(RDKafkaType::RD_KAFKA_PRODUCER)
            .context(context)
            .try_build()?;
        let admin_client = Self {
            queue: unsafe {
                NativeQueue::from_ptr(rdkafka2_sys::rd_kafka_queue_get_background(
                    native_client.native_ptr(),
                ))
            }
            .into(),
            inner: native_client.into(),
        };

        Ok(admin_client)
    }
}

impl<C> AdminClient<C> {
    /// Creates new topics according to the provided `NewTopic` specifications.
    ///
    /// Note that while the API supports creating multiple topics at once, it
    /// is not transactional. Creation of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub async fn create_topics<I>(&self, topics: I, opts: AdminOptions) -> Result<Vec<TopicResult>>
    where
        I: IntoIterator,
        I::Item: Into<NewTopic>,
    {
        let client = self.inner.native_ptr();
        let (topics, mut err_buf) = topics.into_iter().map(Ok::<_, KafkaError>).try_fold(
            (vec![], ErrBuf::new()),
            |(mut topics, mut err_buf), next| {
                err_buf.clear();
                topics.push(next?.into().to_native(&mut err_buf)?);
                Ok::<_, KafkaError>((topics, err_buf))
            },
        )?;

        err_buf.clear();
        let (opts, rx) = opts.to_native(
            RDKafkaAdminOp::RD_KAFKA_ADMIN_OP_CREATETOPICS,
            client,
            &mut err_buf,
        )?;

        unsafe {
            rdkafka2_sys::rd_kafka_CreateTopics(
                client,
                topics.as_c_array(),
                topics.len(),
                opts.0.ptr(),
                self.queue.ptr(),
            );
        }

        topics::handle_create_topics_result(rx).await
    }

    pub async fn describe_cluster(&self, opts: AdminOptions) -> Result<Cluster> {
        let client = self.inner.native_ptr();

        let mut err_buf = ErrBuf::new();
        let (opts, rx) = opts.to_native(
            RDKafkaAdminOp::RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER,
            client,
            &mut err_buf,
        )?;

        unsafe {
            rdkafka2_sys::rd_kafka_DescribeCluster(client, opts.0.ptr(), self.queue.ptr());
        }

        cluster::handle_describe_cluster_result(rx).await
    }

    pub fn topic_from_name(&self, name: &str) -> Result<Topic> {
        self.inner.topic_from_name(name)
    }

    pub fn blocking_metadata<T>(&self, timeout: T) -> Result<Metadata>
    where
        T: Into<Timeout>,
    {
        const ALL_TOPICS: i32 = 1;

        unsafe {
            let mut metadata_ptr = std::ptr::null_mut() as *mut RDKafkaMetadata;
            let metadata_ptr_ptr = &mut metadata_ptr as *const *mut RDKafkaMetadata;
            let err = rdkafka2_sys::rd_kafka_metadata(
                self.inner.native_ptr(),
                ALL_TOPICS,
                std::ptr::null_mut(),
                metadata_ptr_ptr as *mut *const _,
                timeout.into().as_millis(),
            );

            if let Some(err) = RDKafkaErrorCode::from(err).error() {
                return Err(KafkaError::MetadataFetch(err));
            }

            let metadata = NativeMetadata::from_ptr(metadata_ptr);
            cluster::handle_metadata_result(metadata.ptr() as *const RDKafkaMetadata)
                .map_err(KafkaError::MetadataFetch)
        }
    }

    pub fn blocking_metadata_for_topic<T>(&self, topic: Topic, timeout: T) -> Result<Metadata>
    where
        T: Into<Timeout>,
    {
        const SELECTED_TOPIC: i32 = 0;

        unsafe {
            let mut metadata_ptr = std::ptr::null_mut() as *mut RDKafkaMetadata;
            let metadata_ptr_ptr = &mut metadata_ptr as *const *mut RDKafkaMetadata;
            let err = rdkafka2_sys::rd_kafka_metadata(
                self.inner.native_ptr(),
                SELECTED_TOPIC,
                topic.ptr(),
                metadata_ptr_ptr as *mut *const _,
                timeout.into().as_millis(),
            );

            if let Some(err) = RDKafkaErrorCode::from(err).error() {
                return Err(KafkaError::MetadataFetch(err));
            }

            let metadata = NativeMetadata::from_ptr(metadata_ptr);
            cluster::handle_metadata_result(metadata.ptr() as *const RDKafkaMetadata)
                .map_err(KafkaError::MetadataFetch)
        }
    }
}

impl<C> AdminClient<C>
where
    C: Send + Sync + 'static,
{
    #[cfg(feature = "tokio")]
    pub async fn metadata<T>(&self, timeout: T) -> Result<Metadata>
    where
        T: Into<Timeout>,
    {
        let admin = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let timeout = timeout.into();
        tokio::task::spawn_blocking(move || {
            let _ = tx.send(admin.blocking_metadata(timeout));
        });

        rx.await.map_err(|_| KafkaError::Canceled).and_then(|x| x)
    }

    #[cfg(feature = "tokio")]
    pub async fn metadata_for_topic<T>(&self, topic: Topic, timeout: T) -> Result<Metadata>
    where
        T: Into<Timeout>,
    {
        let admin = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let timeout = timeout.into();
        tokio::task::spawn_blocking(move || {
            let _ = tx.send(admin.blocking_metadata_for_topic(topic, timeout));
        });

        rx.await.map_err(|_| KafkaError::Canceled).and_then(|x| x)
    }
}

impl<C> Clone for AdminClient<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            queue: self.queue.clone(),
        }
    }
}

pub(super) trait AsCArray<T> {
    fn as_c_array(&self) -> *mut *mut T;
}

impl<T> AsCArray<T> for Vec<NativePtr<T>>
where
    T: KafkaDrop,
{
    fn as_c_array(&self) -> *mut *mut T {
        self.as_ptr() as *mut *mut T
    }
}

impl From<String> for NewTopic {
    fn from(name: String) -> Self {
        NewTopic::builder().name(name).build()
    }
}

pub(super) fn and_then_event(
    test: RDKafkaEventType,
) -> impl Fn(NativeEvent) -> BoxFuture<'static, Result<(RDKafkaEventType, NativeEvent)>> {
    move |evt| {
        let r#type = unsafe { rdkafka2_sys::rd_kafka_event_type(evt.0.ptr()) };
        let ret = RDKafkaEventType::try_from(r#type)
            .map_err(|err| KafkaError::UnknownEvent(err.number))
            .and_then(|t| {
                (t == test).then_some(t).ok_or(KafkaError::InvalidEvent {
                    actual: t,
                    expected: RDKafkaEventType::CreateTopicsResult,
                })
            })
            .map(|t| (t, evt));
        async { ret }.boxed()
    }
}
