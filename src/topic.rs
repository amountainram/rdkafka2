use crate::{
    IntoOpaque,
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::{ErrBuf, check_rdkafka_invalid_arg},
};
use rdkafka2_sys::{
    RDKafkaConfErrorCode, rd_kafka_DeleteTopic_t, rd_kafka_NewTopic_t, rd_kafka_topic_conf_t,
    rd_kafka_topic_t,
};
use std::{collections::HashMap, ffi::CString, fmt, marker::PhantomData};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone)]
pub struct NativeTopic(NativePtr<rd_kafka_topic_t>);

impl From<&NativeTopic> for NativeTopic {
    fn from(value: &NativeTopic) -> Self {
        value.clone()
    }
}

impl NativeTopic {
    pub(crate) fn ptr(&self) -> *mut rd_kafka_topic_t {
        self.0.ptr()
    }

    pub(crate) fn from_ptr(ptr: NativePtr<rd_kafka_topic_t>) -> Self {
        Self(ptr)
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct NativeTopicConf(NativePtr<rd_kafka_topic_conf_t>);

impl NativeTopicConf {
    pub(crate) fn ptr(&self) -> *mut rd_kafka_topic_conf_t {
        self.0.ptr()
    }

    pub(crate) unsafe fn from_ptr(ptr: *mut rd_kafka_topic_conf_t) -> Self {
        unsafe { Self(NativePtr::from_ptr(ptr)) }
    }
}

impl NativeTopicConf {
    pub(crate) fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut err_buf = ErrBuf::new();
        let key_c = CString::new(key)?;
        let value_c = CString::new(value)?;
        let ret = unsafe {
            rdkafka2_sys::rd_kafka_topic_conf_set(
                self.0.ptr(),
                key_c.as_ptr(),
                value_c.as_ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        if let Some(err) = RDKafkaConfErrorCode::from(ret).error() {
            return Err(KafkaError::ClientConfig(*err, err_buf.to_string()));
        }
        Ok(())
    }
}

pub(crate) type NativeNewTopic = NativePtr<rd_kafka_NewTopic_t>;

/// An assignment of partitions to replicas.
///
/// Each element in the outer slice corresponds to the partition with that
/// index. The inner slice specifies the broker IDs to which replicas of that
/// partition should be assigned.
pub type PartitionAssignment = Vec<Vec<i32>>;

/// Replication configuration for a new topic.
#[derive(Debug)]
pub enum TopicReplication {
    /// All partitions should use the same fixed replication factor.
    Fixed(i32),
    /// Each partition should use the replica assignment from
    /// `PartitionAssignment`.
    Variable(PartitionAssignment),
}

impl Default for TopicReplication {
    fn default() -> Self {
        Self::Fixed(1)
    }
}

/// Configuration for a CreateTopic operation.
#[derive(Debug, TypedBuilder)]
pub struct NewTopic {
    /// The name of the new topic.
    #[builder(setter(into))]
    pub name: String,
    /// The initial number of partitions.
    #[builder(default = 1)]
    pub num_partitions: i32,
    /// The initial replication configuration.
    #[builder(default)]
    pub replication: TopicReplication,
    /// The initial configuration parameters for the topic.
    #[builder(default)]
    pub config: HashMap<String, String>,
}

impl<S> From<S> for NewTopic
where
    S: Into<String>,
{
    fn from(name: S) -> Self {
        Self::builder().name(name).build()
    }
}

pub trait Partitioner<D = ()>: Fn(&[u8], i32, D) -> i32 + Send + Sync + 'static {}

impl<D, F> Partitioner<D> for F where F: Fn(&[u8], i32, D) -> i32 + Send + Sync + 'static {}

/// Configuration for a CreateTopic operation.
#[derive(TypedBuilder)]
pub struct TopicConf<D = (), F = fn(&[u8], i32, D) -> i32>
where
    D: IntoOpaque,
    F: Partitioner<D>,
{
    /// The name of the new topic.
    #[builder(setter(into))]
    pub(crate) name: String,
    /// The initial configuration parameters for the topic.
    #[builder(default)]
    pub(crate) configuration: HashMap<String, String>,
    /// A custom partitioner function for the topic.
    #[builder(default, setter(strip_option, suffix = "_with_opaque"))]
    pub(crate) partitioner: Option<F>,
    #[builder(default, setter(skip))]
    _marker: PhantomData<D>,
}

#[allow(dead_code, non_camel_case_types, missing_docs)]
#[automatically_derived]
impl<__name, __configuration>
    TopicConfBuilder<(), Box<dyn Partitioner>, (__name, __configuration, ())>
{
    /// A custom partitioner function for the topic.
    #[allow(
        clippy::used_underscore_binding,
        clippy::no_effect_underscore_binding,
        clippy::type_complexity
    )]
    pub fn partitioner<FIn>(
        self,
        partitioner: FIn,
    ) -> TopicConfBuilder<
        (),
        Box<dyn Partitioner>,
        (__name, __configuration, (Option<Box<dyn Partitioner>>,)),
    >
    where
        FIn: Fn(&[u8], i32) -> i32 + Send + Sync + 'static,
    {
        let partitioner: Box<dyn Partitioner> =
            Box::new(move |key, partition, _| partitioner(key, partition));
        let partitioner = (Some(partitioner),);
        let (name, configuration, ()) = self.fields;
        TopicConfBuilder {
            fields: (name, configuration, partitioner),
            phantom: self.phantom,
        }
    }
}

impl<D, F> std::fmt::Debug for TopicConf<D, F>
where
    D: IntoOpaque,
    F: Partitioner<D> + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic")
            .field("name", &self.name)
            .field("config", &self.configuration)
            .field("partitioner", &self.partitioner)
            .finish()
    }
}

impl<D, F> TopicConf<D, F>
where
    D: IntoOpaque,
    F: Partitioner<D>,
{
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<S> From<S> for TopicConf
where
    S: Into<String>,
{
    fn from(name: S) -> Self {
        Self {
            name: name.into(),
            configuration: Default::default(),
            partitioner: Default::default(),
            _marker: PhantomData,
        }
    }
}

impl NewTopic {
    /// Sets a new parameter in the initial topic configuration.
    pub fn insert<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.config.insert(key.into(), value.into());
        self
    }

    pub(super) fn to_native(&self, err_buf: &mut ErrBuf) -> Result<NativeNewTopic> {
        let name = CString::new(self.name.as_str())?;
        let repl = match &self.replication {
            TopicReplication::Fixed(n) => *n,
            TopicReplication::Variable(partitions) => {
                if partitions.len() as i32 != self.num_partitions {
                    return Err(KafkaError::AdminOpCreation(format!(
                        "replication configuration for topic '{}' assigns {} partition(s), \
                         which does not match the specified number of partitions ({})",
                        self.name,
                        partitions.len(),
                        self.num_partitions,
                    )));
                }
                -1
            }
        };
        // N.B.: we wrap topic immediately, so that it is destroyed via the
        // NativeNewTopic's Drop implementation if replica assignment or config
        // installation fails.
        let topic = unsafe {
            rdkafka2_sys::rd_kafka_NewTopic_new(
                name.as_ptr(),
                self.num_partitions,
                repl,
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        if topic.is_null() {
            return Err(KafkaError::AdminOpCreation(err_buf.to_string()));
        }

        let topic = unsafe { NativeNewTopic::from_ptr(topic) };

        if let TopicReplication::Variable(assignment) = &self.replication {
            for (partition_id, broker_ids) in assignment.iter().enumerate() {
                let res = unsafe {
                    rdkafka2_sys::rd_kafka_NewTopic_set_replica_assignment(
                        topic.ptr(),
                        partition_id as i32,
                        broker_ids.as_ptr() as *mut i32,
                        broker_ids.len(),
                        err_buf.as_mut_ptr(),
                        err_buf.capacity(),
                    )
                };
                check_rdkafka_invalid_arg(res, "topic replica assignments", err_buf)?;
            }
        }
        for (key, val) in self.config.iter() {
            let key_c = CString::new(key.as_str())?;
            let val_c = CString::new(val.as_str())?;
            let res = unsafe {
                rdkafka2_sys::rd_kafka_NewTopic_set_config(
                    topic.ptr(),
                    key_c.as_ptr(),
                    val_c.as_ptr(),
                )
            };
            check_rdkafka_invalid_arg(res, "topic config", err_buf)?;
        }
        Ok(topic)
    }
}

pub(crate) type NativeDeleteTopic = NativePtr<rd_kafka_DeleteTopic_t>;

#[derive(Debug)]
pub struct DeleteTopic(String);

impl<S> From<S> for DeleteTopic
where
    S: Into<String>,
{
    fn from(name: S) -> Self {
        Self(name.into())
    }
}

impl DeleteTopic {
    pub(super) fn to_native(&self) -> Result<NativeDeleteTopic> {
        let name = CString::new(self.0.as_str())?;
        Ok(unsafe {
            NativeDeleteTopic::from_ptr(rdkafka2_sys::rd_kafka_DeleteTopic_new(name.as_ptr()))
        })
    }
}
