use super::{NativeEvent, and_then_event};
use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::{ErrBuf, cstr_to_owned},
};
use futures::TryFutureExt;
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaEventType, RDKafkaNewTopic, RDKafkaRespErr, RDKafkaTopicResult,
};
use std::ffi::CString;
use tokio::sync::oneshot;
use typed_builder::TypedBuilder;

pub(super) fn check_rdkafka_invalid_arg(
    res: RDKafkaRespErr,
    context: &str,
    err_buf: &ErrBuf,
) -> Result<()> {
    match res.into() {
        RDKafkaErrorCode::NoError => Ok(()),
        RDKafkaErrorCode::InvalidArgument => {
            let msg = if err_buf.len() == 0 {
                "invalid argument".into()
            } else {
                err_buf.to_string()
            };
            Err(KafkaError::AdminOpCreation(msg))
        }
        res => Err(KafkaError::AdminOpCreation(format!(
            "setting {context} options returned unexpected error code {res}"
        ))),
    }
}

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

type NativeNewTopic = NativePtr<RDKafkaNewTopic>;

/// Configuration for a CreateTopic operation.
#[derive(Debug, TypedBuilder)]
pub struct NewTopic {
    /// The name of the new topic.
    pub name: String,
    /// The initial number of partitions.
    #[builder(default = 1)]
    pub num_partitions: i32,
    /// The initial replication configuration.
    #[builder(default)]
    pub replication: TopicReplication,
    /// The initial configuration parameters for the topic.
    #[builder(default)]
    pub config: Vec<(String, String)>,
}

impl NewTopic {
    /// Sets a new parameter in the initial topic configuration.
    pub fn set(mut self, key: String, value: String) -> Self {
        self.config.push((key, value));
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
            NativeNewTopic::from_ptr(rdkafka2_sys::rd_kafka_NewTopic_new(
                name.as_ptr(),
                self.num_partitions,
                repl,
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            ))
        };

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

pub type TopicResult = Result<String, (String, RDKafkaErrorCode)>;

fn build_topic_results(topics: *const *const RDKafkaTopicResult, n: usize) -> Vec<TopicResult> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic = unsafe { *topics.add(i) };
        let name = unsafe { cstr_to_owned(rdkafka2_sys::rd_kafka_topic_result_name(topic)) };
        let err = unsafe { rdkafka2_sys::rd_kafka_topic_result_error(topic) };
        if let Some(err) = RDKafkaErrorCode::from(err).error() {
            out.push(Err((name, err)));
        } else {
            out.push(Ok(name));
        }
    }
    out
}

pub(super) async fn handle_create_topics_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<TopicResult>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::CreateTopicsResult))
        .and_then(|(_, evt)| async move {
            let res = unsafe { rdkafka2_sys::rd_kafka_event_CreateTopics_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| {
                    let mut n = 0;
                    let topics =
                        unsafe { rdkafka2_sys::rd_kafka_CreateTopics_result_topics(res, &mut n) };

                    build_topic_results(topics, n)
                })
                .ok_or(KafkaError::AdminOpCreation(
                    "error while creating topics".into(),
                ))
        })
        .await
}
