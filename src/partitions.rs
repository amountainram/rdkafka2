use crate::{
    KafkaError, Result, client::TopicMetadata, ptr::NativePtr, topic::PartitionAssignments,
    util::ErrBuf,
};
use rdkafka2_sys::{RDKafkaErrorCode, rd_kafka_NewPartitions_t};
use std::ffi::CString;
use typed_builder::TypedBuilder;

pub(crate) type NativeNewPartitions = NativePtr<rd_kafka_NewPartitions_t>;

/// Configuration for a CreateTopic operation.
#[derive(Debug, Clone, PartialEq, TypedBuilder)]
pub struct NewPartitions {
    /// The name of the new topic.
    pub topic_metadata: TopicMetadata,
    /// The required number of partitions with their replica assignments.
    #[builder(setter(into))]
    pub partitions: PartitionAssignments,
}

impl NewPartitions {
    pub(super) fn to_native(&self, err_buf: &mut ErrBuf) -> Result<NativeNewPartitions> {
        let current_partitions = self.topic_metadata.partitions.len();
        let next_partitions = self.partitions.as_ref();
        let topic = CString::new(self.topic_metadata.name.as_str())?;
        let new_partitions = unsafe {
            rdkafka2_sys::rd_kafka_NewPartitions_new(
                topic.as_ptr(),
                current_partitions + next_partitions.len(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        if new_partitions.is_null() {
            return Err(KafkaError::AdminOpCreation(err_buf.to_string()));
        }

        let new_partitions = next_partitions
            .iter()
            .enumerate()
            .map(Ok::<_, KafkaError>)
            .try_fold(new_partitions, |ptr, next| {
                let (partition, assignments) = next?;

                if !assignments.is_empty() {
                    let ret = unsafe {
                        rdkafka2_sys::rd_kafka_NewPartitions_set_replica_assignment(
                            ptr,
                            partition as i32,
                            assignments.as_ptr() as *mut _,
                            assignments.len(),
                            err_buf.as_mut_ptr(),
                            err_buf.capacity(),
                        )
                    };

                    if let Some(_) = RDKafkaErrorCode::from(ret).error() {
                        return Err(KafkaError::AdminOpCreation(err_buf.to_string()));
                    }
                }

                Ok(ptr)
            })?;

        if new_partitions.is_null() {
            return Err(KafkaError::AdminOpCreation(err_buf.to_string()));
        }

        unsafe { Ok(NativePtr::from_ptr(new_partitions)) }
    }
}
