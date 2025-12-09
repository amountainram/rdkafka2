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

                    if RDKafkaErrorCode::from(ret).error().is_some() {
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

/// A structure to store and manipulate a list of topics and partitions with optional offsets.
pub struct TopicPartitionList {
    ptr: NativePtr<rdkafka2_sys::rd_kafka_topic_partition_list_t>,
}

unsafe impl Send for TopicPartitionList {}

unsafe impl Sync for TopicPartitionList {}

impl Clone for TopicPartitionList {
    fn clone(&self) -> Self {
        let new_tpl = unsafe { rdkafka2_sys::rd_kafka_topic_partition_list_copy(self.ptr()) };

        unsafe { TopicPartitionList::from_ptr(new_tpl) }
    }
}

impl TopicPartitionList {
    //    /// Creates a new empty list with default capacity.
    //
    //    pub fn new() -> TopicPartitionList {
    //        TopicPartitionList::with_capacity(5)
    //    }
    //
    //    /// Creates a new empty list with the specified capacity.
    //
    //    pub fn with_capacity(capacity: usize) -> TopicPartitionList {
    //        let ptr = unsafe { rdsys::rd_kafka_topic_partition_list_new(capacity as i32) };
    //
    //        unsafe { TopicPartitionList::from_ptr(ptr) }
    //    }

    /// Returns the pointer to the internal librdkafka structure.
    pub fn ptr(&self) -> *mut rdkafka2_sys::rd_kafka_topic_partition_list_t {
        self.ptr.ptr()
    }

    /// Transforms a pointer to the native librdkafka RDTopicPartitionList into a
    /// managed `TopicPartitionList` instance.
    pub(crate) unsafe fn from_ptr(
        ptr: *mut rdkafka2_sys::rd_kafka_topic_partition_list_t,
    ) -> TopicPartitionList {
        unsafe {
            TopicPartitionList {
                ptr: NativePtr::from_ptr(ptr),
            }
        }
    }
}
//
//    /// Given a topic map, generates a new `TopicPartitionList`.
//
//    pub fn from_topic_map(
//        topic_map: &HashMap<(String, i32), Offset>,
//    ) -> KafkaResult<TopicPartitionList> {
//        let mut tpl = TopicPartitionList::with_capacity(topic_map.len());
//
//        for ((topic_name, partition), offset) in topic_map {
//            tpl.add_partition_offset(topic_name, *partition, *offset)?;
//        }
//
//        Ok(tpl)
//    }
//
//
//    /// Returns the number of elements in the list.
//
//    pub fn count(&self) -> usize {
//        self.ptr.cnt as usize
//    }
//
//    /// Returns the capacity of the list.
//
//    pub fn capacity(&self) -> usize {
//        self.ptr.size as usize
//    }
//
//    /// Adds a topic with unassigned partitions to the list.
//
//    pub fn add_topic_unassigned<'a>(&'a mut self, topic: &str) -> TopicPartitionListElem<'a> {
//        self.add_partition(topic, PARTITION_UNASSIGNED)
//    }
//
//    /// Adds a topic and partition to the list.
//
//    pub fn add_partition<'a>(
//        &'a mut self,
//
//        topic: &str,
//
//        partition: i32,
//    ) -> TopicPartitionListElem<'a> {
//        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
//
//        let tp_ptr = unsafe {
//            rdsys::rd_kafka_topic_partition_list_add(self.ptr(), topic_c.as_ptr(), partition)
//        };
//
//        unsafe { TopicPartitionListElem::from_ptr(self, &mut *tp_ptr) }
//    }
//
//    /// Adds a topic and partition range to the list.
//
//    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
//        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
//
//        unsafe {
//            rdsys::rd_kafka_topic_partition_list_add_range(
//                self.ptr(),
//                topic_c.as_ptr(),
//                start_partition,
//                stop_partition,
//            );
//        }
//    }
//
//    /// Sets the offset for an already created topic partition. It will fail if the topic partition
//
//    /// isn't in the list.
//
//    pub fn set_partition_offset(
//        &mut self,
//
//        topic: &str,
//
//        partition: i32,
//
//        offset: Offset,
//    ) -> KafkaResult<()> {
//        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
//
//        let kafka_err = match offset.to_raw() {
//            Some(offset) => unsafe {
//                rdsys::rd_kafka_topic_partition_list_set_offset(
//                    self.ptr(),
//                    topic_c.as_ptr(),
//                    partition,
//                    offset,
//                )
//            },
//
//            None => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_ARG,
//        };
//
//        if kafka_err.is_error() {
//            Err(KafkaError::SetPartitionOffset(kafka_err.into()))
//        } else {
//            Ok(())
//        }
//    }
//
//    /// Adds a topic and partition to the list, with the specified offset.
//
//    pub fn add_partition_offset(
//        &mut self,
//
//        topic: &str,
//
//        partition: i32,
//
//        offset: Offset,
//    ) -> KafkaResult<()> {
//        self.add_partition(topic, partition);
//
//        self.set_partition_offset(topic, partition, offset)
//    }
//
//    /// Given a topic name and a partition number, returns the corresponding list element.
//
//    pub fn find_partition(
//        &self,
//
//        topic: &str,
//
//        partition: i32,
//    ) -> Option<TopicPartitionListElem<'_>> {
//        let topic_c = CString::new(topic).expect("Topic name is not UTF-8");
//
//        let elem_ptr = unsafe {
//            rdsys::rd_kafka_topic_partition_list_find(self.ptr(), topic_c.as_ptr(), partition)
//        };
//
//        if elem_ptr.is_null() {
//            None
//        } else {
//            Some(unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) })
//        }
//    }
//
//    /// Sets all partitions in the list to the specified offset.
//
//    pub fn set_all_offsets(&mut self, offset: Offset) -> Result<(), KafkaError> {
//        if self.count() == 0 {
//            return Ok(());
//        }
//
//        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
//
//        for elem_ptr in slice {
//            let mut elem = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
//
//            elem.set_offset(offset)?;
//        }
//
//        Ok(())
//    }
//
//    /// Returns all the elements of the list.
//
//    pub fn elements(&self) -> Vec<TopicPartitionListElem<'_>> {
//        let mut vec = Vec::with_capacity(self.count());
//
//        if self.count() == 0 {
//            return vec;
//        }
//
//        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
//
//        for elem_ptr in slice {
//            vec.push(TopicPartitionListElem::from_ptr(self, &mut *elem_ptr));
//        }
//
//        vec
//    }
//
//    /// Returns all the elements of the list that belong to the specified topic.
//
//    pub fn elements_for_topic<'a>(&'a self, topic: &str) -> Vec<TopicPartitionListElem<'a>> {
//        let mut vec = Vec::with_capacity(self.count());
//
//        if self.count() == 0 {
//            return vec;
//        }
//
//        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
//
//        for elem_ptr in slice {
//            let tp = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
//
//            if tp.topic() == topic {
//                vec.push(tp);
//            }
//        }
//
//        vec
//    }
//
//    /// Returns a hashmap-based representation of the list.
//
//    pub fn to_topic_map(&self) -> HashMap<(String, i32), Offset> {
//        self.elements()
//            .iter()
//            .map(|elem| ((elem.topic().to_owned(), elem.partition()), elem.offset()))
//            .collect()
//    }
//}
//
//impl PartialEq for TopicPartitionList {
//    fn eq(&self, other: &TopicPartitionList) -> bool {
//        if self.count() != other.count() {
//            return false;
//        }
//
//        self.elements().iter().all(|elem| {
//            if let Some(other_elem) = other.find_partition(elem.topic(), elem.partition()) {
//                elem == &other_elem
//            } else {
//                false
//            }
//        })
//    }
//}
//
//impl Default for TopicPartitionList {
//    fn default() -> Self {
//        Self::new()
//    }
//}
//
//impl fmt::Debug for TopicPartitionList {
//    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//        write!(f, "TPL {{")?;
//
//        for (i, elem) in self.elements().iter().enumerate() {
//            if i > 0 {
//                write!(f, "; ")?;
//            }
//
//            write!(
//                f,
//                "{}/{}: offset={:?} metadata={:?}, error={:?}",
//                elem.topic(),
//                elem.partition(),
//                elem.offset(),
//                elem.metadata(),
//                elem.error(),
//            )?;
//        }
//
//        write!(f, "}}")
//    }
//}
//
