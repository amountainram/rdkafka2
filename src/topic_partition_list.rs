//! Data structures representing topic, partitions and offsets.
//!
//! Compatible with the `RDKafkaTopicPartitionList` exported by `rdkafka-sys`.

use crate::{
    error::{KafkaError, Result},
    ptr::{KafkaDrop, NativePtr},
};
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaRespErr, RDKafkaTopicPartition, RDKafkaTopicPartitionList,
};
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::slice;
use std::str::{self, FromStr};

const PARTITION_UNASSIGNED: i32 = -1;

const OFFSET_BEGINNING: i64 = rdkafka2_sys::RD_KAFKA_OFFSET_BEGINNING as i64;
const OFFSET_END: i64 = rdkafka2_sys::RD_KAFKA_OFFSET_END as i64;
const OFFSET_STORED: i64 = rdkafka2_sys::RD_KAFKA_OFFSET_STORED as i64;
const OFFSET_INVALID: i64 = rdkafka2_sys::RD_KAFKA_OFFSET_INVALID as i64;
const OFFSET_TAIL_BASE: i64 = rdkafka2_sys::RD_KAFKA_OFFSET_TAIL_BASE as i64;

/// A Kafka offset.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Offset {
    /// Start consuming from the beginning of the partition.
    Beginning,
    /// Start consuming from the end of the partition.
    End,
    /// Start consuming from the stored offset.
    Stored,
    /// Offset not assigned or invalid.
    Invalid,
    /// A specific offset to consume from.
    ///
    /// Note that while the offset is a signed integer, negative offsets will be
    /// rejected when passed to librdkafka.
    Offset(i64),
    /// An offset relative to the end of the partition.
    ///
    /// Note that while the offset is a signed integer, negative offsets will
    /// be rejected when passed to librdkafka.
    OffsetTail(i64),
}

impl Offset {
    /// Converts the integer representation of an offset used by librdkafka to
    /// an `Offset`.
    pub fn from_raw(raw_offset: i64) -> Offset {
        match raw_offset {
            OFFSET_BEGINNING => Offset::Beginning,
            OFFSET_END => Offset::End,
            OFFSET_STORED => Offset::Stored,
            OFFSET_INVALID => Offset::Invalid,
            n if n <= OFFSET_TAIL_BASE => Offset::OffsetTail(-(n - OFFSET_TAIL_BASE)),
            n => Offset::Offset(n),
        }
    }

    /// Converts the `Offset` to the internal integer representation used by
    /// librdkafka.
    ///
    /// Returns `None` if the offset cannot be represented in librdkafka's
    /// internal representation.
    pub fn to_raw(self) -> Option<i64> {
        match self {
            Offset::Beginning => Some(OFFSET_BEGINNING),
            Offset::End => Some(OFFSET_END),
            Offset::Stored => Some(OFFSET_STORED),
            Offset::Invalid => Some(OFFSET_INVALID),
            Offset::Offset(n) if n >= 0 => Some(n),
            Offset::OffsetTail(n) if n > 0 => Some(OFFSET_TAIL_BASE - n),
            Offset::Offset(_) | Offset::OffsetTail(_) => None,
        }
    }
}

// TODO: implement Debug
/// One element of the topic partition list.
pub struct TopicPartitionListElem<'a> {
    ptr: &'a mut RDKafkaTopicPartition,
}

impl<'a> TopicPartitionListElem<'a> {
    // _owner_list serves as a marker so that the lifetime isn't too long
    fn from_ptr(
        _owner_list: &'a TopicPartitionList,
        ptr: &'a mut RDKafkaTopicPartition,
    ) -> TopicPartitionListElem<'a> {
        TopicPartitionListElem { ptr }
    }

    //  FIXME:
    // Returns the topic name.
    //pub fn topic(&self) -> &str {
    //    unsafe {
    //        let c_str = self.ptr.topic;
    //        CStr::from_ptr(c_str)
    //            .to_str()
    //            .expect("Topic name is not UTF-8")
    //    }
    //}
    //
    ///// Returns the optional error associated to the specific entry in the TPL.
    //pub fn error(&self) -> KafkaResult<()> {
    //    let kafka_err = self.ptr.err;
    //    if kafka_err.is_error() {
    //        Err(KafkaError::OffsetFetch(kafka_err.into()))
    //    } else {
    //        Ok(())
    //    }
    //}
    //
    ///// Returns the partition number.
    //pub fn partition(&self) -> i32 {
    //    self.ptr.partition
    //}
    //
    ///// Returns the offset.
    //pub fn offset(&self) -> Offset {
    //    let raw_offset = self.ptr.offset;
    //    Offset::from_raw(raw_offset)
    //}
    //
    ///// Sets the offset.
    //pub fn set_offset(&mut self, offset: Offset) -> KafkaResult<()> {
    //    match offset.to_raw() {
    //        Some(offset) => {
    //            self.ptr.offset = offset;
    //            Ok(())
    //        }
    //        None => Err(KafkaError::SetPartitionOffset(
    //            RDKafkaErrorCode::InvalidArgument,
    //        )),
    //    }
    //}
    //
    ///// Returns the optional metadata associated with the entry.
    //pub fn metadata(&self) -> &str {
    //    let bytes = unsafe { util::ptr_to_slice(self.ptr.metadata, self.ptr.metadata_size) };
    //    str::from_utf8(bytes).expect("Metadata is not UTF-8")
    //}
    //
    ///// Sets the optional metadata associated with the entry.
    //pub fn set_metadata<M>(&mut self, metadata: M)
    //where
    //    M: AsRef<str>,
    //{
    //    let metadata = metadata.as_ref();
    //    let buf = unsafe { libc::malloc(metadata.len()) };
    //    unsafe { libc::memcpy(buf, metadata.as_ptr() as *const c_void, metadata.len()) };
    //    self.ptr.metadata = buf;
    //    self.ptr.metadata_size = metadata.len();
    //}
}

//impl<'a> PartialEq for TopicPartitionListElem<'a> {
//    fn eq(&self, other: &TopicPartitionListElem<'a>) -> bool {
//        self.topic() == other.topic()
//            && self.partition() == other.partition()
//            && self.offset() == other.offset()
//            && self.metadata() == other.metadata()
//    }
//}

/// A structure to store and manipulate a list of topics and partitions with optional offsets.
pub type TopicPartitionList = NativePtr<RDKafkaTopicPartitionList>;

unsafe impl KafkaDrop for RDKafkaTopicPartitionList {
    const TYPE: &'static str = "topic partition list";
    const DROP: unsafe extern "C" fn(*mut Self) =
        rdkafka2_sys::rd_kafka_topic_partition_list_destroy;
}

impl TopicPartitionList {
    /// Creates a new empty list with the specified capacity.
    pub fn with_capacity(capacity: usize) -> TopicPartitionList {
        let ptr = unsafe { rdkafka2_sys::rd_kafka_topic_partition_list_new(capacity as i32) };
        unsafe { TopicPartitionList::from_ptr(ptr) }
    }

    /// Given a topic map, generates a new `TopicPartitionList`.
    pub fn from_topic_map<'a, I>(
        topic_map: &HashMap<(String, i32), Offset>,
    ) -> Result<TopicPartitionList> {
        let mut tpl = TopicPartitionList::with_capacity(topic_map.len());
        for ((topic_name, partition), offset) in topic_map {
            tpl.add_partition_offset(topic_name, *partition, *offset)?;
        }

        Ok(tpl)
    }

    /// Returns the number of elements in the list.
    pub fn count(&self) -> usize {
        self.ptr().cnt as usize
    }

    /// Returns the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.ptr().size as usize
    }

    /// Adds a topic with unassigned partitions to the list.
    pub fn add_topic_unassigned<'a>(&'a mut self, topic: &str) -> TopicPartitionListElem<'a> {
        self.add_partition(topic, PARTITION_UNASSIGNED)
    }

    /// Adds a topic and partition to the list.
    pub fn add_partition<'a>(
        &'a mut self,
        topic: &str,
        partition: i32,
    ) -> TopicPartitionListElem<'a> {
        let topic_c = CString::from_str(topic).expect("Topic name contains a '\\0' char");
        unsafe {
            let tp_ptr = rdkafka2_sys::rd_kafka_topic_partition_list_add(
                self.ptr(),
                topic_c.as_ptr(),
                partition,
            );
            TopicPartitionListElem::from_ptr(self, &mut *tp_ptr)
        }
    }

    /// Adds a topic and partition range to the list.
    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
        let topic_c = CString::from_str(topic).expect("Topic name contains a '\\0' char");
        unsafe {
            rdkafka2_sys::rd_kafka_topic_partition_list_add_range(
                self.ptr(),
                topic_c.as_ptr(),
                start_partition,
                stop_partition,
            );
        }
    }

    /// Sets the offset for an already created topic partition. It will fail if the topic partition
    /// isn't in the list.
    pub fn set_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> Result<()> {
        let topic_c = CString::from_str(topic).expect("Topic name contains a '\\0' char");
        let kafka_err = match offset.to_raw() {
            Some(offset) => unsafe {
                rdkafka2_sys::rd_kafka_topic_partition_list_set_offset(
                    self.ptr(),
                    topic_c.as_ptr(),
                    partition,
                    offset,
                )
            },
            None => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_ARG,
        };

        if let Some(err) = RDKafkaErrorCode::from(kafka_err).error() {
            Err(KafkaError::SetPartitionOffset(*err))
        } else {
            Ok(())
        }
    }

    /// Adds a topic and partition to the list, with the specified offset.
    pub fn add_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> Result<()> {
        self.add_partition(topic, partition);
        self.set_partition_offset(topic, partition, offset)
    }

    /// Given a topic name and a partition number, returns the corresponding list element.
    pub fn find_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<TopicPartitionListElem<'_>> {
        let topic_c = CString::from_str(topic).expect("Topic name contains a '\\0' char");
        let elem_ptr = unsafe {
            rdkafka2_sys::rd_kafka_topic_partition_list_find(
                self.ptr(),
                topic_c.as_ptr(),
                partition,
            )
        };
        if elem_ptr.is_null() {
            None
        } else {
            Some(unsafe { TopicPartitionListElem::from_ptr(self, &mut *elem_ptr) })
        }
    }

    /// Sets all partitions in the list to the specified offset.
    pub fn set_all_offsets(&mut self, offset: Offset) -> Result<(), KafkaError> {
        if self.count() == 0 {
            return Ok(());
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            let mut elem = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
            elem.set_offset(offset)?;
        }
        Ok(())
    }

    /// Returns all the elements of the list.
    pub fn elements(&self) -> Vec<TopicPartitionListElem<'_>> {
        let mut vec = Vec::with_capacity(self.count());
        if self.count() == 0 {
            return vec;
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            vec.push(TopicPartitionListElem::from_ptr(self, &mut *elem_ptr));
        }
        vec
    }

    /// Returns all the elements of the list that belong to the specified topic.
    pub fn elements_for_topic<'a>(&'a self, topic: &str) -> Vec<TopicPartitionListElem<'a>> {
        let mut vec = Vec::with_capacity(self.count());
        if self.count() == 0 {
            return vec;
        }
        let slice = unsafe { slice::from_raw_parts_mut(self.ptr.elems, self.count()) };
        for elem_ptr in slice {
            let tp = TopicPartitionListElem::from_ptr(self, &mut *elem_ptr);
            if tp.topic() == topic {
                vec.push(tp);
            }
        }
        vec
    }

    /// Returns a hashmap-based representation of the list.
    pub fn to_topic_map(&self) -> HashMap<(String, i32), Offset> {
        self.elements()
            .iter()
            .map(|elem| ((elem.topic().to_owned(), elem.partition()), elem.offset()))
            .collect()
    }
}

impl Clone for TopicPartitionList {
    fn clone(&self) -> Self {
        let new_tpl = unsafe { rdsys::rd_kafka_topic_partition_list_copy(self.ptr()) };
        unsafe { TopicPartitionList::from_ptr(new_tpl) }
    }
}

impl PartialEq for TopicPartitionList {
    fn eq(&self, other: &TopicPartitionList) -> bool {
        if self.count() != other.count() {
            return false;
        }
        self.elements().iter().all(|elem| {
            if let Some(other_elem) = other.find_partition(elem.topic(), elem.partition()) {
                elem == &other_elem
            } else {
                false
            }
        })
    }
}

impl Default for TopicPartitionList {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl fmt::Debug for TopicPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TPL {{")?;
        for (i, elem) in self.elements().iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(
                f,
                "{}/{}: offset={:?} metadata={:?}, error={:?}",
                elem.topic(),
                elem.partition(),
                elem.offset(),
                elem.metadata(),
                elem.error(),
            )?;
        }
        write!(f, "}}")
    }
}
