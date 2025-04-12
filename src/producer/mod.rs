use std::time::Duration;

use crate::{IntoOpaque, client::ClientContext, message::DeliveryResult};
pub use boxed::Producer;
pub use builder::ProducerBuilder;

mod boxed;
mod builder;

pub static DEFAULT_PRODUCER_POLL_INTERVAL_MS: u64 = 100;

/// Trait allowing to customize the partitioning of messages.
pub trait Partitioner {
    /// Return partition to use for `topic_name`.
    /// `topic_name` is the name of a topic to which a message is being produced.
    /// `partition_cnt` is the number of partitions for this topic.
    /// `key` is an optional key of the message.
    /// `is_partition_available` is a function that can be called to check if a partition has an active leader broker.
    ///
    /// It may be called in any thread at any time,
    /// It may be called multiple times for the same message/key.
    /// MUST NOT block or execute for prolonged periods of time.
    /// MUST return a value between 0 and partition_cnt-1, or the
    /// special RD_KAFKA_PARTITION_UA value if partitioning could not be performed.
    /// See documentation for rd_kafka_topic_conf_set_partitioner_cb from librdkafka for more info.
    fn partition(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
        partition_cnt: i32,
        is_partition_available: impl Fn(i32) -> bool,
    ) -> i32;
}

/// Placeholder used when no custom partitioner is needed.
#[derive(Clone)]
pub struct NoCustomPartitioner {}

impl Partitioner for NoCustomPartitioner {
    fn partition(&self, _: &str, _: Option<&[u8]>, _: i32, _: impl Fn(i32) -> bool) -> i32 {
        unreachable!("NoCustomPartitioner should not be called");
    }
}

pub trait ProducerContext<P = NoCustomPartitioner>: ClientContext {
    /// A `DeliveryOpaque` is a user-defined structure that will be passed to
    /// the producer when producing a message, and returned to the `delivery`
    /// method once the message has been delivered, or failed to.
    type DeliveryOpaque: IntoOpaque + Default;

    fn poll_interval() -> Duration {
        Duration::from_millis(DEFAULT_PRODUCER_POLL_INTERVAL_MS)
    }

    fn delivery_message_callback(&self, msg: DeliveryResult<'_>, opaque: Self::DeliveryOpaque);

    /// This method is called when creating producer in order to optionally register custom partitioner.
    /// If custom partitioner is not used then `partitioner` configuration property is used (or its default).
    ///
    /// sticky.partitioning.linger.ms must be 0 to run custom partitioner for messages with null key.
    /// See https://github.com/confluentinc/librdkafka/blob/081fd972fa97f88a1e6d9a69fc893865ffbb561a/src/rdkafka_msg.c#L1192-L1196
    fn get_custom_partitioner(&self) -> Option<&P> {
        None
    }
}
