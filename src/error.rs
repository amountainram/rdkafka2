use rdkafka2_sys::{RDKafkaConfErrorCode, RDKafkaErrorCode, RDKafkaEventType};
use std::ffi::NulError;

pub type Result<T, E = KafkaError> = std::prelude::v1::Result<T, E>;

/// Represents all possible Kafka errors.
///
/// If applicable, check the underlying [`RDKafkaErrorCode`] to get details.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaError {
    /// Creation of admin operation failed.
    AdminOpCreation(String),
    /// The admin operation itself failed.
    AdminOp(RDKafkaErrorCode),
    /// Api operation could not be received in the listening queue
    AdminApiError,
    /// The client was dropped before the operation completed.
    Canceled,
    /// Invalid client configuration.
    ClientConfig(RDKafkaConfErrorCode, String),
    /// Client creation failed.
    ClientCreation(String),
    /// Consumer commit failed.
    ConsumerCommit(RDKafkaErrorCode),
    /// Consumer queue close failed.
    ConsumerQueueClose(RDKafkaErrorCode),
    /// Flushing failed
    Flush(RDKafkaErrorCode),
    /// Global error.
    Global(RDKafkaErrorCode),
    /// Group list fetch failed.
    GroupListFetch(RDKafkaErrorCode),
    /// Message consumption failed.
    MessageConsumption(RDKafkaErrorCode),
    /// Message consumption failed with fatal error.
    MessageConsumptionFatal(RDKafkaErrorCode),
    /// Message production error.
    MessageProduction(RDKafkaErrorCode),
    /// Metadata fetch error.
    MetadataFetch(RDKafkaErrorCode),
    /// No message was received.
    NoMessageReceived,
    /// Unexpected null pointer
    Nul(NulError),
    /// Offset fetch failed.
    OffsetFetch(RDKafkaErrorCode),
    /// End of partition reached.
    PartitionEOF(i32),
    /// Pause/Resume failed.
    PauseResume(String),
    /// Rebalance failed.
    Rebalance(RDKafkaErrorCode),
    /// Seeking a partition failed.
    Seek(String),
    /// Setting partition offset failed.
    SetPartitionOffset(RDKafkaErrorCode),
    /// Offset store failed.
    StoreOffset(RDKafkaErrorCode),
    /// Subscription creation failed.
    Subscription(RDKafkaErrorCode),
    /// Transaction error.
    // FIXME: figure out what to do with this
    //
    //Transaction(RDKafkaError),
    /// Mock Cluster error
    MockCluster(RDKafkaErrorCode),

    UnknownEvent(i32),
    UnknownResource(i32),
    UnknownAclOperation(i32),
    UnknownAclResourcePatternType(i32),
    UnknownAclPermissionType(i32),
    SaslOauthbearerConfig(RDKafkaErrorCode),
    InvalidEvent {
        actual: RDKafkaEventType,
        expected: RDKafkaEventType,
    },
}

impl From<NulError> for KafkaError {
    fn from(value: NulError) -> Self {
        Self::Nul(value)
    }
}
