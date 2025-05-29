use super::{
    NativeEvent, and_then_event,
    topics::{TopicResult, build_topic_results},
};
use crate::{KafkaError, Result};
use futures::{TryFutureExt, channel::oneshot};
use rdkafka2_sys::RDKafkaEventType;

pub(super) async fn handle_create_partitions_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<TopicResult>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::CreatePartitionsResult))
        .and_then(|(_, evt)| async move {
            let res = unsafe { rdkafka2_sys::rd_kafka_event_CreatePartitions_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| {
                    let mut n = 0;
                    let topics = unsafe {
                        rdkafka2_sys::rd_kafka_CreatePartitions_result_topics(res, &mut n)
                    };

                    build_topic_results(topics, n)
                })
                .ok_or(KafkaError::AdminOpCreation(
                    "error while creating topics".into(),
                ))
        })
        .await
}
