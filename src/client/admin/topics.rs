use super::{NativeEvent, and_then_event};
use crate::{
    error::{KafkaError, Result},
    util::cstr_to_owned,
};
use futures::TryFutureExt;
use rdkafka2_sys::{RDKafkaErrorCode, RDKafkaEventType, rd_kafka_topic_result_t};
use tokio::sync::oneshot;

pub type TopicResult = Result<String, (String, RDKafkaErrorCode)>;

fn build_topic_results(
    topics: *const *const rd_kafka_topic_result_t,
    n: usize,
) -> Vec<TopicResult> {
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

pub(super) async fn handle_delete_topics_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<TopicResult>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::DeleteTopicsResult))
        .and_then(|(_, evt)| async move {
            let res = unsafe { rdkafka2_sys::rd_kafka_event_DeleteTopics_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| {
                    let mut n = 0;
                    let topics =
                        unsafe { rdkafka2_sys::rd_kafka_DeleteTopics_result_topics(res, &mut n) };

                    build_topic_results(topics, n)
                })
                .ok_or(KafkaError::AdminOpCreation(
                    "error while creating topics".into(),
                ))
        })
        .await
}
