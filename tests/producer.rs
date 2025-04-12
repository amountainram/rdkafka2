use futures::{Stream, TryStreamExt};
use rand::{Rng, distr::Alphanumeric};
use rdkafka2::{
    KafkaError, RDKafkaLogLevel,
    client::ClientContext,
    config::ClientConfig,
    message::{DeliveryResult, OwnedMessage},
    producer::{Producer, ProducerContext},
};
use rdkafka2_sys::RDKafkaErrorCode;
use rstest::{fixture, rstest};
use std::env;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

fn generate_random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[fixture]
fn topic_name() -> String {
    generate_random_string(10)
}

fn kafka_broker() -> &'static str {
    static LOCALHOST_BROKER: &str = "localhost:9092";
    static DEFAULT_CI_DOCKER_BROKER: &str = "docker:9092";

    env::var("CI")
        .map(|_| DEFAULT_CI_DOCKER_BROKER)
        .unwrap_or(LOCALHOST_BROKER)
}

type OwnedDeliveryResult = Result<OwnedMessage, (KafkaError, OwnedMessage)>;

struct DeliveryStreamContext {
    tx: mpsc::UnboundedSender<OwnedDeliveryResult>,
}

impl ClientContext for DeliveryStreamContext {}

impl ProducerContext for DeliveryStreamContext {
    type DeliveryOpaque = ();

    fn delivery_message_callback(&self, dr: DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        let report = match dr {
            Ok(m) => m.try_detach().map(Ok),
            Err((err, msg)) => msg.try_detach().map(|msg| Err((err, msg))),
        }
        .expect("no UTF8 errors");
        self.tx.send(report).expect("msg to be sent");
    }
}

fn test_producer(
    config: ClientConfig,
) -> (
    Producer<DeliveryStreamContext>,
    impl Stream<Item = OwnedDeliveryResult> + Send,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let delivery_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
    (
        Producer::builder()
            .config(config)
            .log_level(RDKafkaLogLevel::Debug)
            .context(DeliveryStreamContext { tx })
            .try_build()
            .expect("client to be built"),
        delivery_stream,
    )
}

#[rstest]
#[case(
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("allow.auto.create.topics", "false"),
        ("topic.metadata.propagation.max.ms", "2"),
        // 👆 this allows fetch metadata to be performed
        // within 2ms from test startup
        ("log_level", "7"),
        ("debug", "all"),
    ])
)]
#[tokio::test]
async fn unknown_topic(#[case] config: ClientConfig, topic_name: String) {
    use rdkafka2::message::BaseRecord;

    let record = BaseRecord::builder()
        .key(r#"{"id":"1"}"#.as_bytes())
        .payload(r#"{"id":"2"}"#.as_bytes())
        .topic(topic_name.as_str())
        .build();
    let (producer, delivery_stream) = test_producer(config);
    producer.send(record).expect("message to be produced");
    assert_eq!(
        delivery_stream
            .map_ok(|_| ())
            .take(1)
            .try_collect::<()>()
            .await
            .map_err(|(err, ..)| err),
        Err(KafkaError::MessageProduction(
            RDKafkaErrorCode::UnknownTopicOrPartition
        ))
    );

    drop(producer);
}

#[rstest]
#[case(
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("log_level", "7"),
        ("debug", "all"),
    ])
)]
#[tokio::test]
async fn simple_producer(#[case] config: ClientConfig, topic_name: String) {
    use rdkafka2::message::BaseRecord;

    let record = BaseRecord::builder()
        .key(r#"{"id":"1"}"#.as_bytes())
        .payload(r#"{"id":"2"}"#.as_bytes())
        .topic(topic_name.as_str())
        .build();
    let (producer, delivery_stream) = test_producer(config);
    producer.send(record).expect("message to be produced");

    let produced = delivery_stream
        .map_ok(|_| ())
        .take(1)
        .try_collect::<Vec<()>>()
        .await
        .map_err(|(err, ..)| err);
    assert!(produced.is_ok());
    assert_eq!(produced.unwrap().len(), 1);

    drop(producer);
}
