use futures::{Stream, TryStreamExt};
use rand::{Rng, distr::Alphanumeric};
use rdkafka2::{
    KafkaError, RDKafkaLogLevel,
    client::{ClientContext, Topic},
    config::ClientConfig,
    message::{DeliveryResult, OwnedBaseRecord, OwnedMessage},
    producer::{Producer, ProducerContext},
    topic::TopicSettings,
};
use rdkafka2_sys::RDKafkaErrorCode;
use rstest::{fixture, rstest};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

pub mod common;

#[fixture]
fn config() -> ClientConfig {
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("log_level", "7"),
        ("debug", "all"),
    ])
}

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
    //static DEFAULT_CI_DOCKER_BROKER: &str = "docker:9092";
    //
    //env::var("CI")
    //    .map(|_| DEFAULT_CI_DOCKER_BROKER)
    //    .unwrap_or(LOCALHOST_BROKER)

    LOCALHOST_BROKER
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
#[tokio::test(flavor = "current_thread")]
async fn unknown_topic(topic_name: String) {
    use rdkafka2::message::BaseRecord;

    let config = ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("allow.auto.create.topics", "false"),
        ("topic.metadata.propagation.max.ms", "2"),
        // 👆 this allows fetch metadata to be performed
        // within 2ms from test startup
        ("log_level", "7"),
        ("debug", "all"),
    ]);

    let (producer, delivery_stream) = test_producer(config);
    let topic = producer
        .register_topic(topic_name)
        .expect("topic to be registered");
    let record = BaseRecord::builder()
        .key(r#"{"id":"1"}"#.as_bytes())
        .payload(r#"{"id":"2"}"#.as_bytes())
        .topic(&topic)
        .build();
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
#[tokio::test]
async fn simple_producer(config: ClientConfig, topic_name: String) {
    use rdkafka2::message::BaseRecord;

    let (producer, delivery_stream) = test_producer(config);
    let topic = producer
        .register_topic(topic_name)
        .expect("topic to be registered");
    let record = BaseRecord::builder()
        .key(r#"{"id":"1"}"#.as_bytes())
        .payload(r#"{"id":"2"}"#.as_bytes())
        .topic(&topic)
        .build();
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

async fn create_multiple_partitions_topic(config: &ClientConfig, name: &str) {
    let admin_client = common::test_admin_client(config.clone());
    let topic = rdkafka2::topic::NewTopic::builder()
        .name(name)
        .num_partitions(3)
        .replication(rdkafka2::topic::TopicReplication::Fixed(1))
        .build();
    admin_client
        .create_topics(vec![topic], Default::default())
        .await
        .unwrap_or_else(|_| panic!("topic {} to be created", name));
}

fn create_records(topic: Topic, count: usize) -> Vec<OwnedBaseRecord> {
    (0..count)
        .map(|i| {
            OwnedBaseRecord::builder()
                .topic(topic.clone())
                .key(format!(r#"{{"id":"{}"}}"#, i).as_bytes().to_vec())
                .payload(format!(r#"{{"id":"{}"}}"#, i).as_bytes().to_vec())
                .build()
        })
        .collect()
}

#[rstest]
#[tokio::test]
async fn custom_partitioner_production(config: ClientConfig, topic_name: String) {
    create_multiple_partitions_topic(&config, topic_name.as_str()).await;

    let (producer, delivery_stream) = test_producer(config);
    let topic_conf = TopicSettings::builder()
        .name(topic_name.as_str())
        .partitioner(|_, _| 2)
        .build();
    let topic = producer
        .register_topic(topic_conf)
        .expect("topic to be registered");

    let records = create_records(topic, 10);
    for record in records {
        producer
            .send(record.as_ref())
            .expect("message to be produced");
    }

    let produced = delivery_stream
        .map_ok(|_| ())
        .take(10)
        .try_collect::<Vec<()>>()
        .await
        .map_err(|(err, ..)| err);
    assert!(produced.is_ok());
    assert_eq!(produced.unwrap().len(), 10);

    drop(producer);
}

#[rstest]
#[tokio::test]
async fn topic_registration(config: ClientConfig, topic_name: String) {
    let (producer, _) = test_producer(config);
    let topic = producer
        .register_topic(topic_name.as_str())
        .expect("topic to be registered");

    assert_eq!(topic.name(), topic_name);

    assert_eq!(
        producer.register_topic(topic_name.as_str()),
        Err(KafkaError::TopicAlreadyRegistered(topic_name.clone()))
    );

    drop(topic);

    let topic_conf = TopicSettings::builder()
        .name(topic_name.as_str())
        .partitioner(|_, _| 0)
        .build();
    assert!(producer.register_topic(topic_conf).is_ok());
}
