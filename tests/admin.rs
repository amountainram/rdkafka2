use backon::{ConstantBuilder, RetryableWithContext};
use rand::{Rng, distr::Alphanumeric};
use rdkafka2::{
    KafkaError, RDKafkaLogLevel,
    client::{AdminClient, Cluster, DefaultClientContext, Node, PartitionMetadata, TopicMetadata},
    config::ClientConfig,
};
use rdkafka2_sys::RDKafkaErrorCode;
use rstest::{fixture, rstest};
use std::time::Duration;

fn generate_random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[fixture]
fn topic_names() -> Vec<String> {
    (0..10).map(|_| generate_random_string(10)).collect()
}

fn kafka_host() -> &'static str {
    const LOCALHOST_HOST: &str = "localhost";
    //const DEFAULT_CI_DOCKER_HOST: &str = "docker";

    //env::var("CI")
    //    .map(|_| DEFAULT_CI_DOCKER_HOST)
    //    .unwrap_or(LOCALHOST_HOST)
    LOCALHOST_HOST
}

fn kafka_broker() -> &'static str {
    const LOCALHOST_BROKER: &str = "localhost:9092";
    //const DEFAULT_CI_DOCKER_BROKER: &str = "docker:9092";

    //env::var("CI")
    //    .map(|_| DEFAULT_CI_DOCKER_BROKER)
    //    .unwrap_or(LOCALHOST_BROKER)
    LOCALHOST_BROKER
}

fn test_admin_client(config: ClientConfig) -> AdminClient {
    AdminClient::builder()
        .config(config)
        .log_level(RDKafkaLogLevel::Debug)
        .context(DefaultClientContext.into())
        .try_build()
        .expect("client to be built")
}

#[rstest]
#[case(
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("log_level", "7"),
        ("debug", "all"),
    ])
)]
#[tokio::test(flavor = "current_thread")]
async fn create_and_delete_topics(#[case] config: ClientConfig, topic_names: Vec<String>) {
    let admin_client = test_admin_client(config);
    admin_client
        .create_topics(topic_names.clone(), Default::default())
        .await
        .expect("topics to be created");

    let first = topic_names.first().unwrap();
    let metadata = admin_client
        .metadata_for_topic(first, Duration::from_secs(2))
        .await
        .expect("valid metadata");
    assert_eq!(metadata.topics.len(), 1);
    assert_eq!(
        metadata.topics.first(),
        Some(
            &TopicMetadata::builder()
                .name(first)
                .partitions(vec![
                    PartitionMetadata::builder()
                        .id(0)
                        .leader(1)
                        .replicas(vec![1])
                        .isrs(vec![1])
                        .build()
                ])
                .build()
        )
    );

    admin_client
        .delete_topics(topic_names.clone(), Default::default())
        .await
        .expect("topics to be created");

    let (_, error) = (|(admin_client, topic): (AdminClient, String)| async move {
        let result = match admin_client
            .metadata_for_topic(&topic, Duration::from_secs(2))
            .await
        {
            Err(err) => Ok(err),
            Ok(m) => Err(m),
        };

        ((admin_client, topic), result)
    })
    .retry(ConstantBuilder::default())
    .context((admin_client, first.to_string()))
    .await;

    assert_eq!(
        error.unwrap(),
        KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition)
    );
}

#[rstest]
#[case(
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("log_level", "7"),
        ("debug", "all"),
    ])
)]
#[tokio::test(flavor = "current_thread")]
async fn describe_cluster(#[case] config: ClientConfig) {
    let admin_client = test_admin_client(config);
    let cluster = admin_client
        .describe_cluster(Default::default())
        .await
        .expect("cluster description");

    assert_eq!(
        cluster,
        Cluster::builder()
            .id("YTM1MjQxY2UyY2NmNDZjMG")
            .nodes(vec![
                Node::builder().id(1).host(kafka_host()).port(9092).build()
            ])
            .controller(Node::builder().id(1).host(kafka_host()).port(9092).build())
            .build()
    );
}
