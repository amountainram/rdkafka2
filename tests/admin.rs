use backon::{ConstantBuilder, RetryableWithContext};
use rand::{Rng, distr::Alphanumeric};
use rdkafka2::{
    KafkaError, Timeout,
    client::{
        AclBinding, AclBindingFilter, AdminClient, Cluster, Node, ResourceType, ResourceTypeRequest,
    },
    config::ClientConfig,
    partitions::NewPartitions,
};
use rdkafka2_sys::RDKafkaErrorCode;
use rstest::{fixture, rstest};
use std::time::Duration;

pub mod common;

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
    LOCALHOST_HOST
}

fn kafka_broker() -> &'static str {
    const LOCALHOST_BROKER: &str = "localhost:9092";
    LOCALHOST_BROKER
}

#[fixture]
fn config() -> ClientConfig {
    ClientConfig::from_iter([
        ("bootstrap.servers", kafka_broker()),
        ("log_level", "7"),
        ("debug", "all"),
    ])
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn create_and_delete_topics(config: ClientConfig, topic_names: Vec<String>) {
    let admin_client = common::test_admin_client(config);

    common::test_create_topics(&admin_client, topic_names.as_slice()).await;

    let first = topic_names.first().unwrap();
    let metadata = admin_client
        .metadata(Duration::from_secs(2))
        .await
        .expect("valid metadata");
    let topic_metadata = metadata.topic(first).expect("topic metadata should exist");
    assert_eq!(topic_metadata.name.as_str(), first.as_str());
    assert_eq!(topic_metadata.partitions.len(), 1);
    let partition_metadata = topic_metadata
        .partitions
        .first()
        .expect("partition metadata should exist");
    assert_eq!(*partition_metadata.id, 0);

    common::test_delete_topics(&admin_client, topic_names.as_slice()).await;

    let topic = admin_client.register_topic(first).unwrap();
    assert_eq!(
        admin_client
            .metadata_for_topic(topic.clone(), Duration::from_secs(2))
            .await
            .unwrap_err(),
        KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition)
    );
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn update_partitions_and_assignments(config: ClientConfig) {
    let admin_client = common::test_admin_client(config);

    let topic_name = generate_random_string(10);

    common::test_create_topics(&admin_client, vec![&topic_name]).await;

    let topic = admin_client.register_topic(&topic_name).unwrap();
    let metadata = admin_client
        .metadata_for_topic(topic, Duration::from_secs(2))
        .await
        .expect("valid metadata");
    let broker_ids = metadata.broker_ids();
    let topic_metadata = metadata.topic(&topic_name).expect("topic metadata").clone();
    let new_partitions = NewPartitions::builder()
        .topic_metadata(topic_metadata)
        .partitions(vec![broker_ids.clone(), broker_ids.clone(), broker_ids])
        .build();

    admin_client
        .create_partitions(vec![new_partitions], Default::default())
        .await
        .expect("partitions to be created")
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("partitions creation to succeed");

    let metadata = common::retrieve_metadata(&admin_client, |m| {
        m.topic(&topic_name)
            .expect("topic metadata should exist")
            .partitions
            .len()
            > 1
    })
    .await;

    let topic_metadata = metadata
        .topic(&topic_name)
        .expect("topic metadata should exist")
        .clone();
    assert_eq!(topic_metadata.partitions.len(), 4);
    for (index, partition) in topic_metadata.partitions.iter().enumerate() {
        assert_eq!(*partition.id, index as i32);
        assert_eq!(*partition.leader, 1);
        assert_eq!(partition.replicas.len(), 1);
        assert_eq!(partition.isrs.len(), 1);
    }

    common::test_delete_topics(&admin_client, vec![&topic_name]).await;
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn retrieve_configs(config: ClientConfig) {
    let admin_client = common::test_admin_client(config);

    let resource = admin_client
        .describe_resource("1", ResourceTypeRequest::Broker, Default::default())
        .await
        .expect("topics to be created");

    println!("Resource: {:?}", resource);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn describe_cluster(config: ClientConfig) {
    let admin_client = common::test_admin_client(config);

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

async fn test_acls(client: &AdminClient, expected: Vec<AclBinding>) {
    let expected_len = expected.len();
    let (_, res) = (|client: AdminClient| async move {
        let acls = client
            .describe_acls(AclBindingFilter::any(), None::<Timeout>)
            .await
            .expect("acls to be listed");
        if acls.len() == expected_len {
            (client, Ok(acls))
        } else {
            (
                client,
                Err(KafkaError::AdminOpCreation(
                    "ACLS expected len not matching".to_string(),
                )),
            )
        }
    })
    .retry(ConstantBuilder::default())
    .context(client.clone())
    .await;

    assert_eq!(
        res.expect("acls to be retrieved").into_inner(),
        expected
            .into_iter()
            .map(Ok::<_, KafkaError>)
            .collect::<Vec<_>>()
    );
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn acls_crud_ops(config: ClientConfig) {
    let admin_client = common::test_admin_client(config);

    test_acls(&admin_client, vec![]).await;

    admin_client
        .create_acls(
            vec![
                AclBinding::builder()
                    .operation(rdkafka2::client::AclOperation::All)
                    .host(kafka_host())
                    .permission_type(rdkafka2::client::AclPermissionType::Allow)
                    .resource_pattern_type(rdkafka2::client::ResourcePatternType::Prefixed)
                    .principal("User:admin")
                    .name("fd")
                    .resource_type(ResourceType::Topic)
                    .build(),
            ],
            None::<Timeout>,
        )
        .await
        .expect("acl request to be ok")
        .flatten()
        .expect("acls to be created");

    let test_topic_acl = vec![
        AclBinding::builder()
            .operation(rdkafka2::client::AclOperation::All)
            .host(kafka_host())
            .permission_type(rdkafka2::client::AclPermissionType::Allow)
            .resource_pattern_type(rdkafka2::client::ResourcePatternType::Prefixed)
            .principal("User:admin")
            .name("fd")
            .resource_type(ResourceType::Topic)
            .build(),
    ];

    test_acls(&admin_client, test_topic_acl.clone()).await;

    let deleted = admin_client
        .delete_acls(vec![AclBindingFilter::any()], None::<Timeout>)
        .await
        .expect("acls to be deleted")
        .deep_flatten()
        .expect("acls to be recovered");

    test_acls(&admin_client, vec![]).await;

    assert_eq!(deleted, test_topic_acl);
}
