#![cfg(feature = "tokio")]

use backon::{ConstantBuilder, RetryableWithContext};
use rdkafka2::{
    RDKafkaLogLevel,
    client::{AdminClient, DefaultClientContext, Metadata},
    config::ClientConfig,
    topic::{DeleteTopic, NewTopic},
};
use std::time::Duration;

pub fn test_admin_client(config: ClientConfig) -> AdminClient {
    AdminClient::builder()
        .config(config)
        .log_level(RDKafkaLogLevel::Debug)
        .context(DefaultClientContext.into())
        .try_build()
        .expect("client to be built")
}

pub async fn test_create_topics<I>(client: &AdminClient, topics: I)
where
    I: IntoIterator,
    I::Item: Into<NewTopic>,
{
    let (topic_names, new_topics) =
        topics
            .into_iter()
            .fold((vec![], vec![]), |(mut names, mut new_topics), next| {
                let new_topic: NewTopic = next.into();
                names.push(new_topic.name.clone());
                new_topics.push(new_topic);
                (names, new_topics)
            });
    client
        .create_topics(new_topics, Default::default())
        .await
        .expect("topics to be created");

    let topics_num = topic_names.len();
    let (_, result) = (|(client, topics): (AdminClient, Vec<String>)| async move {
        let metadata = client
            .metadata(Duration::from_secs(2))
            .await
            .expect("valid metadata");

        let created_topics_count = metadata
            .topics
            .iter()
            .filter(|t| topics.contains(&t.name))
            .count();

        let result = if created_topics_count != topics.len() {
            Err(format!(
                "Expected {} topics to be created, but found {}",
                topics.len(),
                created_topics_count
            ))
        } else {
            Ok(created_topics_count)
        };

        ((client, topics), result)
    })
    .retry(ConstantBuilder::default())
    .context((client.clone(), topic_names))
    .await;

    assert_eq!(result, Ok(topics_num));
}

pub async fn retrieve_metadata<P>(client: &AdminClient, predicate: P) -> Metadata
where
    P: Fn(&Metadata) -> bool,
{
    let (_, result) = (|(client, p): (AdminClient, P)| async move {
        let metadata = client
            .metadata(Duration::from_secs(2))
            .await
            .expect("valid metadata");

        let result = if p(&metadata) {
            Ok(metadata)
        } else {
            Err("Metadata does not match the predicate".to_string())
        };

        ((client, p), result)
    })
    .retry(ConstantBuilder::default())
    .context((client.clone(), predicate))
    .await;

    result.expect("metadata retrieval")
}

pub async fn test_delete_topics<I>(client: &AdminClient, topics: I)
where
    I: IntoIterator,
    I::Item: Into<DeleteTopic>,
{
    let (topic_names, delete_topics) =
        topics
            .into_iter()
            .fold((vec![], vec![]), |(mut names, mut delete_topics), next| {
                let delete_topic: DeleteTopic = next.into();
                names.push(delete_topic.name().to_string());
                delete_topics.push(delete_topic);
                (names, delete_topics)
            });

    client
        .delete_topics(delete_topics, Default::default())
        .await
        .expect("topics to be created");

    let (_, result) = (|(client, topics): (AdminClient, Vec<String>)| async move {
        let metadata = client
            .metadata(Duration::from_secs(2))
            .await
            .expect("valid metadata");

        let topics_still_avail = metadata
            .topics
            .iter()
            .filter(|t| topics.contains(&t.name))
            .count();

        let result = if topics_still_avail != 0 {
            Err(format!(
                "Expected {} topics to be deleted, but still found {}",
                topics.len(),
                topics_still_avail
            ))
        } else {
            Ok(())
        };

        ((client, topics), result)
    })
    .retry(ConstantBuilder::default())
    .context((client.clone(), topic_names))
    .await;

    assert!(result.is_ok());
}
