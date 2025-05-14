use rdkafka2::{
    RDKafkaLogLevel,
    client::{AdminClient, DefaultClientContext},
    config::ClientConfig,
};

pub fn test_admin_client(config: ClientConfig) -> AdminClient {
    AdminClient::builder()
        .config(config)
        .log_level(RDKafkaLogLevel::Debug)
        .context(DefaultClientContext.into())
        .try_build()
        .expect("client to be built")
}
