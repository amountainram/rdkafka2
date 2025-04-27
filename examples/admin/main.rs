use rdkafka2::{
    client::{
        AclBindingFilter, AclPermissionTypeRequest, AdminClient, DefaultClientContext,
        ResourcePatternType, ResourceType,
    },
    config::ClientConfig,
};
use std::{env, path::Path, str::FromStr, time::Duration};

#[derive(Debug)]
enum AuthMechanism {
    Scram,
    OAuthBearer,
}

impl FromStr for AuthMechanism {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "scram" => Ok(AuthMechanism::Scram),
            "oauthbearer" => Ok(AuthMechanism::OAuthBearer),
            _ => Err(format!("Invalid variant mechanism: {}", s)),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();
    let arg = env::args()
        .nth(1)
        .or_else(|| {
            log::error!("Expected one argument: 'scram' or 'oauthbearer'");
            None
        })
        .unwrap();
    let config_file = match arg
        .parse::<AuthMechanism>()
        .inspect_err(|err| {
            log::error!("{err}");
        })
        .unwrap()
    {
        AuthMechanism::Scram => "scram.json",
        AuthMechanism::OAuthBearer => "oauthbearer.json",
    };

    let config: ClientConfig = {
        let path = Path::new("examples")
            .join("admin")
            .join("config")
            .join(config_file);
        let cfd_str = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Config file to be placed at {path:?}"));
        serde_json::from_str::<ClientConfig>(&cfd_str).expect("Failed to parse config file")
    };
    let admin = AdminClient::builder()
        .config(config)
        .context(DefaultClientContext.into())
        .try_build()
        .expect("Failed to create admin client");

    println!("{:?}", admin.describe_cluster(Default::default()).await);

    println!(
        "{:?}",
        admin
            .describe_acls(
                AclBindingFilter::builder()
                    //.filter_by_name("fd-farm-data")
                    .filter_by_resource_type(ResourceType::Topic)
                    .filter_by_resource_pattern_type(ResourcePatternType::Prefixed)
                    //.filter_by_principal("User:farm-data-user")
                    //.filter_by_host("*")
                    //.filter_by_operation(AclOperationRequest::All)
                    .filter_by_permission_type(AclPermissionTypeRequest::Allow)
                    .build(),
                Duration::from_secs(5)
            )
            .await
    );
}
