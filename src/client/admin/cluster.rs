use super::{
    AclOperation, NativeEvent, ResourceType,
    acls::{NativeConfigResource, ResourceTypeRequest},
    and_then_event,
};
use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::cstr_to_owned,
};
use futures::{TryFutureExt, channel::oneshot};
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaEventType, rd_kafka_AclOperation_t, rd_kafka_ConfigResource_t,
    rd_kafka_Node_t, rd_kafka_ResourceType_t, rd_kafka_metadata_t,
};
use std::{collections::HashMap, ffi::CString, ops::Deref};
use typed_builder::TypedBuilder;

pub(super) type NativeMetadata = NativePtr<rd_kafka_metadata_t>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct BrokerId(pub(crate) i32);

impl Deref for BrokerId {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: BrokerId,
    pub leader: BrokerId,
    #[builder(default)]
    pub replicas: Vec<BrokerId>,
    #[builder(default)]
    pub isrs: Vec<i32>,
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct TopicMetadata {
    #[builder(setter(into))]
    pub name: String,
    #[builder(default)]
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct Metadata {
    #[builder(default)]
    pub brokers: Vec<BrokerMetadata>,
    #[builder(default)]
    pub topics: Vec<TopicMetadata>,
    pub orig_broker_id: i32,
    #[builder(setter(into))]
    pub orig_broker_name: String,
}

impl Metadata {
    pub fn brokers(&self) -> &[BrokerMetadata] {
        &self.brokers
    }

    pub fn broker_ids(&self) -> Vec<BrokerId> {
        self.brokers().iter().map(|b| b.id).collect::<Vec<_>>()
    }

    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|t| t.name == name)
    }
}

pub(super) fn handle_metadata_result(
    value: *const rd_kafka_metadata_t,
) -> Result<Metadata, RDKafkaErrorCode> {
    let metadata = unsafe { *value };

    let broker_cnt = metadata.broker_cnt as usize;
    let mut brokers = Vec::with_capacity(broker_cnt);
    for i in 0..broker_cnt {
        unsafe {
            let broker = *metadata.brokers.add(i);
            brokers.push(BrokerMetadata {
                id: BrokerId(broker.id),
                host: cstr_to_owned(broker.host),
                port: broker.port as u16,
            });
        }
    }

    let topic_cnt = metadata.topic_cnt as usize;
    let mut topics = Vec::with_capacity(topic_cnt);
    for i in 0..topic_cnt {
        unsafe {
            let topic = *metadata.topics.add(i);
            if let Some(err) = RDKafkaErrorCode::from(topic.err).error() {
                return Err(err);
            }

            let partition_cnt = topic.partition_cnt as usize;
            let mut partitions = Vec::with_capacity(partition_cnt);
            for i in 0..partition_cnt {
                let partition = *topic.partitions.add(i);
                if let Some(err) = RDKafkaErrorCode::from(partition.err).error() {
                    return Err(err);
                }

                let replicas_cnt = partition.replica_cnt as usize;
                let mut replicas = Vec::with_capacity(replicas_cnt);
                for i in 0..replicas_cnt {
                    replicas.push(BrokerId(*partition.replicas.add(i)));
                }
                let isrs_cnt = partition.replica_cnt as usize;
                let mut isrs = Vec::with_capacity(isrs_cnt);
                for i in 0..isrs_cnt {
                    isrs.push(*partition.isrs.add(i));
                }
                partitions.push(PartitionMetadata {
                    id: BrokerId(partition.id),
                    leader: BrokerId(partition.leader),
                    replicas,
                    isrs,
                });
            }

            topics.push(TopicMetadata {
                name: cstr_to_owned(topic.topic),
                partitions,
            });
        }
    }

    Ok(Metadata {
        brokers,
        topics,
        orig_broker_id: metadata.orig_broker_id,
        orig_broker_name: unsafe { cstr_to_owned(metadata.orig_broker_name) },
    })
}

pub type AuthorizedOperation = String;

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct Node {
    pub id: i32,
    #[builder(setter(into))]
    pub host: String,
    pub port: u16,
    #[builder(default, setter(strip_option, into))]
    pub rack: Option<String>,
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct Cluster {
    #[builder(setter(into))]
    pub id: String,
    pub nodes: Vec<Node>,
    pub controller: Node,
    #[builder(default)]
    pub authorized_operations: Vec<AclOperation>,
}

fn build_authorized_operations(
    native_authorized_operations: *const rd_kafka_AclOperation_t,
    n: usize,
) -> Vec<AclOperation> {
    let mut ops = Vec::with_capacity(n);
    for i in 0..n {
        unsafe {
            let native_op = *native_authorized_operations.add(i);
            ops.push(AclOperation::try_from(native_op).unwrap());
        }
    }

    ops.sort();

    ops
}

pub(crate) fn build_cluster_node(native_node: *const rd_kafka_Node_t) -> Node {
    unsafe {
        let rack = rdkafka2_sys::rd_kafka_Node_rack(native_node);
        let rack = (!rack.is_null()).then(|| cstr_to_owned(rack));

        Node {
            id: rdkafka2_sys::rd_kafka_Node_id(native_node),
            host: cstr_to_owned(rdkafka2_sys::rd_kafka_Node_host(native_node)),
            port: rdkafka2_sys::rd_kafka_Node_port(native_node),
            rack,
        }
    }
}

fn build_cluster_nodes(native_node: *mut *const rd_kafka_Node_t, n: usize) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let native_node = unsafe { *native_node.add(i) };
        nodes.push(build_cluster_node(native_node));
    }

    nodes.sort_by(|a, b| a.id.cmp(&b.id));

    nodes
}

pub(super) async fn handle_describe_cluster_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Cluster> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::DescribeClusterResult))
        .and_then(|(_, evt)| async move {
            let res = unsafe { rdkafka2_sys::rd_kafka_event_DescribeCluster_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| {
                    let nodes = unsafe {
                        let mut n = 0;
                        let native_nodes =
                            rdkafka2_sys::rd_kafka_DescribeCluster_result_nodes(res, &mut n);
                        build_cluster_nodes(native_nodes, n)
                    };
                    let authorized_operations = unsafe {
                        let mut n = 0;
                        let native_authorized_operations =
                            rdkafka2_sys::rd_kafka_DescribeCluster_result_authorized_operations(
                                res, &mut n,
                            );
                        build_authorized_operations(native_authorized_operations, n)
                    };
                    let controller = unsafe {
                        let native_node =
                            rdkafka2_sys::rd_kafka_DescribeCluster_result_controller(res);
                        build_cluster_node(native_node)
                    };
                    let id = unsafe {
                        cstr_to_owned(rdkafka2_sys::rd_kafka_DescribeCluster_result_cluster_id(
                            res,
                        ))
                    };

                    Cluster {
                        id,
                        nodes,
                        controller,
                        authorized_operations,
                    }
                })
                .ok_or(KafkaError::AdminOpCreation(
                    "error while creating topics".into(),
                ))
        })
        .await
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct ConfigResourceRequest {
    #[builder(default = ResourceTypeRequest::Any, setter(into))]
    pub r#type: ResourceTypeRequest,
    #[builder(setter(into))]
    pub name: String,
}

impl<S> From<S> for ConfigResourceRequest
where
    S: Into<String>,
{
    fn from(name: S) -> Self {
        Self {
            r#type: ResourceTypeRequest::Any,
            name: name.into(),
        }
    }
}

impl ConfigResourceRequest {
    pub(super) fn to_native(&self) -> Result<NativeConfigResource> {
        let name = CString::new(self.name.as_str())?;
        Ok(unsafe {
            NativeConfigResource::from_ptr(rdkafka2_sys::rd_kafka_ConfigResource_new(
                self.r#type.into(),
                name.as_ptr(),
            ))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub value: Option<String>,
    pub source: String,
    pub is_read_only: bool,
    pub is_default: bool,
    pub is_sensitive: bool,
    pub is_synonym: bool,
    pub synonyms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    pub r#type: ResourceType,
    pub name: String,
    pub config: HashMap<String, ConfigEntry>,
}

fn build_config_resource(
    native_resource: *const rd_kafka_ConfigResource_t,
) -> Result<ConfigResource> {
    unsafe {
        let native_type = rdkafka2_sys::rd_kafka_ConfigResource_type(native_resource);
        let r#type = match native_type {
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY
            | rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE__CNT
            | rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_UNKNOWN => {
                Err(KafkaError::UnknownResource(native_type as i32))
            }
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC => Ok(ResourceType::Topic),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP => todo!(),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER => Ok(ResourceType::Broker),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID => todo!(),
        }?;

        let mut n = 0;
        let name = cstr_to_owned(rdkafka2_sys::rd_kafka_ConfigResource_name(native_resource));
        let configs = rdkafka2_sys::rd_kafka_ConfigResource_configs(native_resource, &mut n);
        let mut config = HashMap::with_capacity(n);
        for i in 0..n {
            let entry = *configs.add(i);
            let name = cstr_to_owned(rdkafka2_sys::rd_kafka_ConfigEntry_name(entry));
            let value = rdkafka2_sys::rd_kafka_ConfigEntry_value(entry);

            config.insert(
                name,
                ConfigEntry {
                    value: (!value.is_null()).then(|| cstr_to_owned(value)),
                    source: cstr_to_owned(rdkafka2_sys::rd_kafka_ConfigSource_name(
                        rdkafka2_sys::rd_kafka_ConfigEntry_source(entry),
                    )),
                    is_read_only: rdkafka2_sys::rd_kafka_ConfigEntry_is_read_only(entry) == 1,
                    is_default: rdkafka2_sys::rd_kafka_ConfigEntry_is_default(entry) == 1,
                    is_sensitive: rdkafka2_sys::rd_kafka_ConfigEntry_is_sensitive(entry) == 1,
                    is_synonym: rdkafka2_sys::rd_kafka_ConfigEntry_is_synonym(entry) == 1,
                    synonyms: {
                        let mut n = 0;
                        let native_synonyms =
                            rdkafka2_sys::rd_kafka_ConfigEntry_synonyms(entry, &mut n);
                        let mut synonyms = Vec::with_capacity(n);
                        for i in 0..n {
                            let synonym = *native_synonyms.add(i);
                            synonyms.push(cstr_to_owned(rdkafka2_sys::rd_kafka_ConfigEntry_name(
                                synonym,
                            )));
                        }
                        synonyms
                    },
                },
            );
        }

        Ok(ConfigResource {
            r#type,
            name,
            config,
        })
    }
}

fn build_config_resources(
    native_node: *mut *const rd_kafka_ConfigResource_t,
    n: usize,
) -> Vec<Result<ConfigResource>> {
    let mut resources = Vec::with_capacity(n);
    for i in 0..n {
        unsafe {
            let native_resource = *native_node.add(i);
            if let Some(err) =
                RDKafkaErrorCode::from(rdkafka2_sys::rd_kafka_ConfigResource_error(native_resource))
                    .error()
            {
                resources.push(Err(KafkaError::AdminOp(err)));
            } else {
                resources.push(build_config_resource(native_resource));
            }
        }
    }

    resources
}

pub(super) async fn handle_describe_configs_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<Result<ConfigResource>>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::DescribeConfigsResult))
        .and_then(|(_, evt)| async move {
            let res = unsafe { rdkafka2_sys::rd_kafka_event_DescribeConfigs_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| unsafe {
                    let mut n = 0;
                    let native_resources =
                        rdkafka2_sys::rd_kafka_DescribeConfigs_result_resources(res, &mut n);
                    build_config_resources(native_resources, n)
                })
                .ok_or(KafkaError::AdminOpCreation(
                    "error while creating config resources".into(),
                ))
        })
        .await
}
