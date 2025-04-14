use super::{NativeEvent, and_then_event};
use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::cstr_to_owned,
};
use futures::TryFutureExt;
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaEventType, rd_kafka_AclOperation_t, rd_kafka_Node_t,
    rd_kafka_metadata_t,
};
use tokio::sync::oneshot;
use typed_builder::TypedBuilder;

pub(super) type NativeMetadata = NativePtr<rd_kafka_metadata_t>;

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, TypedBuilder, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    #[builder(default)]
    pub replicas: Vec<i32>,
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
                id: broker.id,
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
                    replicas.push(*partition.replicas.add(i));
                }
                let isrs_cnt = partition.replica_cnt as usize;
                let mut isrs = Vec::with_capacity(isrs_cnt);
                for i in 0..isrs_cnt {
                    isrs.push(*partition.isrs.add(i));
                }
                partitions.push(PartitionMetadata {
                    id: partition.id,
                    leader: partition.leader,
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
    pub authorized_operations: Vec<AuthorizedOperation>,
}

fn build_authorized_operations(
    native_authorized_operations: *const rd_kafka_AclOperation_t,
    n: usize,
) -> Vec<AuthorizedOperation> {
    let mut ops = Vec::with_capacity(n);
    for i in 0..n {
        unsafe {
            let native_op = *native_authorized_operations.add(i);
            ops.push(cstr_to_owned(rdkafka2_sys::rd_kafka_AclOperation_name(
                native_op,
            )));
        }
    }

    ops.sort();

    ops
}

fn build_cluster_node(native_node: *const rd_kafka_Node_t) -> Node {
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
