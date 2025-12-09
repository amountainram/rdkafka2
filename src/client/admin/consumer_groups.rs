use super::{and_then_event, cluster::build_cluster_node};
use crate::{
    KafkaError, Result,
    client::{AclOperation, NativeEvent, Node},
    util::{cstr_to_owned, cstr_to_owned_option},
};
use futures::{TryFutureExt, channel::oneshot};
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaEventType, rd_kafka_ConsumerGroupDescription_authorized_operations,
    rd_kafka_ConsumerGroupDescription_coordinator,
    rd_kafka_ConsumerGroupDescription_is_simple_consumer_group,
    rd_kafka_ConsumerGroupDescription_member, rd_kafka_ConsumerGroupDescription_member_count,
    rd_kafka_ConsumerGroupDescription_partition_assignor, rd_kafka_ConsumerGroupDescription_state,
    rd_kafka_ConsumerGroupDescription_t, rd_kafka_MemberAssignment_partitions,
    rd_kafka_MemberDescription_assignment, rd_kafka_MemberDescription_client_id,
    rd_kafka_MemberDescription_consumer_id, rd_kafka_MemberDescription_group_instance_id,
    rd_kafka_MemberDescription_host, rd_kafka_MemberDescription_target_assignment,
    rd_kafka_consumer_group_state_t, rd_kafka_consumer_group_type_t,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum ConsumerGroupType {
    Unknown = rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN as u32,
    Consumer = rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER as u32,
    Classic = rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC as u32,
}

impl TryFrom<rd_kafka_consumer_group_type_t> for ConsumerGroupType {
    type Error = i32;

    fn try_from(value: rd_kafka_consumer_group_type_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN => {
                Ok(Self::Unknown)
            }
            rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER => {
                Ok(Self::Consumer)
            }
            rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC => {
                Ok(Self::Classic)
            }
            value => Err(value as i32),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum ConsumerGroupState {
    Unknown = rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN as u32,
    PreparingRebalance =
        rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE as u32,
    CompletingRebalance =
        rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE as u32,
    Stable = rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_STABLE as u32,
    Dead = rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_DEAD as u32,
    Empty = rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY as u32,
}

impl TryFrom<rd_kafka_consumer_group_state_t> for ConsumerGroupState {
    type Error = i32;

    fn try_from(value: rd_kafka_consumer_group_state_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN => {
                Ok(Self::Unknown)
            }
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE => {
                Ok(Self::PreparingRebalance)
            }
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE => {
                Ok(Self::CompletingRebalance)
            }
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_STABLE => {
                Ok(Self::Stable)
            }
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_DEAD => Ok(Self::Dead),
            rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY => Ok(Self::Empty),
            value => Err(value as i32),
        }
    }
}

#[derive(Debug)]
pub struct ConsumerGroupMemberDescription {
    pub client_id: String,
    pub group_instance_id: String,
    pub consumer_id: String,
    pub host: String,
}

#[derive(Debug)]
pub struct ConsumerGroupDescription {
    pub name: String,
    pub r#type: ConsumerGroupType,
    pub state: ConsumerGroupState,
    pub is_simple: bool,
    pub coordinator: Node,
    pub partition_assignor: Option<String>,
    pub authorized_operations: Vec<AclOperation>,
    pub members: Vec<ConsumerGroupMemberDescription>,
}

pub type ConsumerGroupDescriptionResult =
    Result<ConsumerGroupDescription, (String, RDKafkaErrorCode)>;

fn build_cg_description(
    name: String,
    gc_desc: *const rd_kafka_ConsumerGroupDescription_t,
) -> Result<ConsumerGroupDescription> {
    unsafe {
        let gc_type = rdkafka2_sys::rd_kafka_ConsumerGroupDescription_type(gc_desc)
            .try_into()
            .map_err(KafkaError::UnknownConsumerGroupType)?;

        let state = rd_kafka_ConsumerGroupDescription_state(gc_desc)
            .try_into()
            .map_err(KafkaError::UnknownConsumerGroupState)?;

        let is_simple = rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(gc_desc) != 0;

        let coordinator =
            build_cluster_node(rd_kafka_ConsumerGroupDescription_coordinator(gc_desc));

        let partition_assignor = cstr_to_owned_option(
            rd_kafka_ConsumerGroupDescription_partition_assignor(gc_desc),
        );

        let mut acl_ops_cnt = 0;
        let acl_ops =
            rd_kafka_ConsumerGroupDescription_authorized_operations(gc_desc, &mut acl_ops_cnt);
        let mut authorized_operations = Vec::with_capacity(acl_ops_cnt);
        for i in 0..acl_ops_cnt {
            let acl_op = *acl_ops.add(i);
            authorized_operations.push(acl_op.try_into().map_err(KafkaError::UnknownAclOperation)?);
        }

        let member_count = rd_kafka_ConsumerGroupDescription_member_count(gc_desc);
        let mut members = Vec::with_capacity(member_count);
        for i in 0..member_count {
            let member = rd_kafka_ConsumerGroupDescription_member(gc_desc, i);

            let client_id = cstr_to_owned(rd_kafka_MemberDescription_client_id(member));

            let group_instance_id =
                cstr_to_owned(rd_kafka_MemberDescription_group_instance_id(member));

            let consumer_id = cstr_to_owned(rd_kafka_MemberDescription_consumer_id(member));

            let host = cstr_to_owned(rd_kafka_MemberDescription_host(member));

            let assignment = rd_kafka_MemberDescription_assignment(member);
            let target_assignment = rd_kafka_MemberDescription_target_assignment(member);
            // assignments
            let _ass_tpl = rd_kafka_MemberAssignment_partitions(assignment);
            let _target_ass_tpl = rd_kafka_MemberAssignment_partitions(target_assignment);

            members.push(ConsumerGroupMemberDescription {
                client_id,
                group_instance_id,
                consumer_id,
                host,
            });
        }

        Ok(ConsumerGroupDescription {
            name,
            r#type: gc_type,
            state,
            is_simple,
            coordinator,
            partition_assignor,
            authorized_operations,
            members,
        })
    }
}

fn build_cg_descriptions(
    gc_descriptions: *const *const rd_kafka_ConsumerGroupDescription_t,
    n: usize,
) -> Result<Vec<ConsumerGroupDescriptionResult>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let gc_desc = unsafe { *gc_descriptions.add(i) };
        let group_id = unsafe {
            cstr_to_owned(rdkafka2_sys::rd_kafka_ConsumerGroupDescription_group_id(
                gc_desc,
            ))
        };
        let resp_err = unsafe {
            rdkafka2_sys::rd_kafka_error_code(
                rdkafka2_sys::rd_kafka_ConsumerGroupDescription_error(gc_desc),
            )
        };
        if let Some(err) = RDKafkaErrorCode::from(resp_err).error() {
            out.push(Err((group_id, err)));
        } else {
            out.push(Ok(build_cg_description(group_id, gc_desc)?));
        }
    }

    Ok(out)
}

pub(super) async fn handle_describe_consumer_groups_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<ConsumerGroupDescriptionResult>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(
            RDKafkaEventType::DescribeConsumerGroupsResult,
        ))
        .and_then(|(_, evt)| async move {
            let res =
                unsafe { rdkafka2_sys::rd_kafka_event_DescribeConsumerGroups_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| unsafe {
                    let mut n = 0;
                    let descriptions =
                        rdkafka2_sys::rd_kafka_DescribeConsumerGroups_result_groups(res, &mut n);
                    build_cg_descriptions(descriptions, n)
                })
                .transpose()?
                .ok_or(KafkaError::AdminOpCreation(
                    "error while describing consumer groups".into(),
                ))
        })
        .await
}
