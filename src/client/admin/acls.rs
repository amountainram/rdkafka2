use super::{NativeEvent, and_then_event};
use crate::{
    error::{KafkaError, Result},
    ptr::NativePtr,
    util::{ErrBuf, cstr_to_owned},
};
use futures::{TryFutureExt, channel::oneshot};
use rdkafka2_sys::{
    RDKafkaErrorCode, RDKafkaEventType, rd_kafka_AclBinding_error, rd_kafka_AclBinding_host,
    rd_kafka_AclBinding_name, rd_kafka_AclBinding_operation, rd_kafka_AclBinding_permission_type,
    rd_kafka_AclBinding_principal, rd_kafka_AclBinding_resource_pattern_type,
    rd_kafka_AclBinding_restype, rd_kafka_AclBinding_t, rd_kafka_AclBindingFilter_t,
    rd_kafka_AclOperation_t, rd_kafka_AclPermissionType_t, rd_kafka_ConfigResource_t,
    rd_kafka_CreateAcls_result_acls, rd_kafka_DeleteAcls_result_response_error,
    rd_kafka_DeleteAcls_result_response_matching_acls, rd_kafka_DeleteAcls_result_responses,
    rd_kafka_DescribeAcls_result_acls, rd_kafka_ResourcePatternType_t, rd_kafka_ResourceType_t,
    rd_kafka_acl_result_error, rd_kafka_acl_result_t, rd_kafka_error_code,
    rd_kafka_event_CreateAcls_result, rd_kafka_event_DeleteAcls_result,
    rd_kafka_event_DescribeAcls_result, rd_kafka_event_error,
};
use std::{ffi::CString, mem::ManuallyDrop};
use typed_builder::TypedBuilder;

pub(crate) type NativeConfigResource = NativePtr<rd_kafka_ConfigResource_t>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ResourceTypeRequest {
    #[default]
    Any = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY as i32,
    Topic = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC as i32,
    Group = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP as i32,
    Broker = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER as i32,
    TransactionalId = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID as i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ResourceType {
    Topic = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC as i32,
    Group = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP as i32,
    Broker = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER as i32,
    TransactionalId = rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID as i32,
}

impl From<ResourceType> for rd_kafka_ResourceType_t {
    fn from(value: ResourceType) -> Self {
        match value {
            ResourceType::Topic => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
            ResourceType::Group => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP,
            ResourceType::Broker => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER,
            ResourceType::TransactionalId => {
                rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
            }
        }
    }
}

impl From<ResourceType> for ResourceTypeRequest {
    fn from(value: ResourceType) -> Self {
        match value {
            ResourceType::Topic => ResourceTypeRequest::Topic,
            ResourceType::Group => ResourceTypeRequest::Group,
            ResourceType::Broker => ResourceTypeRequest::Broker,
            ResourceType::TransactionalId => ResourceTypeRequest::TransactionalId,
        }
    }
}

impl From<ResourceTypeRequest> for rd_kafka_ResourceType_t {
    fn from(value: ResourceTypeRequest) -> Self {
        match value {
            ResourceTypeRequest::Any => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY,
            ResourceTypeRequest::Topic => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
            ResourceTypeRequest::Group => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP,
            ResourceTypeRequest::Broker => rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER,
            ResourceTypeRequest::TransactionalId => {
                rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
            }
        }
    }
}

impl TryFrom<rd_kafka_ResourceType_t> for ResourceType {
    type Error = i32;

    fn try_from(value: rd_kafka_ResourceType_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC => Ok(ResourceType::Topic),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP => Ok(ResourceType::Group),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER => Ok(ResourceType::Broker),
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID => {
                Ok(ResourceType::TransactionalId)
            }
            _ => Err(value as i32),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AclOperationRequest {
    /// In a filter, matches any AclOperation
    #[default]
    Any = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ANY as u32,
    /// ALL operation
    All = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL as u32,
    /// READ operation
    Read = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ as u32,
    /// WRITE operation
    Write = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE as u32,
    /// CREATE operation
    Create = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE as u32,
    /// DELETE operation
    Delete = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE as u32,
    /// ALTER operation
    Alter = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER as u32,
    /// DESCRIBE operation
    Describe = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE as u32,
    /// CLUSTER_ACTION operation
    ClusterAction = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION as u32,
    /// DESCRIBE_CONFIGS operation
    DescribeConfigs = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS as u32,
    /// ALTER_CONFIGS operation
    AlterConfigs = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS as u32,
    /// IDEMPOTENT_WRITE operation
    IdempotentWrite = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE as u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum AclOperation {
    /// ALL operation
    All = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL as u32,
    /// READ operation
    Read = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ as u32,
    /// WRITE operation
    Write = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE as u32,
    /// CREATE operation
    Create = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE as u32,
    /// DELETE operation
    Delete = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE as u32,
    /// ALTER operation
    Alter = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER as u32,
    /// DESCRIBE operation
    Describe = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE as u32,
    /// CLUSTER_ACTION operation
    ClusterAction = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION as u32,
    /// DESCRIBE_CONFIGS operation
    DescribeConfigs = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS as u32,
    /// ALTER_CONFIGS operation
    AlterConfigs = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS as u32,
    /// IDEMPOTENT_WRITE operation
    IdempotentWrite = rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE as u32,
}

impl From<AclOperation> for rd_kafka_AclOperation_t {
    fn from(value: AclOperation) -> Self {
        match value {
            AclOperation::All => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL,
            AclOperation::Read => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ,
            AclOperation::Write => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE,
            AclOperation::Create => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE,
            AclOperation::Delete => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE,
            AclOperation::Alter => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER,
            AclOperation::Describe => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE,
            AclOperation::ClusterAction => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION
            }
            AclOperation::DescribeConfigs => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS
            }
            AclOperation::AlterConfigs => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS
            }
            AclOperation::IdempotentWrite => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE
            }
        }
    }
}

impl From<AclOperationRequest> for rd_kafka_AclOperation_t {
    fn from(value: AclOperationRequest) -> Self {
        match value {
            AclOperationRequest::Any => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ANY,
            AclOperationRequest::All => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL,
            AclOperationRequest::Read => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ,
            AclOperationRequest::Write => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE,
            AclOperationRequest::Create => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE,
            AclOperationRequest::Delete => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE,
            AclOperationRequest::Alter => rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER,
            AclOperationRequest::Describe => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE
            }
            AclOperationRequest::ClusterAction => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION
            }
            AclOperationRequest::DescribeConfigs => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS
            }
            AclOperationRequest::AlterConfigs => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS
            }
            AclOperationRequest::IdempotentWrite => {
                rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE
            }
        }
    }
}

impl TryFrom<rd_kafka_AclOperation_t> for AclOperation {
    type Error = i32;

    fn try_from(value: rd_kafka_AclOperation_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL => Ok(AclOperation::All),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ => Ok(AclOperation::Read),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE => Ok(AclOperation::Write),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE => Ok(AclOperation::Create),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE => Ok(AclOperation::Delete),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER => Ok(AclOperation::Alter),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE => Ok(AclOperation::Describe),
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION => {
                Ok(AclOperation::ClusterAction)
            }
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS => {
                Ok(AclOperation::DescribeConfigs)
            }
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS => {
                Ok(AclOperation::AlterConfigs)
            }
            rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE => {
                Ok(AclOperation::IdempotentWrite)
            }
            _ => Err(value as i32),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ResourcePatternTypeRequest {
    /// Any (used for lookups)
    #[default]
    Any = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_ANY as u32,
    /// Match: will perform pattern matching
    Match = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_MATCH as u32,
    /// Literal: A literal resource name
    Literal = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL as u32,
    /// Prefixed: A prefixed resource name
    Prefixed = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED as u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ResourcePatternType {
    /// Literal: A literal resource name
    Literal = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL as u32,
    /// Prefixed: A prefixed resource name
    Prefixed = rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED as u32,
}

impl From<ResourcePatternType> for rd_kafka_ResourcePatternType_t {
    fn from(value: ResourcePatternType) -> Self {
        match value {
            ResourcePatternType::Literal => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL
            }
            ResourcePatternType::Prefixed => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED
            }
        }
    }
}

impl From<ResourcePatternType> for ResourcePatternTypeRequest {
    fn from(value: ResourcePatternType) -> Self {
        match value {
            ResourcePatternType::Literal => ResourcePatternTypeRequest::Literal,
            ResourcePatternType::Prefixed => ResourcePatternTypeRequest::Prefixed,
        }
    }
}

impl From<ResourcePatternTypeRequest> for rd_kafka_ResourcePatternType_t {
    fn from(value: ResourcePatternTypeRequest) -> Self {
        match value {
            ResourcePatternTypeRequest::Any => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_ANY
            }
            ResourcePatternTypeRequest::Match => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_MATCH
            }
            ResourcePatternTypeRequest::Literal => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL
            }
            ResourcePatternTypeRequest::Prefixed => {
                rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED
            }
        }
    }
}

impl TryFrom<rd_kafka_ResourcePatternType_t> for ResourcePatternType {
    type Error = i32;

    fn try_from(value: rd_kafka_ResourcePatternType_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL => {
                Ok(ResourcePatternType::Literal)
            }
            rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED => {
                Ok(ResourcePatternType::Prefixed)
            }
            _ => Err(value as i32),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AclPermissionTypeRequest {
    #[default]
    Any = rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ANY as u32,
    Allow = rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW as u32,
    Deny = rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY as u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AclPermissionType {
    Allow = rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW as u32,
    Deny = rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY as u32,
}

impl From<AclPermissionType> for rd_kafka_AclPermissionType_t {
    fn from(value: AclPermissionType) -> Self {
        match value {
            AclPermissionType::Allow => {
                rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
            }
            AclPermissionType::Deny => {
                rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY
            }
        }
    }
}

impl From<AclPermissionTypeRequest> for rd_kafka_AclPermissionType_t {
    fn from(value: AclPermissionTypeRequest) -> Self {
        match value {
            AclPermissionTypeRequest::Any => {
                rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ANY
            }
            AclPermissionTypeRequest::Allow => {
                rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
            }
            AclPermissionTypeRequest::Deny => {
                rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY
            }
        }
    }
}

impl TryFrom<rd_kafka_AclPermissionType_t> for AclPermissionType {
    type Error = i32;

    fn try_from(value: rd_kafka_AclPermissionType_t) -> Result<Self, Self::Error> {
        match value {
            rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW => {
                Ok(AclPermissionType::Allow)
            }
            rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY => {
                Ok(AclPermissionType::Deny)
            }
            _ => Err(value as i32),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(into, prefix = "filter_by_")))]
pub struct AclBindingFilter {
    resource_type: ResourceTypeRequest,
    #[builder(setter(strip_option))]
    name: Option<String>,
    resource_pattern_type: ResourcePatternTypeRequest,
    #[builder(setter(strip_option))]
    principal: Option<String>,
    #[builder(setter(strip_option))]
    host: Option<String>,
    operation: AclOperationRequest,
    permission_type: AclPermissionTypeRequest,
}

impl AclBindingFilter {
    pub fn any() -> Self {
        Self {
            resource_type: Default::default(),
            name: Default::default(),
            resource_pattern_type: Default::default(),
            principal: Default::default(),
            host: Default::default(),
            operation: Default::default(),
            permission_type: Default::default(),
        }
    }
}

pub(super) type NativeAclBindingFilter = NativePtr<rd_kafka_AclBindingFilter_t>;

impl AclBindingFilter {
    pub(super) fn to_native(&self, err_buf: &mut ErrBuf) -> Result<NativeAclBindingFilter> {
        let mut name = ManuallyDrop::new(self.name.as_deref().map(CString::new).transpose()?);
        let mut principal =
            ManuallyDrop::new(self.principal.as_deref().map(CString::new).transpose()?);
        let mut host = ManuallyDrop::new(self.host.as_deref().map(CString::new).transpose()?);
        let native_acl_binding_filter = unsafe {
            rdkafka2_sys::rd_kafka_AclBindingFilter_new(
                self.resource_type.into(),
                name.as_ref()
                    .map(|s| s.as_ptr())
                    .unwrap_or(std::ptr::null_mut()),
                self.resource_pattern_type.into(),
                principal
                    .as_ref()
                    .map(|s| s.as_ptr())
                    .unwrap_or(std::ptr::null_mut()),
                host.as_ref()
                    .map(|s| s.as_ptr())
                    .unwrap_or(std::ptr::null_mut()),
                self.operation.into(),
                self.permission_type.into(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        unsafe {
            ManuallyDrop::drop(&mut name);
            ManuallyDrop::drop(&mut principal);
            ManuallyDrop::drop(&mut host);
        }

        (!native_acl_binding_filter.is_null())
            .then(|| unsafe { NativeAclBindingFilter::from_ptr(native_acl_binding_filter) })
            .ok_or(KafkaError::AdminOpCreation(err_buf.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
pub struct AclBinding {
    pub resource_type: ResourceType,
    #[builder(setter(into))]
    pub name: String,
    pub resource_pattern_type: ResourcePatternType,
    #[builder(setter(into))]
    pub principal: String,
    #[builder(setter(into))]
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: AclPermissionType,
}

pub(super) type NativeAclBinding = NativePtr<rd_kafka_AclBinding_t>;

impl AclBinding {
    pub(super) fn to_native(&self, err_buf: &mut ErrBuf) -> Result<NativeAclBinding> {
        let native_acl_binding = unsafe {
            let name = CString::new(self.name.as_str())?;
            let pricipal = CString::new(self.principal.as_str())?;
            let host = CString::new(self.host.as_str())?;

            rdkafka2_sys::rd_kafka_AclBinding_new(
                self.resource_type.into(),
                name.as_ptr(),
                self.resource_pattern_type.into(),
                pricipal.as_ptr(),
                host.as_ptr(),
                self.operation.into(),
                self.permission_type.into(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };

        (!native_acl_binding.is_null())
            .then(|| unsafe { NativeAclBinding::from_ptr(native_acl_binding) })
            .ok_or(KafkaError::AdminOpCreation(err_buf.to_string()))
    }
}

fn build_acl_binding(acl: *const rd_kafka_AclBinding_t) -> Result<AclBinding> {
    unsafe {
        let resp_err = rd_kafka_error_code(rd_kafka_AclBinding_error(acl));
        if let Some(err) = RDKafkaErrorCode::from(resp_err).error() {
            Err(KafkaError::AdminOp(err))
        } else {
            let acl_binding = AclBinding {
                resource_type: rd_kafka_AclBinding_restype(acl)
                    .try_into()
                    .map_err(KafkaError::UnknownResource)?,
                name: cstr_to_owned(rd_kafka_AclBinding_name(acl)),
                host: cstr_to_owned(rd_kafka_AclBinding_host(acl)),
                principal: cstr_to_owned(rd_kafka_AclBinding_principal(acl)),
                operation: rd_kafka_AclBinding_operation(acl)
                    .try_into()
                    .map_err(KafkaError::UnknownAclOperation)?,
                resource_pattern_type: rd_kafka_AclBinding_resource_pattern_type(acl)
                    .try_into()
                    .map_err(KafkaError::UnknownAclResourcePatternType)?,
                permission_type: rd_kafka_AclBinding_permission_type(acl)
                    .try_into()
                    .map_err(KafkaError::UnknownAclPermissionType)?,
            };

            Ok(acl_binding)
        }
    }
}

pub(super) async fn handle_describe_acls_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<Result<AclBinding>>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::DescribeAclsResult))
        .and_then(|(_, evt)| async move {
            unsafe {
                println!("{:?}", rd_kafka_event_error(evt.0.ptr()));
                if let Some(err) = RDKafkaErrorCode::from(rd_kafka_event_error(evt.0.ptr())).error()
                {
                    return Err(KafkaError::AdminOp(err));
                }
            }

            let res = unsafe { rd_kafka_event_DescribeAcls_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| unsafe {
                    let mut n = 0;
                    let native_acl_bindings = rd_kafka_DescribeAcls_result_acls(res, &mut n);
                    let mut acl_bindings = Vec::with_capacity(n);
                    for i in 0..n {
                        let native_acl_binding = *native_acl_bindings.add(i);
                        acl_bindings.push(build_acl_binding(native_acl_binding));
                    }

                    acl_bindings
                })
                .ok_or_else(|| {
                    KafkaError::AdminOpCreation("error while creating ACL descriptions".to_string())
                })
        })
        .await
}

fn build_acl_results(acls: *const *const rd_kafka_acl_result_t, n: usize) -> Vec<Result<()>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let acl = unsafe { *acls.add(i) };
        let err = unsafe { rd_kafka_error_code(rd_kafka_acl_result_error(acl)) };
        if let Some(err) = RDKafkaErrorCode::from(err).error() {
            out.push(Err(KafkaError::AdminOp(err)));
        } else {
            out.push(Ok(()));
        }
    }

    out
}

pub(super) async fn handle_create_acls_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<Result<()>>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::CreateAclsResult))
        .and_then(|(_, evt)| async move {
            unsafe {
                println!("{:?}", rd_kafka_event_error(evt.0.ptr()));
                if let Some(err) = RDKafkaErrorCode::from(rd_kafka_event_error(evt.0.ptr())).error()
                {
                    return Err(KafkaError::AdminOp(err));
                }
            }

            let res = unsafe { rd_kafka_event_CreateAcls_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| unsafe {
                    let mut n = 0;
                    let native_acl_results = rd_kafka_CreateAcls_result_acls(res, &mut n);
                    build_acl_results(native_acl_results, n)
                })
                .ok_or_else(|| {
                    KafkaError::AdminOpCreation("error while creating ACL rules".to_string())
                })
        })
        .await
}

pub(super) async fn handle_delete_acls_result(
    rx: oneshot::Receiver<NativeEvent>,
) -> Result<Vec<Result<Vec<Result<AclBinding>>>>> {
    rx.map_err(|_| KafkaError::AdminApiError)
        .and_then(and_then_event(RDKafkaEventType::DeleteAclsResult))
        .and_then(|(_, evt)| async move {
            unsafe {
                println!("{:?}", rd_kafka_event_error(evt.0.ptr()));
                if let Some(err) = RDKafkaErrorCode::from(rd_kafka_event_error(evt.0.ptr())).error()
                {
                    return Err(KafkaError::AdminOp(err));
                }
            }

            let res = unsafe { rd_kafka_event_DeleteAcls_result(evt.0.ptr()) };

            (!res.is_null())
                .then(|| unsafe {
                    let mut n = 0;
                    let native_acl_binding_responses =
                        rd_kafka_DeleteAcls_result_responses(res, &mut n);
                    let mut acl_bindings = Vec::with_capacity(n);
                    for i in 0..n {
                        let native_acl_binding_response = *native_acl_binding_responses.add(i);
                        let ret = RDKafkaErrorCode::from(rd_kafka_error_code(
                            rd_kafka_DeleteAcls_result_response_error(native_acl_binding_response),
                        ));
                        if let Some(err) = ret.error() {
                            acl_bindings.push(Err(KafkaError::AdminOp(err)));
                        } else {
                            let mut m = 0;
                            let native_acl_bindings =
                                rd_kafka_DeleteAcls_result_response_matching_acls(
                                    native_acl_binding_response,
                                    &mut m,
                                );
                            let mut acl_bindings_part = Vec::with_capacity(m);
                            for j in 0..m {
                                let native_acl_binding = *native_acl_bindings.add(j);
                                acl_bindings_part.push(build_acl_binding(native_acl_binding));
                            }
                            acl_bindings.push(Ok(acl_bindings_part));
                        }
                    }

                    acl_bindings
                })
                .ok_or_else(|| {
                    KafkaError::AdminOpCreation("error while creating ACL rules".to_string())
                })
        })
        .await
}
