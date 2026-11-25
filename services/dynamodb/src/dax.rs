//! Local metadata emulation for Amazon DAX control-plane operations
//! (`X-Amz-Target: AmazonDAXV3.<Operation>`).
//!
//! Emulates cluster, subnet group, parameter group, and tag lifecycle
//! deterministically in the DynamoDB service state.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::errors::ApiError;
use crate::model::Tag;
use crate::server::OpResult;

const ACCOUNT_ID: &str = "000000000000";
pub const DEFAULT_SUBNET_GROUP: &str = "default";
pub const DEFAULT_PARAMETER_GROUP: &str = "default.dax1.0";

// --- Persisted & response models --------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxEndpoint {
    #[serde(rename = "Address")]
    pub address: String,
    #[serde(rename = "Port")]
    pub port: i64,
    #[serde(rename = "URL", default, skip_serializing_if = "String::is_empty")]
    pub url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxNode {
    #[serde(rename = "AvailabilityZone")]
    pub availability_zone: String,
    #[serde(rename = "Endpoint")]
    pub endpoint: DaxEndpoint,
    #[serde(rename = "NodeCreateTime")]
    pub node_create_time: i64,
    #[serde(rename = "NodeId")]
    pub node_id: String,
    #[serde(rename = "NodeStatus")]
    pub node_status: String,
    #[serde(rename = "ParameterGroupStatus")]
    pub parameter_group_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxParameterGroupStatus {
    #[serde(
        rename = "NodeIdsToReboot",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub node_ids_to_reboot: Vec<String>,
    #[serde(rename = "ParameterApplyStatus")]
    pub parameter_apply_status: String,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxSecurityGroupMembership {
    #[serde(rename = "SecurityGroupIdentifier")]
    pub security_group_identifier: String,
    #[serde(rename = "Status")]
    pub status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxSseDescription {
    #[serde(rename = "Status")]
    pub status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxNotificationConfiguration {
    #[serde(rename = "TopicArn", default, skip_serializing_if = "String::is_empty")]
    pub topic_arn: String,
    #[serde(
        rename = "TopicStatus",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub topic_status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxCluster {
    #[serde(rename = "ActiveNodes")]
    pub active_nodes: i64,
    #[serde(rename = "ClusterArn")]
    pub cluster_arn: String,
    #[serde(rename = "ClusterDiscoveryEndpoint")]
    pub cluster_discovery_endpoint: DaxEndpoint,
    #[serde(rename = "ClusterEndpointEncryptionType")]
    pub cluster_endpoint_encryption_type: String,
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(
        rename = "Description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub description: String,
    #[serde(rename = "IamRoleArn")]
    pub iam_role_arn: String,
    #[serde(rename = "NodeType")]
    pub node_type: String,
    #[serde(rename = "Nodes")]
    pub nodes: Vec<DaxNode>,
    #[serde(
        rename = "NotificationConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub notification_configuration: Option<DaxNotificationConfiguration>,
    #[serde(rename = "ParameterGroup")]
    pub parameter_group: DaxParameterGroupStatus,
    #[serde(
        rename = "PreferredMaintenanceWindow",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub preferred_maintenance_window: String,
    #[serde(rename = "SSEDescription")]
    pub sse_description: DaxSseDescription,
    #[serde(
        rename = "SecurityGroups",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub security_groups: Vec<DaxSecurityGroupMembership>,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "SubnetGroup")]
    pub subnet_group: String,
    #[serde(rename = "TotalNodes")]
    pub total_nodes: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxClusterRecord {
    pub cluster: DaxCluster,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxSubnet {
    #[serde(rename = "SubnetAvailabilityZone")]
    pub subnet_availability_zone: String,
    #[serde(rename = "SubnetIdentifier")]
    pub subnet_identifier: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxSubnetGroup {
    #[serde(
        rename = "Description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub description: String,
    #[serde(rename = "SubnetGroupName")]
    pub subnet_group_name: String,
    #[serde(rename = "Subnets")]
    pub subnets: Vec<DaxSubnet>,
    #[serde(rename = "VpcId")]
    pub vpc_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxParameterGroup {
    #[serde(
        rename = "Description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub description: String,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxParameterGroupRecord {
    pub group: DaxParameterGroup,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaxParameter {
    #[serde(rename = "AllowedValues")]
    pub allowed_values: String,
    #[serde(rename = "ChangeType")]
    pub change_type: String,
    #[serde(rename = "DataType")]
    pub data_type: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "IsModifiable")]
    pub is_modifiable: String,
    #[serde(rename = "ParameterName")]
    pub parameter_name: String,
    #[serde(rename = "ParameterType")]
    pub parameter_type: String,
    #[serde(rename = "ParameterValue")]
    pub parameter_value: String,
    #[serde(rename = "Source")]
    pub source: String,
}

// --- Request shapes ---------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DaxSseSpecification {
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateClusterRequest {
    #[serde(rename = "AvailabilityZones")]
    pub availability_zones: Vec<String>,
    #[serde(rename = "ClusterEndpointEncryptionType")]
    pub cluster_endpoint_encryption_type: String,
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "IamRoleArn")]
    pub iam_role_arn: String,
    #[serde(rename = "NodeType")]
    pub node_type: String,
    #[serde(rename = "NotificationTopicArn")]
    pub notification_topic_arn: String,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
    #[serde(rename = "PreferredMaintenanceWindow")]
    pub preferred_maintenance_window: String,
    #[serde(rename = "ReplicationFactor")]
    pub replication_factor: i64,
    #[serde(rename = "SSESpecification")]
    pub sse_specification: Option<DaxSseSpecification>,
    #[serde(rename = "SecurityGroupIds")]
    pub security_group_ids: Vec<String>,
    #[serde(rename = "SubnetGroupName")]
    pub subnet_group_name: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DescribeClustersRequest {
    #[serde(rename = "ClusterNames")]
    pub cluster_names: Vec<String>,
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateClusterRequest {
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(rename = "Description")]
    pub description: Option<String>,
    #[serde(rename = "NotificationTopicArn")]
    pub notification_topic_arn: Option<String>,
    #[serde(rename = "NotificationTopicStatus")]
    pub notification_topic_status: Option<String>,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: Option<String>,
    #[serde(rename = "PreferredMaintenanceWindow")]
    pub preferred_maintenance_window: Option<String>,
    #[serde(rename = "SecurityGroupIds")]
    pub security_group_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteClusterRequest {
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct IncreaseReplicationFactorRequest {
    #[serde(rename = "AvailabilityZones")]
    pub availability_zones: Vec<String>,
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(rename = "NewReplicationFactor")]
    pub new_replication_factor: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DecreaseReplicationFactorRequest {
    #[serde(rename = "AvailabilityZones")]
    pub availability_zones: Vec<String>,
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(rename = "NewReplicationFactor")]
    pub new_replication_factor: i64,
    #[serde(rename = "NodeIdsToRemove")]
    pub node_ids_to_remove: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RebootNodeRequest {
    #[serde(rename = "ClusterName")]
    pub cluster_name: String,
    #[serde(rename = "NodeId")]
    pub node_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateSubnetGroupRequest {
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "SubnetGroupName")]
    pub subnet_group_name: String,
    #[serde(rename = "SubnetIds")]
    pub subnet_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DescribeSubnetGroupsRequest {
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
    #[serde(rename = "SubnetGroupNames")]
    pub subnet_group_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateSubnetGroupRequest {
    #[serde(rename = "Description")]
    pub description: Option<String>,
    #[serde(rename = "SubnetGroupName")]
    pub subnet_group_name: String,
    #[serde(rename = "SubnetIds")]
    pub subnet_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteSubnetGroupRequest {
    #[serde(rename = "SubnetGroupName")]
    pub subnet_group_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateParameterGroupRequest {
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DescribeParameterGroupsRequest {
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
    #[serde(rename = "ParameterGroupNames")]
    pub parameter_group_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ParameterNameValue {
    #[serde(rename = "ParameterName")]
    pub parameter_name: String,
    #[serde(rename = "ParameterValue")]
    pub parameter_value: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateParameterGroupRequest {
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
    #[serde(rename = "ParameterNameValues")]
    pub parameter_name_values: Vec<ParameterNameValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteParameterGroupRequest {
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DescribeParametersRequest {
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
    #[serde(rename = "ParameterGroupName")]
    pub parameter_group_name: String,
    #[serde(rename = "Source")]
    pub source: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DaxListTagsRequest {
    #[serde(rename = "NextToken")]
    pub next_token: String,
    #[serde(rename = "ResourceName")]
    pub resource_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DaxTagResourceRequest {
    #[serde(rename = "ResourceName")]
    pub resource_name: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DaxUntagResourceRequest {
    #[serde(rename = "ResourceName")]
    pub resource_name: String,
    #[serde(rename = "TagKeys")]
    pub tag_keys: Vec<String>,
}

// --- Store & operations -----------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaxState {
    pub clusters: BTreeMap<String, DaxClusterRecord>,
    pub subnet_groups: BTreeMap<String, DaxSubnetGroup>,
    pub parameter_groups: BTreeMap<String, DaxParameterGroupRecord>,
}

pub fn default_dax_parameters() -> BTreeMap<String, String> {
    let mut params = BTreeMap::new();
    params.insert("query-ttl-millis".to_string(), "300000".to_string());
    params.insert("record-ttl-millis".to_string(), "300000".to_string());
    params
}

pub fn cluster_arn(region: &str, name: &str) -> String {
    format!("arn:aws:dax:{region}:{ACCOUNT_ID}:cache/{name}")
}

fn validate_cluster_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > 20 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name must be between 1 and 20 characters",
        ));
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name must start with a lowercase letter",
        ));
    }
    if name.ends_with('-') || name.contains("--") {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name cannot end with a hyphen or contain consecutive hyphens",
        ));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name must contain only lowercase letters, digits, and hyphens",
        ));
    }
    Ok(())
}

fn build_nodes(
    cluster_name: &str,
    region: &str,
    port: i64,
    count: i64,
    azs: &[String],
    now: i64,
) -> Vec<DaxNode> {
    let mut nodes = Vec::with_capacity(count as usize);
    for i in 0..count {
        let suffix = (b'a' + (i as u8 % 26)) as char;
        let az = if !azs.is_empty() {
            azs[(i as usize) % azs.len()].clone()
        } else {
            let az_letter = (b'a' + (i as u8 % 3)) as char;
            format!("{region}{az_letter}")
        };
        let node_id = format!("{cluster_name}-{suffix}");
        let address = format!("{node_id}.dax.{region}.devcloud.local");
        nodes.push(DaxNode {
            availability_zone: az,
            endpoint: DaxEndpoint {
                address,
                port,
                url: String::new(),
            },
            node_create_time: now,
            node_id,
            node_status: "available".to_string(),
            parameter_group_status: "in-sync".to_string(),
        });
    }
    nodes
}

pub fn create_cluster(
    state: &mut DaxState,
    region: &str,
    now: i64,
    req: &CreateClusterRequest,
) -> Result<DaxCluster, ApiError> {
    validate_cluster_name(&req.cluster_name)?;
    if req.node_type.trim().is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "node type is required",
        ));
    }
    if req.iam_role_arn.trim().is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "iam role arn is required",
        ));
    }
    if req.replication_factor < 1 || req.replication_factor > 10 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "replication factor must be between 1 and 10",
        ));
    }
    if state.clusters.contains_key(&req.cluster_name) {
        return Err(ApiError::dax(
            400,
            "ClusterAlreadyExistsFault",
            "cluster already exists",
        ));
    }

    let subnet_group = if req.subnet_group_name.is_empty() {
        DEFAULT_SUBNET_GROUP.to_string()
    } else {
        if req.subnet_group_name != DEFAULT_SUBNET_GROUP
            && !state.subnet_groups.contains_key(&req.subnet_group_name)
        {
            return Err(ApiError::dax(
                400,
                "SubnetGroupNotFoundFault",
                "subnet group not found",
            ));
        }
        req.subnet_group_name.clone()
    };

    let param_group = if req.parameter_group_name.is_empty() {
        DEFAULT_PARAMETER_GROUP.to_string()
    } else {
        if req.parameter_group_name != DEFAULT_PARAMETER_GROUP
            && !state
                .parameter_groups
                .contains_key(&req.parameter_group_name)
        {
            return Err(ApiError::dax(
                400,
                "ParameterGroupNotFoundFault",
                "parameter group not found",
            ));
        }
        req.parameter_group_name.clone()
    };

    let encryption_type = if req.cluster_endpoint_encryption_type.is_empty() {
        "NONE".to_string()
    } else if matches!(
        req.cluster_endpoint_encryption_type.as_str(),
        "NONE" | "TLS"
    ) {
        req.cluster_endpoint_encryption_type.clone()
    } else {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster endpoint encryption type must be NONE or TLS",
        ));
    };

    let (scheme, port) = if encryption_type == "TLS" {
        ("daxs", 9111)
    } else {
        ("dax", 8111)
    };
    let host = format!("{}.dax.{region}.devcloud.local", req.cluster_name);
    let endpoint = DaxEndpoint {
        address: host.clone(),
        port,
        url: format!("{scheme}://{host}:{port}"),
    };
    let nodes = build_nodes(
        &req.cluster_name,
        region,
        port,
        req.replication_factor,
        &req.availability_zones,
        now,
    );
    let sse_status = if req
        .sse_specification
        .as_ref()
        .map(|s| s.enabled)
        .unwrap_or(false)
    {
        "ENABLED"
    } else {
        "DISABLED"
    };
    let notification = if req.notification_topic_arn.is_empty() {
        None
    } else {
        Some(DaxNotificationConfiguration {
            topic_arn: req.notification_topic_arn.clone(),
            topic_status: "active".to_string(),
        })
    };
    let security_groups = req
        .security_group_ids
        .iter()
        .map(|id| DaxSecurityGroupMembership {
            security_group_identifier: id.clone(),
            status: "active".to_string(),
        })
        .collect();
    let mut tags = BTreeMap::new();
    for tag in &req.tags {
        if tag.key.is_empty() {
            return Err(ApiError::dax(
                400,
                "InvalidParameterValueException",
                "tag key is required",
            ));
        }
        tags.insert(tag.key.clone(), tag.value.clone());
    }

    let cluster = DaxCluster {
        active_nodes: req.replication_factor,
        cluster_arn: cluster_arn(region, &req.cluster_name),
        cluster_discovery_endpoint: endpoint,
        cluster_endpoint_encryption_type: encryption_type,
        cluster_name: req.cluster_name.clone(),
        description: req.description.clone(),
        iam_role_arn: req.iam_role_arn.clone(),
        node_type: req.node_type.clone(),
        nodes,
        notification_configuration: notification,
        parameter_group: DaxParameterGroupStatus {
            node_ids_to_reboot: Vec::new(),
            parameter_apply_status: "in-sync".to_string(),
            parameter_group_name: param_group,
        },
        preferred_maintenance_window: if req.preferred_maintenance_window.is_empty() {
            "sun:05:00-sun:06:00".to_string()
        } else {
            req.preferred_maintenance_window.clone()
        },
        sse_description: DaxSseDescription {
            status: sse_status.to_string(),
        },
        security_groups,
        status: "available".to_string(),
        subnet_group,
        total_nodes: req.replication_factor,
    };
    state.clusters.insert(
        req.cluster_name.clone(),
        DaxClusterRecord {
            cluster: cluster.clone(),
            tags,
        },
    );
    Ok(cluster)
}

pub fn describe_clusters(
    state: &DaxState,
    req: &DescribeClustersRequest,
) -> Result<(Vec<DaxCluster>, Option<String>), ApiError> {
    if req.max_results < 0 || req.max_results > 100 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "max results must be between 1 and 100",
        ));
    }
    let all: Vec<DaxCluster> = if req.cluster_names.is_empty() {
        state.clusters.values().map(|r| r.cluster.clone()).collect()
    } else {
        let mut selected = Vec::with_capacity(req.cluster_names.len());
        for name in &req.cluster_names {
            let Some(rec) = state.clusters.get(name) else {
                return Err(ApiError::dax(
                    400,
                    "ClusterNotFoundFault",
                    "cluster not found",
                ));
            };
            selected.push(rec.cluster.clone());
        }
        selected
    };
    paginate_named(all, |c| &c.cluster_name, &req.next_token, req.max_results)
}

pub fn update_cluster(
    state: &mut DaxState,
    req: &UpdateClusterRequest,
) -> Result<DaxCluster, ApiError> {
    if req.cluster_name.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name is required",
        ));
    }
    if let Some(pg) = &req.parameter_group_name {
        if pg != DEFAULT_PARAMETER_GROUP && !state.parameter_groups.contains_key(pg) {
            return Err(ApiError::dax(
                400,
                "ParameterGroupNotFoundFault",
                "parameter group not found",
            ));
        }
    }
    let Some(rec) = state.clusters.get_mut(&req.cluster_name) else {
        return Err(ApiError::dax(
            400,
            "ClusterNotFoundFault",
            "cluster not found",
        ));
    };
    if let Some(desc) = &req.description {
        rec.cluster.description = desc.clone();
    }
    if let Some(window) = &req.preferred_maintenance_window {
        rec.cluster.preferred_maintenance_window = window.clone();
    }
    if let Some(pg) = &req.parameter_group_name {
        rec.cluster.parameter_group.parameter_group_name = pg.clone();
        rec.cluster.parameter_group.parameter_apply_status = "in-sync".to_string();
    }
    if let Some(sgs) = &req.security_group_ids {
        rec.cluster.security_groups = sgs
            .iter()
            .map(|id| DaxSecurityGroupMembership {
                security_group_identifier: id.clone(),
                status: "active".to_string(),
            })
            .collect();
    }
    if req.notification_topic_arn.is_some() || req.notification_topic_status.is_some() {
        let mut cfg = rec
            .cluster
            .notification_configuration
            .clone()
            .unwrap_or_default();
        if let Some(arn) = &req.notification_topic_arn {
            cfg.topic_arn = arn.clone();
            if cfg.topic_status.is_empty() {
                cfg.topic_status = "active".to_string();
            }
        }
        if let Some(status) = &req.notification_topic_status {
            cfg.topic_status = status.clone();
        }
        rec.cluster.notification_configuration = Some(cfg);
    }
    Ok(rec.cluster.clone())
}

pub fn delete_cluster(
    state: &mut DaxState,
    req: &DeleteClusterRequest,
) -> Result<DaxCluster, ApiError> {
    if req.cluster_name.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "cluster name is required",
        ));
    }
    let Some(mut rec) = state.clusters.remove(&req.cluster_name) else {
        return Err(ApiError::dax(
            400,
            "ClusterNotFoundFault",
            "cluster not found",
        ));
    };
    rec.cluster.status = "deleting".to_string();
    Ok(rec.cluster)
}

pub fn increase_replication_factor(
    state: &mut DaxState,
    region: &str,
    now: i64,
    req: &IncreaseReplicationFactorRequest,
) -> Result<DaxCluster, ApiError> {
    let Some(rec) = state.clusters.get_mut(&req.cluster_name) else {
        return Err(ApiError::dax(
            400,
            "ClusterNotFoundFault",
            "cluster not found",
        ));
    };
    if req.new_replication_factor <= rec.cluster.total_nodes || req.new_replication_factor > 10 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "new replication factor must be greater than current total nodes and at most 10",
        ));
    }
    let port = rec.cluster.cluster_discovery_endpoint.port;
    let existing_ids: BTreeSet<String> = rec
        .cluster
        .nodes
        .iter()
        .map(|n| n.node_id.clone())
        .collect();
    let needed = (req.new_replication_factor - rec.cluster.total_nodes) as usize;
    let mut added = 0usize;
    for idx in 0..26u8 {
        if added == needed {
            break;
        }
        let suffix = (b'a' + idx) as char;
        let node_id = format!("{}-{suffix}", rec.cluster.cluster_name);
        if existing_ids.contains(&node_id) {
            continue;
        }
        let az = if !req.availability_zones.is_empty() {
            req.availability_zones[added % req.availability_zones.len()].clone()
        } else {
            format!("{region}{}", (b'a' + (idx % 3)) as char)
        };
        rec.cluster.nodes.push(DaxNode {
            availability_zone: az,
            endpoint: DaxEndpoint {
                address: format!("{node_id}.dax.{region}.devcloud.local"),
                port,
                url: String::new(),
            },
            node_create_time: now,
            node_id,
            node_status: "available".to_string(),
            parameter_group_status: "in-sync".to_string(),
        });
        added += 1;
    }
    rec.cluster.total_nodes = rec.cluster.nodes.len() as i64;
    rec.cluster.active_nodes = rec.cluster.total_nodes;
    Ok(rec.cluster.clone())
}

pub fn decrease_replication_factor(
    state: &mut DaxState,
    req: &DecreaseReplicationFactorRequest,
) -> Result<DaxCluster, ApiError> {
    let Some(rec) = state.clusters.get_mut(&req.cluster_name) else {
        return Err(ApiError::dax(
            400,
            "ClusterNotFoundFault",
            "cluster not found",
        ));
    };
    if req.new_replication_factor < 1 || req.new_replication_factor >= rec.cluster.total_nodes {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "new replication factor must be at least 1 and less than current total nodes",
        ));
    }
    let to_remove = (rec.cluster.total_nodes - req.new_replication_factor) as usize;
    if !req.node_ids_to_remove.is_empty() {
        for id in &req.node_ids_to_remove {
            if !rec.cluster.nodes.iter().any(|n| &n.node_id == id) {
                return Err(ApiError::dax(400, "NodeNotFoundFault", "node not found"));
            }
        }
        if req.node_ids_to_remove.len() > to_remove {
            return Err(ApiError::dax(
                400,
                "InvalidParameterCombinationException",
                "too many node ids to remove for new replication factor",
            ));
        }
        rec.cluster
            .nodes
            .retain(|n| !req.node_ids_to_remove.contains(&n.node_id));
    }
    while rec.cluster.nodes.len() as i64 > req.new_replication_factor {
        rec.cluster.nodes.pop();
    }
    rec.cluster.total_nodes = rec.cluster.nodes.len() as i64;
    rec.cluster.active_nodes = rec.cluster.total_nodes;
    Ok(rec.cluster.clone())
}

pub fn reboot_node(state: &mut DaxState, req: &RebootNodeRequest) -> Result<DaxCluster, ApiError> {
    let Some(rec) = state.clusters.get_mut(&req.cluster_name) else {
        return Err(ApiError::dax(
            400,
            "ClusterNotFoundFault",
            "cluster not found",
        ));
    };
    if !rec.cluster.nodes.iter().any(|n| n.node_id == req.node_id) {
        return Err(ApiError::dax(400, "NodeNotFoundFault", "node not found"));
    }
    Ok(rec.cluster.clone())
}

// --- Subnet groups ----------------------------------------------------------

pub fn create_subnet_group(
    state: &mut DaxState,
    region: &str,
    req: &CreateSubnetGroupRequest,
) -> Result<DaxSubnetGroup, ApiError> {
    if req.subnet_group_name.trim().is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "subnet group name is required",
        ));
    }
    if req.subnet_ids.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "subnet ids are required",
        ));
    }
    if state.subnet_groups.contains_key(&req.subnet_group_name) {
        return Err(ApiError::dax(
            400,
            "SubnetGroupAlreadyExistsFault",
            "subnet group already exists",
        ));
    }
    let group = DaxSubnetGroup {
        description: req.description.clone(),
        subnet_group_name: req.subnet_group_name.clone(),
        subnets: build_subnets(region, &req.subnet_ids),
        vpc_id: "vpc-00000000".to_string(),
    };
    state
        .subnet_groups
        .insert(req.subnet_group_name.clone(), group.clone());
    Ok(group)
}

pub fn describe_subnet_groups(
    state: &DaxState,
    req: &DescribeSubnetGroupsRequest,
) -> Result<(Vec<DaxSubnetGroup>, Option<String>), ApiError> {
    if req.max_results < 0 || req.max_results > 100 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "max results must be between 1 and 100",
        ));
    }
    let all: Vec<DaxSubnetGroup> = if req.subnet_group_names.is_empty() {
        state.subnet_groups.values().cloned().collect()
    } else {
        let mut selected = Vec::with_capacity(req.subnet_group_names.len());
        for name in &req.subnet_group_names {
            let Some(group) = state.subnet_groups.get(name) else {
                return Err(ApiError::dax(
                    400,
                    "SubnetGroupNotFoundFault",
                    "subnet group not found",
                ));
            };
            selected.push(group.clone());
        }
        selected
    };
    paginate_named(
        all,
        |g| &g.subnet_group_name,
        &req.next_token,
        req.max_results,
    )
}

pub fn update_subnet_group(
    state: &mut DaxState,
    region: &str,
    req: &UpdateSubnetGroupRequest,
) -> Result<DaxSubnetGroup, ApiError> {
    let Some(group) = state.subnet_groups.get_mut(&req.subnet_group_name) else {
        return Err(ApiError::dax(
            400,
            "SubnetGroupNotFoundFault",
            "subnet group not found",
        ));
    };
    if let Some(desc) = &req.description {
        group.description = desc.clone();
    }
    if let Some(ids) = &req.subnet_ids {
        if ids.is_empty() {
            return Err(ApiError::dax(
                400,
                "InvalidParameterValueException",
                "subnet ids cannot be empty",
            ));
        }
        group.subnets = build_subnets(region, ids);
    }
    Ok(group.clone())
}

pub fn delete_subnet_group(
    state: &mut DaxState,
    req: &DeleteSubnetGroupRequest,
) -> Result<String, ApiError> {
    if !state.subnet_groups.contains_key(&req.subnet_group_name) {
        return Err(ApiError::dax(
            400,
            "SubnetGroupNotFoundFault",
            "subnet group not found",
        ));
    }
    if state
        .clusters
        .values()
        .any(|c| c.cluster.subnet_group == req.subnet_group_name)
    {
        return Err(ApiError::dax(
            400,
            "SubnetGroupInUseFault",
            "subnet group is in use by a cluster",
        ));
    }
    state.subnet_groups.remove(&req.subnet_group_name);
    Ok(format!("Subnet group {} deleted.", req.subnet_group_name))
}

fn build_subnets(region: &str, ids: &[String]) -> Vec<DaxSubnet> {
    ids.iter()
        .enumerate()
        .map(|(idx, id)| {
            let az = format!("{region}{}", (b'a' + (idx as u8 % 3)) as char);
            DaxSubnet {
                subnet_availability_zone: az,
                subnet_identifier: id.clone(),
            }
        })
        .collect()
}

// --- Parameter groups -------------------------------------------------------

pub fn create_parameter_group(
    state: &mut DaxState,
    req: &CreateParameterGroupRequest,
) -> Result<DaxParameterGroup, ApiError> {
    if req.parameter_group_name.trim().is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "parameter group name is required",
        ));
    }
    if req.parameter_group_name == DEFAULT_PARAMETER_GROUP
        || state
            .parameter_groups
            .contains_key(&req.parameter_group_name)
    {
        return Err(ApiError::dax(
            400,
            "ParameterGroupAlreadyExistsFault",
            "parameter group already exists",
        ));
    }
    let group = DaxParameterGroup {
        description: req.description.clone(),
        parameter_group_name: req.parameter_group_name.clone(),
    };
    state.parameter_groups.insert(
        req.parameter_group_name.clone(),
        DaxParameterGroupRecord {
            group: group.clone(),
            parameters: default_dax_parameters(),
        },
    );
    Ok(group)
}

pub fn describe_parameter_groups(
    state: &DaxState,
    req: &DescribeParameterGroupsRequest,
) -> Result<(Vec<DaxParameterGroup>, Option<String>), ApiError> {
    if req.max_results < 0 || req.max_results > 100 {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "max results must be between 1 and 100",
        ));
    }
    let all: Vec<DaxParameterGroup> = if req.parameter_group_names.is_empty() {
        state
            .parameter_groups
            .values()
            .map(|r| r.group.clone())
            .collect()
    } else {
        let mut selected = Vec::with_capacity(req.parameter_group_names.len());
        for name in &req.parameter_group_names {
            if name == DEFAULT_PARAMETER_GROUP && !state.parameter_groups.contains_key(name) {
                selected.push(DaxParameterGroup {
                    description: "Default parameter group for DAX".to_string(),
                    parameter_group_name: DEFAULT_PARAMETER_GROUP.to_string(),
                });
                continue;
            }
            let Some(rec) = state.parameter_groups.get(name) else {
                return Err(ApiError::dax(
                    400,
                    "ParameterGroupNotFoundFault",
                    "parameter group not found",
                ));
            };
            selected.push(rec.group.clone());
        }
        selected
    };
    paginate_named(
        all,
        |g| &g.parameter_group_name,
        &req.next_token,
        req.max_results,
    )
}

pub fn update_parameter_group(
    state: &mut DaxState,
    req: &UpdateParameterGroupRequest,
) -> Result<DaxParameterGroup, ApiError> {
    if req.parameter_group_name == DEFAULT_PARAMETER_GROUP {
        return Err(ApiError::dax(
            400,
            "InvalidParameterGroupStateFault",
            "cannot modify default parameter group",
        ));
    }
    let Some(rec) = state.parameter_groups.get_mut(&req.parameter_group_name) else {
        return Err(ApiError::dax(
            400,
            "ParameterGroupNotFoundFault",
            "parameter group not found",
        ));
    };
    for nv in &req.parameter_name_values {
        if !matches!(
            nv.parameter_name.as_str(),
            "query-ttl-millis" | "record-ttl-millis"
        ) {
            return Err(ApiError::dax(
                400,
                "InvalidParameterValueException",
                format!("unsupported parameter {}", nv.parameter_name),
            ));
        }
        if nv.parameter_value.parse::<u64>().is_err() {
            return Err(ApiError::dax(
                400,
                "InvalidParameterValueException",
                format!(
                    "parameter {} must be a non-negative integer",
                    nv.parameter_name
                ),
            ));
        }
        rec.parameters
            .insert(nv.parameter_name.clone(), nv.parameter_value.clone());
    }
    Ok(rec.group.clone())
}

pub fn delete_parameter_group(
    state: &mut DaxState,
    req: &DeleteParameterGroupRequest,
) -> Result<String, ApiError> {
    if req.parameter_group_name == DEFAULT_PARAMETER_GROUP {
        return Err(ApiError::dax(
            400,
            "InvalidParameterGroupStateFault",
            "cannot delete default parameter group",
        ));
    }
    if !state
        .parameter_groups
        .contains_key(&req.parameter_group_name)
    {
        return Err(ApiError::dax(
            400,
            "ParameterGroupNotFoundFault",
            "parameter group not found",
        ));
    }
    if state
        .clusters
        .values()
        .any(|c| c.cluster.parameter_group.parameter_group_name == req.parameter_group_name)
    {
        return Err(ApiError::dax(
            400,
            "InvalidParameterGroupStateFault",
            "parameter group is in use by a cluster",
        ));
    }
    state.parameter_groups.remove(&req.parameter_group_name);
    Ok(format!(
        "Parameter group {} deleted.",
        req.parameter_group_name
    ))
}

pub fn describe_parameters(
    state: &DaxState,
    req: &DescribeParametersRequest,
) -> Result<Vec<DaxParameter>, ApiError> {
    let params = if req.parameter_group_name == DEFAULT_PARAMETER_GROUP
        && !state
            .parameter_groups
            .contains_key(&req.parameter_group_name)
    {
        default_dax_parameters()
    } else if let Some(rec) = state.parameter_groups.get(&req.parameter_group_name) {
        rec.parameters.clone()
    } else {
        return Err(ApiError::dax(
            400,
            "ParameterGroupNotFoundFault",
            "parameter group not found",
        ));
    };
    let source = if req.parameter_group_name == DEFAULT_PARAMETER_GROUP {
        "system"
    } else {
        "user"
    };
    if !req.source.is_empty() && req.source != source {
        return Ok(Vec::new());
    }
    let list = params
        .into_iter()
        .map(|(name, value)| DaxParameter {
            allowed_values: "0-".to_string(),
            change_type: "IMMEDIATE".to_string(),
            data_type: "integer".to_string(),
            description: format!("DAX cache parameter {name}"),
            is_modifiable: "TRUE".to_string(),
            parameter_name: name,
            parameter_type: "DEFAULT".to_string(),
            parameter_value: value,
            source: source.to_string(),
        })
        .collect();
    Ok(list)
}

// --- Tags -------------------------------------------------------------------

fn cluster_for_arn_mut<'a>(
    state: &'a mut DaxState,
    arn: &str,
) -> Result<&'a mut DaxClusterRecord, ApiError> {
    state
        .clusters
        .values_mut()
        .find(|rec| rec.cluster.cluster_arn == arn || rec.cluster.cluster_name == arn)
        .ok_or_else(|| ApiError::dax(400, "ClusterNotFoundFault", "cluster not found"))
}

fn cluster_for_arn<'a>(state: &'a DaxState, arn: &str) -> Result<&'a DaxClusterRecord, ApiError> {
    state
        .clusters
        .values()
        .find(|rec| rec.cluster.cluster_arn == arn || rec.cluster.cluster_name == arn)
        .ok_or_else(|| ApiError::dax(400, "ClusterNotFoundFault", "cluster not found"))
}

pub fn list_tags(state: &DaxState, req: &DaxListTagsRequest) -> Result<Vec<Tag>, ApiError> {
    if req.resource_name.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidARNFault",
            "resource name is required",
        ));
    }
    let rec = cluster_for_arn(state, &req.resource_name)?;
    Ok(rec
        .tags
        .iter()
        .map(|(k, v)| Tag {
            key: k.clone(),
            value: v.clone(),
        })
        .collect())
}

pub fn tag_resource(
    state: &mut DaxState,
    req: &DaxTagResourceRequest,
) -> Result<Vec<Tag>, ApiError> {
    if req.resource_name.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidARNFault",
            "resource name is required",
        ));
    }
    if req.tags.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "tags are required",
        ));
    }
    let rec = cluster_for_arn_mut(state, &req.resource_name)?;
    for tag in &req.tags {
        if tag.key.is_empty() {
            return Err(ApiError::dax(
                400,
                "InvalidParameterValueException",
                "tag key is required",
            ));
        }
        rec.tags.insert(tag.key.clone(), tag.value.clone());
    }
    Ok(rec
        .tags
        .iter()
        .map(|(k, v)| Tag {
            key: k.clone(),
            value: v.clone(),
        })
        .collect())
}

pub fn untag_resource(
    state: &mut DaxState,
    req: &DaxUntagResourceRequest,
) -> Result<Vec<Tag>, ApiError> {
    if req.resource_name.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidARNFault",
            "resource name is required",
        ));
    }
    if req.tag_keys.is_empty() {
        return Err(ApiError::dax(
            400,
            "InvalidParameterValueException",
            "tag keys are required",
        ));
    }
    let rec = cluster_for_arn_mut(state, &req.resource_name)?;
    for key in &req.tag_keys {
        rec.tags.remove(key);
    }
    Ok(rec
        .tags
        .iter()
        .map(|(k, v)| Tag {
            key: k.clone(),
            value: v.clone(),
        })
        .collect())
}

// --- Wire helpers -----------------------------------------------------------

fn paginate_named<T>(
    items: Vec<T>,
    name_of: impl Fn(&T) -> &str,
    next_token: &str,
    max_results: i64,
) -> Result<(Vec<T>, Option<String>), ApiError> {
    let mut start = 0usize;
    if !next_token.is_empty() {
        match items.iter().position(|item| name_of(item) == next_token) {
            Some(pos) => start = pos + 1,
            None => {
                return Err(ApiError::dax(
                    400,
                    "InvalidParameterValueException",
                    "invalid next token",
                ));
            }
        }
    }
    let mut end = items.len();
    if max_results > 0 && start + (max_results as usize) < end {
        end = start + max_results as usize;
    }
    let next = if end < items.len() {
        Some(name_of(&items[end - 1]).to_string())
    } else {
        None
    };
    Ok((
        items.into_iter().skip(start).take(end - start).collect(),
        next,
    ))
}

pub fn json_obj_response(entries: Vec<(&str, Value)>) -> OpResult {
    let mut map = Map::new();
    for (k, v) in entries {
        map.insert(k.to_string(), v);
    }
    Ok(crate::wire_json::to_vec(&Value::Object(map)))
}
