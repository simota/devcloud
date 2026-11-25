//! DynamoDB Global Tables (v1 `2017.11.29` & v2 `2019.11.21` replica updates)
//! and Table Replica Auto Scaling metadata models and validation helpers.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::errors::ApiError;
use crate::model::{BillingModeSummary, TableDescription};

// --- Global Tables v2 (UpdateTable.ReplicaUpdates / TableDescription.Replicas)

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProvisionedThroughputOverride {
    #[serde(
        rename = "ReadCapacityUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub read_capacity_units: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableClassSummary {
    #[serde(
        rename = "TableClass",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub table_class: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaGlobalSecondaryIndexDescription {
    #[serde(
        rename = "IndexName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub index_name: String,
    #[serde(
        rename = "ProvisionedThroughputOverride",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput_override: Option<ProvisionedThroughputOverride>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaDescription {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(
        rename = "ReplicaStatus",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub replica_status: String,
    #[serde(
        rename = "KMSMasterKeyId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub kms_master_key_id: String,
    #[serde(
        rename = "ProvisionedThroughputOverride",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput_override: Option<ProvisionedThroughputOverride>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub global_secondary_indexes: Vec<ReplicaGlobalSecondaryIndexDescription>,
    #[serde(
        rename = "ReplicaTableClassSummary",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_table_class_summary: Option<TableClassSummary>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaGlobalSecondaryIndexUpdateInput {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "ProvisionedThroughputOverride")]
    pub provisioned_throughput_override: Option<ProvisionedThroughputOverride>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateReplicationGroupMemberAction {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(rename = "KMSMasterKeyId")]
    pub kms_master_key_id: String,
    #[serde(rename = "ProvisionedThroughputOverride")]
    pub provisioned_throughput_override: Option<ProvisionedThroughputOverride>,
    #[serde(rename = "GlobalSecondaryIndexes")]
    pub global_secondary_indexes: Vec<ReplicaGlobalSecondaryIndexUpdateInput>,
    #[serde(rename = "TableClassOverride")]
    pub table_class_override: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateReplicationGroupMemberAction {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(rename = "KMSMasterKeyId")]
    pub kms_master_key_id: Option<String>,
    #[serde(rename = "ProvisionedThroughputOverride")]
    pub provisioned_throughput_override: Option<ProvisionedThroughputOverride>,
    #[serde(rename = "GlobalSecondaryIndexes")]
    pub global_secondary_indexes: Vec<ReplicaGlobalSecondaryIndexUpdateInput>,
    #[serde(rename = "TableClassOverride")]
    pub table_class_override: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteReplicationGroupMemberAction {
    #[serde(rename = "RegionName")]
    pub region_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicationGroupUpdate {
    #[serde(rename = "Create")]
    pub create: Option<CreateReplicationGroupMemberAction>,
    #[serde(rename = "Update")]
    pub update: Option<UpdateReplicationGroupMemberAction>,
    #[serde(rename = "Delete")]
    pub delete: Option<DeleteReplicationGroupMemberAction>,
}

pub fn apply_replica_updates(
    description: &mut TableDescription,
    updates: &[ReplicationGroupUpdate],
) -> Result<(), ApiError> {
    for update in updates {
        let count = update.create.is_some() as u8
            + update.update.is_some() as u8
            + update.delete.is_some() as u8;
        if count != 1 {
            return Err(ApiError::validation(
                "each replica update must contain exactly one action",
            ));
        }
        if let Some(create) = &update.create {
            if create.region_name.trim().is_empty() {
                return Err(ApiError::validation("replica region name is required"));
            }
            if description
                .replicas
                .iter()
                .any(|r| r.region_name == create.region_name)
            {
                return Err(ApiError::new(
                    400,
                    "ReplicaAlreadyExistsException",
                    "replica already exists",
                ));
            }
            validate_throughput_override(&create.provisioned_throughput_override)?;
            let gsis = build_replica_gsis(description, &create.global_secondary_indexes)?;
            let table_class = validate_table_class_override(&create.table_class_override)?;
            description.replicas.push(ReplicaDescription {
                region_name: create.region_name.clone(),
                replica_status: "ACTIVE".to_string(),
                kms_master_key_id: create.kms_master_key_id.clone(),
                provisioned_throughput_override: create.provisioned_throughput_override.clone(),
                global_secondary_indexes: gsis,
                replica_table_class_summary: table_class,
            });
            description.global_table_version = "2019.11.21".to_string();
        } else if let Some(upd) = &update.update {
            if upd.region_name.trim().is_empty() {
                return Err(ApiError::validation("replica region name is required"));
            }
            validate_throughput_override(&upd.provisioned_throughput_override)?;
            let gsis = if upd.global_secondary_indexes.is_empty() {
                None
            } else {
                Some(build_replica_gsis(
                    description,
                    &upd.global_secondary_indexes,
                )?)
            };
            let table_class = match &upd.table_class_override {
                Some(cls) => validate_table_class_override(cls)?,
                None => None,
            };
            let Some(replica) = description
                .replicas
                .iter_mut()
                .find(|r| r.region_name == upd.region_name)
            else {
                return Err(ApiError::new(
                    400,
                    "ReplicaNotFoundException",
                    "replica not found",
                ));
            };
            if let Some(kms) = &upd.kms_master_key_id {
                replica.kms_master_key_id = kms.clone();
            }
            if upd.provisioned_throughput_override.is_some() {
                replica.provisioned_throughput_override =
                    upd.provisioned_throughput_override.clone();
            }
            if let Some(new_gsis) = gsis {
                replica.global_secondary_indexes = new_gsis;
            }
            if table_class.is_some() {
                replica.replica_table_class_summary = table_class;
            }
        } else if let Some(del) = &update.delete {
            if del.region_name.trim().is_empty() {
                return Err(ApiError::validation("replica region name is required"));
            }
            let Some(pos) = description
                .replicas
                .iter()
                .position(|r| r.region_name == del.region_name)
            else {
                return Err(ApiError::new(
                    400,
                    "ReplicaNotFoundException",
                    "replica not found",
                ));
            };
            description.replicas.remove(pos);
        }
    }
    Ok(())
}

fn validate_throughput_override(
    override_cfg: &Option<ProvisionedThroughputOverride>,
) -> Result<(), ApiError> {
    if let Some(ov) = override_cfg {
        if let Some(rcu) = ov.read_capacity_units {
            if rcu < 1 {
                return Err(ApiError::validation(
                    "read capacity units override must be at least 1",
                ));
            }
        }
    }
    Ok(())
}

fn validate_table_class_override(value: &str) -> Result<Option<TableClassSummary>, ApiError> {
    if value.is_empty() {
        return Ok(None);
    }
    if matches!(value, "STANDARD" | "STANDARD_INFREQUENT_ACCESS") {
        Ok(Some(TableClassSummary {
            table_class: value.to_string(),
        }))
    } else {
        Err(ApiError::validation(
            "table class override must be STANDARD or STANDARD_INFREQUENT_ACCESS",
        ))
    }
}

fn build_replica_gsis(
    description: &TableDescription,
    inputs: &[ReplicaGlobalSecondaryIndexUpdateInput],
) -> Result<Vec<ReplicaGlobalSecondaryIndexDescription>, ApiError> {
    let mut out = Vec::with_capacity(inputs.len());
    for gsi in inputs {
        if gsi.index_name.is_empty() {
            return Err(ApiError::validation(
                "replica global secondary index name is required",
            ));
        }
        if !description
            .global_secondary_indexes
            .iter()
            .any(|existing| existing.index_name == gsi.index_name)
        {
            return Err(ApiError::validation("global secondary index not found"));
        }
        validate_throughput_override(&gsi.provisioned_throughput_override)?;
        out.push(ReplicaGlobalSecondaryIndexDescription {
            index_name: gsi.index_name.clone(),
            provisioned_throughput_override: gsi.provisioned_throughput_override.clone(),
        });
    }
    Ok(out)
}

// --- Global Tables v1 (2017.11.29) ------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Replica {
    #[serde(rename = "RegionName")]
    pub region_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalTableDescription {
    #[serde(rename = "CreationDateTime")]
    pub creation_date_time: i64,
    #[serde(rename = "GlobalTableArn")]
    pub global_table_arn: String,
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "GlobalTableStatus")]
    pub global_table_status: String,
    #[serde(rename = "ReplicationGroup")]
    pub replication_group: Vec<Replica>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalTableSummary {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "ReplicationGroup")]
    pub replication_group: Vec<Replica>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalTableRecord {
    pub description: GlobalTableDescription,
    #[serde(
        rename = "replicaSettings",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub replica_settings: BTreeMap<String, ReplicaSettingsDescription>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateGlobalTableRequest {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "ReplicationGroup")]
    pub replication_group: Vec<Replica>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GlobalTableNameRequest {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ListGlobalTablesRequest {
    #[serde(rename = "ExclusiveStartGlobalTableName")]
    pub exclusive_start_global_table_name: String,
    #[serde(rename = "Limit")]
    pub limit: i64,
    #[serde(rename = "RegionName")]
    pub region_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateReplicaAction {
    #[serde(rename = "RegionName")]
    pub region_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteReplicaAction {
    #[serde(rename = "RegionName")]
    pub region_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaUpdate {
    #[serde(rename = "Create")]
    pub create: Option<CreateReplicaAction>,
    #[serde(rename = "Delete")]
    pub delete: Option<DeleteReplicaAction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateGlobalTableRequest {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "ReplicaUpdates")]
    pub replica_updates: Vec<ReplicaUpdate>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaGlobalSecondaryIndexSettingsDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(
        rename = "ProvisionedReadCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_read_capacity_auto_scaling_settings: Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ProvisionedReadCapacityUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_read_capacity_units: Option<i64>,
    #[serde(
        rename = "ProvisionedWriteCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_write_capacity_auto_scaling_settings: Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ProvisionedWriteCapacityUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_write_capacity_units: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSettingsDescription {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(rename = "ReplicaStatus")]
    pub replica_status: String,
    #[serde(
        rename = "ReplicaBillingModeSummary",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_billing_mode_summary: Option<BillingModeSummary>,
    #[serde(
        rename = "ReplicaGlobalSecondaryIndexSettings",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub replica_global_secondary_index_settings:
        Vec<ReplicaGlobalSecondaryIndexSettingsDescription>,
    #[serde(
        rename = "ReplicaProvisionedReadCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_read_capacity_auto_scaling_settings:
        Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ReplicaProvisionedReadCapacityUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_read_capacity_units: Option<i64>,
    #[serde(
        rename = "ReplicaProvisionedWriteCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_write_capacity_auto_scaling_settings:
        Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ReplicaProvisionedWriteCapacityUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_write_capacity_units: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GlobalTableGlobalSecondaryIndexSettingsUpdate {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "ProvisionedWriteCapacityUnits")]
    pub provisioned_write_capacity_units: Option<i64>,
    #[serde(rename = "ProvisionedWriteCapacityAutoScalingSettingsUpdate")]
    pub provisioned_write_capacity_auto_scaling_settings_update: Option<AutoScalingSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaGlobalSecondaryIndexSettingsUpdate {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "ProvisionedReadCapacityUnits")]
    pub provisioned_read_capacity_units: Option<i64>,
    #[serde(rename = "ProvisionedReadCapacityAutoScalingSettingsUpdate")]
    pub provisioned_read_capacity_auto_scaling_settings_update: Option<AutoScalingSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaSettingsUpdate {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(rename = "ReplicaProvisionedReadCapacityUnits")]
    pub replica_provisioned_read_capacity_units: Option<i64>,
    #[serde(rename = "ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate")]
    pub replica_provisioned_read_capacity_auto_scaling_settings_update:
        Option<AutoScalingSettingsUpdate>,
    #[serde(rename = "ReplicaGlobalSecondaryIndexSettingsUpdate")]
    pub replica_global_secondary_index_settings_update:
        Vec<ReplicaGlobalSecondaryIndexSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateGlobalTableSettingsRequest {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "GlobalTableBillingMode")]
    pub global_table_billing_mode: String,
    #[serde(rename = "GlobalTableProvisionedWriteCapacityUnits")]
    pub global_table_provisioned_write_capacity_units: Option<i64>,
    #[serde(rename = "GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate")]
    pub global_table_provisioned_write_capacity_auto_scaling_settings_update:
        Option<AutoScalingSettingsUpdate>,
    #[serde(rename = "GlobalTableGlobalSecondaryIndexSettingsUpdate")]
    pub global_table_global_secondary_index_settings_update:
        Vec<GlobalTableGlobalSecondaryIndexSettingsUpdate>,
    #[serde(rename = "ReplicaSettingsUpdate")]
    pub replica_settings_update: Vec<ReplicaSettingsUpdate>,
}

#[derive(Debug, Serialize)]
pub struct GlobalTableDescriptionResponse {
    #[serde(rename = "GlobalTableDescription")]
    pub global_table_description: GlobalTableDescription,
}

#[derive(Debug, Serialize)]
pub struct ListGlobalTablesResponse {
    #[serde(rename = "GlobalTables")]
    pub global_tables: Vec<GlobalTableSummary>,
    #[serde(
        rename = "LastEvaluatedGlobalTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_global_table_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GlobalTableSettingsResponse {
    #[serde(rename = "GlobalTableName")]
    pub global_table_name: String,
    #[serde(rename = "ReplicaSettings")]
    pub replica_settings: Vec<ReplicaSettingsDescription>,
}

// --- Auto Scaling (Describe/UpdateTableReplicaAutoScaling) -------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AutoScalingTargetTrackingScalingPolicyConfigurationDescription {
    #[serde(
        rename = "DisableScaleIn",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_scale_in: Option<bool>,
    #[serde(
        rename = "ScaleInCooldown",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scale_in_cooldown: Option<i64>,
    #[serde(
        rename = "ScaleOutCooldown",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scale_out_cooldown: Option<i64>,
    #[serde(rename = "TargetValue")]
    pub target_value: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AutoScalingPolicyDescription {
    #[serde(
        rename = "PolicyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub policy_name: String,
    #[serde(
        rename = "TargetTrackingScalingPolicyConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub target_tracking_scaling_policy_configuration:
        Option<AutoScalingTargetTrackingScalingPolicyConfigurationDescription>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AutoScalingSettingsDescription {
    #[serde(
        rename = "AutoScalingDisabled",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub auto_scaling_disabled: Option<bool>,
    #[serde(
        rename = "AutoScalingRoleArn",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub auto_scaling_role_arn: String,
    #[serde(
        rename = "MaximumUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub maximum_units: Option<i64>,
    #[serde(
        rename = "MinimumUnits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub minimum_units: Option<i64>,
    #[serde(
        rename = "ScalingPolicies",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub scaling_policies: Vec<AutoScalingPolicyDescription>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct AutoScalingTargetTrackingScalingPolicyConfigurationUpdate {
    #[serde(rename = "DisableScaleIn")]
    pub disable_scale_in: Option<bool>,
    #[serde(rename = "ScaleInCooldown")]
    pub scale_in_cooldown: Option<i64>,
    #[serde(rename = "ScaleOutCooldown")]
    pub scale_out_cooldown: Option<i64>,
    #[serde(rename = "TargetValue")]
    pub target_value: f64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct AutoScalingPolicyUpdate {
    #[serde(rename = "PolicyName")]
    pub policy_name: String,
    #[serde(rename = "TargetTrackingScalingPolicyConfiguration")]
    pub target_tracking_scaling_policy_configuration:
        Option<AutoScalingTargetTrackingScalingPolicyConfigurationUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct AutoScalingSettingsUpdate {
    #[serde(rename = "AutoScalingDisabled")]
    pub auto_scaling_disabled: Option<bool>,
    #[serde(rename = "AutoScalingRoleArn")]
    pub auto_scaling_role_arn: Option<String>,
    #[serde(rename = "MaximumUnits")]
    pub maximum_units: Option<i64>,
    #[serde(rename = "MinimumUnits")]
    pub minimum_units: Option<i64>,
    #[serde(rename = "ScalingPolicyUpdate")]
    pub scaling_policy_update: Option<AutoScalingPolicyUpdate>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaGlobalSecondaryIndexAutoScalingDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(
        rename = "ProvisionedReadCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_read_capacity_auto_scaling_settings: Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ProvisionedWriteCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_write_capacity_auto_scaling_settings: Option<AutoScalingSettingsDescription>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaAutoScalingDescription {
    #[serde(
        rename = "GlobalSecondaryIndexes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub global_secondary_indexes: Vec<ReplicaGlobalSecondaryIndexAutoScalingDescription>,
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(
        rename = "ReplicaProvisionedReadCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_read_capacity_auto_scaling_settings:
        Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "ReplicaProvisionedWriteCapacityAutoScalingSettings",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replica_provisioned_write_capacity_auto_scaling_settings:
        Option<AutoScalingSettingsDescription>,
    #[serde(rename = "ReplicaStatus")]
    pub replica_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableAutoScalingDescription {
    #[serde(rename = "Replicas", default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<ReplicaAutoScalingDescription>,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TableStatus")]
    pub table_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableAutoScalingState {
    #[serde(
        rename = "writeCapacity",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub write_capacity: Option<AutoScalingSettingsDescription>,
    #[serde(
        rename = "gsiWriteCapacity",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub gsi_write_capacity: BTreeMap<String, AutoScalingSettingsDescription>,
    #[serde(
        rename = "replicaReadCapacity",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub replica_read_capacity: BTreeMap<String, AutoScalingSettingsDescription>,
    #[serde(
        rename = "replicaGsiReadCapacity",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub replica_gsi_read_capacity:
        BTreeMap<String, BTreeMap<String, AutoScalingSettingsDescription>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GlobalSecondaryIndexAutoScalingUpdate {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "ProvisionedWriteCapacityAutoScalingUpdate")]
    pub provisioned_write_capacity_auto_scaling_update: Option<AutoScalingSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaGlobalSecondaryIndexAutoScalingUpdate {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "ProvisionedReadCapacityAutoScalingUpdate")]
    pub provisioned_read_capacity_auto_scaling_update: Option<AutoScalingSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReplicaAutoScalingUpdate {
    #[serde(rename = "RegionName")]
    pub region_name: String,
    #[serde(rename = "ReplicaGlobalSecondaryIndexUpdates")]
    pub replica_global_secondary_index_updates: Vec<ReplicaGlobalSecondaryIndexAutoScalingUpdate>,
    #[serde(rename = "ReplicaProvisionedReadCapacityAutoScalingUpdate")]
    pub replica_provisioned_read_capacity_auto_scaling_update: Option<AutoScalingSettingsUpdate>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateTableReplicaAutoScalingRequest {
    #[serde(rename = "GlobalSecondaryIndexUpdates")]
    pub global_secondary_index_updates: Vec<GlobalSecondaryIndexAutoScalingUpdate>,
    #[serde(rename = "ProvisionedWriteCapacityAutoScalingUpdate")]
    pub provisioned_write_capacity_auto_scaling_update: Option<AutoScalingSettingsUpdate>,
    #[serde(rename = "ReplicaUpdates")]
    pub replica_updates: Vec<ReplicaAutoScalingUpdate>,
    #[serde(rename = "TableName")]
    pub table_name: String,
}

#[derive(Debug, Serialize)]
pub struct TableAutoScalingDescriptionResponse {
    #[serde(rename = "TableAutoScalingDescription")]
    pub table_auto_scaling_description: TableAutoScalingDescription,
}

pub fn apply_autoscaling_update(
    existing: Option<&AutoScalingSettingsDescription>,
    update: &AutoScalingSettingsUpdate,
) -> Result<AutoScalingSettingsDescription, ApiError> {
    let mut next = existing.cloned().unwrap_or_default();
    if let Some(disabled) = update.auto_scaling_disabled {
        next.auto_scaling_disabled = Some(disabled);
    }
    if let Some(role) = &update.auto_scaling_role_arn {
        next.auto_scaling_role_arn = role.clone();
    }
    if let Some(min) = update.minimum_units {
        if min < 1 {
            return Err(ApiError::validation("minimum units must be at least 1"));
        }
        next.minimum_units = Some(min);
    }
    if let Some(max) = update.maximum_units {
        if max < 1 {
            return Err(ApiError::validation("maximum units must be at least 1"));
        }
        next.maximum_units = Some(max);
    }
    if let (Some(min), Some(max)) = (next.minimum_units, next.maximum_units) {
        if min > max {
            return Err(ApiError::validation(
                "minimum units cannot exceed maximum units",
            ));
        }
    }
    if let Some(policy_upd) = &update.scaling_policy_update {
        let cfg = policy_upd
            .target_tracking_scaling_policy_configuration
            .as_ref()
            .ok_or_else(|| {
                ApiError::validation("target tracking scaling policy configuration is required")
            })?;
        if !cfg.target_value.is_finite() || cfg.target_value <= 0.0 || cfg.target_value > 100.0 {
            return Err(ApiError::validation(
                "target value must be greater than 0 and at most 100",
            ));
        }
        if let Some(c) = cfg.scale_in_cooldown {
            if c < 0 {
                return Err(ApiError::validation(
                    "scale in cooldown must be non-negative",
                ));
            }
        }
        if let Some(c) = cfg.scale_out_cooldown {
            if c < 0 {
                return Err(ApiError::validation(
                    "scale out cooldown must be non-negative",
                ));
            }
        }
        let policy_name = if policy_upd.policy_name.is_empty() {
            "DynamoDBTargetTrackingScalingPolicy".to_string()
        } else {
            policy_upd.policy_name.clone()
        };
        let desc = AutoScalingPolicyDescription {
            policy_name: policy_name.clone(),
            target_tracking_scaling_policy_configuration: Some(
                AutoScalingTargetTrackingScalingPolicyConfigurationDescription {
                    disable_scale_in: cfg.disable_scale_in,
                    scale_in_cooldown: cfg.scale_in_cooldown,
                    scale_out_cooldown: cfg.scale_out_cooldown,
                    target_value: cfg.target_value,
                },
            ),
        };
        if let Some(slot) = next
            .scaling_policies
            .iter_mut()
            .find(|p| p.policy_name == policy_name)
        {
            *slot = desc;
        } else {
            next.scaling_policies.push(desc);
        }
    }
    Ok(next)
}

pub fn table_replica_regions(description: &TableDescription, home_region: &str) -> Vec<String> {
    let mut regions = Vec::new();
    let mut seen = BTreeSet::new();
    if seen.insert(home_region.to_string()) {
        regions.push(home_region.to_string());
    }
    for replica in &description.replicas {
        if seen.insert(replica.region_name.clone()) {
            regions.push(replica.region_name.clone());
        }
    }
    regions
}

pub fn build_table_autoscaling_description(
    description: &TableDescription,
    home_region: &str,
    state: Option<&TableAutoScalingState>,
) -> TableAutoScalingDescription {
    let default_disabled = AutoScalingSettingsDescription {
        auto_scaling_disabled: Some(true),
        ..Default::default()
    };
    let replicas = table_replica_regions(description, home_region)
        .into_iter()
        .map(|region| {
            let read_settings = state
                .and_then(|s| s.replica_read_capacity.get(&region))
                .cloned()
                .unwrap_or_else(|| default_disabled.clone());
            let write_settings = state
                .and_then(|s| s.write_capacity.as_ref())
                .cloned()
                .unwrap_or_else(|| default_disabled.clone());
            let gsis = description
                .global_secondary_indexes
                .iter()
                .map(|gsi| {
                    let gsi_read = state
                        .and_then(|s| s.replica_gsi_read_capacity.get(&region))
                        .and_then(|m| m.get(&gsi.index_name))
                        .cloned()
                        .unwrap_or_else(|| default_disabled.clone());
                    let gsi_write = state
                        .and_then(|s| s.gsi_write_capacity.get(&gsi.index_name))
                        .cloned()
                        .unwrap_or_else(|| default_disabled.clone());
                    ReplicaGlobalSecondaryIndexAutoScalingDescription {
                        index_name: gsi.index_name.clone(),
                        index_status: gsi.index_status.clone(),
                        provisioned_read_capacity_auto_scaling_settings: Some(gsi_read),
                        provisioned_write_capacity_auto_scaling_settings: Some(gsi_write),
                    }
                })
                .collect();
            ReplicaAutoScalingDescription {
                global_secondary_indexes: gsis,
                region_name: region,
                replica_provisioned_read_capacity_auto_scaling_settings: Some(read_settings),
                replica_provisioned_write_capacity_auto_scaling_settings: Some(write_settings),
                replica_status: "ACTIVE".to_string(),
            }
        })
        .collect();
    TableAutoScalingDescription {
        replicas,
        table_name: description.table_name.clone(),
        table_status: description.table_status.clone(),
    }
}
