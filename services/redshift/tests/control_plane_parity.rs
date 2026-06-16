//! 1:1 port of `internal/services/redshift/cluster_test.rs` (provisioned
//! cluster / snapshot / tags / parameter-group / serverless control plane).

mod common;

use common::{query_request, serverless_request};
use devcloud_redshift::{Config, Server};

fn cfg() -> Config {
    Config::default()
}

#[test]
fn describe_clusters_uses_aws_query_xml_shape() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let rec = query_request(&server, "Action=DescribeClusters&Version=2012-12-01");
    assert_eq!(rec.status, 200, "body = {}", rec.body);
    for want in [
        "DescribeClustersResponse",
        "ClusterIdentifier>devcloud",
        "DBName>dev",
        "Port>15439",
    ] {
        assert!(
            rec.body.contains(want),
            "response missing {want:?}: {}",
            rec.body
        );
    }
}

#[test]
fn create_and_delete_cluster_update_management_metadata() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });

    let create = query_request(
        &server,
        "Action=CreateCluster&ClusterIdentifier=analytics&DBName=warehouse&MasterUsername=analyst&NodeType=ra3.xlplus&NumberOfNodes=2",
    );
    assert_eq!(create.status, 200, "body = {}", create.body);
    for want in [
        "CreateClusterResponse",
        "ClusterIdentifier>analytics",
        "DBName>warehouse",
        "NodeType>ra3.xlplus",
        "NumberOfNodes>2",
    ] {
        assert!(
            create.body.contains(want),
            "CreateCluster missing {want:?}: {}",
            create.body
        );
    }

    let snapshot = server.service_snapshot();
    assert_eq!(snapshot.clusters.len(), 2, "clusters after create");

    let delete = query_request(&server, "Action=DeleteCluster&ClusterIdentifier=analytics");
    assert_eq!(delete.status, 200, "body = {}", delete.body);
    assert!(delete.body.contains("DeleteClusterResponse"));
    assert!(delete.body.contains("ClusterIdentifier>analytics"));

    let snapshot = server.service_snapshot();
    assert_eq!(snapshot.clusters.len(), 1);
    assert_eq!(snapshot.clusters[0].cluster_identifier, "devcloud");
}

#[test]
fn management_cluster_snapshots_are_created_listed_deleted_and_persisted() {
    let dir = temp_dir("snapshots");
    let storage_path = dir.to_string_lossy().to_string();
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        storage_path: storage_path.clone(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        node_type: "ra3.xlplus".to_string(),
        number_of_nodes: 2,
        user: "dev".to_string(),
        ..cfg()
    });

    let create = query_request(
        &server,
        "Action=CreateClusterSnapshot&ClusterIdentifier=devcloud&SnapshotIdentifier=snap-1",
    );
    assert_eq!(create.status, 200, "body = {}", create.body);
    for want in [
        "CreateClusterSnapshotResponse",
        "SnapshotIdentifier>snap-1",
        "ClusterIdentifier>devcloud",
        "Status>available",
        "NodeType>ra3.xlplus",
        "NumberOfNodes>2",
    ] {
        assert!(
            create.body.contains(want),
            "CreateClusterSnapshot missing {want:?}: {}",
            create.body
        );
    }

    let reloaded = Server::new(Config {
        sql_addr: "127.0.0.1:25439".to_string(),
        storage_path: storage_path.clone(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let describe = query_request(
        &reloaded,
        "Action=DescribeClusterSnapshots&ClusterIdentifier=devcloud",
    );
    assert_eq!(describe.status, 200, "body = {}", describe.body);
    for want in [
        "DescribeClusterSnapshotsResponse",
        "SnapshotIdentifier>snap-1",
        "ClusterIdentifier>devcloud",
        "Port>15439",
    ] {
        assert!(
            describe.body.contains(want),
            "DescribeClusterSnapshots missing {want:?}: {}",
            describe.body
        );
    }

    let delete = query_request(
        &reloaded,
        "Action=DeleteClusterSnapshot&SnapshotIdentifier=snap-1",
    );
    assert_eq!(delete.status, 200, "body = {}", delete.body);
    assert!(delete.body.contains("DeleteClusterSnapshotResponse"));

    let describe2 = query_request(
        &reloaded,
        "Action=DescribeClusterSnapshots&ClusterIdentifier=devcloud",
    );
    assert!(
        !describe2.body.contains("SnapshotIdentifier>snap-1"),
        "deleted snapshot still listed"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn management_cluster_snapshot_rejects_unknown_cluster_without_secrets() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        password: "local-password".to_string(),
        ..cfg()
    });
    let rec = query_request(
        &server,
        "Action=CreateClusterSnapshot&ClusterIdentifier=missing&SnapshotIdentifier=snap-1",
    );
    assert_eq!(rec.status, 404, "body = {}", rec.body);
    assert!(
        !rec.body.contains("local-password"),
        "leaked credential: {}",
        rec.body
    );
}

#[test]
fn management_restore_from_cluster_snapshot_creates_cluster_metadata_only() {
    let dir = temp_dir("restore");
    let storage_path = dir.to_string_lossy().to_string();
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        storage_path: storage_path.clone(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        node_type: "ra3.xlplus".to_string(),
        number_of_nodes: 2,
        user: "dev".to_string(),
        password: "local-password".to_string(),
        ..cfg()
    });

    let create = query_request(
        &server,
        "Action=CreateClusterSnapshot&ClusterIdentifier=devcloud&SnapshotIdentifier=snap-restore",
    );
    assert_eq!(create.status, 200, "body = {}", create.body);

    let restore = query_request(
        &server,
        "Action=RestoreFromClusterSnapshot&ClusterIdentifier=restored&SnapshotIdentifier=snap-restore",
    );
    assert_eq!(restore.status, 200, "body = {}", restore.body);
    for want in [
        "RestoreFromClusterSnapshotResponse",
        "ClusterIdentifier>restored",
        "DBName>dev",
        "NodeType>ra3.xlplus",
        "NumberOfNodes>2",
    ] {
        assert!(
            restore.body.contains(want),
            "restore missing {want:?}: {}",
            restore.body
        );
    }
    assert!(
        !restore.body.contains("local-password"),
        "leaked credential"
    );

    let reloaded = Server::new(Config {
        sql_addr: "127.0.0.1:25439".to_string(),
        storage_path,
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let describe = query_request(&reloaded, "Action=DescribeClusters");
    assert_eq!(describe.status, 200, "body = {}", describe.body);
    assert!(
        describe.body.contains("ClusterIdentifier>restored"),
        "restored cluster not persisted"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn management_tags_are_attached_listed_and_deleted() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let resource = "arn:aws:redshift:us-east-1:000000000000:cluster:devcloud";

    let create = query_request(
        &server,
        &format!(
            "Action=CreateTags&ResourceName={res}&Tags.member.1.Key=env&Tags.member.1.Value=local&Tags.member.2.Key=owner&Tags.member.2.Value=dev",
            res = url_encode(resource)
        ),
    );
    assert_eq!(create.status, 200, "body = {}", create.body);
    assert!(create.body.contains("CreateTagsResponse"));

    let describe = query_request(
        &server,
        &format!("Action=DescribeTags&ResourceName={}", url_encode(resource)),
    );
    assert_eq!(describe.status, 200, "body = {}", describe.body);
    for want in [
        "DescribeTagsResponse",
        &format!("ResourceName>{resource}"),
        "Key>env",
        "Value>local",
        "Key>owner",
        "Value>dev",
    ] {
        assert!(
            describe.body.contains(want),
            "DescribeTags missing {want:?}: {}",
            describe.body
        );
    }

    let delete = query_request(
        &server,
        &format!(
            "Action=DeleteTags&ResourceName={}&TagKeys.member.1=env",
            url_encode(resource)
        ),
    );
    assert_eq!(delete.status, 200, "body = {}", delete.body);
    assert!(delete.body.contains("DeleteTagsResponse"));

    let describe2 = query_request(
        &server,
        &format!("Action=DescribeTags&ResourceName={}", url_encode(resource)),
    );
    assert!(
        !describe2.body.contains("Key>env"),
        "env tag survived delete"
    );
    assert!(describe2.body.contains("Key>owner"), "owner tag missing");
}

#[test]
fn describe_cluster_parameter_groups_and_parameters() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        password: "local-password".to_string(),
        ..cfg()
    });

    let groups = query_request(
        &server,
        "Action=DescribeClusterParameterGroups&ParameterGroupName=default.redshift-1.0",
    );
    assert_eq!(groups.status, 200, "body = {}", groups.body);
    for want in [
        "DescribeClusterParameterGroupsResponse",
        "ParameterGroupName>default.redshift-1.0",
        "ParameterGroupFamily>redshift-1.0",
    ] {
        assert!(
            groups.body.contains(want),
            "groups missing {want:?}: {}",
            groups.body
        );
    }

    let parameters = query_request(
        &server,
        "Action=DescribeClusterParameters&ParameterGroupName=default.redshift-1.0",
    );
    assert_eq!(parameters.status, 200, "body = {}", parameters.body);
    for want in [
        "DescribeClusterParametersResponse",
        "ParameterName>datestyle",
        "ParameterName>enable_user_activity_logging",
        "ParameterName>max_query_execution_time",
    ] {
        assert!(
            parameters.body.contains(want),
            "parameters missing {want:?}: {}",
            parameters.body
        );
    }
    assert!(
        !parameters.body.contains("local-password"),
        "leaked credential"
    );
}

#[test]
fn describe_cluster_parameters_rejects_unknown_group_without_secrets() {
    let server = Server::new(Config {
        password: "local-password".to_string(),
        ..cfg()
    });
    let rec = query_request(
        &server,
        "Action=DescribeClusterParameters&ParameterGroupName=missing",
    );
    assert_eq!(rec.status, 404, "body = {}", rec.body);
    assert!(!rec.body.contains("local-password"));
}

#[test]
fn get_cluster_credentials_returns_local_credentials() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        cluster_identifier: "devcloud".to_string(),
        user: "dev".to_string(),
        password: "local-password".to_string(),
        ..cfg()
    });
    let rec = query_request(
        &server,
        "Action=GetClusterCredentials&ClusterIdentifier=devcloud&DbUser=analyst&DurationSeconds=60",
    );
    assert_eq!(rec.status, 200, "body = {}", rec.body);
    for want in [
        "GetClusterCredentialsResponse",
        "DbUser>analyst",
        "DbPassword>local-password",
        "Expiration>",
        "RequestId>devcloud-redshift",
    ] {
        assert!(
            rec.body.contains(want),
            "credentials missing {want:?}: {}",
            rec.body
        );
    }
}

#[test]
fn get_cluster_credentials_rejects_unknown_cluster() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        ..cfg()
    });
    let rec = query_request(
        &server,
        "Action=GetClusterCredentials&ClusterIdentifier=missing",
    );
    assert_eq!(rec.status, 404, "body = {}", rec.body);
    assert!(!rec.body.contains("local-password"));
}

#[test]
fn serverless_metadata_targets_use_configured_cluster() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        cluster_identifier: "analytics".to_string(),
        database: "warehouse".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });

    let workgroups = serverless_request(&server, "ListWorkgroups", "{}");
    assert_eq!(workgroups.status, 200, "body = {}", workgroups.body);
    for want in [
        r#""workgroupName":"analytics""#,
        r#""namespaceName":"warehouse""#,
        r#""port":15439"#,
        r#""status":"AVAILABLE""#,
    ] {
        assert!(
            workgroups.body.contains(want),
            "ListWorkgroups missing {want:?}: {}",
            workgroups.body
        );
    }

    let namespace = serverless_request(&server, "GetNamespace", r#"{"namespaceName":"warehouse"}"#);
    assert_eq!(namespace.status, 200, "body = {}", namespace.body);
    for want in [
        r#""namespaceName":"warehouse""#,
        r#""dbName":"warehouse""#,
        r#""status":"AVAILABLE""#,
    ] {
        assert!(
            namespace.body.contains(want),
            "GetNamespace missing {want:?}: {}",
            namespace.body
        );
    }
}

#[test]
fn serverless_metadata_rejects_unknown_workgroup_without_secrets() {
    let server = Server::new(Config {
        cluster_identifier: "analytics".to_string(),
        database: "warehouse".to_string(),
        password: "local-password".to_string(),
        ..cfg()
    });
    let rec = serverless_request(&server, "GetWorkgroup", r#"{"workgroupName":"missing"}"#);
    assert_eq!(rec.status, 404, "body = {}", rec.body);
    assert!(!rec.body.contains("local-password"));
}

#[test]
fn snapshot_uses_configured_cluster_metadata() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        region: "ap-northeast-1".to_string(),
        cluster_identifier: "local-cluster".to_string(),
        database: "warehouse".to_string(),
        node_type: "ra3.xlplus".to_string(),
        number_of_nodes: 2,
        storage_path: ".devcloud/data/redshift".to_string(),
        user: "analyst".to_string(),
        ..cfg()
    });
    let snapshot = server.service_snapshot();
    assert_eq!(snapshot.status, "running");
    assert!(snapshot.running);
    assert_eq!(snapshot.region, "ap-northeast-1");
    assert_eq!(snapshot.clusters.len(), 1);
    let cluster = &snapshot.clusters[0];
    assert_eq!(cluster.cluster_identifier, "local-cluster");
    assert_eq!(cluster.database_name, "warehouse");
    assert_eq!(cluster.node_type, "ra3.xlplus");
    assert_eq!(cluster.number_of_nodes, 2);
    assert_eq!(cluster.endpoint.address, "127.0.0.1");
    assert_eq!(cluster.endpoint.port, 15439);
    assert_eq!(cluster.master_username, "analyst");
}

#[test]
fn health_reports_running_without_secrets() {
    let server = Server::new(Config {
        sql_addr: "127.0.0.1:15439".to_string(),
        api_addr: "127.0.0.1:19099".to_string(),
        user: "dev".to_string(),
        ..cfg()
    });
    let mut headers = std::collections::BTreeMap::new();
    let rec = server.dispatch_http("GET", "/health", "", &mut headers, b"");
    assert_eq!(rec.status, 200);
    assert!(!rec.body.contains("password"));
    assert!(rec.body.contains("\"service\":\"redshift\""));
    assert!(rec.body.contains("\"status\":\"running\""));
    assert!(rec.body.contains("\"running\":true"));
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-redshift-cp-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp storage dir");
    dir
}

/// Minimal percent-encoding for the ARN colons used in tag requests.
fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}
