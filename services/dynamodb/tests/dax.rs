//! Tests for DAX control-plane metadata emulation (`AmazonDAXV3.*`) on the
//! DynamoDB service endpoint.

use std::collections::BTreeMap;

use devcloud_dynamodb::server::{Config, Server};
use devcloud_dynamodb::sigv4::{signature_for_request_with_service, Credentials, SignedRequest};
use serde_json::{json, Value};

fn parse(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("valid JSON")
}

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_200_000);
    s
}

#[test]
fn dax_cluster_subnet_param_group_and_tags_lifecycle() {
    let dir = tempdir();
    let mut s = server(&dir);

    // Create subnet group.
    let sg = s.dispatch(
        "AmazonDAXV3.CreateSubnetGroup",
        br#"{"SubnetGroupName":"app-subnets","Description":"local subnets","SubnetIds":["subnet-1","subnet-2"]}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(sg.status, 200);
    let sg_json = parse(&sg.body);
    assert_eq!(sg_json["SubnetGroup"]["SubnetGroupName"], "app-subnets");
    assert_eq!(
        sg_json["SubnetGroup"]["Subnets"].as_array().unwrap().len(),
        2
    );

    // Create parameter group & update a parameter.
    let pg = s.dispatch(
        "AmazonDAXV3.CreateParameterGroup",
        br#"{"ParameterGroupName":"app-params","Description":"custom ttl"}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(pg.status, 200);

    let pg_upd = s.dispatch(
        "AmazonDAXV3.UpdateParameterGroup",
        br#"{"ParameterGroupName":"app-params","ParameterNameValues":[{"ParameterName":"query-ttl-millis","ParameterValue":"60000"}]}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(pg_upd.status, 200);

    let params = parse(
        &s.dispatch(
            "AmazonDAXV3.DescribeParameters",
            br#"{"ParameterGroupName":"app-params"}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    let query_ttl = params["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["ParameterName"] == "query-ttl-millis")
        .unwrap();
    assert_eq!(query_ttl["ParameterValue"], "60000");

    // Create DAX cluster referencing the subnet & parameter groups.
    let created = s.dispatch(
        "AmazonDAXV3.CreateCluster",
        br#"{
            "ClusterName":"my-dax",
            "NodeType":"dax.r5.large",
            "ReplicationFactor":2,
            "IamRoleArn":"arn:aws:iam::000000000000:role/dax",
            "SubnetGroupName":"app-subnets",
            "ParameterGroupName":"app-params",
            "ClusterEndpointEncryptionType":"TLS",
            "SSESpecification":{"Enabled":true},
            "Tags":[{"Key":"env","Value":"dev"}]
        }"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(created.status, 200);
    let c = parse(&created.body);
    assert_eq!(c["Cluster"]["ClusterName"], "my-dax");
    assert_eq!(c["Cluster"]["Status"], "available");
    assert_eq!(c["Cluster"]["TotalNodes"], 2);
    assert_eq!(c["Cluster"]["ActiveNodes"], 2);
    assert_eq!(c["Cluster"]["ClusterDiscoveryEndpoint"]["Port"], 9111);
    assert!(c["Cluster"]["ClusterDiscoveryEndpoint"]["URL"]
        .as_str()
        .unwrap()
        .starts_with("daxs://"));
    assert_eq!(c["Cluster"]["SSEDescription"]["Status"], "ENABLED");
    let arn = c["Cluster"]["ClusterArn"].as_str().unwrap().to_string();
    assert_eq!(arn, "arn:aws:dax:us-east-1:000000000000:cache/my-dax");

    // In-use subnet group & parameter group cannot be deleted.
    let del_sg = s.dispatch(
        "AmazonDAXV3.DeleteSubnetGroup",
        br#"{"SubnetGroupName":"app-subnets"}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(del_sg.status, 400);
    assert_eq!(del_sg.error_type.as_deref(), Some("SubnetGroupInUseFault"));
    assert!(parse(&del_sg.body)["__type"]
        .as_str()
        .unwrap()
        .starts_with("com.amazonaws.dax.v3#"));

    let del_pg = s.dispatch(
        "AmazonDAXV3.DeleteParameterGroup",
        br#"{"ParameterGroupName":"app-params"}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(del_pg.status, 400);
    assert_eq!(
        del_pg.error_type.as_deref(),
        Some("InvalidParameterGroupStateFault")
    );

    // Increase & decrease replication factor, reboot node, update cluster.
    let inc = parse(
        &s.dispatch(
            "AmazonDAXV3.IncreaseReplicationFactor",
            br#"{"ClusterName":"my-dax","NewReplicationFactor":3}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    assert_eq!(inc["Cluster"]["TotalNodes"], 3);
    assert_eq!(inc["Cluster"]["Nodes"].as_array().unwrap().len(), 3);

    let dec = parse(
        &s.dispatch(
            "AmazonDAXV3.DecreaseReplicationFactor",
            br#"{"ClusterName":"my-dax","NewReplicationFactor":1,"NodeIdsToRemove":["my-dax-b","my-dax-c"]}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    assert_eq!(dec["Cluster"]["TotalNodes"], 1);
    assert_eq!(dec["Cluster"]["Nodes"][0]["NodeId"], "my-dax-a");

    let reboot = s.dispatch(
        "AmazonDAXV3.RebootNode",
        br#"{"ClusterName":"my-dax","NodeId":"my-dax-a"}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(reboot.status, 200);

    // Tagging lifecycle on DAX cluster ARN.
    let tag_req = json!({
        "ResourceName": arn,
        "Tags": [{"Key": "team", "Value": "platform"}]
    });
    let tagged = parse(
        &s.dispatch(
            "AmazonDAXV3.TagResource",
            tag_req.to_string().as_bytes(),
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    assert_eq!(tagged["Tags"].as_array().unwrap().len(), 2);

    let untag_req = json!({
        "ResourceName": arn,
        "TagKeys": ["env"]
    });
    let untagged = parse(
        &s.dispatch(
            "AmazonDAXV3.UntagResource",
            untag_req.to_string().as_bytes(),
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    assert_eq!(
        untagged["Tags"],
        json!([{"Key": "team", "Value": "platform"}])
    );

    // Reload from disk preserves DAX resources.
    let mut s2 = server(&dir);
    let described = parse(
        &s2.dispatch(
            "AmazonDAXV3.DescribeClusters",
            br#"{"ClusterNames":["my-dax"]}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .body,
    );
    assert_eq!(described["Clusters"][0]["TotalNodes"], 1);

    // Delete cluster, then subnet group & parameter group can be deleted.
    let deleted = s2.dispatch(
        "AmazonDAXV3.DeleteCluster",
        br#"{"ClusterName":"my-dax"}"#,
        "relaxed",
        None,
        1_780_200_000,
    );
    assert_eq!(deleted.status, 200);
    assert_eq!(parse(&deleted.body)["Cluster"]["Status"], "deleting");

    assert_eq!(
        s2.dispatch(
            "AmazonDAXV3.DeleteSubnetGroup",
            br#"{"SubnetGroupName":"app-subnets"}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .status,
        200
    );
    assert_eq!(
        s2.dispatch(
            "AmazonDAXV3.DeleteParameterGroup",
            br#"{"ParameterGroupName":"app-params"}"#,
            "relaxed",
            None,
            1_780_200_000,
        )
        .status,
        200
    );
}

#[test]
fn dax_strict_sigv4_uses_dax_service_name() {
    let dir = tempdir();
    let mut s = server(&dir);
    let body = br#"{}"#;
    let payload_hash = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(body);
        hex::encode(h.finalize())
    };
    let date_stamp = "20260501";
    let amz_date = "20260501T000000Z";
    let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target";
    let mut headers = BTreeMap::new();
    headers.insert(
        "content-type".to_string(),
        "application/x-amz-json-1.1".to_string(),
    );
    headers.insert("x-amz-content-sha256".to_string(), payload_hash.clone());
    headers.insert("x-amz-date".to_string(), amz_date.to_string());
    headers.insert(
        "x-amz-target".to_string(),
        "AmazonDAXV3.DescribeClusters".to_string(),
    );

    let creds = Credentials {
        auth_mode: "strict",
        region: "us-east-1",
        access_key_id: "dev",
        secret_access_key: "dev",
    };
    let unsigned = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:18000",
        headers: &headers,
        body,
    };
    let sig = signature_for_request_with_service(
        &creds,
        &unsigned,
        date_stamp,
        "us-east-1",
        "dax",
        signed_headers,
        &payload_hash,
    );
    headers.insert(
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential=dev/{date_stamp}/us-east-1/dax/aws4_request, SignedHeaders={signed_headers}, Signature={sig}"
        ),
    );
    let signed = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:18000",
        headers: &headers,
        body,
    };

    let out = s.dispatch(
        "AmazonDAXV3.DescribeClusters",
        body,
        "strict",
        Some(&signed),
        1_780_200_000,
    );
    assert_eq!(out.status, 200);
    assert_eq!(parse(&out.body)["Clusters"], json!([]));
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-dax-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
