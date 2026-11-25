//! Lambda Layers: PublishLayerVersion, GetLayerVersion, GetLayerVersionByArn,
//! ListLayers, ListLayerVersions, DeleteLayerVersion, Content.Location download,
//! attaching Layers to functions/versions, and executing Python & Node handlers
//! with attached layers overlaid in order.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::zip::{build_stored, build_stored_with_modes};
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

struct Env {
    server: Arc<Server>,
    dir: PathBuf,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-layers-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config_for(dir: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:19010".into(),
        endpoint: "http://127.0.0.1:19010".into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        object_store_root: Some(dir.join("s3/buckets")),
        ..Config::default()
    }
}

fn env(tag: &str) -> Env {
    let dir = temp_dir(tag);
    Env {
        server: Arc::new(Server::new(config_for(&dir))),
        dir,
    }
}

fn has(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

async fn call(
    server: &Arc<Server>,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let req = Request {
        method: method.into(),
        raw_path,
        query,
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        body: body.to_vec(),
    };
    let r = process(server, &req).await;
    Resp {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

#[tokio::test]
async fn layer_crud_filtering_s3_source_and_persistence() {
    let e = env("crud");
    let zip1 = build_stored(&[("python/helper.py", b"VALUE = 1\n")]);
    let pub1 = call(
        &e.server,
        "POST",
        "/2018-10-31/layers/shared-utils/versions",
        &[],
        json!({
            "Description": "first version",
            "Content": { "ZipFile": b64(&zip1) },
            "CompatibleRuntimes": ["python3.12"],
            "CompatibleArchitectures": ["x86_64"],
            "LicenseInfo": "MIT",
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(pub1.status, 201, "{}", String::from_utf8_lossy(&pub1.body));
    let j1 = pub1.json();
    assert_eq!(j1["Version"], 1);
    assert_eq!(
        j1["LayerArn"],
        "arn:aws:lambda:us-east-1:000000000000:layer:shared-utils"
    );
    assert_eq!(
        j1["LayerVersionArn"],
        "arn:aws:lambda:us-east-1:000000000000:layer:shared-utils:1"
    );
    assert_eq!(j1["LicenseInfo"], "MIT");

    // Download via Content.Location.
    let loc = j1["Content"]["Location"].as_str().unwrap();
    let loc_path = loc.strip_prefix("http://127.0.0.1:19010").unwrap();
    let dl = call(&e.server, "GET", loc_path, &[], b"").await;
    assert_eq!(dl.status, 200);
    assert!(dl.body.starts_with(b"PK"));

    // Publish version 2 from shared local S3 store.
    let store = devcloud_s3::store::FileBucketStore::new(e.dir.join("s3/buckets"));
    store.create_bucket("layer-bucket").unwrap();
    let zip2 = build_stored(&[("python/helper.py", b"VALUE = 2\n")]);
    store
        .put_object(devcloud_s3::objops::PutObjectInput {
            bucket: "layer-bucket".into(),
            key: "utils-v2.zip".into(),
            body: zip2,
            ..Default::default()
        })
        .unwrap();
    let pub2 = call(
        &e.server,
        "POST",
        "/2018-10-31/layers/shared-utils/versions",
        &[],
        json!({
            "Description": "second version",
            "Content": { "S3Bucket": "layer-bucket", "S3Key": "utils-v2.zip" },
            "CompatibleRuntimes": ["python3.12", "nodejs20.x"],
            "CompatibleArchitectures": ["arm64"],
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(pub2.status, 201);
    assert_eq!(pub2.json()["Version"], 2);

    // GetLayerVersion and GetLayerVersionByArn.
    let got = call(
        &e.server,
        "GET",
        "/2018-10-31/layers/shared-utils/versions/1",
        &[],
        b"",
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["Description"], "first version");

    let by_arn = call(
        &e.server,
        "GET",
        "/2018-10-31/layers?find=LayerVersion&Arn=arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Alayer%3Ashared-utils%3A2",
        &[],
        b"",
    )
    .await;
    assert_eq!(by_arn.status, 200);
    assert_eq!(by_arn.json()["Version"], 2);

    // ListLayers & ListLayerVersions with runtime/architecture filtering.
    let list_all = call(&e.server, "GET", "/2018-10-31/layers", &[], b"").await;
    assert_eq!(list_all.json()["Layers"].as_array().unwrap().len(), 1);
    assert_eq!(
        list_all.json()["Layers"][0]["LatestMatchingVersion"]["Version"],
        2
    );

    let list_x86 = call(
        &e.server,
        "GET",
        "/2018-10-31/layers?CompatibleArchitecture=x86_64",
        &[],
        b"",
    )
    .await;
    assert_eq!(
        list_x86.json()["Layers"][0]["LatestMatchingVersion"]["Version"],
        1
    );

    let list_ruby = call(
        &e.server,
        "GET",
        "/2018-10-31/layers?CompatibleRuntime=ruby3.2",
        &[],
        b"",
    )
    .await;
    assert_eq!(list_ruby.json()["Layers"].as_array().unwrap().len(), 0);

    let vers = call(
        &e.server,
        "GET",
        "/2018-10-31/layers/shared-utils/versions",
        &[],
        b"",
    )
    .await;
    let ver_nums: Vec<u64> = vers.json()["LayerVersions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Version"].as_u64().unwrap())
        .collect();
    assert_eq!(ver_nums, vec![2, 1], "newest version first");

    // Persistence across restart.
    let reloaded = Arc::new(Server::new(config_for(&e.dir)));
    assert!(reloaded.load_err().is_none());
    let got_after = call(
        &reloaded,
        "GET",
        "/2018-10-31/layers/shared-utils/versions/2",
        &[],
        b"",
    )
    .await;
    assert_eq!(got_after.status, 200);

    // DeleteLayerVersion: version 2 is gone and next publish gets version 3.
    let del = call(
        &reloaded,
        "DELETE",
        "/2018-10-31/layers/shared-utils/versions/2",
        &[],
        b"",
    )
    .await;
    assert_eq!(del.status, 204);
    assert_eq!(
        call(
            &reloaded,
            "GET",
            "/2018-10-31/layers/shared-utils/versions/2",
            &[],
            b"",
        )
        .await
        .status,
        404
    );
    let pub3 = call(
        &reloaded,
        "POST",
        "/2018-10-31/layers/shared-utils/versions",
        &[],
        json!({ "Content": { "ZipFile": b64(&zip1) } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(pub3.status, 201);
    assert_eq!(pub3.json()["Version"], 3);
}

#[tokio::test]
async fn python_and_node_execute_with_attached_layers_and_overlay_order() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("exec");

    // Layer A: python helper + shared module + node module + executable bin tool.
    let layer_a = build_stored_with_modes(&[
        (
            "python/layer_a.py",
            b"def greet():\n    return 'hello-from-a'\n",
            0o100644,
        ),
        ("python/shared.py", b"WHO = 'layer-a'\n", 0o100644),
        (
            "nodejs/node_modules/mylib/index.js",
            b"module.exports = { answer: 42 };\n",
            0o100644,
        ),
        (
            "bin/layer-echo",
            b"#!/bin/sh\necho from-layer-bin\n",
            0o100755,
        ),
    ]);
    // Layer B overrides python/shared.py.
    let layer_b = build_stored(&[("python/shared.py", b"WHO = 'layer-b'\n")]);

    let arn_a = call(
        &e.server,
        "POST",
        "/2018-10-31/layers/base-layer/versions",
        &[],
        json!({ "Content": { "ZipFile": b64(&layer_a) } })
            .to_string()
            .as_bytes(),
    )
    .await
    .json()["LayerVersionArn"]
        .as_str()
        .unwrap()
        .to_string();

    let arn_b = call(
        &e.server,
        "POST",
        "/2018-10-31/layers/override-layer/versions",
        &[],
        json!({ "Content": { "ZipFile": b64(&layer_b) } })
            .to_string()
            .as_bytes(),
    )
    .await
    .json()["LayerVersionArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Validation: more than 5 layers or duplicate layers rejected.
    let fn_zip = build_stored(&[(
        "app.py",
        b"import subprocess, layer_a, shared\ndef handler(e, c):\n    out = subprocess.check_output(['layer-echo']).decode().strip()\n    return {'greet': layer_a.greet(), 'who': shared.WHO, 'bin': out}\n",
    )]);
    let dup = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        json!({
            "FunctionName": "bad-layers",
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Layers": [arn_a, arn_a],
            "Code": { "ZipFile": b64(&fn_zip) },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(dup.status, 400);

    let img_with_layers = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        json!({
            "FunctionName": "bad-img-layers",
            "PackageType": "Image",
            "Role": "r",
            "Layers": [arn_a],
            "Code": { "ImageUri": "example:latest" },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(img_with_layers.status, 400);

    // Create Python function with [A, B] and Publish=true (version 1).
    let created = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        json!({
            "FunctionName": "layered-py",
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Layers": [arn_a, arn_b],
            "Publish": true,
            "Code": { "ZipFile": b64(&fn_zip) },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    assert_eq!(created.json()["Layers"].as_array().unwrap().len(), 2);

    let res = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/layered-py/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(res.status, 200, "{}", String::from_utf8_lossy(&res.body));
    assert_eq!(res.header("X-Amz-Function-Error"), None);
    assert_eq!(res.json()["greet"], "hello-from-a");
    assert_eq!(
        res.json()["who"],
        "layer-b",
        "later layer overrides earlier"
    );
    assert_eq!(res.json()["bin"], "from-layer-bin");

    // Remove layer B from $LATEST: $LATEST now sees layer-a, while version 1 keeps layer-b.
    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/layered-py/configuration",
        &[],
        json!({ "Layers": [arn_a] }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 200);
    assert_eq!(updated.json()["Layers"].as_array().unwrap().len(), 1);

    let latest_res = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/layered-py/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(latest_res.json()["who"], "layer-a");

    let v1_res = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/layered-py/invocations?Qualifier=1",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(v1_res.json()["who"], "layer-b");

    // Node.js (CJS + ESM) also imports from attached layer.
    if has("node") {
        let node_zip = build_stored(&[
            (
                "index.js",
                b"const mylib = require('mylib');\nexports.handler = async () => mylib;\n",
            ),
            (
                "esm.mjs",
                b"import mylib from 'mylib';\nexport const handler = async () => mylib;\n",
            ),
        ]);
        for (name, handler) in [
            ("node-cjs-layer", "index.handler"),
            ("node-esm-layer", "esm.handler"),
        ] {
            let r = call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                json!({
                    "FunctionName": name,
                    "Runtime": "nodejs20.x",
                    "Role": "r",
                    "Handler": handler,
                    "Layers": [arn_a],
                    "Code": { "ZipFile": b64(&node_zip) },
                })
                .to_string()
                .as_bytes(),
            )
            .await;
            assert_eq!(r.status, 201);
            let inv = call(
                &e.server,
                "POST",
                &format!("/2015-03-31/functions/{name}/invocations"),
                &[],
                b"{}",
            )
            .await;
            assert_eq!(inv.status, 200, "{}", String::from_utf8_lossy(&inv.body));
            assert_eq!(inv.header("X-Amz-Function-Error"), None);
            assert_eq!(inv.json()["answer"], 42);
        }
    }

    // Deleting a layer version from the catalog keeps existing attached functions working,
    // while preventing new functions from attaching it.
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2018-10-31/layers/base-layer/versions/1",
            &[],
            b"",
        )
        .await
        .status,
        204
    );
    let still_works = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/layered-py/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(still_works.status, 200);
    assert_eq!(still_works.json()["greet"], "hello-from-a");

    let new_fn_missing = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        json!({
            "FunctionName": "too-late",
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Layers": [arn_a],
            "Code": { "ZipFile": b64(&fn_zip) },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(new_fn_missing.status, 400);

    e.server.shutdown().await;
}
