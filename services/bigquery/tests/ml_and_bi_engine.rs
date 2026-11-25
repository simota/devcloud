//! Tests for BigQuery ML (models REST API + CREATE MODEL / DROP MODEL /
//! ML.PREDICT / ML.EVALUATE / ML.WEIGHTS) and BI Engine (Reservation API v1 +
//! query job `biEngineStatistics`).

use devcloud_bigquery::model::{
    BiReservation, JobResource, ModelResource, ModelsListResponse, QueryResponse,
};
use devcloud_bigquery::routes::{handle, Request};
use devcloud_bigquery::server::{Config, Server};
use serde_json::json;

fn new_server(dir: &std::path::Path) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    })
}

fn create_dataset(server: &Server, dataset_id: &str) {
    let body = format!(r#"{{"datasetReference":{{"datasetId":"{dataset_id}"}}}}"#);
    let res = server.create_dataset("local-project", body.as_bytes());
    assert_eq!(res.status, 200, "create dataset: {}", res.body_str());
}

fn run_query(server: &Server, sql: &str) -> QueryResponse {
    let body = serde_json::json!({
        "query": sql,
        "useLegacySql": false
    })
    .to_string();
    let res = handle(
        server,
        &Request::new(
            "POST",
            "/bigquery/v2/projects/local-project/queries",
            body.as_bytes(),
        ),
    );
    assert_eq!(
        res.status,
        200,
        "query failed ({}): {}",
        res.status,
        res.body_str()
    );
    serde_json::from_slice(&res.body).expect("decode QueryResponse")
}

// --- BigQuery ML ------------------------------------------------------------

#[test]
fn ml_linear_regression_train_predict_evaluate_weights_and_model_rest_crud() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset(&server, "ml_ds");

    // Exact linear relationship: y = 2*x1 + 3*x2 + 5
    let tbl = server.create_table(
        "local-project",
        "ml_ds",
        br#"{
            "tableReference":{"tableId":"train_linear"},
            "schema":{"fields":[
                {"name":"x1","type":"FLOAT"},
                {"name":"x2","type":"FLOAT"},
                {"name":"y","type":"FLOAT"}
            ]}
        }"#,
    );
    assert_eq!(tbl.status, 200, "create table: {}", tbl.body_str());

    let ins = server.insert_rows(
        "local-project",
        "ml_ds",
        "train_linear",
        br#"{"rows":[
            {"insertId":"1","json":{"x1":1.0,"x2":0.0,"y":7.0}},
            {"insertId":"2","json":{"x1":0.0,"x2":1.0,"y":8.0}},
            {"insertId":"3","json":{"x1":2.0,"x2":1.0,"y":12.0}},
            {"insertId":"4","json":{"x1":3.0,"x2":2.0,"y":17.0}},
            {"insertId":"5","json":{"x1":4.0,"x2":3.0,"y":22.0}}
        ]}"#,
    );
    assert_eq!(ins.status, 200, "insert rows: {}", ins.body_str());

    // Dry run validates CREATE MODEL without persisting
    let dry_body = r#"{
        "query":"CREATE MODEL `local-project.ml_ds.lin_model` OPTIONS(model_type='LINEAR_REG', input_label_cols=['y']) AS SELECT x1, x2, y FROM `local-project.ml_ds.train_linear`",
        "useLegacySql":false,
        "dryRun":true
    }"#;
    let dry = server.query_rows("local-project", dry_body.as_bytes());
    assert_eq!(dry.status, 200, "dry run create model: {}", dry.body_str());
    let not_persisted = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models/lin_model",
            b"",
        ),
    );
    assert_eq!(not_persisted.status, 404);

    // Train model
    run_query(
        &server,
        "CREATE MODEL `local-project.ml_ds.lin_model` OPTIONS(model_type='LINEAR_REG', input_label_cols=['y']) AS SELECT x1, x2, y FROM `local-project.ml_ds.train_linear`",
    );

    // Duplicate CREATE MODEL without OR REPLACE / IF NOT EXISTS must fail
    let dup = server.query_rows(
        "local-project",
        br#"{"query":"CREATE MODEL `local-project.ml_ds.lin_model` OPTIONS(model_type='LINEAR_REG', input_label_cols=['y']) AS SELECT x1, x2, y FROM `local-project.ml_ds.train_linear`","useLegacySql":false}"#,
    );
    assert_eq!(dup.status, 400);
    assert!(dup.body_str().contains("already exists"));

    // IF NOT EXISTS succeeds
    run_query(
        &server,
        "CREATE MODEL IF NOT EXISTS `local-project.ml_ds.lin_model` OPTIONS(model_type='LINEAR_REG', input_label_cols=['y']) AS SELECT x1, x2, y FROM `local-project.ml_ds.train_linear`",
    );

    // REST API: models.get / models.list / models.patch
    let get_res = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models/lin_model",
            b"",
        ),
    );
    assert_eq!(get_res.status, 200, "models.get: {}", get_res.body_str());
    let model: ModelResource = serde_json::from_slice(&get_res.body).expect("decode model");
    assert_eq!(model.model_type, "LINEAR_REG");
    assert_eq!(model.feature_columns.len(), 2);
    assert_eq!(model.label_columns.len(), 1);
    assert_eq!(model.label_columns[0].name, "y");
    assert_eq!(model.training_runs.len(), 1);

    let patch_res = handle(
        &server,
        &Request::new(
            "PATCH",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models/lin_model",
            br#"{"friendlyName":"Linear Model","description":"OLS test","labels":{"env":"dev"}}"#,
        ),
    );
    assert_eq!(
        patch_res.status,
        200,
        "models.patch: {}",
        patch_res.body_str()
    );
    let patched: ModelResource = serde_json::from_slice(&patch_res.body).expect("decode patched");
    assert_eq!(patched.friendly_name, "Linear Model");
    assert_eq!(patched.description, "OLS test");
    assert_eq!(
        patched
            .labels
            .as_ref()
            .and_then(|l| l.get("env"))
            .map(String::as_str),
        Some("dev")
    );

    let list_res = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models",
            b"",
        ),
    );
    assert_eq!(list_res.status, 200);
    let listed: ModelsListResponse = serde_json::from_slice(&list_res.body).expect("decode list");
    assert_eq!(listed.models.len(), 1);
    assert_eq!(listed.models[0].model_reference.model_id, "lin_model");

    // ML.WEIGHTS recovers intercept=5, x1=2, x2=3
    let weights = run_query(
        &server,
        "SELECT processed_input, weight FROM ML.WEIGHTS(MODEL `local-project.ml_ds.lin_model`)",
    );
    assert_eq!(weights.total_rows, "3");
    assert_eq!(weights.rows[0].f[0].v, json!("__INTERCEPT__"));
    assert!((weights.rows[0].f[1].v.as_f64().unwrap() - 5.0).abs() < 1e-6);
    assert_eq!(weights.rows[1].f[0].v, json!("x1"));
    assert!((weights.rows[1].f[1].v.as_f64().unwrap() - 2.0).abs() < 1e-6);
    assert_eq!(weights.rows[2].f[0].v, json!("x2"));
    assert!((weights.rows[2].f[1].v.as_f64().unwrap() - 3.0).abs() < 1e-6);

    // ML.PREDICT via TABLE and subquery
    let pred = run_query(
        &server,
        "SELECT predicted_y, x1, x2 FROM ML.PREDICT(MODEL `local-project.ml_ds.lin_model`, TABLE `local-project.ml_ds.train_linear`) ORDER BY x1 LIMIT 2",
    );
    assert_eq!(pred.total_rows, "2");
    // Row x1=0, x2=1 -> y=8.0; row x1=1, x2=0 -> y=7.0
    assert!((pred.rows[0].f[0].v.as_f64().unwrap() - 8.0).abs() < 1e-6);
    assert!((pred.rows[1].f[0].v.as_f64().unwrap() - 7.0).abs() < 1e-6);

    // ML.EVALUATE returns near-zero MAE/MSE and R^2 = 1.0
    let eval = run_query(
        &server,
        "SELECT mean_absolute_error, mean_squared_error, r2_score FROM ML.EVALUATE(MODEL `local-project.ml_ds.lin_model`, (SELECT x1, x2, y FROM `local-project.ml_ds.train_linear`))",
    );
    assert_eq!(eval.total_rows, "1");
    assert!(eval.rows[0].f[0].v.as_f64().unwrap() < 1e-6);
    assert!(eval.rows[0].f[1].v.as_f64().unwrap() < 1e-6);
    assert!((eval.rows[0].f[2].v.as_f64().unwrap() - 1.0).abs() < 1e-6);

    // Dataset delete without deleteContents=true rejects while model exists
    let blocked_ds_del = handle(
        &server,
        &Request::new(
            "DELETE",
            "/bigquery/v2/projects/local-project/datasets/ml_ds",
            b"",
        ),
    );
    assert_eq!(blocked_ds_del.status, 409);

    // Extra non-feature columns (like STRING id) are preserved through ML.PREDICT
    let pred_tbl = server.create_table(
        "local-project",
        "ml_ds",
        br#"{
            "tableReference":{"tableId":"score_inputs"},
            "schema":{"fields":[
                {"name":"row_key","type":"STRING"},
                {"name":"x1","type":"FLOAT"},
                {"name":"x2","type":"FLOAT"}
            ]}
        }"#,
    );
    assert_eq!(pred_tbl.status, 200);
    let pred_ins = server.insert_rows(
        "local-project",
        "ml_ds",
        "score_inputs",
        br#"{"rows":[{"insertId":"p1","json":{"row_key":"item-A","x1":5.0,"x2":2.0}}]}"#,
    );
    assert_eq!(pred_ins.status, 200);
    let pred_with_id = run_query(
        &server,
        "SELECT row_key, predicted_y FROM ML.PREDICT(MODEL `local-project.ml_ds.lin_model`, TABLE `local-project.ml_ds.score_inputs`)",
    );
    assert_eq!(pred_with_id.total_rows, "1");
    assert_eq!(pred_with_id.rows[0].f[0].v, json!("item-A"));
    // 2*5 + 3*2 + 5 = 21
    assert!((pred_with_id.rows[0].f[1].v.as_f64().unwrap() - 21.0).abs() < 1e-6);

    // DROP MODEL removes the model
    run_query(&server, "DROP MODEL `local-project.ml_ds.lin_model`");
    let after_drop = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models/lin_model",
            b"",
        ),
    );
    assert_eq!(after_drop.status, 404);
    run_query(
        &server,
        "DROP MODEL IF EXISTS `local-project.ml_ds.lin_model`",
    );
}

#[test]
fn ml_logistic_regression_and_fail_closed_validation() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset(&server, "ml_ds");

    let tbl = server.create_table(
        "local-project",
        "ml_ds",
        br#"{
            "tableReference":{"tableId":"train_cls"},
            "schema":{"fields":[
                {"name":"score","type":"FLOAT"},
                {"name":"label","type":"INTEGER"}
            ]}
        }"#,
    );
    assert_eq!(tbl.status, 200);
    let ins = server.insert_rows(
        "local-project",
        "ml_ds",
        "train_cls",
        br#"{"rows":[
            {"insertId":"1","json":{"score":1.0,"label":0}},
            {"insertId":"2","json":{"score":2.0,"label":0}},
            {"insertId":"3","json":{"score":3.0,"label":0}},
            {"insertId":"4","json":{"score":8.0,"label":1}},
            {"insertId":"5","json":{"score":9.0,"label":1}},
            {"insertId":"6","json":{"score":10.0,"label":1}}
        ]}"#,
    );
    assert_eq!(ins.status, 200);

    run_query(
        &server,
        "CREATE OR REPLACE MODEL `local-project.ml_ds.cls_model` OPTIONS(model_type='LOGISTIC_REG', max_iterations=300) AS SELECT score, label FROM `local-project.ml_ds.train_cls`",
    );

    let pred = run_query(
        &server,
        "SELECT predicted_label, score FROM ML.PREDICT(MODEL `local-project.ml_ds.cls_model`, (SELECT score FROM `local-project.ml_ds.train_cls`)) ORDER BY score",
    );
    assert_eq!(pred.total_rows, "6");
    assert_eq!(pred.rows[0].f[0].v, json!("0"));
    assert_eq!(pred.rows[1].f[0].v, json!("0"));
    assert_eq!(pred.rows[2].f[0].v, json!("0"));
    assert_eq!(pred.rows[3].f[0].v, json!("1"));
    assert_eq!(pred.rows[4].f[0].v, json!("1"));
    assert_eq!(pred.rows[5].f[0].v, json!("1"));

    let eval = run_query(
        &server,
        "SELECT precision, recall, accuracy, f1_score, log_loss FROM ML.EVALUATE(MODEL `local-project.ml_ds.cls_model`, TABLE `local-project.ml_ds.train_cls`)",
    );
    assert_eq!(eval.total_rows, "1");
    assert!((eval.rows[0].f[0].v.as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!((eval.rows[0].f[1].v.as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!((eval.rows[0].f[2].v.as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!((eval.rows[0].f[3].v.as_f64().unwrap() - 1.0).abs() < 1e-6);
    assert!(eval.rows[0].f[4].v.as_f64().unwrap() < 0.2);

    // Unsupported model_type fails closed
    let bad_type = server.query_rows(
        "local-project",
        br#"{"query":"CREATE MODEL `local-project.ml_ds.bad` OPTIONS(model_type='BOOSTED_TREE_CLASSIFIER') AS SELECT score, label FROM `local-project.ml_ds.train_cls`","useLegacySql":false}"#,
    );
    assert_eq!(bad_type.status, 400);
    assert!(bad_type
        .body_str()
        .contains("unsupported BigQuery ML model_type"));

    // REST delete removes model
    let del = handle(
        &server,
        &Request::new(
            "DELETE",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models/cls_model",
            b"",
        ),
    );
    assert_eq!(del.status, 204);
}

// --- BI Engine --------------------------------------------------------------

#[test]
fn bi_engine_reservation_api_and_query_statistics() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset(&server, "analytics");
    for tbl_name in ["fast_table", "other_table"] {
        let body = format!(
            r#"{{"tableReference":{{"tableId":"{tbl_name}"}},"schema":{{"fields":[{{"name":"id","type":"STRING"}}]}}}}"#
        );
        let res = server.create_table("local-project", "analytics", body.as_bytes());
        assert_eq!(res.status, 200);
        let ins = server.insert_rows(
            "local-project",
            "analytics",
            tbl_name,
            br#"{"rows":[{"insertId":"1","json":{"id":"a"}}]}"#,
        );
        assert_eq!(ins.status, 200);
    }

    // Default BI reservation has size "0" and queries omit biEngineStatistics
    let get_default = handle(
        &server,
        &Request::new(
            "GET",
            "/v1/projects/local-project/locations/US/biReservation",
            b"",
        ),
    );
    assert_eq!(
        get_default.status,
        200,
        "get default: {}",
        get_default.body_str()
    );
    let default_res: BiReservation =
        serde_json::from_slice(&get_default.body).expect("decode default reservation");
    assert_eq!(
        default_res.name,
        "projects/local-project/locations/US/biReservation"
    );
    assert_eq!(default_res.size, "0");

    let job_before = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"before_bi","location":"US"},"configuration":{"query":{"query":"SELECT id FROM `local-project.analytics.fast_table`","useLegacySql":false}}}"#,
    );
    assert_eq!(job_before.status, 200);
    assert!(
        !job_before.body_str().contains("biEngineStatistics"),
        "inactive BI Engine reservation must not emit biEngineStatistics: {}",
        job_before.body_str()
    );

    // PATCH reservation with size > 0 and preferredTables
    let patch_body = r#"{
        "size":"1073741824",
        "preferredTables":[{"datasetId":"analytics","tableId":"fast_table"}]
    }"#;
    let patched = handle(
        &server,
        &Request::new(
            "PATCH",
            "/v1/projects/local-project/locations/US/biReservation",
            patch_body.as_bytes(),
        ),
    );
    assert_eq!(
        patched.status,
        200,
        "patch reservation: {}",
        patched.body_str()
    );
    let res_obj: BiReservation = serde_json::from_slice(&patched.body).expect("decode patched");
    assert_eq!(res_obj.size, "1073741824");
    assert_eq!(res_obj.preferred_tables.len(), 1);
    assert_eq!(res_obj.preferred_tables[0].project_id, "local-project");
    assert!(!res_obj.update_time.is_empty());

    // Query against preferred table -> biEngineMode FULL, BI_ENGINE_PREFERRED
    let job_preferred = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"bi_preferred","location":"US"},"configuration":{"query":{"query":"SELECT id FROM `local-project.analytics.fast_table`","useLegacySql":false}}}"#,
    );
    assert_eq!(job_preferred.status, 200);
    let parsed_pref: JobResource =
        serde_json::from_slice(&job_preferred.body).expect("decode preferred job");
    let stats = parsed_pref
        .statistics
        .query
        .bi_engine_statistics
        .expect("biEngineStatistics must be set");
    assert_eq!(stats.bi_engine_mode, "FULL");
    assert_eq!(stats.acceleration_mode, "BI_ENGINE_PREFERRED");

    // Query against non-preferred table -> biEngineMode DISABLED with reason
    let job_other = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"bi_other","location":"US"},"configuration":{"query":{"query":"SELECT id FROM `local-project.analytics.other_table`","useLegacySql":false}}}"#,
    );
    assert_eq!(job_other.status, 200);
    let parsed_other: JobResource =
        serde_json::from_slice(&job_other.body).expect("decode other job");
    let other_stats = parsed_other
        .statistics
        .query
        .bi_engine_statistics
        .expect("biEngineStatistics must be set");
    assert_eq!(other_stats.bi_engine_mode, "DISABLED");
    assert_eq!(other_stats.bi_engine_reasons.len(), 1);
    assert_eq!(other_stats.bi_engine_reasons[0].code, "OTHER");

    // PATCH with updateMask=size updates only size and preserves preferredTables
    let mask_patch = handle(
        &server,
        &Request::new(
            "PATCH",
            "/v1/projects/local-project/locations/US/biReservation?updateMask=size",
            br#"{"size":2147483648}"#,
        ),
    );
    assert_eq!(
        mask_patch.status,
        200,
        "mask patch: {}",
        mask_patch.body_str()
    );
    let mask_obj: BiReservation = serde_json::from_slice(&mask_patch.body).expect("decode");
    assert_eq!(mask_obj.size, "2147483648");
    assert_eq!(mask_obj.preferred_tables.len(), 1);

    // Clearing preferredTables accelerates all tables with BI_ENGINE mode
    let clear_pref = handle(
        &server,
        &Request::new(
            "PATCH",
            "/v1/projects/local-project/locations/US/biReservation?updateMask=preferredTables",
            br#"{"preferredTables":[]}"#,
        ),
    );
    assert_eq!(clear_pref.status, 200);
    let job_all = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"bi_all","location":"US"},"configuration":{"query":{"query":"SELECT id FROM `local-project.analytics.other_table`","useLegacySql":false}}}"#,
    );
    assert_eq!(job_all.status, 200);
    let parsed_all: JobResource = serde_json::from_slice(&job_all.body).expect("decode");
    let all_stats = parsed_all
        .statistics
        .query
        .bi_engine_statistics
        .expect("biEngineStatistics");
    assert_eq!(all_stats.bi_engine_mode, "FULL");
    assert_eq!(all_stats.acceleration_mode, "BI_ENGINE");

    // Non-table scalar query reports DISABLED with UNSUPPORTED_SQL_TEXT
    let job_scalar = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"bi_scalar","location":"US"},"configuration":{"query":{"query":"SELECT 1 + 1 AS two","useLegacySql":false}}}"#,
    );
    assert_eq!(job_scalar.status, 200);
    let parsed_scalar: JobResource = serde_json::from_slice(&job_scalar.body).expect("decode");
    let scalar_stats = parsed_scalar
        .statistics
        .query
        .bi_engine_statistics
        .expect("biEngineStatistics");
    assert_eq!(scalar_stats.bi_engine_mode, "DISABLED");
    assert_eq!(
        scalar_stats.bi_engine_reasons[0].code,
        "UNSUPPORTED_SQL_TEXT"
    );

    // Resetting size to 0 disables BI Engine statistics again
    let reset_size = handle(
        &server,
        &Request::new(
            "PATCH",
            "/v1/projects/local-project/locations/US/biReservation?updateMask=size",
            br#"{"size":"0"}"#,
        ),
    );
    assert_eq!(reset_size.status, 200);
    let job_off = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"bi_off","location":"US"},"configuration":{"query":{"query":"SELECT id FROM `local-project.analytics.fast_table`","useLegacySql":false}}}"#,
    );
    assert_eq!(job_off.status, 200);
    assert!(!job_off.body_str().contains("biEngineStatistics"));

    // Negative size rejected
    let bad_size = handle(
        &server,
        &Request::new(
            "PATCH",
            "/v1/projects/local-project/locations/US/biReservation",
            br#"{"size":"-1"}"#,
        ),
    );
    assert_eq!(bad_size.status, 400);
}

#[test]
fn strict_auth_applies_to_models_and_bi_reservation_without_token_leak() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        auth_mode: "strict".to_string(),
        bearer_token: "super-secret-token".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    });

    let unauth_bi = handle(
        &server,
        &Request::new(
            "GET",
            "/v1/projects/local-project/locations/US/biReservation",
            b"",
        ),
    );
    assert_eq!(unauth_bi.status, 401);
    assert!(!unauth_bi.body_str().contains("super-secret-token"));

    let mut auth_req = Request::new(
        "GET",
        "/v1/projects/local-project/locations/US/biReservation",
        b"",
    );
    auth_req.authorization = "Bearer super-secret-token".to_string();
    let ok_bi = handle(&server, &auth_req);
    assert_eq!(ok_bi.status, 200);

    let unauth_models = handle(
        &server,
        &Request::new(
            "GET",
            "/bigquery/v2/projects/local-project/datasets/ml_ds/models",
            b"",
        ),
    );
    assert_eq!(unauth_models.status, 401);
    assert!(!unauth_models.body_str().contains("super-secret-token"));
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-ml-bi-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
