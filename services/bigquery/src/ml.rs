//! Deterministic local subset of BigQuery ML:
//! - `CREATE [OR REPLACE] MODEL [IF NOT EXISTS] <model> OPTIONS(...) AS <SELECT>`
//! - `DROP MODEL [IF EXISTS] <model>`
//! - `ML.PREDICT(MODEL <model>, (<SELECT>) | TABLE <table>)`
//! - `ML.EVALUATE(MODEL <model>, (<SELECT>) | TABLE <table>)`
//! - `ML.WEIGHTS(MODEL <model>)`
//!
//! Supported model types: `LINEAR_REG` (closed-form OLS / ridge normal
//! equations) and `LOGISTIC_REG` (deterministic binary batch gradient descent).
//! Unsupported model types and non-numeric features fail closed.

use serde_json::Value;

use crate::model::{
    IterationResult, ModelReference, ModelResource, ModelWeights, StandardSqlDataType,
    StandardSqlField, TableCell, TableDataRow, TableFieldSchema, TrainingOptions, TrainingRun,
};
use crate::responses::{dataset_etag, default_string, unix_millis_string};
use crate::server::{now_unix_nanos, Server};
use crate::sql_eval::QueryExecutionResult;
use crate::sql_expr::{
    find_top_level_keyword, split_top_level_commas, standard_sql_to_legacy_type,
};
use crate::sql_parser::{consume_balanced_parens, parse_table_identifier, MlTableFunctionKind};
use crate::tabledata_handlers::format_rfc3339_nanos;
use crate::validation::validate_resource_id;

struct ParsedCreateModel {
    or_replace: bool,
    if_not_exists: bool,
    project_id: String,
    dataset_id: String,
    model_id: String,
    model_type: String,
    label_column: String,
    l2_reg: Option<f64>,
    max_iterations: Option<i64>,
    training_query: String,
}

impl Server {
    pub(crate) fn is_create_model_query(raw_query: &str) -> bool {
        let upper = raw_query.trim().to_ascii_uppercase();
        upper.starts_with("CREATE MODEL ") || upper.starts_with("CREATE OR REPLACE MODEL ")
    }

    pub(crate) fn is_drop_model_query(raw_query: &str) -> bool {
        raw_query
            .trim()
            .to_ascii_uppercase()
            .starts_with("DROP MODEL ")
    }

    pub(crate) fn execute_create_model(
        &self,
        raw_query: &str,
        request_project_id: &str,
        dry_run: bool,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        let parsed = parse_create_model(raw_query, request_project_id)?;
        validate_resource_id(&parsed.project_id, "project")?;
        validate_resource_id(&parsed.dataset_id, "dataset")?;
        validate_resource_id(&parsed.model_id, "model")?;

        let dataset = self
            .read_dataset(&parsed.project_id, &parsed.dataset_id)
            .map_err(|err| err.to_string())?;
        if dataset.is_none() {
            return Err(format!(
                "not found: dataset {}:{}",
                parsed.project_id, parsed.dataset_id
            ));
        }

        let existing = self
            .read_model(&parsed.project_id, &parsed.dataset_id, &parsed.model_id)
            .map_err(|err| err.to_string())?;
        if existing.is_some() {
            if parsed.if_not_exists {
                return Ok(QueryExecutionResult::default());
            }
            if !parsed.or_replace {
                return Err(format!(
                    "already exists: model {}:{}.{}",
                    parsed.project_id, parsed.dataset_id, parsed.model_id
                ));
            }
        }

        let train_result = self.execute_query_for_view(
            request_project_id,
            &parsed.training_query,
            dry_run,
            depth + 1,
        )?;

        let label_idx = train_result
            .fields
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(&parsed.label_column))
            .ok_or_else(|| {
                format!(
                    "input label column {:?} not found in training query",
                    parsed.label_column
                )
            })?;
        let label_field = &train_result.fields[label_idx];
        let label_type =
            standard_sql_to_legacy_type(&default_string(label_field.field_type.clone(), "FLOAT"));
        if !is_ml_numeric_or_bool(&label_type) {
            return Err("BigQuery ML label column must be numeric or BOOLEAN".to_string());
        }

        let mut feature_indices = Vec::new();
        let mut feature_fields = Vec::new();
        for (i, field) in train_result.fields.iter().enumerate() {
            if i == label_idx {
                continue;
            }
            let ft =
                standard_sql_to_legacy_type(&default_string(field.field_type.clone(), "STRING"));
            if !is_ml_numeric_or_bool(&ft) {
                return Err(
                    "BigQuery ML local subset supports numeric and BOOLEAN feature columns only"
                        .to_string(),
                );
            }
            feature_indices.push(i);
            feature_fields.push((field.name.clone(), ft));
        }
        if feature_indices.is_empty() {
            return Err("BigQuery ML requires at least one feature column".to_string());
        }

        if dry_run {
            return Ok(QueryExecutionResult::default());
        }

        let mut x_rows: Vec<Vec<f64>> = Vec::new();
        let mut y_vals: Vec<f64> = Vec::new();
        for row in &train_result.rows {
            let Some(label_val) = cell_to_f64(&row.f[label_idx].v) else {
                continue;
            };
            let mut feats = Vec::with_capacity(feature_indices.len());
            let mut valid = true;
            for &idx in &feature_indices {
                match cell_to_f64(&row.f[idx].v) {
                    Some(v) if v.is_finite() => feats.push(v),
                    _ => {
                        valid = false;
                        break;
                    }
                }
            }
            if valid && label_val.is_finite() {
                x_rows.push(feats);
                y_vals.push(label_val);
            }
        }
        if x_rows.is_empty() {
            return Err("BigQuery ML training query produced no valid numeric rows".to_string());
        }

        let (intercept, weights, training_loss, loss_type, max_iter) =
            match parsed.model_type.as_str() {
                "LINEAR_REG" => {
                    let l2 = parsed.l2_reg.unwrap_or(0.0);
                    let (b, w, mse) = fit_linear_regression(&x_rows, &y_vals, l2)?;
                    (b, w, mse, "MEAN_SQUARED_LOSS", 1i64)
                }
                "LOGISTIC_REG" => {
                    for &y in &y_vals {
                        if (y - 0.0).abs() > 1e-9 && (y - 1.0).abs() > 1e-9 {
                            return Err(
                                "LOGISTIC_REG requires binary labels (0/1 or FALSE/TRUE)"
                                    .to_string(),
                            );
                        }
                    }
                    let l2 = parsed.l2_reg.unwrap_or(1e-4);
                    let iters = parsed.max_iterations.unwrap_or(200);
                    let (b, w, log_loss) = fit_logistic_regression(&x_rows, &y_vals, l2, iters)?;
                    (b, w, log_loss, "MEAN_LOG_LOSS", iters)
                }
                _ => {
                    return Err(
                        "unsupported BigQuery ML model_type; local subset supports LINEAR_REG and LOGISTIC_REG"
                            .to_string(),
                    )
                }
            };

        let now = now_unix_nanos();
        let model_resource = ModelResource {
            etag: dataset_etag(now),
            model_reference: ModelReference {
                project_id: parsed.project_id.clone(),
                dataset_id: parsed.dataset_id.clone(),
                model_id: parsed.model_id.clone(),
            },
            creation_time: unix_millis_string(now),
            last_modified_time: unix_millis_string(now),
            location: self.default_location().to_string(),
            model_type: parsed.model_type.clone(),
            training_runs: vec![TrainingRun {
                training_options: Some(TrainingOptions {
                    max_iterations: max_iter,
                    loss_type: loss_type.to_string(),
                    input_label_columns: vec![label_field.name.clone()],
                    l2_regularization: parsed.l2_reg,
                }),
                start_time: format_rfc3339_nanos(now),
                results: vec![IterationResult {
                    index: 0,
                    duration_ms: "1".to_string(),
                    training_loss: Some(training_loss),
                }],
            }],
            feature_columns: feature_fields
                .iter()
                .map(|(name, ft)| StandardSqlField {
                    name: name.clone(),
                    field_type: Some(Box::new(StandardSqlDataType {
                        type_kind: legacy_to_standard_type(ft),
                        ..Default::default()
                    })),
                })
                .collect(),
            label_columns: vec![StandardSqlField {
                name: label_field.name.clone(),
                field_type: Some(Box::new(StandardSqlDataType {
                    type_kind: legacy_to_standard_type(&label_type),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        let model_weights = ModelWeights {
            model_type: parsed.model_type,
            label_column: label_field.name.clone(),
            feature_columns: feature_fields.into_iter().map(|(n, _)| n).collect(),
            intercept,
            weights,
            label_type,
        };

        self.write_model(&model_resource)
            .map_err(|err| err.to_string())?;
        self.write_model_weights(
            &parsed.project_id,
            &parsed.dataset_id,
            &parsed.model_id,
            &model_weights,
        )
        .map_err(|err| err.to_string())?;

        Ok(QueryExecutionResult::default())
    }

    pub(crate) fn execute_drop_model(
        &self,
        raw_query: &str,
        request_project_id: &str,
        dry_run: bool,
    ) -> Result<QueryExecutionResult, String> {
        let trimmed = raw_query.strip_suffix(';').unwrap_or(raw_query).trim();
        let after_drop = trimmed["DROP MODEL ".len()..].trim();
        let upper = after_drop.to_ascii_uppercase();
        let (if_exists, ident) = if upper.starts_with("IF EXISTS ") {
            (true, after_drop["IF EXISTS ".len()..].trim())
        } else {
            (false, after_drop)
        };
        let (project_id, dataset_id, model_id) = parse_table_identifier(ident, request_project_id)?;
        validate_resource_id(&project_id, "project")?;
        validate_resource_id(&dataset_id, "dataset")?;
        validate_resource_id(&model_id, "model")?;

        let existing = self
            .read_model(&project_id, &dataset_id, &model_id)
            .map_err(|err| err.to_string())?;
        if existing.is_none() {
            if if_exists {
                return Ok(QueryExecutionResult::default());
            }
            return Err(format!(
                "not found: model {project_id}:{dataset_id}.{model_id}"
            ));
        }
        if !dry_run {
            let dir = self.model_dir(&project_id, &dataset_id, &model_id);
            std::fs::remove_dir_all(&dir).map_err(|err| err.to_string())?;
        }
        Ok(QueryExecutionResult::default())
    }

    pub(crate) fn execute_ml_table_function(
        &self,
        kind: MlTableFunctionKind,
        model_project_id: &str,
        model_dataset_id: &str,
        model_id: &str,
        input_sql: Option<&str>,
        request_project_id: &str,
        dry_run: bool,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        validate_resource_id(model_project_id, "project")?;
        validate_resource_id(model_dataset_id, "dataset")?;
        validate_resource_id(model_id, "model")?;

        let model = self
            .read_model(model_project_id, model_dataset_id, model_id)
            .map_err(|err| err.to_string())?;
        if model.is_none() {
            return Err(format!(
                "not found: model {model_project_id}:{model_dataset_id}.{model_id}"
            ));
        }
        let weights = self
            .read_model_weights(model_project_id, model_dataset_id, model_id)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| {
                format!(
                    "model {model_project_id}:{model_dataset_id}.{model_id} has no trained weights"
                )
            })?;

        match kind {
            MlTableFunctionKind::Weights => {
                let fields = vec![
                    TableFieldSchema {
                        name: "processed_input".to_string(),
                        field_type: "STRING".to_string(),
                        mode: "NULLABLE".to_string(),
                        ..Default::default()
                    },
                    TableFieldSchema {
                        name: "weight".to_string(),
                        field_type: "FLOAT".to_string(),
                        mode: "NULLABLE".to_string(),
                        ..Default::default()
                    },
                ];
                if dry_run {
                    return Ok(QueryExecutionResult {
                        fields,
                        rows: Vec::new(),
                    });
                }
                let mut rows = Vec::with_capacity(weights.feature_columns.len() + 1);
                rows.push(TableDataRow {
                    f: vec![
                        TableCell {
                            v: Value::String("__INTERCEPT__".to_string()),
                        },
                        TableCell {
                            v: float_cell(weights.intercept),
                        },
                    ],
                });
                for (name, w) in weights.feature_columns.iter().zip(weights.weights.iter()) {
                    rows.push(TableDataRow {
                        f: vec![
                            TableCell {
                                v: Value::String(name.clone()),
                            },
                            TableCell { v: float_cell(*w) },
                        ],
                    });
                }
                Ok(QueryExecutionResult { fields, rows })
            }
            MlTableFunctionKind::Predict => {
                let input_sql =
                    input_sql.ok_or_else(|| "ML.PREDICT requires an input query".to_string())?;
                let input =
                    self.execute_query_for_view(request_project_id, input_sql, dry_run, depth + 1)?;
                let feat_indices = map_feature_indices(&input.fields, &weights.feature_columns)?;
                let pred_col_name = format!("predicted_{}", weights.label_column);
                let pred_field_type = if weights.model_type == "LOGISTIC_REG" {
                    if weights.label_type == "BOOLEAN" {
                        "BOOLEAN".to_string()
                    } else {
                        "INTEGER".to_string()
                    }
                } else {
                    "FLOAT".to_string()
                };
                let mut fields = Vec::with_capacity(input.fields.len() + 1);
                fields.push(TableFieldSchema {
                    name: pred_col_name,
                    field_type: pred_field_type.clone(),
                    mode: "NULLABLE".to_string(),
                    ..Default::default()
                });
                fields.extend(input.fields.iter().cloned());
                if dry_run {
                    return Ok(QueryExecutionResult {
                        fields,
                        rows: Vec::new(),
                    });
                }
                let mut rows = Vec::with_capacity(input.rows.len());
                for row in input.rows {
                    let mut score = weights.intercept;
                    let mut all_present = true;
                    for (&idx, &w) in feat_indices.iter().zip(weights.weights.iter()) {
                        match cell_to_f64(&row.f[idx].v) {
                            Some(x) => score += w * x,
                            None => {
                                all_present = false;
                                break;
                            }
                        }
                    }
                    let pred_cell = if !all_present {
                        TableCell { v: Value::Null }
                    } else if weights.model_type == "LOGISTIC_REG" {
                        let prob = sigmoid(score);
                        let positive = prob >= 0.5;
                        let v = if pred_field_type == "BOOLEAN" {
                            Value::String(positive.to_string())
                        } else {
                            Value::String(if positive { "1" } else { "0" }.to_string())
                        };
                        TableCell { v }
                    } else {
                        TableCell {
                            v: float_cell(score),
                        }
                    };
                    let mut out_cells = Vec::with_capacity(row.f.len() + 1);
                    out_cells.push(pred_cell);
                    out_cells.extend(row.f);
                    rows.push(TableDataRow { f: out_cells });
                }
                Ok(QueryExecutionResult { fields, rows })
            }
            MlTableFunctionKind::Evaluate => {
                let input_sql =
                    input_sql.ok_or_else(|| "ML.EVALUATE requires an input query".to_string())?;
                let input =
                    self.execute_query_for_view(request_project_id, input_sql, dry_run, depth + 1)?;
                let feat_indices = map_feature_indices(&input.fields, &weights.feature_columns)?;
                let label_idx = input
                    .fields
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(&weights.label_column))
                    .ok_or_else(|| {
                        format!(
                            "label column {:?} not found in ML.EVALUATE input",
                            weights.label_column
                        )
                    })?;

                if weights.model_type == "LINEAR_REG" {
                    let fields = ["mean_absolute_error", "mean_squared_error", "r2_score"]
                        .into_iter()
                        .map(|n| TableFieldSchema {
                            name: n.to_string(),
                            field_type: "FLOAT".to_string(),
                            mode: "NULLABLE".to_string(),
                            ..Default::default()
                        })
                        .collect();
                    if dry_run {
                        return Ok(QueryExecutionResult {
                            fields,
                            rows: Vec::new(),
                        });
                    }
                    let mut ys = Vec::new();
                    let mut preds = Vec::new();
                    for row in &input.rows {
                        let Some(y) = cell_to_f64(&row.f[label_idx].v) else {
                            continue;
                        };
                        let mut score = weights.intercept;
                        let mut valid = true;
                        for (&idx, &w) in feat_indices.iter().zip(weights.weights.iter()) {
                            match cell_to_f64(&row.f[idx].v) {
                                Some(x) => score += w * x,
                                None => {
                                    valid = false;
                                    break;
                                }
                            }
                        }
                        if valid {
                            ys.push(y);
                            preds.push(score);
                        }
                    }
                    if ys.is_empty() {
                        return Err("ML.EVALUATE input produced no valid rows".to_string());
                    }
                    let n = ys.len() as f64;
                    let mae = ys
                        .iter()
                        .zip(preds.iter())
                        .map(|(y, p)| (y - p).abs())
                        .sum::<f64>()
                        / n;
                    let mse = ys
                        .iter()
                        .zip(preds.iter())
                        .map(|(y, p)| (y - p).powi(2))
                        .sum::<f64>()
                        / n;
                    let mean_y = ys.iter().sum::<f64>() / n;
                    let ss_tot: f64 = ys.iter().map(|y| (y - mean_y).powi(2)).sum();
                    let ss_res: f64 = ys
                        .iter()
                        .zip(preds.iter())
                        .map(|(y, p)| (y - p).powi(2))
                        .sum();
                    let r2 = if ss_tot == 0.0 {
                        if ss_res == 0.0 {
                            1.0
                        } else {
                            0.0
                        }
                    } else {
                        1.0 - (ss_res / ss_tot)
                    };
                    return Ok(QueryExecutionResult {
                        fields,
                        rows: vec![TableDataRow {
                            f: vec![
                                TableCell { v: float_cell(mae) },
                                TableCell { v: float_cell(mse) },
                                TableCell { v: float_cell(r2) },
                            ],
                        }],
                    });
                }

                // LOGISTIC_REG evaluation
                let fields = ["precision", "recall", "accuracy", "f1_score", "log_loss"]
                    .into_iter()
                    .map(|n| TableFieldSchema {
                        name: n.to_string(),
                        field_type: "FLOAT".to_string(),
                        mode: "NULLABLE".to_string(),
                        ..Default::default()
                    })
                    .collect();
                if dry_run {
                    return Ok(QueryExecutionResult {
                        fields,
                        rows: Vec::new(),
                    });
                }
                let mut tp = 0.0f64;
                let mut fp = 0.0f64;
                let mut tn = 0.0f64;
                let mut fn_count = 0.0f64;
                let mut log_loss_sum = 0.0f64;
                let mut count = 0usize;
                for row in &input.rows {
                    let Some(y) = cell_to_f64(&row.f[label_idx].v) else {
                        continue;
                    };
                    let mut score = weights.intercept;
                    let mut valid = true;
                    for (&idx, &w) in feat_indices.iter().zip(weights.weights.iter()) {
                        match cell_to_f64(&row.f[idx].v) {
                            Some(x) => score += w * x,
                            None => {
                                valid = false;
                                break;
                            }
                        }
                    }
                    if !valid {
                        continue;
                    }
                    let prob = sigmoid(score).clamp(1e-12, 1.0 - 1e-12);
                    let pred_pos = prob >= 0.5;
                    let actual_pos = y >= 0.5;
                    match (pred_pos, actual_pos) {
                        (true, true) => tp += 1.0,
                        (true, false) => fp += 1.0,
                        (false, false) => tn += 1.0,
                        (false, true) => fn_count += 1.0,
                    }
                    let y_bin = if actual_pos { 1.0 } else { 0.0 };
                    log_loss_sum += -(y_bin * prob.ln() + (1.0 - y_bin) * (1.0 - prob).ln());
                    count += 1;
                }
                if count == 0 {
                    return Err("ML.EVALUATE input produced no valid rows".to_string());
                }
                let precision = if tp + fp > 0.0 { tp / (tp + fp) } else { 0.0 };
                let recall = if tp + fn_count > 0.0 {
                    tp / (tp + fn_count)
                } else {
                    0.0
                };
                let accuracy = (tp + tn) / count as f64;
                let f1 = if precision + recall > 0.0 {
                    2.0 * precision * recall / (precision + recall)
                } else {
                    0.0
                };
                let log_loss = log_loss_sum / count as f64;
                Ok(QueryExecutionResult {
                    fields,
                    rows: vec![TableDataRow {
                        f: vec![
                            TableCell {
                                v: float_cell(precision),
                            },
                            TableCell {
                                v: float_cell(recall),
                            },
                            TableCell {
                                v: float_cell(accuracy),
                            },
                            TableCell { v: float_cell(f1) },
                            TableCell {
                                v: float_cell(log_loss),
                            },
                        ],
                    }],
                })
            }
        }
    }
}

fn parse_create_model(
    raw_query: &str,
    default_project_id: &str,
) -> Result<ParsedCreateModel, String> {
    let trimmed = raw_query.strip_suffix(';').unwrap_or(raw_query).trim();
    let upper = trimmed.to_ascii_uppercase();
    let (or_replace, after_create) = if upper.starts_with("CREATE OR REPLACE MODEL ") {
        (true, trimmed["CREATE OR REPLACE MODEL ".len()..].trim())
    } else if upper.starts_with("CREATE MODEL ") {
        (false, trimmed["CREATE MODEL ".len()..].trim())
    } else {
        return Err("invalid CREATE MODEL statement".to_string());
    };

    let upper_after = after_create.to_ascii_uppercase();
    let (if_not_exists, rest) = if upper_after.starts_with("IF NOT EXISTS ") {
        (true, after_create["IF NOT EXISTS ".len()..].trim())
    } else {
        (false, after_create)
    };

    let Some(options_pos) = find_top_level_keyword(rest, "OPTIONS") else {
        return Err("CREATE MODEL requires OPTIONS(...)".to_string());
    };
    let model_ident = rest[..options_pos].trim();
    let (project_id, dataset_id, model_id) =
        parse_table_identifier(model_ident, default_project_id)?;

    let after_options_kw = rest[options_pos + "OPTIONS".len()..].trim_start();
    let (options_inner, after_options) = consume_balanced_parens(after_options_kw)?;
    let after_options = after_options.trim();
    let upper_after_opt = after_options.to_ascii_uppercase();
    let training_query = if upper_after_opt.starts_with("AS ") {
        after_options["AS ".len()..].trim().to_string()
    } else if upper_after_opt.starts_with("AS\n") || upper_after_opt.starts_with("AS\t") {
        after_options[2..].trim().to_string()
    } else {
        return Err("CREATE MODEL requires AS <SELECT query>".to_string());
    };
    if training_query.is_empty() {
        return Err("CREATE MODEL requires AS <SELECT query>".to_string());
    }

    let mut model_type = String::new();
    let mut label_column = "label".to_string();
    let mut l2_reg: Option<f64> = None;
    let mut max_iterations: Option<i64> = None;

    for opt in split_top_level_commas(options_inner)? {
        let opt = opt.trim();
        if opt.is_empty() {
            continue;
        }
        let Some((key, val)) = opt.split_once('=') else {
            return Err("invalid CREATE MODEL option".to_string());
        };
        let key = key.trim().to_ascii_lowercase();
        let val = val.trim();
        match key.as_str() {
            "model_type" => {
                model_type = unquote_sql_string(val)?.to_ascii_uppercase();
            }
            "input_label_cols" => {
                let inner = val
                    .strip_prefix('[')
                    .and_then(|s| s.strip_suffix(']'))
                    .ok_or_else(|| "input_label_cols must be a string array".to_string())?;
                let cols = split_top_level_commas(inner)?;
                if cols.len() != 1 {
                    return Err(
                        "BigQuery ML local subset supports exactly one input_label_cols entry"
                            .to_string(),
                    );
                }
                label_column = unquote_sql_string(cols[0].trim())?;
                if label_column.is_empty() {
                    return Err("input_label_cols entry must not be empty".to_string());
                }
            }
            "l2_reg" => {
                let v = val
                    .parse::<f64>()
                    .map_err(|_| "l2_reg must be a non-negative number".to_string())?;
                if !v.is_finite() || v < 0.0 {
                    return Err("l2_reg must be a non-negative number".to_string());
                }
                l2_reg = Some(v);
            }
            "max_iterations" => {
                let v = val
                    .parse::<i64>()
                    .map_err(|_| "max_iterations must be a positive integer".to_string())?;
                if !(1..=5000).contains(&v) {
                    return Err("max_iterations must be between 1 and 5000".to_string());
                }
                max_iterations = Some(v);
            }
            _ => return Err("unsupported CREATE MODEL option".to_string()),
        }
    }

    if model_type != "LINEAR_REG" && model_type != "LOGISTIC_REG" {
        return Err(
            "unsupported BigQuery ML model_type; local subset supports LINEAR_REG and LOGISTIC_REG"
                .to_string(),
        );
    }

    Ok(ParsedCreateModel {
        or_replace,
        if_not_exists,
        project_id,
        dataset_id,
        model_id,
        model_type,
        label_column,
        l2_reg,
        max_iterations,
        training_query,
    })
}

fn unquote_sql_string(val: &str) -> Result<String, String> {
    if (val.starts_with('\'') && val.ends_with('\''))
        || (val.starts_with('"') && val.ends_with('"'))
    {
        if val.len() >= 2 {
            return Ok(val[1..val.len() - 1].to_string());
        }
    }
    Err("option value must be a quoted string".to_string())
}

fn is_ml_numeric_or_bool(field_type: &str) -> bool {
    matches!(
        field_type.to_ascii_uppercase().as_str(),
        "INTEGER" | "INT64" | "FLOAT" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC" | "BOOLEAN" | "BOOL"
    )
}

fn legacy_to_standard_type(field_type: &str) -> String {
    match field_type.to_ascii_uppercase().as_str() {
        "INTEGER" | "INT64" => "INT64".to_string(),
        "FLOAT" | "FLOAT64" => "FLOAT64".to_string(),
        "BOOLEAN" | "BOOL" => "BOOL".to_string(),
        other => other.to_string(),
    }
}

fn cell_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Null => None,
        Value::Number(n) => n.as_f64(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        Value::String(s) => {
            if let Ok(f) = s.trim().parse::<f64>() {
                Some(f)
            } else if s.eq_ignore_ascii_case("true") {
                Some(1.0)
            } else if s.eq_ignore_ascii_case("false") {
                Some(0.0)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn float_cell(v: f64) -> Value {
    let clean = if v.abs() < 1e-12 { 0.0 } else { v };
    serde_json::Number::from_f64(clean)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn map_feature_indices(
    fields: &[TableFieldSchema],
    feature_columns: &[String],
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::with_capacity(feature_columns.len());
    for feat in feature_columns {
        let Some(idx) = fields
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(feat))
        else {
            return Err(format!("missing required model feature column {feat:?}"));
        };
        indices.push(idx);
    }
    Ok(indices)
}

fn sigmoid(z: f64) -> f64 {
    if z >= 0.0 {
        1.0 / (1.0 + (-z).exp())
    } else {
        let ez = z.exp();
        ez / (1.0 + ez)
    }
}

/// Ordinary least squares (with optional L2 ridge penalty) solved via Gaussian
/// elimination with partial pivoting on `(X^T X + λ I) w = X^T y`.
fn fit_linear_regression(
    x_rows: &[Vec<f64>],
    y_vals: &[f64],
    l2_reg: f64,
) -> Result<(f64, Vec<f64>, f64), String> {
    let k = x_rows[0].len();
    let d = k + 1; // index 0 = intercept
    let mut xtx = vec![vec![0.0f64; d]; d];
    let mut xty = vec![0.0f64; d];

    for (row, &y) in x_rows.iter().zip(y_vals.iter()) {
        for i in 0..d {
            let xi = if i == 0 { 1.0 } else { row[i - 1] };
            xty[i] += xi * y;
            for j in 0..d {
                let xj = if j == 0 { 1.0 } else { row[j - 1] };
                xtx[i][j] += xi * xj;
            }
        }
    }

    let mut mat = xtx.clone();
    for i in 1..d {
        mat[i][i] += l2_reg;
    }
    let coeffs = solve_linear_system(mat, xty.clone()).or_else(|_| {
        // Fallback ridge for collinear or underdetermined feature matrices.
        let mut ridge = xtx;
        for i in 0..d {
            ridge[i][i] += l2_reg.max(1e-6);
        }
        solve_linear_system(ridge, xty)
    })?;

    let intercept = coeffs[0];
    let weights = coeffs[1..].to_vec();
    let mse = x_rows
        .iter()
        .zip(y_vals.iter())
        .map(|(row, &y)| {
            let pred = intercept
                + row
                    .iter()
                    .zip(weights.iter())
                    .map(|(x, w)| x * w)
                    .sum::<f64>();
            (pred - y).powi(2)
        })
        .sum::<f64>()
        / x_rows.len() as f64;
    Ok((intercept, weights, mse))
}

fn solve_linear_system(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Result<Vec<f64>, String> {
    let n = b.len();
    for col in 0..n {
        let mut pivot_row = col;
        let mut max_val = a[col][col].abs();
        for row in (col + 1)..n {
            if a[row][col].abs() > max_val {
                max_val = a[row][col].abs();
                pivot_row = row;
            }
        }
        if max_val < 1e-12 {
            return Err("singular matrix".to_string());
        }
        if pivot_row != col {
            a.swap(col, pivot_row);
            b.swap(col, pivot_row);
        }
        let pivot = a[col][col];
        for row in (col + 1)..n {
            let factor = a[row][col] / pivot;
            for c in col..n {
                a[row][c] -= factor * a[col][c];
            }
            b[row] -= factor * b[col];
        }
    }
    let mut x = vec![0.0f64; n];
    for i in (0..n).rev() {
        let mut sum = b[i];
        for j in (i + 1)..n {
            sum -= a[i][j] * x[j];
        }
        x[i] = sum / a[i][i];
    }
    Ok(x)
}

/// Deterministic batch gradient descent for binary logistic regression with
/// internal z-score feature scaling, returning weights in original feature scale.
fn fit_logistic_regression(
    x_rows: &[Vec<f64>],
    y_vals: &[f64],
    l2_reg: f64,
    max_iterations: i64,
) -> Result<(f64, Vec<f64>, f64), String> {
    let n = x_rows.len() as f64;
    let k = x_rows[0].len();
    let mut means = vec![0.0f64; k];
    for row in x_rows {
        for j in 0..k {
            means[j] += row[j];
        }
    }
    for m in &mut means {
        *m /= n;
    }
    let mut stds = vec![0.0f64; k];
    for row in x_rows {
        for j in 0..k {
            stds[j] += (row[j] - means[j]).powi(2);
        }
    }
    for s in &mut stds {
        *s = (*s / n).sqrt();
        if *s < 1e-9 {
            *s = 1.0;
        }
    }

    let mut b = 0.0f64;
    let mut w = vec![0.0f64; k];
    let lr = 0.5f64;
    for _ in 0..max_iterations {
        let mut grad_b = 0.0f64;
        let mut grad_w = vec![0.0f64; k];
        for (row, &y) in x_rows.iter().zip(y_vals.iter()) {
            let mut z = b;
            for j in 0..k {
                z += w[j] * ((row[j] - means[j]) / stds[j]);
            }
            let err = sigmoid(z) - y;
            grad_b += err;
            for j in 0..k {
                grad_w[j] += err * ((row[j] - means[j]) / stds[j]);
            }
        }
        b -= lr * (grad_b / n);
        for j in 0..k {
            w[j] -= lr * ((grad_w[j] / n) + l2_reg * w[j]);
        }
    }

    // Transform back to unscaled feature space.
    let mut orig_w = vec![0.0f64; k];
    let mut orig_b = b;
    for j in 0..k {
        orig_w[j] = w[j] / stds[j];
        orig_b -= orig_w[j] * means[j];
    }

    let mut loss = 0.0f64;
    for (row, &y) in x_rows.iter().zip(y_vals.iter()) {
        let mut z = orig_b;
        for j in 0..k {
            z += orig_w[j] * row[j];
        }
        let p = sigmoid(z).clamp(1e-12, 1.0 - 1e-12);
        loss += -(y * p.ln() + (1.0 - y) * (1.0 - p).ln());
    }
    loss /= n;
    Ok((orig_b, orig_w, loss))
}
