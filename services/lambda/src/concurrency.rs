//! Function reserved concurrency enforcement and provisioned concurrency
//! configurations.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::server::{
    effective_qualifier, invalid_param, parse_json, parse_pagination, validation, Reply, Server,
};
use crate::time_fmt::now_lambda;

pub(crate) const ACCOUNT_CONCURRENT_EXECUTIONS: i64 = 1000;
pub(crate) const MIN_UNRESERVED_CONCURRENT_EXECUTIONS: i64 = 100;

/// Stored provisioned concurrency config for one published version or alias.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ProvisionedConcurrencyConfig {
    pub requested: i64,
    pub last_modified: String,
}

impl ProvisionedConcurrencyConfig {
    pub(crate) fn to_json(&self, function_arn: Option<&str>) -> Value {
        let mut v = json!({
            "RequestedProvisionedConcurrentExecutions": self.requested,
            "AvailableProvisionedConcurrentExecutions": self.requested,
            "AllocatedProvisionedConcurrentExecutions": self.requested,
            "Status": "READY",
            "LastModified": self.last_modified,
        });
        if let Some(arn) = function_arn {
            v["FunctionArn"] = json!(arn);
        }
        v
    }
}

/// Tracks active in-flight invocations per function so `ReservedConcurrentExecutions`
/// is enforced deterministically on `Invoke`.
#[derive(Default)]
pub(crate) struct ConcurrencyTracker {
    in_flight: Arc<Mutex<HashMap<String, usize>>>,
}

/// RAII permit held for the duration of one invocation (sync or async);
/// releasing it decrements the function's active count even on timeout/abort.
pub(crate) struct ConcurrencyPermit {
    in_flight: Arc<Mutex<HashMap<String, usize>>>,
    function_name: String,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut counts = self.in_flight.lock().unwrap();
        if let Some(n) = counts.get_mut(&self.function_name) {
            if *n <= 1 {
                counts.remove(&self.function_name);
            } else {
                *n -= 1;
            }
        }
    }
}

impl ConcurrencyTracker {
    pub(crate) fn try_acquire(
        &self,
        function_name: &str,
        reserved_limit: Option<i64>,
    ) -> Result<ConcurrencyPermit, Reply> {
        let mut counts = self.in_flight.lock().unwrap();
        let current = counts.get(function_name).copied().unwrap_or(0);
        if let Some(limit) = reserved_limit {
            if limit <= 0 || current >= limit as usize {
                return Err(Reply::error(
                    429,
                    "TooManyRequestsException",
                    "Rate Exceeded.",
                ));
            }
        }
        counts.insert(function_name.to_string(), current + 1);
        Ok(ConcurrencyPermit {
            in_flight: Arc::clone(&self.in_flight),
            function_name: function_name.to_string(),
        })
    }
}

impl Server {
    fn provisioned_not_found(&self, name: &str, qualifier: &str) -> Reply {
        Reply::error(
            404,
            "ProvisionedConcurrencyConfigNotFoundException",
            &format!(
                "No Provisioned Concurrency Config found for function: {}:{qualifier}",
                self.function_arn(name)
            ),
        )
    }

    /// `PUT /2017-10-31/functions/<id>/concurrency` (`PutFunctionConcurrency`).
    pub fn put_function_concurrency(&self, id: &str, body: &[u8]) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let reserved = match req
            .get("ReservedConcurrentExecutions")
            .and_then(Value::as_i64)
        {
            Some(n) if n >= 0 => n,
            None => {
                return validation("1 validation error detected: Value null at 'reservedConcurrentExecutions' failed to satisfy constraint: Member must not be null");
            }
            Some(n) => {
                return validation(&format!(
                    "1 validation error detected: Value '{n}' at 'reservedConcurrentExecutions' failed to satisfy constraint: Member must have value greater than or equal to 0"
                ));
            }
        };
        let result = self.commit(|st| {
            let current_reserved = st
                .functions
                .get(&name)
                .ok_or_else(|| self.not_found(&name))?
                .reserved_concurrent_executions
                .unwrap_or(0);
            let total_after = st.total_reserved_concurrency() - current_reserved + reserved;
            if ACCOUNT_CONCURRENT_EXECUTIONS - total_after < MIN_UNRESERVED_CONCURRENT_EXECUTIONS {
                return Err(Reply::error(
                    400,
                    "LimitExceededException",
                    &format!(
                        "Specified ReservedConcurrentExecutions for function decreases account's UnreservedConcurrentExecution below its minimum value of [{MIN_UNRESERVED_CONCURRENT_EXECUTIONS}]."
                    ),
                ));
            }
            let rec = st.functions.get_mut(&name).unwrap();
            let total_provisioned: i64 = rec
                .provisioned_concurrency
                .values()
                .map(|c| c.requested)
                .sum();
            if reserved < total_provisioned {
                return Err(invalid_param(&format!(
                    "ReservedConcurrentExecutions ({reserved}) cannot be less than total ProvisionedConcurrentExecutions ({total_provisioned})"
                )));
            }
            rec.reserved_concurrent_executions = Some(reserved);
            Ok(reserved)
        });
        match result {
            Ok(n) => Reply::json(200, &json!({ "ReservedConcurrentExecutions": n })),
            Err(r) => r,
        }
    }

    /// `GET /2019-09-30/functions/<id>/concurrency` (`GetFunctionConcurrency`).
    pub fn get_function_concurrency(&self, id: &str) -> Reply {
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        match rec.reserved_concurrent_executions {
            Some(n) => Reply::json(200, &json!({ "ReservedConcurrentExecutions": n })),
            None => Reply::json(200, &json!({})),
        }
    }

    /// `DELETE /2017-10-31/functions/<id>/concurrency` (`DeleteFunctionConcurrency`).
    pub fn delete_function_concurrency(&self, id: &str) -> Reply {
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            rec.reserved_concurrent_executions = None;
            Ok(())
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }

    /// Validates `(function_name, qualifier)` for provisioned concurrency:
    /// requires a published version or alias (never `$LATEST` or unqualified).
    fn resolve_provisioned_target(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Result<(String, String), Reply> {
        let qualifier = effective_qualifier(id, query.get("Qualifier").map(String::as_str))?;
        let Some(q) = qualifier else {
            return Err(invalid_param(
                "Provisioned Concurrency Configs require a published version or alias Qualifier",
            ));
        };
        if q == "$LATEST" {
            return Err(invalid_param(
                "Provisioned Concurrency Configs are not supported for $LATEST version",
            ));
        }
        let resolved = self.lookup(id, query.get("Qualifier").map(String::as_str))?;
        Ok((resolved.function_name, q))
    }

    /// `PUT /2019-09-30/functions/<id>/provisioned-concurrency?Qualifier=<q>`.
    pub fn put_provisioned_concurrency_config(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let requested = match req
            .get("ProvisionedConcurrentExecutions")
            .and_then(Value::as_i64)
        {
            Some(n) if n >= 1 => n,
            None => {
                return validation("1 validation error detected: Value null at 'provisionedConcurrentExecutions' failed to satisfy constraint: Member must not be null");
            }
            Some(n) => {
                return validation(&format!(
                    "1 validation error detected: Value '{n}' at 'provisionedConcurrentExecutions' failed to satisfy constraint: Member must have value greater than or equal to 1"
                ));
            }
        };
        let (name, qualifier) = match self.resolve_provisioned_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let cfg = ProvisionedConcurrencyConfig {
            requested,
            last_modified: now_lambda(),
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            let existing = rec
                .provisioned_concurrency
                .get(&qualifier)
                .map(|c| c.requested)
                .unwrap_or(0);
            let total_provisioned: i64 = rec
                .provisioned_concurrency
                .values()
                .map(|c| c.requested)
                .sum::<i64>()
                - existing
                + requested;
            if let Some(reserved) = rec.reserved_concurrent_executions {
                if total_provisioned > reserved {
                    return Err(invalid_param(&format!(
                        "Total ProvisionedConcurrentExecutions ({total_provisioned}) cannot exceed function ReservedConcurrentExecutions ({reserved})"
                    )));
                }
            } else if total_provisioned > ACCOUNT_CONCURRENT_EXECUTIONS - MIN_UNRESERVED_CONCURRENT_EXECUTIONS {
                return Err(Reply::error(
                    400,
                    "LimitExceededException",
                    "Requested ProvisionedConcurrentExecutions exceeds available account concurrency",
                ));
            }
            rec.provisioned_concurrency.insert(qualifier.clone(), cfg.clone());
            Ok(())
        });
        match result {
            Ok(()) => Reply::json(202, &cfg.to_json(None)),
            Err(r) => r,
        }
    }

    /// `GET /2019-09-30/functions/<id>/provisioned-concurrency` — routes to
    /// `ListProvisionedConcurrencyConfigs` when `List=ALL` or no qualifier is
    /// given, otherwise `GetProvisionedConcurrencyConfig`.
    pub fn get_or_list_provisioned_concurrency(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let list_all = query.get("List").map(String::as_str) == Some("ALL");
        let qualifier = match effective_qualifier(id, query.get("Qualifier").map(String::as_str)) {
            Ok(q) => q,
            Err(r) => return r,
        };
        if list_all || qualifier.is_none() {
            self.list_provisioned_concurrency_configs(id, query)
        } else {
            self.get_provisioned_concurrency_config(id, query)
        }
    }

    pub fn get_provisioned_concurrency_config(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let (name, qualifier) = match self.resolve_provisioned_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        match rec.provisioned_concurrency.get(&qualifier) {
            Some(cfg) => Reply::json(200, &cfg.to_json(None)),
            None => self.provisioned_not_found(&name, &qualifier),
        }
    }

    pub fn list_provisioned_concurrency_configs(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let name = match self.lookup_unqualified_or_latest(id) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.functions.get(&name) else {
            return self.not_found(&name);
        };
        let all: Vec<Value> = rec
            .provisioned_concurrency
            .iter()
            .map(|(q, cfg)| {
                let arn = self.qualified_function_arn(&name, Some(q));
                cfg.to_json(Some(&arn))
            })
            .collect();
        let page: Vec<Value> = all.iter().skip(start).take(max_items).cloned().collect();
        let mut out = json!({ "ProvisionedConcurrencyConfigs": page });
        let next = start.saturating_add(max_items);
        if next < all.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    /// `DELETE /2019-09-30/functions/<id>/provisioned-concurrency?Qualifier=<q>`.
    pub fn delete_provisioned_concurrency_config(
        &self,
        id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let (name, qualifier) = match self.resolve_provisioned_target(id, query) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let result = self.commit(|st| {
            let rec = st
                .functions
                .get_mut(&name)
                .ok_or_else(|| self.not_found(&name))?;
            rec.provisioned_concurrency
                .remove(&qualifier)
                .ok_or_else(|| self.provisioned_not_found(&name, &qualifier))?;
            Ok(())
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_enforces_reserved_limit_and_releases_on_drop() {
        let tracker = ConcurrencyTracker::default();
        assert_eq!(tracker.try_acquire("f", Some(0)).err().unwrap().status, 429);
        let p1 = tracker.try_acquire("f", Some(2)).unwrap();
        let p2 = tracker.try_acquire("f", Some(2)).unwrap();
        assert_eq!(tracker.try_acquire("f", Some(2)).err().unwrap().status, 429);
        // Other functions have their own budget.
        let _other = tracker.try_acquire("g", Some(1)).unwrap();
        drop(p1);
        let _p3 = tracker.try_acquire("f", Some(2)).unwrap();
        drop(p2);
    }

    #[test]
    fn tracker_allows_unreserved_invocations() {
        let tracker = ConcurrencyTracker::default();
        let _a = tracker.try_acquire("f", None).unwrap();
        let _b = tracker.try_acquire("f", None).unwrap();
    }
}
