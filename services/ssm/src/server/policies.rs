use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::broadcast;

use super::Server;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyEventKind {
    ExpirationNotification,
    NoChangeNotification,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyEvent {
    pub kind: PolicyEventKind,
    pub parameter_name: String,
    pub parameter_version: u64,
}

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_MILLISECOND: i128 = 1_000_000;

pub(super) fn expiration_timestamp(timestamp: &str) -> Option<i128> {
    let date = OffsetDateTime::parse(timestamp, &Rfc3339).ok()?;
    Some(date.unix_timestamp_nanos())
}

fn notification_offset(attributes: &Value) -> Option<i128> {
    let before = attributes["Before"].as_str()?.parse::<u64>().ok()?;
    let seconds = match attributes["Unit"].as_str()? {
        "Hours" => 3_600,
        "Days" => 86_400,
        _ => return None,
    };
    (before > 0).then_some(i128::from(before) * seconds * NANOS_PER_SECOND)
}

impl Server {
    /// Subscribe to live policy notifications within this process.
    pub fn subscribe_policy_events(&self) -> broadcast::Receiver<PolicyEvent> {
        self.policy_events.subscribe()
    }

    pub(crate) fn evaluate_policies(&self) -> Result<(), String> {
        let mut parameters = self.state.lock().unwrap();
        let now = self.time_since_epoch().as_nanos() as i128;
        let mut expired = Vec::new();
        let mut notifications = Vec::new();

        for (name, parameter) in parameters.iter().filter(|(_, p)| !p.policies.is_empty()) {
            let policies: Vec<_> = parameter
                .policies
                .iter()
                .enumerate()
                .filter_map(|(index, policy)| {
                    let text: Value = serde_json::from_str(&policy.policy_text).ok()?;
                    Some((index, policy.policy_type.as_str(), text))
                })
                .collect();
            let expiration = policies
                .iter()
                .filter(|(_, kind, _)| *kind == "Expiration")
                .filter_map(|(_, _, text)| {
                    expiration_timestamp(text["Attributes"]["Timestamp"].as_str()?)
                })
                .min();
            if expiration.is_some_and(|deadline| now >= deadline) {
                expired.push(name.clone());
                continue;
            }

            for (index, kind, text) in policies {
                if parameter.notified_policies.contains(&index) {
                    continue;
                }
                let Some(offset) = notification_offset(&text["Attributes"]) else {
                    continue;
                };
                let (kind, due) = match kind {
                    "ExpirationNotification" => (
                        PolicyEventKind::ExpirationNotification,
                        expiration.map(|deadline| deadline - offset),
                    ),
                    "NoChangeNotification" => {
                        let last_modified_ms = (parameter.last_modified * 1_000.0).round() as i64;
                        (
                            PolicyEventKind::NoChangeNotification,
                            Some(i128::from(last_modified_ms) * NANOS_PER_MILLISECOND + offset),
                        )
                    }
                    _ => continue,
                };
                if due.is_some_and(|deadline| now >= deadline) {
                    notifications.push((
                        index,
                        PolicyEvent {
                            kind,
                            parameter_name: name.clone(),
                            parameter_version: parameter.version,
                        },
                    ));
                }
            }
        }

        if expired.is_empty() && notifications.is_empty() {
            return Ok(());
        }
        let mut updated = parameters.clone();
        for name in expired {
            updated.remove(&name);
        }
        for (index, event) in &notifications {
            updated
                .get_mut(&event.parameter_name)
                .unwrap()
                .notified_policies
                .push(*index);
        }
        self.persist(&updated)?;
        *parameters = updated;
        for (_, event) in notifications {
            let _ = self.policy_events.send(event);
        }
        Ok(())
    }
}
