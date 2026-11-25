use std::io::{self, Read};
use std::path::Path;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecretRef {
    project: Option<String>,
    secret: String,
    version: String,
}

fn project_segment(project: &str) -> bool {
    project
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && project
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

fn secret_segment(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
}

fn version_number(value: &str) -> Option<u64> {
    if value.starts_with('0') || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

impl SecretRef {
    pub(crate) fn parse(source: &Value) -> Option<Self> {
        let selector = source.get("secretKeyRef")?;
        let resource = selector.get("secret")?.as_str()?;
        let segments: Vec<_> = resource.split('/').collect();
        let (project, secret) = match segments.as_slice() {
            [secret] => (None, *secret),
            ["projects", project, "secrets", secret] if project_segment(project) => {
                (Some(project.to_string()), *secret)
            }
            _ => return None,
        };
        let version = match selector.get("version") {
            Some(version) => version.as_str()?,
            None => "latest",
        };
        let alias = version.len() <= 63
            && version
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
            && secret_segment(version)
            && !version.eq_ignore_ascii_case("new")
            && !version.eq_ignore_ascii_case("latest");
        if !secret_segment(secret)
            || secret.len() > 255
            || !(version == "latest" || version_number(version).is_some() || alias)
        {
            return None;
        }
        Some(Self {
            project,
            secret: secret.into(),
            version: version.into(),
        })
    }

    pub(crate) fn read(&self, storage: &Path, service_project: &str) -> io::Result<String> {
        let project = self.project.as_deref().unwrap_or(service_project);
        if storage.as_os_str().is_empty() || !project_segment(project) {
            return Err(io::ErrorKind::NotFound.into());
        }
        let directory = storage.join("secrets").join(project).join(&self.secret);
        let path = if self.version == "latest" {
            let mut latest = None;
            for entry in directory.read_dir()? {
                let entry = entry?;
                if let Some(version) = entry.file_name().to_str().and_then(version_number) {
                    latest = latest.max(Some(version));
                }
            }
            directory.join(latest.ok_or(io::ErrorKind::NotFound)?.to_string())
        } else {
            directory.join(&self.version)
        };
        // Secret Manager limits each payload to 64 KiB. Unix environments
        // cannot carry embedded NULs.
        const MAX_BYTES: u64 = 64 * 1024;
        if !path.metadata()?.is_file() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut value = String::new();
        std::fs::File::open(path)?
            .take(MAX_BYTES + 1)
            .read_to_string(&mut value)?;
        if value.len() as u64 > MAX_BYTES || value.contains('\0') {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(value)
    }
}
