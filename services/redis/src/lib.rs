use std::net::ToSocketAddrs;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub addr: String,
    pub data_dir: PathBuf,
    pub binary: String,
    pub max_memory_mb: u64,
    pub append_only: bool,
    pub auth_mode: String,
    pub password: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            addr: required_env("DEVCLOUD_REDIS_ADDR")?,
            data_dir: PathBuf::from(required_env("DEVCLOUD_REDIS_DATA_DIR")?),
            binary: std::env::var("DEVCLOUD_REDIS_BINARY").unwrap_or_default(),
            max_memory_mb: env_u64("DEVCLOUD_REDIS_MAX_MEMORY_MB").unwrap_or(256),
            append_only: env_bool("DEVCLOUD_REDIS_APPEND_ONLY").unwrap_or(false),
            auth_mode: std::env::var("DEVCLOUD_REDIS_AUTH_MODE")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase(),
            password: std::env::var("DEVCLOUD_REDIS_PASSWORD").unwrap_or_default(),
        }
        .normalized())
    }

    pub fn normalized(mut self) -> Self {
        if self.binary.trim().is_empty() {
            self.binary = "redis-server".to_string();
        }
        if self.max_memory_mb == 0 {
            self.max_memory_mb = 256;
        }
        if self.auth_mode.trim().is_empty() {
            self.auth_mode = "relaxed".to_string();
        }
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        let (host, port) = split_addr(&self.addr)?;
        if host.trim().is_empty() {
            return Err("redis host is required".to_string());
        }
        if port == 0 {
            return Err("redis port must be between 1 and 65535".to_string());
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err("redis data directory is required".to_string());
        }
        match self.auth_mode.as_str() {
            "relaxed" => Ok(()),
            "strict" => {
                if self.password.is_empty() {
                    Err("redis strict auth requires password".to_string())
                } else {
                    Ok(())
                }
            }
            other => Err(format!("unsupported redis auth mode: {other}")),
        }
    }

    pub fn args(&self) -> Result<Vec<String>, String> {
        self.validate()?;
        let (host, port) = split_addr(&self.addr)?;
        let mut args = vec![
            "--bind".to_string(),
            host,
            "--port".to_string(),
            port.to_string(),
            "--dir".to_string(),
            self.data_dir.to_string_lossy().to_string(),
            "--save".to_string(),
            "60 1".to_string(),
            "--appendonly".to_string(),
            redis_bool(self.append_only).to_string(),
            "--maxmemory".to_string(),
            format!("{}mb", self.max_memory_mb),
        ];
        if self.auth_mode == "strict" {
            args.push("--requirepass".to_string());
            args.push(self.password.clone());
        }
        Ok(args)
    }
}

pub fn split_addr(addr: &str) -> Result<(String, u16), String> {
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|e| format!("redis address must be host:port: {e}"))?;
    let Some(first) = addrs.next() else {
        return Err("redis address is required".to_string());
    };
    let host = addr
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']).to_string())
        .unwrap_or_default();
    Ok((host, first.port()))
}

fn redis_bool(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn required_env(key: &str) -> Result<String, String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| format!("{key} is required"))
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.trim().parse::<u64>().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config {
            addr: "127.0.0.1:16380".to_string(),
            data_dir: PathBuf::from("/tmp/devcloud-redis"),
            binary: String::new(),
            max_memory_mb: 128,
            append_only: true,
            auth_mode: "strict".to_string(),
            password: "secret".to_string(),
        }
        .normalized()
    }

    #[test]
    fn args_match_legacy_managed_redis_flags() {
        let args = cfg().args().unwrap().join(" ");
        for want in [
            "--bind 127.0.0.1",
            "--port 16380",
            "--dir /tmp/devcloud-redis",
            "--save 60 1",
            "--appendonly yes",
            "--maxmemory 128mb",
            "--requirepass secret",
        ] {
            assert!(args.contains(want), "missing {want}: {args}");
        }
    }

    #[test]
    fn relaxed_mode_omits_requirepass_and_uses_defaults() {
        let config = Config {
            auth_mode: String::new(),
            password: String::new(),
            max_memory_mb: 0,
            append_only: false,
            ..cfg()
        }
        .normalized();
        let args = config.args().unwrap().join(" ");
        assert!(!args.contains("--requirepass"));
        assert!(args.contains("--appendonly no"));
        assert!(args.contains("--maxmemory 256mb"));
    }

    #[test]
    fn validation_rejects_strict_mode_without_password() {
        let config = Config {
            auth_mode: "strict".to_string(),
            password: String::new(),
            ..cfg()
        };
        assert!(config.validate().unwrap_err().contains("password"));
    }
}
