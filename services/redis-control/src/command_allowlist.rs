//! Port of `internal/services/redis/command_allowlist.rs`.
//!
//! The allowlist is authoritative and enforced server-side: only these commands
//! may run via the `/_control/exec` arbitrary-command surface, classified as
//! read or mutation. The exact set and classes must match the legacy map so the
//! cross-engine `class` field in the exec response is identical.

/// Command class, mirroring legacy `CommandClass` ("read" | "mutation").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandClass {
    Read,
    Mutation,
}

impl CommandClass {
    /// The wire string the legacy reference emits in the exec response `class`.
    pub fn as_str(self) -> &'static str {
        match self {
            CommandClass::Read => "read",
            CommandClass::Mutation => "mutation",
        }
    }
}

/// Returns the class of `command` if it is allowlisted, mirroring
/// `CommandAllowed` (uppercases + trims before lookup).
pub fn command_allowed(command: &str) -> Option<CommandClass> {
    let name = command.trim().to_ascii_uppercase();
    match name.as_str() {
        "GET" | "MGET" | "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "LRANGE"
        | "LINDEX" | "LLEN" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "ZRANGE" | "ZRANGEBYSCORE"
        | "ZSCORE" | "ZCARD" | "TYPE" | "TTL" | "PTTL" | "EXISTS" | "KEYS" | "SCAN" | "DBSIZE"
        | "INFO" | "COMMAND" => Some(CommandClass::Read),
        "SET" | "DEL" | "EXPIRE" | "PERSIST" | "RENAME" | "LPUSH" | "RPUSH" | "HSET" | "SADD"
        | "ZADD" => Some(CommandClass::Mutation),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_mutations_classified() {
        assert_eq!(command_allowed("get"), Some(CommandClass::Read));
        assert_eq!(command_allowed(" SET "), Some(CommandClass::Mutation));
        assert_eq!(command_allowed("scan"), Some(CommandClass::Read));
    }

    #[test]
    fn non_allowlisted_rejected() {
        for cmd in ["FLUSHALL", "CONFIG", "SHUTDOWN", "EVAL", "DEBUG"] {
            assert_eq!(command_allowed(cmd), None);
        }
    }
}
