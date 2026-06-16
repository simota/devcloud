# Redis Compatibility Design

## 1. Position

`devcloud` provides a local Redis surface by managing a real `redis-server` child process (or proxying to a user-supplied external Redis), then exposing a `/dashboard/redis` management UI. The daemon does **not** re-implement RESP. Protocol parity is delegated to upstream `redis-server`; devcloud owns lifecycle, configuration, and management UX. This is the same `managed`/`external` split already established for Redshift in `orchestrator/managed_postgres.rs`.

## 2. Modes

| Mode | Default? | Behavior |
|------|----------|----------|
| `managed` | yes | `devcloud up` spawns `redis-server --port {p} --dir {dir} --save 60 1 --appendonly no [--requirepass ...]` as a child process, propagates SIGTERM on shutdown with a SIGKILL fallback, and writes RDB snapshots under `${storage.path}/redis`. |
| `external` | | `devcloud` validates connectivity to `services.redis.externalUrl` (a `redis://` URL) via `PING` and exposes the dashboard against it. No child process is spawned. |

If `managed` is requested and `redis-server` is missing from `$PATH`, the daemon emits a clear install hint (`brew install redis` / `apt install redis-server`) and refuses to start the service; other services keep running.

## 3. Configuration

```yaml
server:
  redisPort: 16379

auth:
  redis:
    mode: relaxed   # relaxed | strict
    password: ""    # strict mode only

services:
  redis:
    enabled: true
    mode: managed   # managed | external
    binaryPath: ""  # managed; default = $PATH lookup
    externalUrl: "" # external; e.g. redis://localhost:6379/0
    dataDir: redis  # managed; relative to storage.path
    maxMemoryMB: 256  # managed; mapped to --maxmemory
    appendOnly: false # managed; mapped to --appendonly
```

`AuthMode`:
- `relaxed`: do not pass `--requirepass`; clients may send `AUTH` with any value.
- `strict`: pass `--requirepass <password>`; clients must authenticate with the configured password.

## 4. Package Layout

```
services/redis/
  server.rs        Server, NewServer(Config), Run(ctx)
                   - managed: lifecycle + Redis client health probe
                   - external: connection validation
  config.rs        Config, AuthMode, defaults
  client.rs        in-process *redis.Client wrapper (Redis client v9)
  snapshot.rs      Status / KeysSnapshot / KeyDetailSnapshot for dashboard
  command_allowlist.rs    allowed commands + classification (read vs destructive)
  *_test.rs        per-file tests (config validation, allowlist, snapshot JSON shape)

orchestrator/
  managed_redis.rs redis-server child process lifecycle (SIGTERM -> wait -> SIGKILL)
```

## 5. Daemon Wiring

Mirror the Redshift integration shape in `orchestrator/daemon.rs`:

```go
var redisLifecycle managedRedisLifecycle
if d.config.Services.Redis.Enabled {
    redisLifecycle, err = startRedisBackend(ctx, d.config)
    if err != nil { return fmt.Errorf("start redis: %w", err) }
}
if redisLifecycle != nil { defer redisLifecycle.Close() }

redisServer := redissvc.NewServer(redissvc.Config{
    Mode:     d.config.Services.Redis.Mode,
    Addr:     redisAddrFromLifecycle(redisLifecycle, d.config),
    AuthMode: d.config.Auth.Redis.Mode,
    Password: d.config.Auth.Redis.Password,
    DataDir:  redisDataDir(d.config),
})

if d.config.Services.Redis.Enabled {
    dashboardServer.SetRedis(redisServer)
    go func() { errCh <- redisServer.Run(ctx) }()
}
```

`enabledServerCount`, the `up` banner, and the YAML template in `writeDefaultConfig` must include Redis.

## 6. Dashboard

### Backend (`services/dashboard/redis_handlers.rs`)

| Method | Path | Behavior |
|--------|------|----------|
| GET | `/api/redis/status` | mode, address, server version, `connected_clients`, `used_memory_human`, `db0` key count |
| GET | `/api/redis/keys` | SCAN-based, params `match=*`, `count=100`, `cursor=0`; response includes `nextCursor` and `keys: KeySnapshot[]` |
| GET | `/api/redis/keys/{key}` | `type`, `ttlSeconds`, type-specific value preview |
| POST | `/api/redis/command` | body `{"command":"GET","args":["foo"]}` after allowlist check |
| DELETE | `/api/redis/keys/{key}` | `DEL` |
| POST | `/api/redis/keys/{key}/expire` | `EXPIRE` |
| DELETE | `/api/redis/keys?confirm=FLUSHDB` | `FLUSHDB` only (never FLUSHALL) |

All mutations go through `*redissvc.Server.Exec(...)`; raw storage is never touched from dashboard handlers (existing safety rule).

### Allowlist

- Read: `GET MGET HGET HMGET HGETALL HKEYS HVALS HLEN LRANGE LINDEX LLEN SMEMBERS SISMEMBER SCARD ZRANGE ZRANGEBYSCORE ZSCORE ZCARD TYPE TTL PTTL EXISTS KEYS SCAN DBSIZE INFO COMMAND`
- Destructive: `SET DEL EXPIRE PERSIST RENAME LPUSH RPUSH HSET SADD ZADD`
- Rejected (`403`): `CONFIG SET, FLUSHALL, SCRIPT, EVAL, MIGRATE, DEBUG, SHUTDOWN, CLUSTER, REPLICAOF, MODULE`

### Frontend (`web/dashboard/src/app/services/redis/`)

Three-pane layout mirroring `RedshiftDashboard.tsx`:
- **Keys** (left): SCAN paging, match filter, per-key type badge
- **Inspector** (center): TTL badge, type-specific value view, DEL/EXPIRE
- **Command runner** (right): textarea + allowlist hint + structured result

### JSON Contract

Every array field on every response must be initialized to `[]T{}` server-side. `omitempty` is forbidden on required TS array types (recent regression evidence: `null` cascading into `.length`/`[0]` crashes).

## 7. Dependencies

| Dependency | Kind | Note |
|------------|------|------|
| `Redis client crate` | Rust workspace (new) | Dashboard in-process client only |
| `redis-server` binary | external runtime | Required for `managed` mode |

`Redis client` is the de-facto first-party client; pinning to a `v9.x` minor stream is fine.

## 8. Tests

| Layer | Location | Coverage |
|-------|----------|----------|
| Unit | `services/redis/server_test.rs` | Config validation, allowlist enforcement, status snapshot shape |
| Managed lifecycle | `orchestrator/managed_redis_test.rs` | binary-missing error, graceful shutdown, port collision |
| Dashboard | `services/dashboard/redis_test.rs` | all endpoints, empty-keyspace `[]` shape, disallowed command rejection |
| Integration | `services/redis/integration_test.rs` | gated on `redis-server`-in-PATH, real TCP via Redis client |
| E2E | `scripts/redis-e2e.sh` | `devcloud up` -> SET/GET/EXPIRE + dashboard JSON |
| Acceptance gate | `scripts/redis-autoloop/verify.sh` | stages: `foundation` / `config` / `redis-core` / `dashboard-static` / `hardening` / `e2e` / `full` |

## 9. Out of Scope

Cluster, Sentinel, replication, Modules, RESP self-impl, TLS cert management, ACL beyond `requirepass`, AOF tuning, Pub/Sub UI, MULTI/EXEC visualization, Streams browser.

## 10. Risks

| Risk | Mitigation |
|------|-----------|
| `redis-server` missing | install hint, fall back to `external` or disabled |
| Port `16379` collision | explicit error with port from config; user reroutes via `server.redisPort` |
| Zombie child on crash | SIGTERM with timeout then SIGKILL (same as `managed_postgres`) |
| Large keyspace UI lag | dashboard uses `SCAN` only, `KEYS *` removed from allowlist |
| Credential leak | redact password in handler logs; never echo args in command runner logs |
| Redis client breaking change | pin to `v9.x` minor; bump deliberately via Shift |

## 11. References

- `orchestrator/managed_postgres.rs` — managed child process pattern
- `services/redshift/server.rs` — managed/external split
- `services/dashboard/redshift_handlers.rs` — handler / in-process service wiring
- `web/dashboard/src/app/services/redshift/RedshiftDashboard.tsx` — closest 3-pane UI reference
