# Redis Service Autoloop Goal

## Goal

Add a Redis-compatible service to `devcloud` by managing a real `redis-server` process (or a user-supplied external Redis), exposing it through `devcloud up`, and providing a `/dashboard/redis` management UI. Follow `docs/design-redis-compat.md`.

## Why

`devcloud` already runs Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift locally with dashboard visibility. Many backend services and tests depend on Redis for caching, queues, rate limiting, and session storage. The Redis surface should match real client libraries (go-redis, redis-cli, ioredis) and let developers inspect keys, TTLs, and recent commands from the dashboard without leaving `devcloud`. Re-implementing the RESP protocol from scratch is rejected: the protocol surface is wide and the local-test value is in management UX, not in a parallel server. Instead, the daemon owns lifecycle (managed `redis-server` child process) or proxies to an external Redis (`external` mode), and the dashboard speaks to it through `github.com/redis/go-redis/v9`.

## Acceptance Criteria

1. `devcloud up` starts a Redis endpoint on the configured local port, defaulting to `127.0.0.1:6379`, when `services.redis.enabled=true`.
2. In `managed` mode, `devcloud` spawns `redis-server` as a child process with data directory under `${storage.path}/redis`, terminates it gracefully on shutdown (SIGTERM then SIGKILL), and surfaces a clear install hint if the binary is missing.
3. In `external` mode, `devcloud` validates connectivity to `services.redis.externalUrl` via `PING` and exposes the same dashboard surface without owning the process.
4. `redis-cli -h 127.0.0.1 -p 6379 ping` returns `PONG`, and a Go client using `github.com/redis/go-redis/v9` can `SET`, `GET`, `EXPIRE`, `DEL`, and `HSET`/`HGETALL` against the configured port.
5. Auth modes: `relaxed` (default) accepts any password; `strict` requires the configured password and rejects others with the upstream Redis error. Mode is consumed by the daemon when configuring `redis-server` `--requirepass` (managed) or supplied to clients (external).
6. Existing dashboard service registry exposes `redis`, and `/dashboard/redis` loads with status, key browser (`SCAN`-based, pageable), and key inspector for `string`, `list`, `hash`, `set`, `zset`.
7. `/api/redis/status` returns mode, address, server version, `connected_clients`, `used_memory_human`, and `db0` key count without leaking the password.
8. `/api/redis/keys` returns a non-null JSON array even when the keyspace is empty; `/api/redis/keys/{key}` returns `type`, `ttlSeconds` (or `-1`/`-2`), and a type-appropriate value preview.
9. `/api/redis/command` executes only commands on the allowlist (read set + `SET`/`DEL`/`EXPIRE`/`PERSIST`/`RENAME`/`LPUSH`/`RPUSH`/`HSET`/`SADD`/`ZADD`); `CONFIG SET`, `FLUSHALL`, `SCRIPT`, `EVAL`, `DEBUG`, `SHUTDOWN`, `CLUSTER`, `REPLICAOF`, `MODULE` are rejected with `403`.
10. Destructive dashboard actions (`DELETE /api/redis/keys/{key}`, `DELETE /api/redis/keys` for FLUSHDB) require the existing type-to-confirm dialog and never accept FLUSHALL semantics.
11. Dashboard mutations go through the in-process Redis service (`*redissvc.Server.Exec(...)`), never directly via raw storage or `os/exec`, mirroring the existing dashboard safety rule.
12. No credentials, AUTH headers, command argument payloads, or value bodies are written to logs by either the service or dashboard handlers.
13. Existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, Redshift, and dashboard behavior remain compatible; `go test ./...` passes.
14. `VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh` passes on a host with `redis-server` installed; on hosts without `redis-server`, managed-lifecycle and e2e stages skip cleanly with an explicit `[SKIP]` marker rather than `[FAIL]`.

## Out of Scope for This Loop

- Cluster mode, Sentinel, replication, Modules, RedisJSON, RediSearch.
- Re-implementing RESP2/RESP3 in Go; the upstream `redis-server` provides protocol parity.
- Production TLS certificate management; the MVP accepts plaintext on loopback only.
- ACL beyond `requirepass`; user-level ACLs are not modeled in the dashboard.
- AOF persistence tuning; RDB snapshot defaults are sufficient.
- Pub/Sub fan-out visualization in the dashboard (Phase 2 candidate).
- `MULTI`/`EXEC`/`WATCH` transaction visualization in the dashboard.
- Streams (`XADD`/`XREAD`) browser UI.

## Implementation Guidance

- Preserve existing service behavior; do not refactor unrelated packages.
- Prefer small vertical slices: config + daemon wiring first, then managed lifecycle, then dashboard.
- Keep runtime data under `.devcloud/data/redis/`.
- Use `github.com/redis/go-redis/v9` only for the dashboard's in-process client; do not introduce it as a transitive dependency of other services.
- Match the existing Redshift `managed`/`external` mode shape in `internal/app/config.go`, `internal/app/daemon.go`, `internal/app/managed_postgres.go` patterns.
- For dashboard responses, initialize every array field with `[]T{}` (not nil) so `omitempty` does not produce `null` in JSON.
- All shell scripts in this loop must pass `bash -n` (syntax check) and emit a footer line.

## Footer Contract

```text
NEXUS_LOOP_STATUS: READY | CONTINUE | DONE
NEXUS_LOOP_SUMMARY: <single-line summary>
```

`DONE` is permitted only when `VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh` passes, the dashboard returns expected JSON shapes for an empty and a populated keyspace, and `go test ./...` is green.
