# devcloud-s3

Standalone S3-compatible endpoint with a web console. The image runs without the
orchestrator, dashboard, or event relay, and needs no runtime JavaScript tools.

```sh
docker build -f services/s3/Dockerfile -t devcloud-s3 .
docker run --rm \
  -p 127.0.0.1:9000:9000 -p 127.0.0.1:9001:9001 \
  -v devcloud-s3-data:/data devcloud-s3
# S3 API: http://127.0.0.1:9000  Web console: http://127.0.0.1:9001
```

The image runs as uid **10001**. For a bind mount, make the directory writable by
that uid first. Requests are path-style only and capped at 128 MiB.

| Environment variable | Image default | Meaning |
| --- | --- | --- |
| `DEVCLOUD_S3_ADDR` | `0.0.0.0:9000` | S3 API listen address |
| `DEVCLOUD_S3_STORAGE` | `/data` | Bucket storage root (required) |
| `DEVCLOUD_S3_AUTH_MODE` | `relaxed` | `relaxed` or `strict`; any other value refuses to start |
| `DEVCLOUD_S3_ACCESS_KEY_ID` | empty | Access key, required in strict mode |
| `DEVCLOUD_S3_SECRET_ACCESS_KEY` | empty | Secret key, required in strict mode |
| `DEVCLOUD_S3_REGION` | `us-east-1` | Region accepted in the SigV4 credential scope |
| `DEVCLOUD_S3_BUCKETS` | empty | Comma-separated buckets to create at startup; existing buckets are kept |
| `DEVCLOUD_S3_CORS_ALLOW_ORIGINS` | empty | Comma-separated origins (`*` and one-wildcard patterns allowed) for buckets without their own CORS configuration |
| `DEVCLOUD_S3_CONSOLE_ADDR` | `0.0.0.0:9001` | Console listen address; empty disables the console |
| `DEVCLOUD_S3_CONSOLE_ALLOWED_HOSTS` | empty | Extra comma-separated console Host names; `*` disables the check |

## Signature verification

Relaxed mode accepts every request. To verify SigV4:

```sh
docker run --rm -p 127.0.0.1:9000:9000 -p 127.0.0.1:9001:9001 \
  -e DEVCLOUD_S3_AUTH_MODE=strict \
  -e DEVCLOUD_S3_ACCESS_KEY_ID=devkey \
  -e DEVCLOUD_S3_SECRET_ACCESS_KEY=devsecret \
  devcloud-s3
```

Strict mode checks header-signed and presigned requests for SigV4
(`AWS4-HMAC-SHA256`), SigV4a (`AWS4-ECDSA-P256-SHA256` with `x-amz-region-set` /
`X-Amz-Region-Set`), and SigV2 (`Authorization: AWS ...` and query-string
`AWSAccessKeyId`/`Expires`/`Signature`), the payload hash, and SigV4
`aws-chunked` uploads (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, whose
chunks and trailer are each verified, and `STREAMING-UNSIGNED-PAYLOAD-TRAILER`),
and SigV4a `aws-chunked` uploads (`STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`,
whose ECDSA chunk and trailer signatures are each verified; `*`-padded
signatures are accepted). Streaming payloads on presigned URLs return
`NotImplemented`.
It also requires:

- `x-amz-date` (or `Date` for SigV2) within 15 minutes of the server clock
  (`RequestTimeTooSkewed`)
- `host` and every `x-amz-*` header to be signed (for SigV4/SigV4a), except
  `x-amz-user-agent` and `x-amz-content-sha256` (whose value is signed as the
  payload hash)
- the credential scope (and SigV4a region set) to match the configured access
  key, region, and request date

Strict mode also evaluates bucket policies (`Allow`/`Deny`, `Principal`,
`Action`, `Resource`, common `Condition` operators) and canned bucket/object
ACLs (`public-read`, `public-read-write`), allowing anonymous public access when
granted and enforcing explicit `Deny` statements even on authenticated requests.

In every auth mode, `aws-chunked` trailer checksums (`x-amz-checksum-crc32`,
`-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`) are checked against the decoded
payload: a mismatch is `BadDigest`, and a malformed value or a checksum named in
`x-amz-trailer` but missing from the body is `InvalidRequest`.

## Local IAM users, roles, and sessions

The access key above is the owner of every bucket (account `000000000000`).
Other identities come from a local registry, replaced as a whole:

```sh
curl -X PUT http://127.0.0.1:9000/_iam/config -d '{
  "Users": [{"UserName": "bob", "Account": "111111111111",
             "AccessKeyId": "AKIABOB", "SecretAccessKey": "bob-secret",
             "Policies": [{"Statement": [{"Effect": "Allow",
               "Action": ["s3:GetObject", "sts:AssumeRole"], "Resource": "*"}]}]}],
  "Roles": [{"RoleName": "reader", "MaxSessionDuration": 3600,
             "AssumeRolePolicyDocument": {"Statement": [{"Effect": "Allow",
               "Principal": {"AWS": "arn:aws:iam::111111111111:root"},
               "Action": "sts:AssumeRole"}]},
             "Policies": [{"Statement": [{"Effect": "Allow",
               "Action": "s3:GetObject", "Resource": "arn:aws:s3:::demo/*"}]}]}]
}'
```

`POST /_iam/assume-role` with `{"RoleArn", "RoleSessionName", "Policy",
"DurationSeconds"}` returns STS-shaped `Credentials`. In strict mode the
`/_iam/` and `/_kms/` endpoints need the owner key (any registry principal may
call `assume-role`), the role's trust policy must allow the caller, and a caller
from another account also needs `sts:AssumeRole` in its own policies. Requests
signed with a registry key are verified with that key's secret; session keys
also need their `x-amz-security-token` and stop working at expiry. Evaluation
follows AWS: an explicit `Deny` in the bucket, identity, or session policy wins;
an owner-account principal needs an `Allow` from its identity policies or the
bucket policy; another account needs both; a session policy limits both. Relaxed
mode treats every request as the owner and skips trust policies.
`GET /_iam/config` never returns secrets. The registry is stored as `.iam.json`
and `.iam-sessions.json` in the storage root.

## Local KMS keys for SSE-KMS

```sh
curl -X POST http://127.0.0.1:9000/_kms/keys -d '{"Description": "app"}'
curl -X POST http://127.0.0.1:9000/_kms/aliases -d '{"AliasName": "alias/app", "TargetKeyId": "<KeyId>"}'
curl -X POST http://127.0.0.1:9000/_kms/keys/<KeyId>/disable   # or enable, schedule-deletion, cancel-deletion
curl http://127.0.0.1:9000/_kms/keys
```

In strict mode, SSE-KMS writes resolve the key ID, key ARN, or alias (none
means an AWS managed `alias/aws/s3` key created on first use) and store the key
ARN. Missing, disabled, and pending-deletion keys refuse writes, and GET, Select,
and copy reads of objects under a disabled or pending-deletion key; HEAD still
works. Relaxed mode does not consult the keyring: any key ID is stored and
returned as given, and reads are never refused.
Objects are not encrypted with the KMS key at rest, because the store is shared
with other devcloud services that read it directly. The keyring is `.kms.json`
in the storage root.

## CORS

Without configuration no cross-origin access is granted. Configure a bucket with
the standard API:

```sh
aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-cors --bucket demo \
  --cors-configuration '{"CORSRules":[{"AllowedOrigins":["http://localhost:5173"],
    "AllowedMethods":["GET","PUT"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"]}]}'
```

Or set a default for every bucket without its own rules, which also covers
`ListBuckets` and creating buckets:

```sh
docker run ... -e DEVCLOUD_S3_CORS_ALLOW_ORIGINS=http://localhost:5173 devcloud-s3
```

Preflight `OPTIONS` needs no signature, since browsers never sign it; the actual
request is still verified in strict mode. Avoid `*` with relaxed auth on a machine
that browses the web: any site could then read and change your local buckets.

## Web console

The console lists, creates, and deletes buckets, and browses, uploads, downloads,
and deletes objects. Mutations go through the S3 provider protocol, so they behave
like API calls.

- **Relaxed mode:** no login. Publish port 9001 on `127.0.0.1` as shown above.
- **Strict mode:** HTTP Basic, with the access key as the user name and the secret
  key as the password. Basic credentials travel in clear text, so use TLS in front
  of it outside a local machine.
- **Other hostnames:** names containing a dot are refused to prevent DNS rebinding.
  To reach the console as `s3.dev.example.com`, set
  `DEVCLOUD_S3_CONSOLE_ALLOWED_HOSTS=s3.dev.example.com`.

The UI source is `web/s3-console`. `npm run build` there writes
`services/s3/assets/console`, which is committed and embedded into the binary.

## End-to-end tests

`npm run test:e2e` in `web/s3-console` builds this image and tests it through real
sockets, a real browser, and the AWS SDK for JavaScript. It needs Docker and
Playwright's Chromium (`npx playwright install chromium`), and writes under
`web/s3-console/dist/e2e`.
