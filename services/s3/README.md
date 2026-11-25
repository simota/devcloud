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

Presigned SigV4 and SigV4a URLs also accept `STREAMING-UNSIGNED-PAYLOAD-TRAILER`
uploads: sign `x-amz-decoded-content-length` and `x-amz-trailer` into the URL and
use `STREAMING-UNSIGNED-PAYLOAD-TRAILER` as the canonical payload hash (it is
sent as `x-amz-content-sha256`). Signed-chunk streaming payloads on presigned
URLs return `NotImplemented`.

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

### Permissions boundaries and SCPs

The config can also declare customer managed policies and SCPs:

```json
{
  "Users": [{"UserName": "carol", "AccessKeyId": "AKIACAROL", "SecretAccessKey": "carol-secret",
             "Policies": [{"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}],
             "PermissionsBoundary": "arn:aws:iam::000000000000:policy/read-only"}],
  "ManagedPolicies": [{"PolicyName": "read-only",
                       "PolicyDocument": {"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}}],
  "ServiceControlPolicies": [{"PolicyName": "no-deletes", "TargetAccounts": ["111111111111"],
                              "PolicyDocument": {"Statement": [
                                {"Effect": "Allow", "Action": "*", "Resource": "*"},
                                {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*"}]}}]
}
```

A `PermissionsBoundary` (users and roles; it applies to the role's sessions)
must name a `ManagedPolicies` entry in the same account. In strict mode it must
also allow the action: it limits identity-policy grants and owner-account
bucket-policy grants, except a bucket-policy `Allow` that names the user ARN or
the session ARN itself. SCPs apply to every user and session in their
`TargetAccounts`: the union of those SCPs must allow the action and any SCP
`Deny` wins, including over bucket policies, ACLs, and `sts:AssumeRole`. As with
an AWS account that has only custom SCPs attached, an account with SCPs that do
not allow an action is denied it. Accounts without SCPs are not restricted; the
owner account `000000000000` is the organization's management account and cannot
be targeted.

### IAM and STS Query APIs

The S3 port also answers the AWS Query protocol at `POST /` (form-encoded
`Action=...`), so SDK IAM and STS clients can point their endpoint at it:

```sh
aws --endpoint-url http://127.0.0.1:9000 sts get-caller-identity
aws --endpoint-url http://127.0.0.1:9000 sts assume-role \
  --role-arn arn:aws:iam::000000000000:role/reader --role-session-name job
aws --endpoint-url http://127.0.0.1:9000 iam list-users
aws --endpoint-url http://127.0.0.1:9000 iam put-user-permissions-boundary \
  --user-name carol --permissions-boundary arn:aws:iam::000000000000:policy/read-only
```

Supported actions: STS `GetCallerIdentity` and `AssumeRole`; IAM `GetUser`,
`ListUsers`, `GetRole`, `ListRoles`, `GetPolicy`, `PutUserPermissionsBoundary`,
`DeleteUserPermissionsBoundary`, `PutRolePermissionsBoundary`, and
`DeleteRolePermissionsBoundary`. In strict mode requests must be signed for the
`sts` or `iam` service (IAM also accepts `us-east-1`); the owner key may call
everything and is reported as `arn:aws:iam::000000000000:root`; registry
principals need the IAM action within their session policy, boundary, and SCPs,
and see only their own account. `GetCallerIdentity` needs no permission, and
`AssumeRole` follows the role's trust policy (refusing unknown roles with
`AccessDenied`, as STS does). Lists are returned in one page. Other IAM actions
are `InvalidAction`; manage users, roles, and policies with `PUT /_iam/config`.

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

### KMS API

The same keyring answers a local subset of the AWS KMS JSON API at `POST /`
(`X-Amz-Target: TrentService.<Operation>`):

```sh
aws --endpoint-url http://127.0.0.1:9000 kms create-key --description app
aws --endpoint-url http://127.0.0.1:9000 kms encrypt --key-id alias/app \
  --plaintext fileb://secret.txt --encryption-context tenant=a
aws --endpoint-url http://127.0.0.1:9000 kms generate-data-key --key-id alias/app --key-spec AES_256
```

Operations: `CreateKey`, `DescribeKey`, `ListKeys`, `EnableKey`, `DisableKey`,
`ScheduleKeyDeletion`, `CancelKeyDeletion`, `CreateAlias`, `DeleteAlias`,
`ListAliases`, `Encrypt`, `Decrypt`, `ReEncrypt`, `GenerateDataKey`, and
`GenerateDataKeyWithoutPlaintext`, for symmetric (`SYMMETRIC_DEFAULT`) keys.
Keys accept key IDs, key ARNs, alias names, and alias ARNs. `Decrypt` needs the
same encryption context that `Encrypt` used (`InvalidCiphertextException`
otherwise), and refuses a `KeyId` that did not encrypt the blob
(`IncorrectKeyException`). Disabled and pending-deletion keys refuse
cryptography. Each key's 256-bit key material is created on first use and kept
in `.kms-material.json` in the storage root (never returned); it is derived from
the clock and a counter, so use it for local tests only, and ciphertext blobs
are only valid for this emulator. In strict mode requests must be signed for the
`kms` service; registry principals in the owner account need `kms:<Operation>`
on the key ARN (`*` for `CreateKey`, `ListKeys`, `ListAliases`), other accounts
are refused (there are no key policies), and the AWS managed `alias/aws/s3` key
cannot be used directly. Asymmetric and HMAC keys, grants, key policies,
rotation, imported key material, and multi-Region keys are not emulated.

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
