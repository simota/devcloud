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

Strict mode checks header-signed requests, presigned URLs (GET and PUT, including
expiry), the payload hash, and `aws-chunked` uploads
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, whose chunks and trailer are each
verified, and `STREAMING-UNSIGNED-PAYLOAD-TRAILER`). It also requires:

- `x-amz-date` within 15 minutes of the server clock (`RequestTimeTooSkewed`)
- `host` and every `x-amz-*` header to be signed, except `x-amz-user-agent` and
  `x-amz-content-sha256` (whose value is signed as the payload hash)
- the credential scope to match the configured access key, region, and request date

Trailer checksums such as `x-amz-checksum-crc32` are accepted but not validated.
SigV2 and SigV4a are not implemented.

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
