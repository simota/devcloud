// The business flows, shared by the per-use-case tests and the demo recording.
import { execFile as execFileCb } from 'node:child_process'
import * as fs from 'node:fs/promises'
import * as os from 'node:os'
import * as path from 'node:path'
import { Readable } from 'node:stream'
import { promisify } from 'node:util'
import {
  CreateBucketCommand,
  DeleteBucketCorsCommand,
  GetBucketCorsCommand,
  GetObjectCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  PutBucketCorsCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { expect, type Page } from '@playwright/test'
import {
  ACCESS_KEY,
  Container,
  OUT,
  REPO_ROOT,
  SECRET_KEY,
  inspectExposedPorts,
  pageFetch,
  port,
  raw,
  rawSocket,
  revision,
  sdk,
  serveWebOrigin,
  signedChunkUpload,
  sleep,
  strictEnv,
  waitForHttp,
  type Recorder,
} from './harness'

const execFile = promisify(execFileCb)

const errorName = async (call: Promise<unknown>): Promise<string | null> => {
  try {
    await call
    return null
  } catch (error) {
    return (error as Error).name
  }
}

const readBody = async (client: S3Client, bucket: string, key: string): Promise<string | null> => {
  try {
    const object = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
    return (await object.Body?.transformToString()) ?? null
  } catch {
    return null
  }
}

const corsFor = (origin: string) => ({
  CORSRules: [{ AllowedOrigins: [origin], AllowedMethods: ['GET', 'PUT'], AllowedHeaders: ['*'], ExposeHeaders: ['ETag'], MaxAgeSeconds: 60 }],
})

const apiStep = (recorder: Recorder, checkpoint: string, title: string, label: string, system = 's3') =>
  recorder.step({ system, screen: checkpoint.toLowerCase(), title, checkpoint, action: { kind: 'api', label } })

// ---------------------------------------------------------------- UC-001

export async function browserUsesCors(recorder: Recorder, page: Page): Promise<void> {
  const rev = await revision()
  recorder.systems = { webapp: rev, s3: rev }
  const web = await serveWebOrigin(port('web'))
  const s3Plain = port('s3plain')
  const s3Default = port('s3default')
  const s3Strict = port('s3strict')
  const containers = await Promise.all([
    Container.run('plain', {}, { s3: s3Plain }),
    Container.run('default', { DEVCLOUD_S3_CORS_ALLOW_ORIGINS: web.url }, { s3: s3Default }),
    Container.run('strict', strictEnv, { s3: s3Strict }),
  ])
  try {
    await Promise.all([s3Plain, s3Default, s3Strict].map(waitForHttp))
    await page.goto(web.url)
    const plain = sdk(s3Plain)
    const configured = recorder.id('configured')
    const unconfigured = recorder.id('unconfigured')
    await plain.send(new CreateBucketCommand({ Bucket: unconfigured }))

    // CP-001: the configuration API
    const scratch = recorder.id('scratch')
    await plain.send(new CreateBucketCommand({ Bucket: scratch }))
    const rejected = await raw(s3Plain, 'PUT', `/${scratch}?cors`, {}, '<CORSConfiguration></CORSConfiguration>')
    await errorName(plain.send(new PutBucketCorsCommand({ Bucket: scratch, CORSConfiguration: corsFor(web.url) })))
    const saved = await plain.send(new GetBucketCorsCommand({ Bucket: scratch })).catch(() => null)
    await errorName(plain.send(new DeleteBucketCorsCommand({ Bucket: scratch })))
    const afterDelete = await errorName(plain.send(new GetBucketCorsCommand({ Bucket: scratch })))
    await recorder.step({ system: 'webapp', screen: 'app', title: 'Web app on another origin', action: { kind: 'api', label: 'PutBucketCors' } }, true)
    await apiStep(recorder, 'CP-001', 'Bucket CORS configuration', 'GET ?cors')
    await recorder.api('CP-001', {
      saved: saved?.CORSRules?.[0]?.AllowedOrigins?.[0] === web.url,
      invalid_rejected: rejected.status === 400,
      deleted: afterDelete === 'NoSuchCORSConfiguration',
    }, { saved: true, invalid_rejected: true, deleted: true })

    await plain.send(new CreateBucketCommand({ Bucket: configured }))
    await errorName(plain.send(new PutBucketCorsCommand({ Bucket: configured, CORSConfiguration: corsFor(web.url) })))
    const objectUrl = `http://127.0.0.1:${s3Plain}/${configured}/note.txt`

    // CP-002: preflight and upload from the browser
    const preflight = (method: string, origin = web.url) =>
      raw(s3Plain, 'OPTIONS', `/${configured}/note.txt`, { origin, 'access-control-request-method': method })
    const allowed = await preflight('PUT')
    const denied = await preflight('DELETE')
    const put = await pageFetch(page, objectUrl, { method: 'PUT', body: 'from the browser', headers: { 'x-amz-meta-source': 'browser' } })
    await recorder.show(`Upload from ${web.url}: ${put.ok ? `HTTP ${put.status}` : 'blocked by the browser'}`)
    await recorder.step({ system: 'webapp', screen: 'app', title: 'Upload from the browser', checkpoint: 'CP-002', action: { kind: 'api', label: 'PUT object' } }, true)
    await recorder.ui('CP-002', {
      preflight_status: allowed.status,
      denied_status: denied.status,
      browser_put_ok: put.ok && put.status === 200,
    }, { preflight_status: 200, denied_status: 403, browser_put_ok: true })

    // CP-003: reading responses from the browser
    const get = await pageFetch(page, objectUrl)
    const missing = await pageFetch(page, `http://127.0.0.1:${s3Plain}/${configured}/absent.txt`)
    const otherOrigin = await raw(s3Plain, 'GET', `/${configured}/note.txt`, { origin: 'https://other.example' })
    await recorder.show(`Read: ${get.ok ? `"${get.body}" ETag ${get.etag}` : 'blocked by the browser'}`)
    await recorder.show(`Missing object: ${missing.ok ? `HTTP ${missing.status}` : 'blocked by the browser'}`)
    await recorder.step({ system: 'webapp', screen: 'app', title: 'Read the object and an error', checkpoint: 'CP-003', action: { kind: 'api', label: 'GET object' } }, true)
    await recorder.ui('CP-003', {
      body_read: get.body === 'from the browser',
      etag_visible: Boolean(get.etag),
      error_status: missing.status,
      other_origin_header: otherOrigin.headers['access-control-allow-origin'] ?? null,
    }, { body_read: true, etag_visible: true, error_status: 404, other_origin_header: null })

    // CP-004: no configuration, the server default, and a bucket's own rules
    const blocked = await pageFetch(page, `http://127.0.0.1:${s3Plain}/${unconfigured}`)
    const viaDefault = sdk(s3Default)
    const defaulted = recorder.id('defaulted')
    const ownRule = recorder.id('own-rule')
    await viaDefault.send(new CreateBucketCommand({ Bucket: defaulted }))
    await viaDefault.send(new CreateBucketCommand({ Bucket: ownRule }))
    await errorName(viaDefault.send(new PutBucketCorsCommand({ Bucket: ownRule, CORSConfiguration: corsFor('https://other.example') })))
    const defaultAllowed = await pageFetch(page, `http://127.0.0.1:${s3Default}/${defaulted}`)
    const ownRuleResult = await pageFetch(page, `http://127.0.0.1:${s3Default}/${ownRule}`)
    await recorder.show(`No configuration: ${blocked.ok ? 'allowed' : 'blocked by the browser'}`)
    await recorder.show(`Server default: ${defaultAllowed.ok ? 'allowed' : 'blocked by the browser'}`)
    await recorder.show(`Bucket with its own rule: ${ownRuleResult.ok ? 'allowed' : 'blocked by the browser'}`)
    await recorder.step({ system: 'webapp', screen: 'app', title: 'Default and bucket rules', checkpoint: 'CP-004', action: { kind: 'api', label: 'strict server' } }, true)
    await recorder.ui('CP-004', {
      unconfigured_blocked: !blocked.ok,
      default_allowed: defaultAllowed.ok,
      own_rule_wins: !ownRuleResult.ok,
    }, { unconfigured_blocked: true, default_allowed: true, own_rule_wins: true })

    // CP-005: strict mode answers the preflight but still refuses the unsigned request
    const signed = sdk(s3Strict)
    const strictBucket = recorder.id('strict')
    await signed.send(new CreateBucketCommand({ Bucket: strictBucket }))
    await errorName(signed.send(new PutBucketCorsCommand({ Bucket: strictBucket, CORSConfiguration: corsFor(web.url) })))
    const strictPreflight = await raw(s3Strict, 'OPTIONS', `/${strictBucket}/x`, { origin: web.url, 'access-control-request-method': 'PUT' })
    const unsigned = await pageFetch(page, `http://127.0.0.1:${s3Strict}/${strictBucket}/x`, { method: 'PUT', body: 'unsigned' })
    await recorder.show(`Unsigned upload to the strict server: ${unsigned.ok ? `HTTP ${unsigned.status}` : 'blocked by the browser'}`)
    await recorder.step({ system: 'webapp', screen: 'app', title: 'Unsigned request to a strict server', checkpoint: 'CP-005' }, true)
    await recorder.ui('CP-005', {
      preflight_status: strictPreflight.status,
      unsigned_status: unsigned.status,
    }, { preflight_status: 200, unsigned_status: 403 })
  } finally {
    await Promise.all(containers.map((container) => container.remove()))
    await web.close()
  }
}

// ---------------------------------------------------------------- UC-002

const RESERVED_KEYS = ['a b.txt', '日本語/ファイル.txt', 'p+q=r&s.txt', "x(1)!'*.txt"]

export async function sdkSignsOverSocket(recorder: Recorder): Promise<void> {
  recorder.systems = { s3: await revision() }
  const strictPort = port('s3strict')
  const relaxedPort = port('s3plain')
  const containers = await Promise.all([
    Container.run('strict', strictEnv, { s3: strictPort }),
    Container.run('plain', {}, { s3: relaxedPort }),
  ])
  try {
    await Promise.all([strictPort, relaxedPort].map(waitForHttp))
    const strict = sdk(strictPort)
    const relaxed = sdk(relaxedPort)
    const bucket = recorder.id('sdk')
    await strict.send(new CreateBucketCommand({ Bucket: bucket }))
    await relaxed.send(new CreateBucketCommand({ Bucket: bucket }))

    // CP-001: keys that need percent-encoding
    let reservedOk = 0
    for (const key of RESERVED_KEYS) {
      const failed = await errorName(strict.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: 'hello' })))
      if (!failed && (await readBody(strict, bucket, key)) === 'hello') reservedOk += 1
    }
    await apiStep(recorder, 'CP-001', 'Keys with reserved characters', 'PutObject, GetObject')
    await recorder.api('CP-001', { keys: RESERVED_KEYS.length, keys_ok: reservedOk }, { keys: 4, keys_ok: 4 })

    // CP-002: signatures that must not verify
    await strict.send(new PutObjectCommand({ Bucket: bucket, Key: 'plain.txt', Body: 'plain' }))
    const wrongSecret = await errorName(sdk(strictPort, { secret: 'not-the-secret' }).send(new ListBucketsCommand({})))
    const presigned = new URL(await getSignedUrl(strict, new GetObjectCommand({ Bucket: bucket, Key: 'plain.txt' }), { expiresIn: 60 }))
    const retargeted = await raw(strictPort, 'GET', `/${bucket}/other.txt${presigned.search}`)
    await apiStep(recorder, 'CP-002', 'Wrong secret and retargeted URL', 'ListBuckets')
    await recorder.api('CP-002', {
      wrong_secret: wrongSecret,
      retargeted_status: retargeted.status,
      retargeted_code: /<Code>(\w+)<\/Code>/.exec(retargeted.body.toString())?.[1] ?? null,
    }, { wrong_secret: 'SignatureDoesNotMatch', retargeted_status: 403, retargeted_code: 'SignatureDoesNotMatch' })

    // CP-003: a client clock one hour behind
    const skewed = await errorName(sdk(strictPort, { clockOffsetMs: -3_600_000 }).send(new ListBucketsCommand({})))
    await apiStep(recorder, 'CP-003', 'Client clock one hour behind', 'ListBuckets')
    await recorder.api('CP-003', { code: skewed }, { code: 'RequestTimeTooSkewed' })

    // CP-004: signed chunks
    const chunks = [Buffer.from('hello '), Buffer.from('world')]
    const signedUpload = await signedChunkUpload(strictPort, `/${bucket}/chunks.txt`, chunks)
    const signedBody = await readBody(strict, bucket, 'chunks.txt')
    const tampered = await signedChunkUpload(strictPort, `/${bucket}/tampered.txt`, chunks, true)
    await apiStep(recorder, 'CP-004', 'Signed chunk upload', 'PUT aws-chunked')
    await recorder.api('CP-004', {
      valid_status: signedUpload.status,
      same_body: signedBody === 'hello world',
      tampered_code: /<Code>(\w+)<\/Code>/.exec(tampered.body.toString())?.[1] ?? null,
    }, { valid_status: 200, same_body: true, tampered_code: 'SignatureDoesNotMatch' })

    // CP-005: the SDK's trailing-checksum stream
    const stream = () => Readable.from([Buffer.from('stream '), Buffer.from('body')])
    await errorName(relaxed.send(new PutObjectCommand({ Bucket: bucket, Key: 'stream.txt', Body: stream(), ContentLength: 11 })))
    await errorName(strict.send(new PutObjectCommand({ Bucket: bucket, Key: 'stream.txt', Body: stream(), ContentLength: 11 })))
    await apiStep(recorder, 'CP-005', 'SDK stream upload', 'PutObject stream')
    await recorder.api('CP-005', {
      relaxed_same: (await readBody(relaxed, bucket, 'stream.txt')) === 'stream body',
      strict_same: (await readBody(strict, bucket, 'stream.txt')) === 'stream body',
    }, { relaxed_same: true, strict_same: true })

    // CP-006: presigned URLs
    const url = async (expiresIn: number) => new URL(await getSignedUrl(strict, new GetObjectCommand({ Bucket: bucket, Key: 'plain.txt' }), { expiresIn }))
    const valid = await url(60)
    const shortLived = await url(1)
    const longer = new URL(valid)
    longer.searchParams.set('X-Amz-Expires', '600')
    await sleep(2_500)
    await apiStep(recorder, 'CP-006', 'Presigned URLs', 'GET presigned')
    await recorder.api('CP-006', {
      valid: (await raw(strictPort, 'GET', valid.pathname + valid.search)).status,
      expired: (await raw(strictPort, 'GET', shortLived.pathname + shortLived.search)).status,
      tampered: (await raw(strictPort, 'GET', longer.pathname + longer.search)).status,
    }, { valid: 200, expired: 403, tampered: 403 })

    // CP-007: HTTP framing
    const head = (key: string, headers: string) => `PUT /${bucket}/${key} HTTP/1.1\r\nHost: 127.0.0.1\r\n${headers}\r\n`
    await rawSocket(relaxedPort, `${head('te.txt', 'Transfer-Encoding: chunked\r\n')}5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n`)
    await relaxed.send(new PutObjectCommand({ Bucket: bucket, Key: 'keep.txt', Body: 'keep me' }))
    const conflict = await rawSocket(relaxedPort, `${head('keep.txt', 'Content-Length: 5\r\nContent-Length: 4\r\n')}hello`)
    await rawSocket(relaxedPort, `${head('same.txt', 'Content-Length: 5\r\nContent-Length: 5\r\n')}hello`)
    await apiStep(recorder, 'CP-007', 'Chunked transfer and repeated lengths', 'PUT over a raw socket')
    await recorder.api('CP-007', {
      chunked_same: (await readBody(relaxed, bucket, 'te.txt')) === 'hello world',
      conflict_refused: conflict === '',
      object_kept: (await readBody(relaxed, bucket, 'keep.txt')) === 'keep me',
      repeated_ok: (await readBody(relaxed, bucket, 'same.txt')) === 'hello',
    }, { chunked_same: true, conflict_refused: true, object_kept: true, repeated_ok: true })
  } finally {
    await Promise.all(containers.map((container) => container.remove()))
  }
}

// ---------------------------------------------------------------- UC-003

const basicAuth = (user: string, password: string) => `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`

const reachable = async (page: Page, url: string): Promise<boolean> => {
  const response = await page.goto(url).catch(() => null)
  return Boolean(response?.ok()) && (await page.title()) === 'devcloud S3 console'
}

async function createBucketInConsole(page: Page, name: string): Promise<void> {
  await page.getByLabel('New bucket').fill(name)
  await page.getByRole('button', { name: 'Create' }).click()
  await expect(page.getByRole('navigation', { name: 'Path' }).getByRole('button', { name })).toBeVisible()
}

export async function consoleManagesObjects(recorder: Recorder, page: Page): Promise<void> {
  const rev = await revision()
  recorder.systems = { console: rev, s3: rev }
  const consolePort = port('console')
  const s3Port = port('s3plain')
  const strictConsolePort = port('consolestrict')
  const web = await serveWebOrigin(port('web'))
  const containers = await Promise.all([
    Container.run('plain', {}, { s3: s3Port, console: consolePort }),
    Container.run('strict', strictEnv, { console: strictConsolePort }),
  ])
  try {
    await waitForHttp(s3Port)
    await sleep(500)
    const consoleUrl = `http://127.0.0.1:${consolePort}/`

    // CP-001: the whole object workflow
    if (!(await reachable(page, consoleUrl))) {
      await recorder.step({ system: 'console', screen: 'console', title: 'Console is unreachable', checkpoint: 'CP-001' }, true)
      await recorder.ui('CP-001', { reachable: false }, { reachable: true, initial_empty: true, uploaded_via_s3: true, downloaded_same: true, deleted: true })
      return
    }
    const initialEmpty = await page.getByText('No buckets yet.').isVisible()
    await recorder.step({ system: 'console', screen: 'home', title: 'Console with no buckets', action: { kind: 'submit', label: 'Create' } }, true)
    const bucket = recorder.id('photos')
    await createBucketInConsole(page, bucket)
    await recorder.step({ system: 'console', screen: 'bucket', title: 'New empty bucket', action: { kind: 'input', label: 'Upload' } }, true)

    const upload = path.join(await fs.mkdtemp(path.join(os.tmpdir(), 'console-')), 'hello world.txt')
    await fs.writeFile(upload, 'hello from the console')
    await page.locator('input[type=file]').setInputFiles(upload)
    await expect(page.getByRole('cell', { name: 'hello world.txt' })).toBeVisible()
    await recorder.step({ system: 'console', screen: 'objects', title: 'Uploaded object in the table', action: { kind: 'click', label: 'Download' } }, true)
    const listed = await sdk(s3Port).send(new ListObjectsV2Command({ Bucket: bucket }))

    const [download] = await Promise.all([page.waitForEvent('download'), page.getByRole('link', { name: 'Download' }).click()])
    const downloaded = await fs.readFile(await download.path(), 'utf8')

    await page.getByRole('button', { name: 'Delete', exact: true }).click()
    await page.getByRole('button', { name: 'Confirm', exact: true }).click()
    await expect(page.getByText('This location is empty.')).toBeVisible()
    await recorder.step({ system: 'console', screen: 'empty', title: 'Object deleted', checkpoint: 'CP-001', action: { kind: 'navigate', label: 'Open the strict console' } }, true)
    const deleted = await errorName(sdk(s3Port).send(new GetObjectCommand({ Bucket: bucket, Key: 'hello world.txt' })))
    await recorder.ui('CP-001', {
      reachable: true,
      initial_empty: initialEmpty,
      uploaded_via_s3: listed.Contents?.[0]?.Key === 'hello world.txt',
      downloaded_same: downloaded === 'hello from the console' && download.suggestedFilename() === 'hello world.txt',
      deleted: deleted === 'NoSuchKey',
    }, { reachable: true, initial_empty: true, uploaded_via_s3: true, downloaded_same: true, deleted: true })

    // CP-002: strict mode asks for the configured credentials
    const noCredentials = await raw(strictConsolePort, 'GET', '/api/buckets')
    const wrongCredentials = await raw(strictConsolePort, 'GET', '/api/buckets', { authorization: basicAuth(ACCESS_KEY, 'wrong') })
    await page.setExtraHTTPHeaders({ authorization: basicAuth(ACCESS_KEY, SECRET_KEY) })
    await page.goto(`http://127.0.0.1:${strictConsolePort}/`)
    const strictBanner = await page.getByText('strict auth').isVisible()
    await createBucketInConsole(page, recorder.id('strict'))
    await recorder.step({ system: 'console', screen: 'strict', title: 'Strict console after signing in', checkpoint: 'CP-002', action: { kind: 'api', label: 'Cross-site requests' } }, true)
    await recorder.ui('CP-002', {
      no_credentials: noCredentials.status,
      wrong_credentials: wrongCredentials.status,
      authorized_create: strictBanner,
    }, { no_credentials: 401, wrong_credentials: 401, authorized_create: true })
    await page.setExtraHTTPHeaders({})

    // CP-003: other sites and other hostnames
    await page.goto(web.url)
    const crossSiteBucket = recorder.id('cross-site')
    const crossSite = await pageFetch(page, `http://127.0.0.1:${consolePort}/api/buckets/${crossSiteBucket}`, { method: 'PUT' })
    const formPost = await raw(consolePort, 'POST', `/api/buckets/${crossSiteBucket}`, { origin: web.url })
    const rebinding = await raw(consolePort, 'GET', '/api/buckets', { host: 'evil.example.com' })
    const buckets = JSON.parse((await raw(consolePort, 'GET', '/api/buckets')).body.toString()) as { buckets: { name: string }[] }
    await recorder.show(`Cross-site PUT: ${crossSite.ok ? `HTTP ${crossSite.status}` : 'blocked by the browser'}`)
    await recorder.step({ system: 'console', screen: 'other-site', title: 'Request from another site', checkpoint: 'CP-003', action: { kind: 'navigate', label: 'Back to the console' } }, true)
    await recorder.api('CP-003', {
      cross_site_blocked: !crossSite.ok,
      form_post_status: formPost.status,
      foreign_host_status: rebinding.status,
      bucket_absent: !buckets.buckets.some((item) => item.name === crossSiteBucket),
    }, { cross_site_blocked: true, form_post_status: 405, foreign_host_status: 403, bucket_absent: true })

    // CP-004: a key containing dot segments
    const dots = recorder.id('dots')
    await sdk(s3Port).send(new CreateBucketCommand({ Bucket: dots }))
    await raw(s3Port, 'PUT', `/${dots}/x/../y`, {}, 'dotted')
    await raw(s3Port, 'PUT', `/${dots}/y`, {}, 'plain')
    await page.goto(`${consoleUrl}?bucket=${dots}&prefix=${encodeURIComponent('x/../')}`)
    await expect(page.getByRole('cell', { name: 'y', exact: true })).toBeVisible()
    await page.getByRole('button', { name: 'Delete', exact: true }).click()
    await page.getByRole('button', { name: 'Confirm', exact: true }).click()
    await expect(page.getByText('This location is empty.')).toBeVisible()
    await recorder.step({ system: 'console', screen: 'dots', title: 'Key with dot segments deleted', checkpoint: 'CP-004', action: { kind: 'click', label: 'Switch bucket' } }, true)
    await recorder.ui('CP-004', {
      dotted_deleted: (await raw(s3Port, 'GET', `/${dots}/x/../y`)).status === 404,
      other_kept: (await raw(s3Port, 'GET', `/${dots}/y`)).body.toString() === 'plain',
    }, { dotted_deleted: true, other_kept: true })

    // CP-005: a late listing from the previous bucket
    const first = recorder.id('aaa')
    const second = recorder.id('bbb')
    for (const name of [first, second]) {
      await sdk(s3Port).send(new CreateBucketCommand({ Bucket: name }))
      await sdk(s3Port).send(new PutObjectCommand({ Bucket: name, Key: `in-${name}.txt`, Body: name }))
    }
    let release = () => {}
    const held = new Promise<void>((resolve) => (release = resolve))
    await page.route(`**/api/buckets/${first}/objects*`, async (route) => {
      await held
      await route.continue()
    })
    await page.goto(consoleUrl)
    await page.getByRole('button', { name: new RegExp(`^${first}`) }).click()
    await page.getByRole('button', { name: new RegExp(`^${second}`) }).click()
    await expect(page.getByRole('cell', { name: `in-${second}.txt` })).toBeVisible()
    release()
    await sleep(800)
    await recorder.step({ system: 'console', screen: 'switched', title: 'Second bucket after the late response', checkpoint: 'CP-005' }, true)
    await recorder.ui('CP-005', {
      stale_shown: await page.getByRole('cell', { name: `in-${first}.txt` }).isVisible(),
      current_shown: await page.getByRole('cell', { name: `in-${second}.txt` }).isVisible(),
    }, { stale_shown: false, current_shown: true })
    await page.unrouteAll({ behavior: 'ignoreErrors' })
  } finally {
    await Promise.all(containers.map((container) => container.remove()))
    await web.close()
  }
}

// ---------------------------------------------------------------- UC-004

export async function imageServesBothPorts(recorder: Recorder): Promise<void> {
  recorder.systems = { image: await revision() }

  // CP-001: invalid auth settings stop the container
  const typo = await Container.run('typo', { DEVCLOUD_S3_AUTH_MODE: 'strcit' })
  const noCredentials = await Container.run('nocreds', { DEVCLOUD_S3_AUTH_MODE: 'strict' })
  try {
    await apiStep(recorder, 'CP-001', 'Invalid auth settings', 'docker run', 'image')
    await recorder.api('CP-001', {
      typo_exit: await typo.exitCodeAfter(3_000),
      no_credentials_exit: await noCredentials.exitCodeAfter(3_000),
    }, { typo_exit: 2, no_credentials_exit: 2 })
  } finally {
    await typo.remove()
    await noCredentials.remove()
  }

  // CP-002: default settings serve both ports
  const s3Port = port('s3plain')
  const consolePort = port('console')
  const container = await Container.run('plain', {}, { s3: s3Port, console: consolePort })
  try {
    await waitForHttp(s3Port)
    await sleep(500)
    const listing = await raw(s3Port, 'GET', '/')
    const home = await raw(consolePort, 'GET', '/').catch(() => null)
    await apiStep(recorder, 'CP-002', 'Default container', 'GET / on 9000 and 9001', 'image')
    await recorder.api('CP-002', {
      s3_status: listing.status,
      console_served: Boolean(home?.body.toString().includes('devcloud S3 console')),
      exposes_console: (await inspectExposedPorts()).includes('9001/tcp'),
    }, { s3_status: 200, console_served: true, exposes_console: true })
  } finally {
    await container.remove()
  }
}

// ---------------------------------------------------------------- UC-005

export async function existingGateStillPasses(recorder: Recorder): Promise<void> {
  recorder.systems = { devcloud: await revision() }
  await recorder.show('Running the existing S3 acceptance gate through devcloud up ...')
  const result = await execFile('bash', ['scripts/s3-autoloop/verify.sh'], {
    cwd: REPO_ROOT,
    maxBuffer: 64 * 1024 * 1024,
    env: {
      ...process.env,
      VERIFY_STAGE: 'full',
      S3_VERIFY_PORT: String(port('verifys3')),
      DASHBOARD_VERIFY_PORT: String(port('verifydashboard')),
      SMTP_VERIFY_PORT: String(port('verifysmtp')),
    },
  }).then(({ stdout }) => ({ code: 0, stdout }), (error: { code?: number; stdout?: string }) => ({ code: error.code ?? 1, stdout: error.stdout ?? '' }))
  const summary = /Verification: (\d+)\/(\d+) passed, (\d+) failed/.exec(result.stdout)
  await fs.mkdir(OUT, { recursive: true })
  await recorder.step({ system: 'devcloud', screen: 'gate', title: 'Existing S3 acceptance gate', checkpoint: 'CP-001' })
  await recorder.api('CP-001', {
    exit_code: result.code,
    failed: summary ? Number(summary[3]) : null,
    complete: Boolean(summary && summary[1] === summary[2]),
  }, { exit_code: 0, failed: 0, complete: true })
}
