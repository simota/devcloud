// Runs the standalone S3 image in Docker and records what each use case observed.
// Everything is written under dist/e2e, which the repository ignores.
import { execFile as execFileCb } from 'node:child_process'
import { createHash, createHmac, randomBytes } from 'node:crypto'
import * as fs from 'node:fs/promises'
import * as http from 'node:http'
import * as net from 'node:net'
import * as path from 'node:path'
import { promisify } from 'node:util'
import { S3Client } from '@aws-sdk/client-s3'
import { expect, type Browser, type BrowserContext, type Page } from '@playwright/test'

const execFile = promisify(execFileCb)

export const REPO_ROOT = path.resolve(process.cwd(), '../..')
export const OUT = 'dist/e2e'
export const ACCESS_KEY = 'e2e-key'
export const SECRET_KEY = 'e2e-secret'
const REGION = 'us-east-1'
const NAMESPACE = (process.env.VERIFY_SCAN_NAMESPACE ?? `local_${randomBytes(4).toString('hex')}`).toLowerCase()
const IMAGE = `devcloud-s3:e2e-${NAMESPACE}`

export type Actual = Record<string, unknown>

// ---------------------------------------------------------------- ports

let nextDefaultPort = 19_200

/** A port leased by the verification recorder, or a local default outside it. */
export function port(name: string): number {
  const leased = process.env[`VERIFY_SCAN_PORT_${name.toUpperCase()}`]
  return leased ? Number(leased) : nextDefaultPort++
}

// ---------------------------------------------------------------- docker

async function docker(...args: string[]): Promise<string> {
  const { stdout } = await execFile('docker', args, { maxBuffer: 16 * 1024 * 1024 })
  return stdout.trim()
}

export async function revision(): Promise<string> {
  const { stdout } = await execFile('git', ['rev-parse', '--short=7', 'HEAD'], { cwd: REPO_ROOT })
  return stdout.trim()
}

export async function buildImage(): Promise<void> {
  await docker('build', '-q', '-f', path.join(REPO_ROOT, 'services/s3/Dockerfile'), '-t', IMAGE, REPO_ROOT)
}

export async function removeImage(): Promise<void> {
  await docker('image', 'rm', '-f', IMAGE).catch(() => '')
}

export async function inspectExposedPorts(): Promise<string> {
  return docker('image', 'inspect', IMAGE, '--format', '{{json .Config.ExposedPorts}}')
}

export class Container {
  private constructor(private readonly name: string) {}

  static async run(label: string, env: Record<string, string>, ports: { s3?: number; console?: number } = {}): Promise<Container> {
    const name = `${NAMESPACE}-${label}`
    const published = [
      ...(ports.s3 ? ['-p', `127.0.0.1:${ports.s3}:9000`] : []),
      ...(ports.console ? ['-p', `127.0.0.1:${ports.console}:9001`] : []),
    ]
    const settings = Object.entries(env).flatMap(([key, value]) => ['-e', `${key}=${value}`])
    await docker('run', '-d', '--name', name, ...published, ...settings, IMAGE)
    return new Container(name)
  }

  /** The exit code once stopped, or null when still running after `waitMs`. */
  async exitCodeAfter(waitMs: number): Promise<number | null> {
    const deadline = Date.now() + waitMs
    while (Date.now() < deadline) {
      const [running, code] = (await docker('inspect', this.name, '--format', '{{.State.Running}} {{.State.ExitCode}}')).split(' ')
      if (running === 'false') return Number(code)
      await sleep(200)
    }
    return null
  }

  async remove(): Promise<void> {
    await docker('rm', '-f', this.name).catch(() => '')
  }
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export async function waitForHttp(targetPort: number): Promise<void> {
  const deadline = Date.now() + 20_000
  for (;;) {
    const response = await raw(targetPort, 'GET', '/').catch(() => null)
    if (response) return
    if (Date.now() > deadline) throw new Error(`nothing answered on port ${targetPort}`)
    await sleep(200)
  }
}

export const strictEnv = {
  DEVCLOUD_S3_AUTH_MODE: 'strict',
  DEVCLOUD_S3_ACCESS_KEY_ID: ACCESS_KEY,
  DEVCLOUD_S3_SECRET_ACCESS_KEY: SECRET_KEY,
}

// ---------------------------------------------------------------- clients

export function sdk(targetPort: number, options: { secret?: string; clockOffsetMs?: number } = {}): S3Client {
  const client = new S3Client({
    endpoint: `http://127.0.0.1:${targetPort}`,
    forcePathStyle: true,
    region: REGION,
    maxAttempts: 1,
    credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: options.secret ?? SECRET_KEY },
  })
  if (options.clockOffsetMs) client.config.systemClockOffset = options.clockOffsetMs
  return client
}

export interface RawResponse {
  status: number
  headers: http.IncomingHttpHeaders
  body: Buffer
}

/** Sends the path byte for byte: `fetch` would resolve dot segments in it. */
export function raw(
  targetPort: number,
  method: string,
  target: string,
  headers: Record<string, string> = {},
  body?: Buffer | string,
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const request = http.request({ host: '127.0.0.1', port: targetPort, method, path: target, headers }, (response) => {
      const chunks: Buffer[] = []
      response.on('data', (chunk: Buffer) => chunks.push(chunk))
      response.on('end', () => resolve({ status: response.statusCode ?? 0, headers: response.headers, body: Buffer.concat(chunks) }))
    })
    request.on('error', reject)
    request.setTimeout(5_000, () => request.destroy(new Error('request timed out')))
    request.end(body)
  })
}

/** Writes bytes to a socket and returns the reply; empty when closed without one. */
export function rawSocket(targetPort: number, wire: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = net.connect(targetPort, '127.0.0.1', () => socket.write(wire))
    let reply = ''
    socket.setEncoding('utf8')
    socket.on('data', (chunk: string) => (reply += chunk))
    socket.on('end', () => resolve(reply))
    socket.on('close', () => resolve(reply))
    socket.on('error', (error) => (reply ? resolve(reply) : reject(error)))
    socket.setTimeout(5_000, () => socket.destroy())
  })
}

const sha256 = (value: string | Buffer) => createHash('sha256').update(value).digest('hex')
const hmac = (key: string | Buffer, value: string) => createHmac('sha256', key).update(value).digest()

/**
 * Uploads with signed chunks (STREAMING-AWS4-HMAC-SHA256-PAYLOAD), which the
 * JavaScript SDK does not send. `tamper` changes one byte after signing.
 */
export async function signedChunkUpload(targetPort: number, target: string, chunks: Buffer[], tamper = false): Promise<RawResponse> {
  const amzDate = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z')
  const date = amzDate.slice(0, 8)
  const scope = `${date}/${REGION}/s3/aws4_request`
  const key = hmac(hmac(hmac(hmac(`AWS4${SECRET_KEY}`, date), REGION), 's3'), 'aws4_request')
  const payloadHash = 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
  const headers: Record<string, string> = {
    'content-encoding': 'aws-chunked',
    host: `127.0.0.1:${targetPort}`,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
    'x-amz-decoded-content-length': String(chunks.reduce((sum, chunk) => sum + chunk.length, 0)),
  }
  const names = Object.keys(headers).sort()
  const canonical = [
    'PUT',
    target,
    '',
    names.map((name) => `${name}:${headers[name]}\n`).join(''),
    names.join(';'),
    payloadHash,
  ].join('\n')
  const seed = hmac(key, ['AWS4-HMAC-SHA256', amzDate, scope, sha256(canonical)].join('\n')).toString('hex')

  const framed: Buffer[] = []
  let previous = seed
  for (const chunk of [...chunks, Buffer.alloc(0)]) {
    previous = hmac(key, ['AWS4-HMAC-SHA256-PAYLOAD', amzDate, scope, previous, sha256(''), sha256(chunk)].join('\n')).toString('hex')
    framed.push(Buffer.from(`${chunk.length.toString(16)};chunk-signature=${previous}\r\n`), chunk, Buffer.from('\r\n'))
  }
  const body = Buffer.concat(framed)
  if (tamper) {
    const at = body.indexOf('\r\n') + 2
    body[at] ^= 1
  }
  return raw(targetPort, 'PUT', target, {
    ...headers,
    authorization: `AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${scope}, SignedHeaders=${names.join(';')}, Signature=${seed}`,
    'content-length': String(body.length),
  }, body)
}

// ---------------------------------------------------------------- browser-visible origin

export interface WebOrigin {
  url: string
  close(): Promise<void>
}

/** A minimal app on its own origin, so requests from its page are cross-origin. */
export async function serveWebOrigin(webPort: number): Promise<WebOrigin> {
  const server = http.createServer((_, response) => {
    response.setHeader('Content-Type', 'text/html; charset=utf-8')
    response.end(`<!doctype html><meta charset="utf-8"><title>Demo web app</title>
      <style>body{font:18px/1.5 system-ui;margin:32px}h1{font-size:24px}li{margin:6px 0;overflow-wrap:anywhere}</style>
      <h1 id="title">Demo web app</h1><ul id="log"></ul>`)
  })
  await new Promise<void>((resolve) => server.listen(webPort, '127.0.0.1', resolve))
  return {
    url: `http://127.0.0.1:${webPort}`,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  }
}

export interface PageFetch {
  ok: boolean
  status: number | null
  etag: string | null
  body: string | null
}

/** `fetch` from inside the page, so the browser enforces CORS. A block is `ok: false`. */
export function pageFetch(page: Page, url: string, init: RequestInit = {}): Promise<PageFetch> {
  return page.evaluate(async ({ url, init }) => {
    try {
      const response = await fetch(url, init)
      return { ok: true, status: response.status, etag: response.headers.get('etag'), body: await response.text() }
    } catch {
      return { ok: false, status: null, etag: null, body: null }
    }
  }, { url, init })
}

// ---------------------------------------------------------------- recording

interface Step {
  system: string
  screen: string
  title: string
  screenshot?: string
  action?: { kind: string; label: string }
  checkpoint?: string
}

interface Checkpoint {
  id: string
  correlation_id: string
  actual: Actual
  evidence?: string[]
  video?: { path: string; start_s: number; end_s: number }
  interval?: [number, number]
}

/** One use case's observations, written even when an assertion fails. */
export class Recorder {
  readonly checkpoints: Checkpoint[] = []
  readonly steps: Step[] = []
  readonly correlationId: string
  systems: Record<string, string> = {}
  startedAt = 0
  endedAt = 0
  private clock = Date.now()
  private segmentStart = 0

  constructor(
    readonly useCase: string,
    public page: Page | null,
    clockStart: number,
  ) {
    this.clock = clockStart
    this.correlationId = `${useCase.toLowerCase()}-${randomBytes(4).toString('hex')}`
    this.startedAt = this.now()
    this.segmentStart = this.startedAt
  }

  now(): number {
    return (Date.now() - this.clock) / 1000
  }

  /** A short name unique to this attempt, valid in a bucket name. */
  id(name: string): string {
    return `${name}-${this.correlationId.slice(-8)}`
  }

  beginCheckpoint(): void {
    this.segmentStart = this.now()
  }

  /** Shows progress in the page, so the recording explains what is happening. */
  async show(line: string): Promise<void> {
    if (!this.page || this.page.isClosed()) return
    await this.page.evaluate((text) => {
      const item = document.createElement('li')
      item.textContent = text
      document.getElementById('log')?.append(item)
    }, line).catch(() => undefined)
  }

  /** Records a value read from a browser screen; its interval is cut from the page video. */
  async ui(id: string, actual: Actual, expected: Actual): Promise<void> {
    await sleep(1_200)
    this.checkpoints.push({ id, correlation_id: this.correlationId, actual, interval: [this.segmentStart, this.now()] })
    expect.soft(actual, id).toEqual(expected)
    this.segmentStart = this.now()
  }

  /** Records a value read from an API, keeping it as an attachment. */
  async api(id: string, actual: Actual, expected: Actual): Promise<void> {
    const file = `${OUT}/evidence/${this.useCase}-${id}.json`
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify({ correlation_id: this.correlationId, actual }, null, 2))
    this.checkpoints.push({
      id,
      correlation_id: this.correlationId,
      actual,
      evidence: [file],
      interval: [this.segmentStart, this.now()],
    })
    await this.show(`${id}: ${JSON.stringify(actual)}`)
    if (this.page) await sleep(800)
    expect.soft(actual, id).toEqual(expected)
    this.segmentStart = this.now()
  }

  async step(step: Omit<Step, 'screenshot'>, screenshot = false): Promise<void> {
    const entry: Step = { ...step }
    if (screenshot && this.page) {
      const file = `${OUT}/steps/${this.useCase}-${String(this.steps.length + 1).padStart(2, '0')}.png`
      await fs.mkdir(path.dirname(file), { recursive: true })
      await this.page.screenshot({ path: file })
      entry.screenshot = file
    }
    this.steps.push(entry)
  }
}

export async function videoDuration(file: string): Promise<number> {
  const { stdout } = await execFile('ffprobe', ['-v', 'error', '-count_packets', '-show_entries', 'format=duration', '-of', 'csv=p=0', file]).catch(() => ({ stdout: '' }))
  const seconds = Number(stdout.trim())
  if (Number.isFinite(seconds) && seconds > 0) return seconds
  // Browser recordings can omit the container duration: read the last packet's time.
  const packets = await execFile('ffprobe', ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'packet=pts_time', '-of', 'csv=p=0', file], { maxBuffer: 64 * 1024 * 1024 })
  const times = packets.stdout.trim().split('\n').map(Number).filter(Number.isFinite)
  return Math.max(...times)
}

/** Opens a recorded page; its video timeline starts at the returned clock. */
export async function openRecordedPage(browser: Browser): Promise<{ context: BrowserContext; page: Page; clockStart: number }> {
  const context = await browser.newContext({
    acceptDownloads: true,
    viewport: { width: 1280, height: 720 },
    recordVideo: { dir: `${OUT}/raw`, size: { width: 1280, height: 720 } },
  })
  const page = await context.newPage()
  return { context, page, clockStart: Date.now() }
}

/** Closes the page's context and finalizes its recording. */
export async function saveVideo(context: BrowserContext, page: Page, name: string): Promise<{ file: string; duration: number }> {
  await context.close()
  const video = page.video()
  if (!video) throw new Error('the page has no video')
  const file = `${OUT}/video/${name}.webm`
  await fs.mkdir(path.dirname(file), { recursive: true })
  await video.saveAs(file)
  return { file, duration: await videoDuration(file) }
}

export async function writeScenario(recorder: Recorder, video?: { file: string; duration: number }): Promise<void> {
  const checkpoints = recorder.checkpoints.map(({ interval, ...checkpoint }) => {
    if (checkpoint.evidence || !video || !interval) return checkpoint
    const end_s = Math.min(interval[1], video.duration)
    const start_s = Math.min(interval[0], Math.max(0, end_s - 0.5))
    return { ...checkpoint, video: { path: video.file, start_s, end_s } }
  })
  await fs.mkdir(OUT, { recursive: true })
  await fs.writeFile(`${OUT}/scenario.json`, JSON.stringify({
    use_case: recorder.useCase,
    run_token: process.env.VERIFY_SCAN_RUN_TOKEN ?? 'local',
    correlation_id: recorder.correlationId,
    systems: recorder.systems,
    checkpoints,
    steps: recorder.steps,
  }, null, 2))
}
