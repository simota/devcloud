import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'

const repository = fileURLToPath(new URL('../../../', import.meta.url))
const binary = resolve(process.env.DEVCLOUD_BINARY ?? join(repository, 'target/debug/devcloud'))
const output = resolve(process.env.PLAYWRIGHT_OUTPUT_DIR ?? join(repository, 'target/dashboard-browser'))
const workspace = await mkdtemp(join(tmpdir(), 'devcloud-dashboard-browser-'))
const reservations = []
for (let index = 0; index < 3; index += 1) {
  const server = createServer()
  await new Promise((yes, no) => { server.once('error', no); server.listen(0, '127.0.0.1', yes) })
  reservations.push(server)
}
const [dashboardPort, relayPort, gcsPort] = reservations.map((server) => server.address().port)
await Promise.all(reservations.map((server) => new Promise((yes) => server.close(yes))))
await mkdir(join(workspace, '.devcloud'))
await mkdir(output, { recursive: true })
await writeFile(join(workspace, '.devcloud/config.yaml'), `project: dashboard-browser
server:
  dashboardPort: ${dashboardPort}
  eventRelayPort: ${relayPort}
  gcsPort: ${gcsPort}
`)

const processHandle = spawn(binary, ['up', 'gcs'], { cwd: workspace, stdio: ['ignore', 'pipe', 'pipe'] })
let serverLog = ''
let spawnError
for (const stream of [processHandle.stdout, processHandle.stderr]) {
  stream.on('data', (chunk) => { serverLog = (serverLog + chunk).slice(-16000) })
}
const exited = new Promise((yes) => {
  processHandle.once('exit', yes)
  processHandle.once('error', (error) => { spawnError = error; yes() })
})
const dashboard = `http://127.0.0.1:${dashboardPort}`
const provider = `http://127.0.0.1:${gcsPort}`
let browser
let page

async function waitUntil(condition, description) {
  const deadline = Date.now() + 20000
  while (Date.now() < deadline) {
    if (spawnError) throw spawnError
    if (processHandle.exitCode !== null || processHandle.signalCode !== null) {
      throw new Error(`devcloud exited while waiting for ${description}: ${serverLog}`)
    }
    if (await condition()) return
    await delay(50)
  }
  throw new Error(`Timed out waiting for ${description}`)
}

async function request(url, options) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(5000) })
  assert.ok(response.ok, `${options?.method ?? 'GET'} ${url}: ${response.status} ${await response.text()}`)
}

async function upload(name) {
  await request(`${provider}/upload/storage/v1/b/review-bucket/o?uploadType=media&name=${encodeURIComponent(name)}`, {
    method: 'POST', headers: { 'content-type': 'text/plain' }, body: `Dashboard browser smoke: ${name}\n`,
  })
}

try {
  await waitUntil(async () => {
    try {
      const response = await fetch(`${provider}/storage/v1/b?project=dashboard-browser`, { signal: AbortSignal.timeout(1000) })
      return response.ok
    } catch { return false }
  }, 'GCS startup')
  await request(`${provider}/storage/v1/b?project=dashboard-browser`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: 'review-bucket' }),
  })
  await upload('before-connect.txt')
  browser = await chromium.launch({ executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH })
  const context = await browser.newContext({ viewport: { width: 1440, height: 1080 }, deviceScaleFactor: 1 })
  await context.addInitScript(() => {
    window.__smokeSockets = []
    const NativeWebSocket = window.WebSocket
    window.WebSocket = class extends NativeWebSocket {
      constructor(...args) {
        super(...args)
        window.__smokeSockets.push(this)
      }
    }
  })
  page = await context.newPage()
  page.setDefaultTimeout(20000)
  const errors = []
  let helloCount = 0
  page.on('pageerror', (error) => errors.push(error.message))
  page.on('websocket', (socket) => {
    socket.on('framereceived', ({ payload }) => {
      try { if (JSON.parse(payload.toString()).type === 'hello') helloCount += 1 } catch { /* protocol control frame */ }
    })
  })
  await page.goto(`${dashboard}/dashboard/gcs`)
  await page.getByText('before-connect.txt', { exact: true }).waitFor()
  await waitUntil(() => helloCount > 0, 'initial event subscription')
  // Settle the hello-triggered snapshot before mutating; otherwise that initial
  // request could include the new object and mask a broken event dispatcher.
  await page.waitForLoadState('networkidle')
  const registryResponse = await fetch(`${dashboard}/api/services`, { signal: AbortSignal.timeout(5000) })
  assert.equal(registryResponse.status, 200)
  const registry = await registryResponse.json()
  assert.equal(registry.services.length, 12)
  assert.deepEqual(registry.services.filter((service) => service.status === 'running').map((service) => service.id), ['gcs'],
    'devcloud up gcs must report only GCS as running')
  assert.ok(registry.services.every((service) => service.id === 'gcs' || service.status === 'disabled'),
    'unselected services must be disabled')
  await page.getByText('1/12 running', { exact: true }).waitFor()

  // A provider mutation must reach the page through the real relay and proxy.
  await upload('live-event.txt')
  await page.getByText('live-event.txt', { exact: true }).waitFor()

  // Hold the browser offline so this mutation cannot be delivered as an event.
  // The reconnect hello must refresh the snapshot without navigating the page.
  const hellosBeforeDisconnect = helloCount
  await context.setOffline(true)
  await page.evaluate(() => { for (const socket of window.__smokeSockets) socket.close() })
  await page.waitForFunction(() => window.__smokeSockets.every((socket) => socket.readyState === WebSocket.CLOSED))
  await upload('after-reconnect.txt')
  assert.equal(await page.getByText('after-reconnect.txt', { exact: true }).count(), 0)
  await context.setOffline(false)
  await waitUntil(() => helloCount > hellosBeforeDisconnect, 'event reconnect')
  await page.getByText('after-reconnect.txt', { exact: true }).waitFor()
  await page.getByText('after-reconnect.txt', { exact: true }).click()
  await page.locator('.object-inspector').getByText('gs://review-bucket/after-reconnect.txt', { exact: true }).waitFor()
  assert.deepEqual(errors, [], 'the dashboard must not throw in the browser')
  await page.screenshot({ path: join(output, 'gcs-live-and-reconnect.png'), fullPage: true })
  await writeFile(join(output, 'result.json'), JSON.stringify({
    passed: ['selected service registry', 'initial snapshot', 'live provider event', 'snapshot after reconnect', 'object inspector'],
    helloCount, pageErrors: errors,
  }, null, 2) + '\n')
  console.log('Dashboard browser smoke passed: selected service registry, initial snapshot, live event, reconnect snapshot, inspector.')
} catch (error) {
  if (page) await page.screenshot({ path: join(output, 'failure.png'), fullPage: true }).catch(() => {})
  throw error
} finally {
  await browser?.close().catch(() => {})
  await writeFile(join(output, 'devcloud.log'), serverLog)
  if (processHandle.pid && processHandle.exitCode === null && processHandle.signalCode === null) {
    processHandle.kill('SIGTERM')
    await Promise.race([exited, delay(5000, undefined, { ref: false })])
    if (processHandle.exitCode === null && processHandle.signalCode === null) processHandle.kill('SIGKILL')
  }
  await exited
  await rm(workspace, { recursive: true, force: true })
}
