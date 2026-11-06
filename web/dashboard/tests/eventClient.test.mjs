import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import ts from 'typescript'

let moduleID = 0

async function setup(t) {
  const timers = new Map()
  let nextTimer = 0
  const sockets = []
  const subscriptions = []
  class WebSocketStub {
    constructor(url) {
      this.url = url
      sockets.push(this)
    }
    close(...args) { this.closed = args }
    open() { this.onopen?.() }
    frame(value) { this.onmessage?.({ data: JSON.stringify(value) }) }
  }
  const oldWindow = Object.getOwnPropertyDescriptor(globalThis, 'window')
  const oldWebSocket = Object.getOwnPropertyDescriptor(globalThis, 'WebSocket')
  Object.defineProperty(globalThis, 'window', {
    configurable: true,
    value: {
      location: { protocol: 'https:', host: 'localhost:18025' },
      setTimeout(callback, delay) {
        timers.set(++nextTimer, { callback, delay })
        return nextTimer
      },
      clearTimeout(id) { timers.delete(id) },
    },
  })
  Object.defineProperty(globalThis, 'WebSocket', { configurable: true, value: WebSocketStub })
  t.after(() => {
    for (const unsubscribe of subscriptions) unsubscribe()
    if (oldWindow) Object.defineProperty(globalThis, 'window', oldWindow)
    else delete globalThis.window
    if (oldWebSocket) Object.defineProperty(globalThis, 'WebSocket', oldWebSocket)
    else delete globalThis.WebSocket
  })
  // Use the installed TypeScript compiler and Node's runner; no browser or
  // additional test dependency is needed to exercise the public subscriber API.
  const source = await readFile(new URL('../src/app/api/eventClient.ts', import.meta.url), 'utf8')
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  })
  const { subscribeDashboardEvents } = await import(
    `data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}#${moduleID++}`
  )
  return {
    subscribe(...args) {
      const unsubscribe = subscribeDashboardEvents(...args)
      subscriptions.push(unsubscribe)
      return unsubscribe
    },
    sockets,
    timers,
    retry() {
      assert.equal(timers.size, 1)
      const [id, timer] = timers.entries().next().value
      timers.delete(id)
      timer.callback()
    },
  }
}

const mail = {
  type: 'mail.received', service: 'mail', timestamp: '2026-10-12T00:00:00Z',
  payload: { messageID: 'mail-1' },
}
const s3 = { type: 's3.object.put', service: 's3', timestamp: mail.timestamp }

test('unwraps actual relay frames and filters the nested service, without notifying on control frames', async (t) => {
  const { subscribe, sockets } = await setup(t)
  const mailEvents = [], s3Events = [], allEvents = []
  t.after(subscribe(['mail'], (event) => mailEvents.push(event)))
  t.after(subscribe(['s3'], (event) => s3Events.push(event)))
  t.after(subscribe([], (event) => allEvents.push(event)))
  assert.equal(sockets.length, 1)
  assert.equal(sockets[0].url, 'wss://localhost:18025/api/events')
  sockets[0].open()
  sockets[0].frame({ type: 'hello', seq: 0 })
  sockets[0].frame({ type: 'event', seq: 1, event: mail })
  sockets[0].frame({ type: 'event', seq: 2, event: s3 })
  sockets[0].frame({ type: 'ping', seq: 2 })
  sockets[0].frame({ type: 'resync', seq: 2, reason: 'slow-consumer' })
  assert.deepEqual(mailEvents, [mail])
  assert.deepEqual(s3Events, [s3])
  assert.deepEqual(allEvents, [mail, s3])
})

test('refreshes each snapshot on resync and reconnect, without inventing service notifications', async (t) => {
  const { subscribe, sockets, retry } = await setup(t)
  let refreshes = 0
  const events = []
  t.after(subscribe(['s3'], () => {}, () => { throw new Error('subscriber failed') }))
  t.after(subscribe(['mail'], (event) => events.push(event), () => refreshes++))
  sockets[0].open()
  sockets[0].frame({ type: 'hello', seq: 0 })
  assert.equal(refreshes, 1)
  sockets[0].frame({ type: 'resync', seq: 5, reason: 'slow-consumer' })
  assert.equal(refreshes, 2)
  sockets[0].onclose()
  retry()
  sockets[1].open()
  sockets[1].frame({ type: 'hello', seq: 10 })
  assert.equal(refreshes, 3)
  assert.deepEqual(events, [])
})

test('refreshes after the first connection failed before any hello arrived', async (t) => {
  const { subscribe, sockets, retry } = await setup(t)
  let refreshes = 0
  subscribe(['mail'], () => {}, () => refreshes++)
  // The initial HTTP snapshot may already have finished before this failure.
  sockets[0].onerror()
  retry()
  sockets[1].open()
  sockets[1].frame({ type: 'hello', seq: 12 })
  assert.equal(refreshes, 1)
})

test('does not deliver malformed frames or let a failing listener block other listeners', async (t) => {
  const { subscribe, sockets } = await setup(t)
  const received = []
  t.after(subscribe([], () => { throw new Error('subscriber failed') }))
  t.after(subscribe([], (event) => received.push(event)))
  for (const frame of [null, [], 42, {}, { type: 'event' },
    { type: 'event', event: { type: 'mail.received' } },
    { type: 'event', event: { ...mail, payload: [] } }]) {
    sockets[0].frame(frame)
  }
  sockets[0].onmessage({ data: '{' })
  sockets[0].onmessage({ data: new Uint8Array([1, 2]) })
  sockets[0].frame({ type: 'event', seq: 1, event: mail })
  assert.deepEqual(received, [mail])
})

test('closes failed sockets and preserves one pending reconnect when new listeners subscribe', async (t) => {
  const { subscribe, sockets, timers, retry } = await setup(t)
  t.after(subscribe(['mail'], () => {}))
  const fail = sockets[0].onerror
  fail()
  fail()
  assert.deepEqual(sockets[0].closed, [])
  assert.equal(timers.values().next().value.delay, 1000)
  t.after(subscribe(['s3'], () => {}))
  assert.equal(sockets.length, 1)
  retry()
  assert.equal(sockets.length, 2)
  sockets[1].onerror()
  assert.equal(timers.values().next().value.delay, 2000)
})

test('last unsubscribe cancels reconnects and stale callbacks cannot reach new subscribers', async (t) => {
  const { subscribe, sockets, timers } = await setup(t)
  const unsubscribe = subscribe(['mail'], () => {})
  const oldMessage = sockets[0].onmessage
  const oldClose = sockets[0].onclose
  unsubscribe()
  unsubscribe()
  assert.deepEqual(sockets[0].closed, [1000, 'no listeners'])
  assert.equal(sockets[0].onmessage, null)
  const received = []
  const stop = subscribe(['mail'], (event) => received.push(event))
  oldMessage({ data: JSON.stringify({ type: 'event', seq: 1, event: mail }) })
  oldClose()
  assert.deepEqual(received, [])
  assert.equal(timers.size, 0)
  sockets[1].onerror()
  assert.equal(timers.size, 1)
  stop()
  assert.equal(timers.size, 0)
})
