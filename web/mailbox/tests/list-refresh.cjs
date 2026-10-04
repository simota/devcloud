const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const ts = require('typescript')

// Exercise the actual hook with deterministic React state and paginated API data.
const slots = []
let cursor = 0
let messages = Array.from({ length: 240 }, (_, id) => ({ id: String(id), subject: `mail ${id}`, from: { name: '', address: 'sender@test' } }))
const calls = []
const react = {
  useState(initial) {
    const index = cursor++
    if (!(index in slots)) slots[index] = initial
    return [slots[index], (value) => { slots[index] = typeof value === 'function' ? value(slots[index]) : value }]
  },
  useRef(initial) {
    const index = cursor++
    if (!(index in slots)) slots[index] = { current: initial }
    return slots[index]
  },
  useCallback: (callback) => callback,
  useEffect() {},
}
const api = {
  async listMessages(query, start = 0, signal, limit = 50) {
    assert.equal(signal.aborted, false)
    assert.ok(limit <= 100)
    const data = query ? messages.filter((item) => item.subject.includes(query)) : messages
    calls.push([start, limit])
    return { total: data.length, start, items: data.slice(start, start + limit) }
  },
  getMessage: async (id) => messages.find((item) => item.id === id),
  errorMessage: () => 'error',
}
const source = fs.readFileSync(path.join(__dirname, '../src/hooks.ts'), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText
const context = { exports: {}, AbortController, Set, window: {}, require(name) {
  if (name === 'react') return react
  if (name === './api') return api
  if (name === './format') return { addressLabel: (address) => address.address }
  throw new Error(`Unexpected module ${name}`)
} }
vm.runInNewContext(compiled, context)
function render(query = '') {
  cursor = 0
  return context.exports.useMessages(query)
}

async function run() {
  await render().refresh({ reset: true })
  await render().loadMore()
  await render().loadMore()
  assert.equal(render().items.length, 150)
  messages.unshift({ id: 'new', subject: 'new mail', from: { name: '', address: 'new@test' } })
  calls.length = 0
  await render().refresh({ notify: true, newMessageID: 'new' })
  assert.equal(render().items.length, 150)
  assert.equal(render().items[0].id, 'new')
  assert.deepEqual(calls, [[0, 100], [100, 50]])
  await render().refresh() // EventSource reconnect
  assert.equal(render().items.length, 150)
  await render().loadMore()
  assert.equal(render().items.length, 200)
  assert.equal(new Set(render().items.map((item) => item.id)).size, 200)
  messages = messages.slice(0, 75)
  await render().refresh()
  assert.equal(render().items.length, 75)
  assert.equal(render().hasMore, false)
  await render('mail').refresh({ reset: true })
  assert.equal(render('mail').items.length, 50)
  console.log('PASS: notify/reconnect preserves expanded pagination; deletions and query reset reconcile')
}
run().catch((error) => { console.error(error); process.exitCode = 1 })
