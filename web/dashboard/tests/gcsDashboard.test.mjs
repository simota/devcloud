import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import * as React from 'react'
import * as jsxRuntime from 'react/jsx-runtime'
import TestRenderer from 'react-test-renderer'
import ts from 'typescript'

const { act, create } = TestRenderer

function deferred() {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}

function object(bucket, name) {
  return {
    name, size: 1, etag: 'etag', contentType: 'text/plain', storageClass: 'STANDARD',
    updated: '2026-10-12T00:00:00Z', generation: '1', metageneration: '1',
    gcsUri: `gs://${bucket}/${name}`,
    downloadUrl: `/api/gcs/buckets/${bucket}/objects/${name}/download`,
  }
}

const objectA = object('bucket-a', 'shared-name')
const objectB = object('bucket-b', 'shared-name')

function component(renderer, name) {
  return renderer.root.find((node) => typeof node.type === 'function' && node.type.name === name)
}

function loadSource(source, imports) {
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
    },
  })
  const module = { exports: {} }
  new Function('require', 'module', 'exports', outputText)((name) => {
    assert.ok(Object.hasOwn(imports, name), `unexpected component import: ${name}`)
    return imports[name]
  }, module, module.exports)
  return module.exports
}

const latestRequestHook = loadSource(
  await readFile(new URL('../src/app/api/hooks/useLatestRequest.ts', import.meta.url), 'utf8'),
  { react: React },
)

async function setup(t, overrides = {}) {
  const listingRequests = []
  const events = {}
  const api = {
    getGCSStatus: async () => ({ status: 'running', running: true }),
    listGCSBuckets: async () => ({
      buckets: ['bucket-a', 'bucket-b'].map((name) => ({ name, objectCount: 1, gcsUri: `gs://${name}` })),
    }),
    listGCSUploadSessions: async () => ({ sessions: [] }),
    listGCSObjects: async (bucket) => {
      listingRequests.push(bucket)
      return { bucket, objects: [bucket === 'bucket-a' ? objectA : objectB] }
    },
    getGCSObject: async (bucket) => bucket === 'bucket-a' ? objectA : objectB,
    deleteGCSObject: async () => {},
    ...overrides,
  }
  const wrap = (tag) => ({ children, ...props }) => React.createElement(tag, props, children)
  const imports = {
    react: React,
    'react/jsx-runtime': jsxRuntime,
    '../../../ui/EmptyState': { EmptyState: () => null },
    '../../../ui/Panel': { Panel: wrap('section') },
    '../../../ui/Button': { Button: wrap('button') },
    '../../../ui/Confirm': { dangerConfirm: (options) => options, useConfirm: () => async () => true },
    '../../api/hooks/useDashboardEvents': { useDashboardEvents: (options) => { events.current = options } },
    '../../api/hooks/useLatestRequest': latestRequestHook,
    './api': api,
  }
  // Render the real component and its hooks. Only API I/O and surrounding UI
  // widgets are substituted; deferred promises control actual response order.
  const source = await readFile(new URL('../src/app/services/gcs/GCSDashboard.tsx', import.meta.url), 'utf8')
  const { GCSDashboard } = loadSource(source, imports)
  let renderer
  await act(async () => { renderer = create(React.createElement(GCSDashboard)) })
  t.after(async () => { await act(async () => { renderer.unmount() }) })
  return { renderer, listingRequests, events }
}

test('a completed deletion reloads the current bucket and cannot replace its rows with the old bucket', async (t) => {
  const deletion = deferred()
  const deletions = []
  const { renderer, listingRequests } = await setup(t, {
    deleteGCSObject: (bucket, name) => {
      deletions.push([bucket, name])
      return deletion.promise
    },
  })
  await act(async () => { await component(renderer, 'ObjectBrowser').props.onDeleteObject(objectA) })
  assert.deepEqual(deletions, [['bucket-a', objectA.name]])
  await act(async () => { component(renderer, 'BucketList').props.onSelectBucket('bucket-b') })
  assert.equal(component(renderer, 'ObjectBrowser').props.objectsState.objects[0].gcsUri, objectB.gcsUri)

  await act(async () => { deletion.resolve() })
  const browser = component(renderer, 'ObjectBrowser').props
  assert.equal(browser.bucketName, 'bucket-b')
  assert.deepEqual(browser.objectsState.objects, [objectB])
  assert.deepEqual(listingRequests, ['bucket-a', 'bucket-b', 'bucket-b'])
})

test('an old inspector response cannot replace the selected object after a bucket change', async (t) => {
  const first = deferred(), second = deferred()
  const requests = []
  const { renderer } = await setup(t, {
    getGCSObject: (bucket, name) => {
      requests.push([bucket, name])
      return bucket === 'bucket-a' ? first.promise : second.promise
    },
  })
  await act(async () => { component(renderer, 'ObjectBrowser').props.onInspectObject(objectA) })
  await act(async () => { component(renderer, 'BucketList').props.onSelectBucket('bucket-b') })
  await act(async () => { component(renderer, 'ObjectBrowser').props.onInspectObject(objectB) })
  await act(async () => { second.resolve(objectB) })
  assert.deepEqual(component(renderer, 'ObjectInspector').props.object, objectB)

  await act(async () => { first.resolve(objectA) })
  assert.deepEqual(component(renderer, 'ObjectInspector').props.object, objectB)
  assert.deepEqual(requests, [['bucket-a', objectA.name], ['bucket-b', objectB.name]])
})

test('a failed obsolete inspector request cannot publish an error in a different bucket', async (t) => {
  const pending = deferred()
  const { renderer } = await setup(t, { getGCSObject: () => pending.promise })
  await act(async () => { component(renderer, 'ObjectBrowser').props.onInspectObject(objectA) })
  await act(async () => { component(renderer, 'BucketList').props.onSelectBucket('bucket-b') })
  await act(async () => { pending.reject(new Error('obsolete inspector error')) })
  assert.equal(component(renderer, 'ObjectInspector').props.object, undefined)
  assert.ok(!JSON.stringify(renderer.toJSON()).includes('obsolete inspector error'))
})

for (const olderResult of ['success', 'error']) {
  test(`an older snapshot ${olderResult} cannot overwrite a newer resync snapshot or selection`, async (t) => {
    const initial = deferred()
    let requests = 0
    const newBuckets = [{ name: 'bucket-b', objectCount: 1, gcsUri: 'gs://bucket-b' }]
    const { renderer, events } = await setup(t, {
      listGCSBuckets: () => ++requests === 1 ? initial.promise : Promise.resolve({ buckets: newBuckets }),
    })
    await act(async () => { events.current.onResync() })
    assert.equal(component(renderer, 'BucketList').props.activeBucket, 'bucket-b')
    await act(async () => {
      if (olderResult === 'success') {
        initial.resolve({ buckets: [{ name: 'bucket-a', objectCount: 1, gcsUri: 'gs://bucket-a' }] })
      } else {
        initial.reject(new Error('obsolete snapshot error'))
      }
    })
    const buckets = component(renderer, 'BucketList').props
    assert.equal(buckets.activeBucket, 'bucket-b')
    assert.deepEqual(buckets.buckets, newBuckets)
    assert.deepEqual(component(renderer, 'ObjectBrowser').props.objectsState.objects, [objectB])
    assert.ok(!JSON.stringify(renderer.toJSON()).includes('obsolete snapshot error'))
  })
}

test('unmounting invalidates pending and subsequently started snapshot callbacks', async () => {
  let beginRequest
  function Consumer() {
    beginRequest = latestRequestHook.useLatestRequest()
    return null
  }
  let renderer
  await act(async () => { renderer = create(React.createElement(Consumer)) })
  const isCurrent = beginRequest()
  assert.equal(isCurrent(), true)
  await act(async () => { renderer.unmount() })
  assert.equal(isCurrent(), false)
  assert.equal(beginRequest()(), false)
})
