import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import * as React from 'react'
import * as jsxRuntime from 'react/jsx-runtime'
import TestRenderer from 'react-test-renderer'
import ts from 'typescript'

const { act, create } = TestRenderer

function loadSource(source, imports) {
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
      jsx: ts.JsxEmit.ReactJSX,
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

const wrap = (tag) => ({ children, ...props }) => React.createElement(tag, props, children)

test('ApplicationAutoScalingDashboard registers and deregisters targets, policies, and scheduled actions', async (t) => {
  const calls = []
  const targets = [
    {
      ServiceNamespace: 'dynamodb',
      ResourceId: 'table/orders',
      ScalableDimension: 'dynamodb:table:WriteCapacityUnits',
      MinCapacity: 1,
      MaxCapacity: 10,
      CreationTime: '2026-01-01T00:00:00Z',
    },
  ]
  const policies = [
    {
      PolicyARN: 'arn:aws:autoscaling:us-east-1:000000000000:scalingPolicy:1:resource/dynamodb/table/orders:policyName/p1',
      PolicyName: 'p1',
      ServiceNamespace: 'dynamodb',
      ResourceId: 'table/orders',
      ScalableDimension: 'dynamodb:table:WriteCapacityUnits',
      PolicyType: 'TargetTrackingScaling',
      Alarms: [],
      CreationTime: '2026-01-01T00:00:00Z',
    },
  ]
  const actions = [
    {
      ScheduledActionName: 'nightly',
      ServiceNamespace: 'dynamodb',
      ResourceId: 'table/orders',
      ScalableDimension: 'dynamodb:table:WriteCapacityUnits',
      Schedule: 'rate(1 day)',
      ScalableTargetAction: { MinCapacity: 2, MaxCapacity: 8 },
      CreationTime: '2026-01-01T00:00:00Z',
    },
  ]
  const api = {
    getApplicationAutoScalingStatus: async () => ({
      service: 'applicationautoscaling',
      status: 'running',
      running: true,
      endpoint: 'http://127.0.0.1:18030',
      region: 'us-east-1',
      storagePath: '.devcloud/data/applicationautoscaling',
      scalableTargetCount: targets.length,
      scalingPolicyCount: policies.length,
      scheduledActionCount: actions.length,
    }),
    listApplicationAutoScalingScalableTargets: async () => ({ scalableTargets: targets }),
    listApplicationAutoScalingScalingPolicies: async () => ({ scalingPolicies: policies }),
    listApplicationAutoScalingScheduledActions: async () => ({ scheduledActions: actions }),
    registerApplicationAutoScalingScalableTarget: async (input) => {
      calls.push(['register', input])
      return {}
    },
    deregisterApplicationAutoScalingScalableTarget: async (input, confirmation) => {
      calls.push(['deregister', input, confirmation])
      return {}
    },
    putApplicationAutoScalingScalingPolicy: async (input) => {
      calls.push(['putPolicy', input])
      return { PolicyARN: 'arn:aws:autoscaling:1', Alarms: [] }
    },
    deleteApplicationAutoScalingScalingPolicy: async (input, confirmation) => {
      calls.push(['deletePolicy', input, confirmation])
      return {}
    },
    putApplicationAutoScalingScheduledAction: async (input) => {
      calls.push(['putSchedule', input])
      return {}
    },
    deleteApplicationAutoScalingScheduledAction: async (input, confirmation) => {
      calls.push(['deleteSchedule', input, confirmation])
      return {}
    },
  }
  const imports = {
    react: React,
    'react/jsx-runtime': jsxRuntime,
    '../../../ui/EmptyState': { EmptyState: () => null },
    '../../../ui/Panel': { Panel: wrap('section') },
    '../../../ui/Button': { Button: wrap('button') },
    '../../../ui/Confirm': { dangerConfirm: (options) => options, useConfirm: () => async () => true },
    '../../api/hooks/useDashboardEvents': { useDashboardEvents: () => {} },
    '../../api/hooks/useLatestRequest': latestRequestHook,
    './api': api,
  }
  const source = await readFile(
    new URL('../src/app/services/applicationautoscaling/ApplicationAutoScalingDashboard.tsx', import.meta.url),
    'utf8',
  )
  const { ApplicationAutoScalingDashboard } = loadSource(source, imports)
  let renderer
  await act(async () => {
    renderer = create(React.createElement(ApplicationAutoScalingDashboard))
  })
  t.after(async () => {
    await act(async () => {
      renderer.unmount()
    })
  })

  const targetInput = renderer.root.findByProps({ 'aria-label': 'Scalable target resource ID' })
  await act(async () => {
    targetInput.props.onChange({ target: { value: 'table/items' } })
  })
  const forms = renderer.root.findAllByType('form')
  await act(async () => {
    forms[0].props.onSubmit({ preventDefault: () => {} })
  })
  assert.equal(calls[0][0], 'register')
  assert.equal(calls[0][1].ResourceId, 'table/items')

  const policyInput = renderer.root.findByProps({ 'aria-label': 'Scaling policy name' })
  await act(async () => {
    policyInput.props.onChange({ target: { value: 'items-policy' } })
  })
  await act(async () => {
    forms[1].props.onSubmit({ preventDefault: () => {} })
  })
  assert.equal(calls[1][0], 'putPolicy')
  assert.equal(calls[1][1].PolicyName, 'items-policy')

  const scheduleInput = renderer.root.findByProps({ 'aria-label': 'Scheduled action name' })
  await act(async () => {
    scheduleInput.props.onChange({ target: { value: 'morning-scale' } })
  })
  await act(async () => {
    forms[2].props.onSubmit({ preventDefault: () => {} })
  })
  assert.equal(calls[2][0], 'putSchedule')
  assert.equal(calls[2][1].ScheduledActionName, 'morning-scale')

  const deregisterBtn = renderer.root.find(
    (node) => node.type === 'button' && node.children.includes('Deregister'),
  )
  await act(async () => {
    deregisterBtn.props.onClick()
  })
  assert.equal(calls[3][0], 'deregister')
  assert.equal(calls[3][2], 'table/orders')
})

test('CloudRunDashboard creates, updates, toggles IAM invoker, and deletes services', async (t) => {
  const calls = []
  const services = [
    {
      name: 'projects/devcloud/locations/us-central1/services/hello',
      uid: 'u1',
      generation: '1',
      createTime: '2026-01-01T00:00:00Z',
      updateTime: '2026-01-01T00:00:00Z',
      uri: 'http://hello.us-central1.devcloud.run.localhost:18095',
      latestReadyRevision: 'projects/devcloud/locations/us-central1/services/hello/revisions/hello-00001-abc',
      latestCreatedRevision: 'projects/devcloud/locations/us-central1/services/hello/revisions/hello-00001-abc',
      ingress: 'INGRESS_TRAFFIC_ALL',
      template: {
        containers: [{ image: 'us-docker.pkg.dev/cloudrun/container/hello', env: [{ name: 'A', value: '1' }] }],
      },
    },
  ]
  const revisions = [
    {
      name: 'projects/devcloud/locations/us-central1/services/hello/revisions/hello-00001-abc',
      createTime: '2026-01-01T00:00:00Z',
      containers: [{ image: 'us-docker.pkg.dev/cloudrun/container/hello' }],
    },
    {
      name: 'projects/devcloud/locations/us-central1/services/hello/revisions/hello-00000-old',
      createTime: '2025-12-31T00:00:00Z',
      containers: [{ image: 'us-docker.pkg.dev/cloudrun/container/hello' }],
    },
  ]
  const api = {
    getCloudRunStatus: async () => ({
      service: 'cloudrun',
      status: 'running',
      running: true,
      endpoint: 'http://127.0.0.1:18095',
      project: 'devcloud',
      region: 'us-central1',
      storagePath: '.devcloud/data/cloudrun',
      serviceCount: 1,
      instanceCount: 0,
    }),
    listCloudRunServices: async () => ({ services }),
    listCloudRunInstances: async () => ({ instances: [] }),
    listCloudRunRevisions: async () => ({ revisions }),
    getCloudRunLogs: async () => ({ lines: [] }),
    getCloudRunIamPolicy: async () => ({ bindings: [] }),
    createCloudRunService: async (input) => {
      calls.push(['create', input])
      return { name: 'op1', done: true }
    },
    updateCloudRunService: async (name, input) => {
      calls.push(['update', name, input])
      return { name: 'op2', done: true }
    },
    deleteCloudRunService: async (name, confirmation) => {
      calls.push(['deleteService', name, confirmation])
      return { name: 'op3', done: true }
    },
    deleteCloudRunRevision: async (serviceName, revId) => {
      calls.push(['deleteRevision', serviceName, revId])
      return { name: 'op4', done: true }
    },
    setCloudRunIamPolicy: async (name, policy) => {
      calls.push(['setIam', name, policy])
      return policy
    },
  }
  const imports = {
    react: React,
    'react/jsx-runtime': jsxRuntime,
    '../../../ui/EmptyState': { EmptyState: () => null },
    '../../../ui/Panel': { Panel: wrap('section') },
    '../../../ui/Button': { Button: wrap('button') },
    '../../../ui/Confirm': { dangerConfirm: (options) => options, useConfirm: () => async () => true },
    '../../api/hooks/useDashboardEvents': { useDashboardEvents: () => {} },
    '../../api/hooks/useLatestRequest': latestRequestHook,
    './api': api,
  }
  const source = await readFile(
    new URL('../src/app/services/cloudrun/CloudRunDashboard.tsx', import.meta.url),
    'utf8',
  )
  const { CloudRunDashboard } = loadSource(source, imports)
  let renderer
  await act(async () => {
    renderer = create(React.createElement(CloudRunDashboard))
  })
  t.after(async () => {
    await act(async () => {
      renderer.unmount()
    })
  })

  const svcInput = renderer.root.findByProps({ 'aria-label': 'New Cloud Run service ID' })
  await act(async () => {
    svcInput.props.onChange({ target: { value: 'api-svc' } })
  })
  const forms = renderer.root.findAllByType('form')
  await act(async () => {
    forms[0].props.onSubmit({ preventDefault: () => {} })
  })
  assert.equal(calls[0][0], 'create')
  assert.equal(calls[0][1].serviceId, 'api-svc')

  await act(async () => {
    forms[1].props.onSubmit({ preventDefault: () => {} })
  })
  assert.equal(calls[1][0], 'update')
  assert.equal(calls[1][1], 'projects/devcloud/locations/us-central1/services/hello')

  const iamBtn = renderer.root.find(
    (node) => node.type === 'button' && node.children.includes('Allow public invoker (allUsers)'),
  )
  await act(async () => {
    iamBtn.props.onClick()
  })
  assert.equal(calls[2][0], 'setIam')

  const delRevBtn = renderer.root.find(
    (node) => node.type === 'button' && node.children.includes('Delete revision'),
  )
  await act(async () => {
    delRevBtn.props.onClick()
  })
  assert.equal(calls[3][0], 'deleteRevision')
  assert.equal(calls[3][2], 'hello-00000-old')

  const delSvcBtn = renderer.root.find(
    (node) => node.type === 'button' && node.children.includes('Delete service'),
  )
  await act(async () => {
    delSvcBtn.props.onClick()
  })
  assert.equal(calls[4][0], 'deleteService')
  assert.equal(calls[4][2], 'hello')
})
