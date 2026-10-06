import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import type { DashboardService } from '../dashboard/types'
import { getLambdaStatus, invokeLambdaFunction, listLambdaFunctions, listLambdaInvocations } from './api'
import type { LambdaFunction, LambdaInvocation, LambdaInvokeResult, LambdaStatus } from './types'

type LambdaState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: LambdaStatus; functions: LambdaFunction[]; invocations: LambdaInvocation[] }
  | { status: 'error'; message: string }

type InvokeState =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'done'; result: LambdaInvokeResult }
  | { status: 'error'; message: string }

type LambdaDashboardProps = {
  service?: DashboardService
}

export function LambdaDashboard({ service }: LambdaDashboardProps): JSX.Element {
  const [state, setState] = useState<LambdaState>({ status: 'loading' })
  const [selected, setSelected] = useState<string>()
  const [eventJSON, setEventJSON] = useState('{}')
  // Keyed by function name; each invoke also records its request id so a late
  // response can only land on the function (and attempt) it belongs to. Maps,
  // not plain objects: `constructor`, `toString`, `__proto__` are valid names.
  const [invokes, setInvokes] = useState<ReadonlyMap<string, InvokeState>>(() => new Map())
  const latestInvoke = useRef(new Map<string, number>())
  const invokeSeq = useRef(0)
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      return
    }
    Promise.all([getLambdaStatus(), listLambdaFunctions(), listLambdaInvocations()])
      .then(([statusPayload, functionsPayload, invocationsPayload]) => {
        setState({
          status: 'success',
          statusPayload,
          functions: functionsPayload.functions,
          invocations: invocationsPayload.invocations,
        })
        setSelected((current) =>
          current && functionsPayload.functions.some((fn) => fn.FunctionName === current)
            ? current
            : functionsPayload.functions[0]?.FunctionName,
        )
      })
      .catch((error: Error) => {
        setState({ status: 'error', message: error.message })
      })
  }, [isDisabled])

  useEffect(() => {
    refresh()
  }, [refresh])

  useDashboardEvents({ topics: ['lambda'], onEvent: refresh, enabled: !isDisabled })

  if (isDisabled) {
    return (
      <Panel title="Lambda">
        <EmptyState
          title="Lambda is disabled"
          description="Enable the Lambda service in devcloud config to deploy and invoke local functions."
        />
      </Panel>
    )
  }

  const functions = state.status === 'success' ? state.functions : []
  const invocations = state.status === 'success' ? state.invocations : []
  const active = functions.find((fn) => fn.FunctionName === selected)
  const invoke: InvokeState = (active && invokes.get(active.FunctionName)) || { status: 'idle' }

  const setInvokeFor = (name: string, requestId: number, next: InvokeState) => {
    if (latestInvoke.current.get(name) !== requestId) {
      return
    }
    setInvokes((prev) => new Map(prev).set(name, next))
  }

  const onInvoke = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!active) {
      return
    }
    const name = active.FunctionName
    invokeSeq.current += 1
    const requestId = invokeSeq.current
    latestInvoke.current.set(name, requestId)
    try {
      JSON.parse(eventJSON || '{}')
    } catch (error) {
      setInvokeFor(name, requestId, { status: 'error', message: `Event is not valid JSON: ${(error as Error).message}` })
      return
    }
    setInvokeFor(name, requestId, { status: 'running' })
    invokeLambdaFunction(name, eventJSON || '{}')
      .then((result) => setInvokeFor(name, requestId, { status: 'done', result }))
      .catch((error: Error) => setInvokeFor(name, requestId, { status: 'error', message: error.message }))
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Status">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {state.status === 'success' ? `${state.statusPayload.status} / ${state.statusPayload.region}` : 'Loading'}
          </span>
          <Button onClick={refresh}>Refresh</Button>
        </div>
        {state.status === 'loading' ? (
          <EmptyState title="Loading Lambda" description="Reading local functions and recent invocations." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="Lambda unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <StatusSummary status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Functions">
        {functions.length === 0 ? (
          <EmptyState title="No functions" description="Functions created via CreateFunction will appear here." />
        ) : (
          <div className="dynamodb-table-list" aria-label="Lambda functions">
            {functions.map((fn) => (
              <button
                type="button"
                className={`dynamodb-table-row${fn.FunctionName === selected ? ' active' : ''}`}
                aria-pressed={fn.FunctionName === selected}
                key={fn.FunctionArn}
                onClick={() => setSelected(fn.FunctionName)}
              >
                <span className="table-row-top">
                  <span className="table-row-name">{fn.FunctionName}</span>
                  <span className="count-pill">{fn.Runtime}</span>
                </span>
                <span className="table-row-meta">{fn.Handler}</span>
                <span className="table-row-tags">
                  <span>{fn.MemorySize} MB</span>
                  <span>{fn.Timeout}s</span>
                  <span>{fn.State}</span>
                </span>
              </button>
            ))}
          </div>
        )}
      </Panel>

      {active ? (
        <Panel title={`Function: ${active.FunctionName}`}>
          <FunctionDetails fn={active} />
          <form className="pubsub-action-form stacked" onSubmit={onInvoke}>
            <label className="compact-filter wide">
              <span>Test event (JSON)</span>
              <textarea
                aria-label="Lambda test event JSON"
                onChange={(event) => setEventJSON(event.target.value)}
                rows={4}
                value={eventJSON}
              />
            </label>
            <Button type="submit" disabled={invoke.status === 'running'}>
              {invoke.status === 'running' ? 'Invoking…' : 'Invoke'}
            </Button>
          </form>
          <InvokeOutput state={invoke} />
        </Panel>
      ) : null}

      <Panel title="Recent invocations">
        {invocations.length === 0 ? (
          <EmptyState title="No invocations yet" description="Invoke a function to see its result and log tail here." />
        ) : (
          <div className="dynamodb-table-list" aria-label="Lambda invocations">
            {invocations.map((inv) => (
              <details className="dynamodb-table-row" key={inv.requestId}>
                <summary className="table-row-top">
                  <span className="table-row-name">{inv.functionName}</span>
                  <span className="count-pill">{inv.status}</span>
                </summary>
                <span className="table-row-meta">
                  {inv.startedAt} · {inv.invocationType} · {inv.durationMs.toFixed(1)} ms
                </span>
                <pre className="redis-value-pre">{inv.logTail || '(no log output)'}</pre>
              </details>
            ))}
          </div>
        )}
      </Panel>
    </div>
  )
}

function StatusSummary({ status }: { status: LambdaStatus }): JSX.Element {
  return (
    <dl className="inspector-list">
      <div>
        <dt>Endpoint</dt>
        <dd>
          <code>{status.endpoint}</code>
        </dd>
      </div>
      <div>
        <dt>Region</dt>
        <dd>{status.region}</dd>
      </div>
      <div>
        <dt>Storage path</dt>
        <dd>
          <code>{status.storagePath}</code>
        </dd>
      </div>
      <div>
        <dt>Functions</dt>
        <dd>{status.functionCount}</dd>
      </div>
      <div>
        <dt>Recorded invocations</dt>
        <dd>{status.invocationCount}</dd>
      </div>
    </dl>
  )
}

function FunctionDetails({ fn }: { fn: LambdaFunction }): JSX.Element {
  const env = Object.keys(fn.Environment?.Variables ?? {})
  return (
    <dl className="inspector-list">
      <div>
        <dt>ARN</dt>
        <dd>
          <code>{fn.FunctionArn}</code>
        </dd>
      </div>
      <div>
        <dt>Role</dt>
        <dd>
          <code>{fn.Role}</code>
        </dd>
      </div>
      <div>
        <dt>Code</dt>
        <dd>
          {fn.CodeSize} bytes · <code>{fn.CodeSha256}</code>
        </dd>
      </div>
      <div>
        <dt>Last modified</dt>
        <dd>{fn.LastModified}</dd>
      </div>
      <div>
        <dt>Environment keys</dt>
        <dd>{env.length > 0 ? env.join(', ') : 'none'}</dd>
      </div>
    </dl>
  )
}

function InvokeOutput({ state }: { state: InvokeState }): JSX.Element | null {
  if (state.status === 'idle' || state.status === 'running') {
    return null
  }
  if (state.status === 'error') {
    return <EmptyState title="Invoke failed" description={state.message} />
  }
  const { result } = state
  return (
    <div aria-label="Lambda invoke result">
      <p className="inspector-muted">
        {result.functionError ? `Function error: ${result.functionError}` : `Succeeded (HTTP ${result.statusCode})`}
      </p>
      <pre className="redis-value-pre">{result.payload}</pre>
      <pre className="redis-value-pre">{result.log}</pre>
    </div>
  )
}
