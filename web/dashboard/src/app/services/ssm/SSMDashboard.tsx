import { useCallback, useEffect, useState } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useLatestRequest } from '../../api/hooks/useLatestRequest'
import type { DashboardService } from '../dashboard/types'
import { getSSMStatus, listSSMParameters } from './api'
import type { SSMParameter, SSMStatus } from './types'

type SSMState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: SSMStatus; parameters: SSMParameter[] }
  | { status: 'error'; message: string }

type SSMDashboardProps = {
  service?: DashboardService
}

export function SSMDashboard({ service }: SSMDashboardProps): JSX.Element {
  const [state, setState] = useState<SSMState>({ status: 'loading' })
  const isDisabled = service?.status === 'disabled'

  const startRefresh = useLatestRequest()
  const refresh = useCallback(() => {
    const isCurrent = startRefresh()
    if (isDisabled) return
    setState({ status: 'loading' })
    Promise.all([getSSMStatus(), listSSMParameters()])
      .then(([statusPayload, parametersPayload]) => {
        if (!isCurrent()) return
        setState({ status: 'success', statusPayload, parameters: parametersPayload.parameters })
      })
      .catch((error: Error) => {
        if (!isCurrent()) return
        setState({ status: 'error', message: error.message })
      })
  }, [startRefresh, isDisabled])

  useEffect(() => {
    refresh()
  }, [refresh])

  if (isDisabled) {
    return (
      <Panel title="SSM Parameter Store">
        <EmptyState
          title="SSM Parameter Store is disabled"
          description="Enable the ssm service in devcloud config to inspect parameters."
        />
      </Panel>
    )
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
          <EmptyState title="Loading SSM Parameter Store" description="Reading local parameter metadata." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="SSM Parameter Store unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? (
          <dl className="inspector-list">
            <div>
              <dt>Endpoint</dt>
              <dd>
                <code>{state.statusPayload.endpoint}</code>
              </dd>
            </div>
            <div>
              <dt>Region</dt>
              <dd>{state.statusPayload.region}</dd>
            </div>
            <div>
              <dt>Storage path</dt>
              <dd>
                <code>{state.statusPayload.storagePath}</code>
              </dd>
            </div>
            <div>
              <dt>Parameters</dt>
              <dd>{state.statusPayload.parameterCount}</dd>
            </div>
          </dl>
        ) : null}
      </Panel>

      <Panel title="Parameters">
        <ParameterList parameters={state.status === 'success' ? state.parameters : []} />
      </Panel>
    </div>
  )
}

function ParameterList({ parameters }: { parameters: SSMParameter[] }): JSX.Element {
  if (parameters.length === 0) {
    return <EmptyState title="No parameters" description="Parameters created via PutParameter will appear here. Values are not shown." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="SSM parameters">
      {parameters.map((parameter) => (
        <section className="dynamodb-table-row" key={parameter.Name}>
          <span className="table-row-top">
            <span className="table-row-name">{parameter.Name}</span>
            <span className="count-pill">v{parameter.Version}</span>
          </span>
          <span className="table-row-meta">{parameter.Description ?? new Date(parameter.LastModifiedDate * 1000).toISOString()}</span>
          <span className="table-row-tags">
            <span>{parameter.Type}</span>
            <span>{parameter.Tier}</span>
            {parameter.KeyId ? <span>{parameter.KeyId}</span> : null}
          </span>
        </section>
      ))}
    </div>
  )
}
