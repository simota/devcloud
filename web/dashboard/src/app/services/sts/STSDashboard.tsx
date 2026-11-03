import { useCallback, useEffect, useState } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useLatestRequest } from '../../api/hooks/useLatestRequest'
import type { DashboardService } from '../dashboard/types'
import { getSTSCallerIdentity, getSTSStatus } from './api'
import type { STSCallerIdentity, STSStatus } from './types'

type STSState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: STSStatus; identity: STSCallerIdentity }
  | { status: 'error'; message: string }

type STSDashboardProps = {
  service?: DashboardService
}

export function STSDashboard({ service }: STSDashboardProps): JSX.Element {
  const [state, setState] = useState<STSState>({ status: 'loading' })
  const isDisabled = service?.status === 'disabled'

  const startRefresh = useLatestRequest()
  const refresh = useCallback(() => {
    const isCurrent = startRefresh()
    if (isDisabled) return
    setState({ status: 'loading' })
    Promise.all([getSTSStatus(), getSTSCallerIdentity()])
      .then(([statusPayload, identity]) => {
        if (!isCurrent()) return
        setState({ status: 'success', statusPayload, identity })
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
      <Panel title="STS">
        <EmptyState title="STS is disabled" description="Enable the sts service in devcloud config to issue temporary credentials." />
      </Panel>
    )
  }

  return (
    <Panel title="Status">
      <div className="dynamodb-toolbar">
        <span className="toolbar-count">
          {state.status === 'success' ? `${state.statusPayload.status} / ${state.statusPayload.region}` : 'Loading'}
        </span>
        <Button onClick={refresh}>Refresh</Button>
      </div>
      {state.status === 'loading' ? <EmptyState title="Loading STS" description="Calling GetCallerIdentity." /> : null}
      {state.status === 'error' ? (
        <EmptyState title="STS unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
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
            <dt>Account</dt>
            <dd>{state.identity.account}</dd>
          </div>
          <div>
            <dt>Caller ARN</dt>
            <dd>
              <code>{state.identity.arn}</code>
            </dd>
          </div>
          <div>
            <dt>Operations</dt>
            <dd>AssumeRole (inline session policies accepted, not enforced), GetCallerIdentity</dd>
          </div>
        </dl>
      ) : null}
    </Panel>
  )
}
