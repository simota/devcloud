import { useCallback, useEffect, useState } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import type { DashboardService } from '../dashboard/types'
import {
  getApplicationAutoScalingStatus,
  listApplicationAutoScalingScalableTargets,
  listApplicationAutoScalingScalingPolicies,
  listApplicationAutoScalingScheduledActions,
} from './api'
import type {
  ApplicationAutoScalingScalableTarget,
  ApplicationAutoScalingScalingPolicy,
  ApplicationAutoScalingScheduledAction,
  ApplicationAutoScalingStatus,
} from './types'

type ApplicationAutoScalingState =
  | { status: 'loading' }
  | {
      status: 'success'
      statusPayload: ApplicationAutoScalingStatus
      scalableTargets: ApplicationAutoScalingScalableTarget[]
      scalingPolicies: ApplicationAutoScalingScalingPolicy[]
      scheduledActions: ApplicationAutoScalingScheduledAction[]
    }
  | { status: 'error'; message: string }

type ApplicationAutoScalingDashboardProps = {
  service?: DashboardService
}

export function ApplicationAutoScalingDashboard({ service }: ApplicationAutoScalingDashboardProps): JSX.Element {
  const [state, setState] = useState<ApplicationAutoScalingState>({ status: 'loading' })
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      setState({
        status: 'success',
        statusPayload: disabledStatus(service),
        scalableTargets: [],
        scalingPolicies: [],
        scheduledActions: [],
      })
      return
    }

    setState({ status: 'loading' })
    Promise.all([
      getApplicationAutoScalingStatus(),
      listApplicationAutoScalingScalableTargets(),
      listApplicationAutoScalingScalingPolicies(),
      listApplicationAutoScalingScheduledActions(),
    ])
      .then(([statusPayload, targetsPayload, policiesPayload, actionsPayload]) => {
        setState({
          status: 'success',
          statusPayload,
          scalableTargets: targetsPayload.scalableTargets,
          scalingPolicies: policiesPayload.scalingPolicies,
          scheduledActions: actionsPayload.scheduledActions,
        })
      })
      .catch((error: Error) => {
        setState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refresh()
  }, [refresh])

  useDashboardEvents({ topics: ['applicationautoscaling'], onEvent: refresh, enabled: !isDisabled })

  if (isDisabled) {
    return (
      <Panel title="Application Auto Scaling">
        <EmptyState
          title="Application Auto Scaling is disabled"
          description="Enable the Application Auto Scaling service in devcloud config to inspect scalable targets, policies, and scheduled actions."
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
          <EmptyState title="Loading Application Auto Scaling" description="Reading local scalable targets, policies, and scheduled actions." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="Application Auto Scaling unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <StatusSummary status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Scalable targets">
        <ScalableTargetList targets={state.status === 'success' ? state.scalableTargets : []} />
      </Panel>

      <Panel title="Scaling policies">
        <ScalingPolicyList policies={state.status === 'success' ? state.scalingPolicies : []} />
      </Panel>

      <Panel title="Scheduled actions">
        <ScheduledActionList actions={state.status === 'success' ? state.scheduledActions : []} />
      </Panel>
    </div>
  )
}

function StatusSummary({ status }: { status: ApplicationAutoScalingStatus }): JSX.Element {
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
        <dt>Scalable targets</dt>
        <dd>{status.scalableTargetCount}</dd>
      </div>
      <div>
        <dt>Scaling policies</dt>
        <dd>{status.scalingPolicyCount}</dd>
      </div>
      <div>
        <dt>Scheduled actions</dt>
        <dd>{status.scheduledActionCount}</dd>
      </div>
    </dl>
  )
}

function ScalableTargetList({ targets }: { targets: ApplicationAutoScalingScalableTarget[] }): JSX.Element {
  if (targets.length === 0) {
    return <EmptyState title="No scalable targets" description="Targets registered via RegisterScalableTarget will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Application Auto Scaling scalable targets">
      {targets.map((target) => (
        <section className="dynamodb-table-row" key={`${target.ResourceId}-${target.ScalableDimension}`}>
          <span className="table-row-top">
            <span className="table-row-name">{target.ResourceId}</span>
            <span className="count-pill">
              {target.MinCapacity}-{target.MaxCapacity}
            </span>
          </span>
          <span className="table-row-meta">{target.ScalableDimension}</span>
          <span className="table-row-tags">
            <span>{target.ServiceNamespace}</span>
            {target.SuspendedState?.DynamicScalingInSuspended ||
            target.SuspendedState?.DynamicScalingOutSuspended ||
            target.SuspendedState?.ScheduledScalingSuspended ? (
              <span>suspended</span>
            ) : null}
          </span>
        </section>
      ))}
    </div>
  )
}

function ScalingPolicyList({ policies }: { policies: ApplicationAutoScalingScalingPolicy[] }): JSX.Element {
  if (policies.length === 0) {
    return <EmptyState title="No scaling policies" description="Policies created via PutScalingPolicy will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Application Auto Scaling scaling policies">
      {policies.map((policy) => (
        <section className="dynamodb-table-row" key={policy.PolicyARN}>
          <span className="table-row-top">
            <span className="table-row-name">{policy.PolicyName}</span>
            <span className="count-pill">{policy.PolicyType}</span>
          </span>
          <span className="table-row-meta">{policy.ResourceId}</span>
          <span className="table-row-tags">
            <span>{policy.ScalableDimension}</span>
          </span>
        </section>
      ))}
    </div>
  )
}

function ScheduledActionList({ actions }: { actions: ApplicationAutoScalingScheduledAction[] }): JSX.Element {
  if (actions.length === 0) {
    return <EmptyState title="No scheduled actions" description="Actions created via PutScheduledAction will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Application Auto Scaling scheduled actions">
      {actions.map((action) => (
        <section className="dynamodb-table-row" key={action.ScheduledActionName}>
          <span className="table-row-top">
            <span className="table-row-name">{action.ScheduledActionName}</span>
          </span>
          <span className="table-row-meta">{action.ResourceId}</span>
          <span className="table-row-tags">
            <span>{action.Schedule}</span>
            {action.Timezone ? <span>{action.Timezone}</span> : null}
          </span>
        </section>
      ))}
    </div>
  )
}

function disabledStatus(service?: DashboardService): ApplicationAutoScalingStatus {
  return {
    service: 'applicationautoscaling',
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:18030',
    region: 'us-east-1',
    storagePath: service?.storagePath ?? '.devcloud/data/applicationautoscaling',
    scalableTargetCount: 0,
    scalingPolicyCount: 0,
    scheduledActionCount: 0,
  }
}
