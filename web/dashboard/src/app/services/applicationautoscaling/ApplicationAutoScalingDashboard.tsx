import { useCallback, useEffect, useState, type FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import { useLatestRequest } from '../../api/hooks/useLatestRequest'
import type { DashboardService } from '../dashboard/types'
import {
  deleteApplicationAutoScalingScalingPolicy,
  deleteApplicationAutoScalingScheduledAction,
  deregisterApplicationAutoScalingScalableTarget,
  getApplicationAutoScalingStatus,
  listApplicationAutoScalingScalableTargets,
  listApplicationAutoScalingScalingPolicies,
  listApplicationAutoScalingScheduledActions,
  putApplicationAutoScalingScalingPolicy,
  putApplicationAutoScalingScheduledAction,
  registerApplicationAutoScalingScalableTarget,
} from './api'
import type {
  ApplicationAutoScalingScalableTarget,
  ApplicationAutoScalingScalingPolicy,
  ApplicationAutoScalingScheduledAction,
  ApplicationAutoScalingStatus,
} from './types'

const DYNAMODB_DIMENSIONS = [
  'dynamodb:table:ReadCapacityUnits',
  'dynamodb:table:WriteCapacityUnits',
  'dynamodb:index:ReadCapacityUnits',
  'dynamodb:index:WriteCapacityUnits',
] as const

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
  const [targetResourceId, setTargetResourceId] = useState('')
  const [targetDimension, setTargetDimension] = useState<string>(DYNAMODB_DIMENSIONS[1])
  const [targetMinCapacity, setTargetMinCapacity] = useState('1')
  const [targetMaxCapacity, setTargetMaxCapacity] = useState('10')

  const [policyName, setPolicyName] = useState('')
  const [policyResourceId, setPolicyResourceId] = useState('')
  const [policyDimension, setPolicyDimension] = useState<string>(DYNAMODB_DIMENSIONS[1])
  const [policyTargetValue, setPolicyTargetValue] = useState('70')

  const [scheduleName, setScheduleName] = useState('')
  const [scheduleResourceId, setScheduleResourceId] = useState('')
  const [scheduleDimension, setScheduleDimension] = useState<string>(DYNAMODB_DIMENSIONS[1])
  const [scheduleExpression, setScheduleExpression] = useState('rate(1 day)')
  const [scheduleMinCapacity, setScheduleMinCapacity] = useState('2')
  const [scheduleMaxCapacity, setScheduleMaxCapacity] = useState('8')

  const [actionMessage, setActionMessage] = useState('')
  const [actionError, setActionError] = useState('')
  const [busyAction, setBusyAction] = useState<string>()
  const confirm = useConfirm()
  const isDisabled = service?.status === 'disabled'

  const startRefresh = useLatestRequest()
  const refresh = useCallback(() => {
    const isCurrent = startRefresh()
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
        if (!isCurrent()) return
        setState({
          status: 'success',
          statusPayload,
          scalableTargets: targetsPayload.scalableTargets,
          scalingPolicies: policiesPayload.scalingPolicies,
          scheduledActions: actionsPayload.scheduledActions,
        })
        const firstTarget = targetsPayload.scalableTargets[0]
        if (firstTarget) {
          setPolicyResourceId((curr) => curr || firstTarget.ResourceId)
          setPolicyDimension((curr) => curr || firstTarget.ScalableDimension)
          setScheduleResourceId((curr) => curr || firstTarget.ResourceId)
          setScheduleDimension((curr) => curr || firstTarget.ScalableDimension)
        }
      })
      .catch((error: Error) => {
        if (!isCurrent()) return
        setState({ status: 'error', message: error.message })
      })
  }, [startRefresh, isDisabled, service])

  useEffect(() => {
    refresh()
  }, [refresh])

  useDashboardEvents({ topics: ['applicationautoscaling'], onEvent: refresh, onResync: refresh, enabled: !isDisabled })

  async function runAction(name: string, action: () => Promise<string>): Promise<void> {
    setBusyAction(name)
    setActionError('')
    setActionMessage('')
    try {
      const message = await action()
      setActionMessage(message)
      refresh()
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Application Auto Scaling operation failed')
    } finally {
      setBusyAction(undefined)
    }
  }

  function handleRegisterTarget(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const resourceId = targetResourceId.trim()
    const min = Number.parseInt(targetMinCapacity, 10)
    const max = Number.parseInt(targetMaxCapacity, 10)
    if (!resourceId) {
      setActionError('ResourceId is required (e.g. table/orders)')
      return
    }
    if (Number.isNaN(min) || Number.isNaN(max) || min < 0 || max < min) {
      setActionError('MinCapacity and MaxCapacity must be valid non-negative integers with max >= min')
      return
    }
    void runAction('register-target', async () => {
      await registerApplicationAutoScalingScalableTarget({
        ServiceNamespace: 'dynamodb',
        ResourceId: resourceId,
        ScalableDimension: targetDimension,
        MinCapacity: min,
        MaxCapacity: max,
      })
      setTargetResourceId('')
      setPolicyResourceId((curr) => curr || resourceId)
      setScheduleResourceId((curr) => curr || resourceId)
      return `Registered scalable target ${resourceId} (${targetDimension})`
    })
  }

  async function handleDeregisterTarget(target: ApplicationAutoScalingScalableTarget): Promise<void> {
    const ok = await confirm(
      dangerConfirm({
        title: 'Deregister scalable target',
        description: `Deregister ${target.ResourceId} (${target.ScalableDimension}) and remove its attached policies and scheduled actions?`,
        target: target.ResourceId,
        confirmLabel: 'Deregister',
      }),
    )
    if (!ok) return
    await runAction(`deregister-${target.ResourceId}-${target.ScalableDimension}`, async () => {
      await deregisterApplicationAutoScalingScalableTarget(
        {
          ServiceNamespace: target.ServiceNamespace,
          ResourceId: target.ResourceId,
          ScalableDimension: target.ScalableDimension,
        },
        target.ResourceId,
      )
      return `Deregistered scalable target ${target.ResourceId}`
    })
  }

  function handlePutPolicy(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const name = policyName.trim()
    const resourceId = policyResourceId.trim()
    const targetValue = Number.parseFloat(policyTargetValue)
    if (!name || !resourceId) {
      setActionError('PolicyName and ResourceId are required')
      return
    }
    if (Number.isNaN(targetValue) || targetValue <= 0) {
      setActionError('TargetValue must be a positive number')
      return
    }
    const metricType = policyDimension.includes('ReadCapacityUnits')
      ? 'DynamoDBReadCapacityUtilization'
      : 'DynamoDBWriteCapacityUtilization'
    void runAction('put-policy', async () => {
      await putApplicationAutoScalingScalingPolicy({
        ServiceNamespace: 'dynamodb',
        PolicyName: name,
        ResourceId: resourceId,
        ScalableDimension: policyDimension,
        PolicyType: 'TargetTrackingScaling',
        TargetTrackingScalingPolicyConfiguration: {
          TargetValue: targetValue,
          PredefinedMetricSpecification: { PredefinedMetricType: metricType },
        },
      })
      setPolicyName('')
      return `Saved scaling policy ${name}`
    })
  }

  async function handleDeletePolicy(policy: ApplicationAutoScalingScalingPolicy): Promise<void> {
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete scaling policy',
        description: `Delete scaling policy ${policy.PolicyName} on ${policy.ResourceId}?`,
        target: policy.PolicyName,
        confirmLabel: 'Delete',
      }),
    )
    if (!ok) return
    await runAction(`delete-policy-${policy.PolicyARN}`, async () => {
      await deleteApplicationAutoScalingScalingPolicy(
        {
          ServiceNamespace: policy.ServiceNamespace,
          PolicyName: policy.PolicyName,
          ResourceId: policy.ResourceId,
          ScalableDimension: policy.ScalableDimension,
        },
        policy.PolicyName,
      )
      return `Deleted scaling policy ${policy.PolicyName}`
    })
  }

  function handlePutScheduledAction(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const name = scheduleName.trim()
    const resourceId = scheduleResourceId.trim()
    const schedule = scheduleExpression.trim()
    const min = Number.parseInt(scheduleMinCapacity, 10)
    const max = Number.parseInt(scheduleMaxCapacity, 10)
    if (!name || !resourceId || !schedule) {
      setActionError('ScheduledActionName, ResourceId, and Schedule are required')
      return
    }
    if (Number.isNaN(min) || Number.isNaN(max) || min < 0 || max < min) {
      setActionError('Scheduled MinCapacity and MaxCapacity must be valid integers with max >= min')
      return
    }
    void runAction('put-schedule', async () => {
      await putApplicationAutoScalingScheduledAction({
        ServiceNamespace: 'dynamodb',
        ScheduledActionName: name,
        ResourceId: resourceId,
        ScalableDimension: scheduleDimension,
        Schedule: schedule,
        ScalableTargetAction: {
          MinCapacity: min,
          MaxCapacity: max,
        },
      })
      setScheduleName('')
      return `Saved scheduled action ${name}`
    })
  }

  async function handleDeleteScheduledAction(action: ApplicationAutoScalingScheduledAction): Promise<void> {
    const dimension = action.ScalableDimension ?? DYNAMODB_DIMENSIONS[1]
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete scheduled action',
        description: `Delete scheduled action ${action.ScheduledActionName} on ${action.ResourceId}?`,
        target: action.ScheduledActionName,
        confirmLabel: 'Delete',
      }),
    )
    if (!ok) return
    await runAction(`delete-schedule-${action.ScheduledActionName}`, async () => {
      await deleteApplicationAutoScalingScheduledAction(
        {
          ServiceNamespace: action.ServiceNamespace,
          ScheduledActionName: action.ScheduledActionName,
          ResourceId: action.ResourceId,
          ScalableDimension: dimension,
        },
        action.ScheduledActionName,
      )
      return `Deleted scheduled action ${action.ScheduledActionName}`
    })
  }

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
        {actionError ? <p className="operation-message error">{actionError}</p> : null}
        {actionMessage ? <p className="operation-message success">{actionMessage}</p> : null}
        {state.status === 'loading' ? (
          <EmptyState title="Loading Application Auto Scaling" description="Reading local scalable targets, policies, and scheduled actions." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="Application Auto Scaling unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <StatusSummary status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Scalable targets">
        <form className="pubsub-action-form" onSubmit={handleRegisterTarget}>
          <label className="compact-filter">
            <span>Resource ID</span>
            <input
              aria-label="Scalable target resource ID"
              onChange={(event) => setTargetResourceId(event.target.value)}
              placeholder="table/orders"
              value={targetResourceId}
            />
          </label>
          <label className="compact-filter">
            <span>Dimension</span>
            <select
              aria-label="Scalable target dimension"
              onChange={(event) => setTargetDimension(event.target.value)}
              value={targetDimension}
            >
              {DYNAMODB_DIMENSIONS.map((dim) => (
                <option key={dim} value={dim}>
                  {dim}
                </option>
              ))}
            </select>
          </label>
          <label className="compact-filter small">
            <span>Min</span>
            <input
              aria-label="Scalable target min capacity"
              inputMode="numeric"
              onChange={(event) => setTargetMinCapacity(event.target.value)}
              value={targetMinCapacity}
            />
          </label>
          <label className="compact-filter small">
            <span>Max</span>
            <input
              aria-label="Scalable target max capacity"
              inputMode="numeric"
              onChange={(event) => setTargetMaxCapacity(event.target.value)}
              value={targetMaxCapacity}
            />
          </label>
          <Button disabled={busyAction === 'register-target'} type="submit">
            Register
          </Button>
        </form>
        <ScalableTargetList
          busyAction={busyAction}
          onDeregister={handleDeregisterTarget}
          targets={state.status === 'success' ? state.scalableTargets : []}
        />
      </Panel>

      <Panel title="Scaling policies">
        <form className="pubsub-action-form" onSubmit={handlePutPolicy}>
          <label className="compact-filter">
            <span>Policy name</span>
            <input
              aria-label="Scaling policy name"
              onChange={(event) => setPolicyName(event.target.value)}
              placeholder="orders-write-target"
              value={policyName}
            />
          </label>
          <label className="compact-filter">
            <span>Resource ID</span>
            <input
              aria-label="Scaling policy resource ID"
              onChange={(event) => setPolicyResourceId(event.target.value)}
              placeholder="table/orders"
              value={policyResourceId}
            />
          </label>
          <label className="compact-filter">
            <span>Dimension</span>
            <select
              aria-label="Scaling policy dimension"
              onChange={(event) => setPolicyDimension(event.target.value)}
              value={policyDimension}
            >
              {DYNAMODB_DIMENSIONS.map((dim) => (
                <option key={dim} value={dim}>
                  {dim}
                </option>
              ))}
            </select>
          </label>
          <label className="compact-filter small">
            <span>Target %</span>
            <input
              aria-label="Scaling policy target value"
              inputMode="decimal"
              onChange={(event) => setPolicyTargetValue(event.target.value)}
              value={policyTargetValue}
            />
          </label>
          <Button disabled={busyAction === 'put-policy'} type="submit">
            Put policy
          </Button>
        </form>
        <ScalingPolicyList
          busyAction={busyAction}
          onDelete={handleDeletePolicy}
          policies={state.status === 'success' ? state.scalingPolicies : []}
        />
      </Panel>

      <Panel title="Scheduled actions">
        <form className="pubsub-action-form" onSubmit={handlePutScheduledAction}>
          <label className="compact-filter">
            <span>Action name</span>
            <input
              aria-label="Scheduled action name"
              onChange={(event) => setScheduleName(event.target.value)}
              placeholder="nightly-scale"
              value={scheduleName}
            />
          </label>
          <label className="compact-filter">
            <span>Resource ID</span>
            <input
              aria-label="Scheduled action resource ID"
              onChange={(event) => setScheduleResourceId(event.target.value)}
              placeholder="table/orders"
              value={scheduleResourceId}
            />
          </label>
          <label className="compact-filter">
            <span>Dimension</span>
            <select
              aria-label="Scheduled action dimension"
              onChange={(event) => setScheduleDimension(event.target.value)}
              value={scheduleDimension}
            >
              {DYNAMODB_DIMENSIONS.map((dim) => (
                <option key={dim} value={dim}>
                  {dim}
                </option>
              ))}
            </select>
          </label>
          <label className="compact-filter">
            <span>Schedule</span>
            <input
              aria-label="Scheduled action schedule"
              onChange={(event) => setScheduleExpression(event.target.value)}
              placeholder="rate(1 day)"
              value={scheduleExpression}
            />
          </label>
          <label className="compact-filter small">
            <span>Min</span>
            <input
              aria-label="Scheduled action min capacity"
              inputMode="numeric"
              onChange={(event) => setScheduleMinCapacity(event.target.value)}
              value={scheduleMinCapacity}
            />
          </label>
          <label className="compact-filter small">
            <span>Max</span>
            <input
              aria-label="Scheduled action max capacity"
              inputMode="numeric"
              onChange={(event) => setScheduleMaxCapacity(event.target.value)}
              value={scheduleMaxCapacity}
            />
          </label>
          <Button disabled={busyAction === 'put-schedule'} type="submit">
            Put schedule
          </Button>
        </form>
        <ScheduledActionList
          actions={state.status === 'success' ? state.scheduledActions : []}
          busyAction={busyAction}
          onDelete={handleDeleteScheduledAction}
        />
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

function ScalableTargetList({
  targets,
  busyAction,
  onDeregister,
}: {
  targets: ApplicationAutoScalingScalableTarget[]
  busyAction?: string
  onDeregister: (target: ApplicationAutoScalingScalableTarget) => Promise<void>
}): JSX.Element {
  if (targets.length === 0) {
    return <EmptyState title="No scalable targets" description="Targets registered via RegisterScalableTarget will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Application Auto Scaling scalable targets">
      {targets.map((target) => {
        const key = `${target.ResourceId}-${target.ScalableDimension}`
        return (
          <section className="dynamodb-table-row" key={key}>
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
            <div>
              <Button
                className="danger"
                disabled={busyAction === `deregister-${key}`}
                onClick={() => void onDeregister(target)}
              >
                Deregister
              </Button>
            </div>
          </section>
        )
      })}
    </div>
  )
}

function ScalingPolicyList({
  policies,
  busyAction,
  onDelete,
}: {
  policies: ApplicationAutoScalingScalingPolicy[]
  busyAction?: string
  onDelete: (policy: ApplicationAutoScalingScalingPolicy) => Promise<void>
}): JSX.Element {
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
          <div>
            <Button
              className="danger"
              disabled={busyAction === `delete-policy-${policy.PolicyARN}`}
              onClick={() => void onDelete(policy)}
            >
              Delete
            </Button>
          </div>
        </section>
      ))}
    </div>
  )
}

function ScheduledActionList({
  actions,
  busyAction,
  onDelete,
}: {
  actions: ApplicationAutoScalingScheduledAction[]
  busyAction?: string
  onDelete: (action: ApplicationAutoScalingScheduledAction) => Promise<void>
}): JSX.Element {
  if (actions.length === 0) {
    return <EmptyState title="No scheduled actions" description="Actions created via PutScheduledAction will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Application Auto Scaling scheduled actions">
      {actions.map((action) => (
        <section className="dynamodb-table-row" key={action.ScheduledActionName}>
          <span className="table-row-top">
            <span className="table-row-name">{action.ScheduledActionName}</span>
            {action.ScalableTargetAction?.MinCapacity !== undefined &&
            action.ScalableTargetAction?.MaxCapacity !== undefined ? (
              <span className="count-pill">
                {action.ScalableTargetAction.MinCapacity}-{action.ScalableTargetAction.MaxCapacity}
              </span>
            ) : null}
          </span>
          <span className="table-row-meta">{action.ResourceId}</span>
          <span className="table-row-tags">
            <span>{action.Schedule}</span>
            {action.ScalableDimension ? <span>{action.ScalableDimension}</span> : null}
            {action.Timezone ? <span>{action.Timezone}</span> : null}
          </span>
          <div>
            <Button
              className="danger"
              disabled={busyAction === `delete-schedule-${action.ScheduledActionName}`}
              onClick={() => void onDelete(action)}
            >
              Delete
            </Button>
          </div>
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
