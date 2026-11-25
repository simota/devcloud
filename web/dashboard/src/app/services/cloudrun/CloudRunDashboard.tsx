import { useCallback, useEffect, useState, type FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import { useLatestRequest } from '../../api/hooks/useLatestRequest'
import type { DashboardService } from '../dashboard/types'
import {
  createCloudRunService,
  deleteCloudRunRevision,
  deleteCloudRunService,
  getCloudRunIamPolicy,
  getCloudRunLogs,
  getCloudRunStatus,
  listCloudRunInstances,
  listCloudRunRevisions,
  listCloudRunServices,
  setCloudRunIamPolicy,
  updateCloudRunService,
} from './api'
import type { CloudRunIamPolicy, CloudRunInstance, CloudRunRevision, CloudRunService, CloudRunStatus } from './types'

type CloudRunState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: CloudRunStatus; services: CloudRunService[]; instances: CloudRunInstance[] }
  | { status: 'error'; message: string }

// Tagged with the service it was fetched for, so a slow response for the
// previously selected service is never rendered under another one.
type DetailState =
  | { service: string; revisions: CloudRunRevision[]; logs: string[]; iamPolicy?: CloudRunIamPolicy }
  | undefined

type CloudRunDashboardProps = {
  service?: DashboardService
}

function shortName(name: string): string {
  return name.split('/').pop() ?? name
}

function parseEnvText(raw: string): { name: string; value: string }[] {
  return raw
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => {
      const idx = line.indexOf('=')
      if (idx === -1) {
        return { name: line, value: '' }
      }
      return { name: line.slice(0, idx).trim(), value: line.slice(idx + 1).trim() }
    })
    .filter((entry) => entry.name.length > 0)
}

function formatEnvText(env?: { name: string; value?: string }[]): string {
  if (!env || env.length === 0) return ''
  return env.map((e) => `${e.name}=${e.value ?? ''}`).join('\n')
}

function parseCommandLine(raw: string): string[] | undefined {
  const parts = raw
    .trim()
    .split(/\s+/)
    .filter((s) => s.length > 0)
  return parts.length > 0 ? parts : undefined
}

function isPublicInvoker(policy?: CloudRunIamPolicy): boolean {
  return Boolean(
    policy?.bindings?.some(
      (b) => b.role === 'roles/run.invoker' && Array.isArray(b.members) && b.members.includes('allUsers'),
    ),
  )
}

export function CloudRunDashboard({ service }: CloudRunDashboardProps): JSX.Element {
  const [state, setState] = useState<CloudRunState>({ status: 'loading' })
  const [selected, setSelected] = useState<string>()
  const [detail, setDetail] = useState<DetailState>()

  const [newServiceId, setNewServiceId] = useState('')
  const [newImage, setNewImage] = useState('us-docker.pkg.dev/cloudrun/container/hello')
  const [newCommand, setNewCommand] = useState('')
  const [newEnv, setNewEnv] = useState('')

  const [editImage, setEditImage] = useState('')
  const [editCommand, setEditCommand] = useState('')
  const [editEnv, setEditEnv] = useState('')

  const [actionMessage, setActionMessage] = useState('')
  const [actionError, setActionError] = useState('')
  const [busyAction, setBusyAction] = useState<string>()
  const confirm = useConfirm()
  const isDisabled = service?.status === 'disabled'

  const startRefresh = useLatestRequest()
  const refresh = useCallback(() => {
    const isCurrent = startRefresh()
    if (isDisabled) {
      return
    }
    Promise.all([getCloudRunStatus(), listCloudRunServices(), listCloudRunInstances()])
      .then(([statusPayload, servicesPayload, instancesPayload]) => {
        if (!isCurrent()) return
        setState({
          status: 'success',
          statusPayload,
          services: servicesPayload.services,
          instances: instancesPayload.instances,
        })
        setSelected((current) =>
          current && servicesPayload.services.some((svc) => svc.name === current)
            ? current
            : servicesPayload.services[0]?.name,
        )
      })
      .catch((error: Error) => {
        if (!isCurrent()) return
        setState({ status: 'error', message: error.message })
      })
  }, [startRefresh, isDisabled])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEffect(() => {
    if (!selected || isDisabled) {
      setDetail(undefined)
      return
    }
    let cancelled = false
    Promise.all([
      listCloudRunRevisions(selected),
      getCloudRunLogs(selected),
      getCloudRunIamPolicy(selected).catch(() => undefined),
    ])
      .then(([revisions, logs, iamPolicy]) => {
        if (!cancelled) {
          setDetail({ service: selected, revisions: revisions.revisions, logs: logs.lines, iamPolicy })
        }
      })
      .catch(() => {
        if (!cancelled) {
          setDetail({ service: selected, revisions: [], logs: [] })
        }
      })
    return () => {
      cancelled = true
    }
  }, [selected, isDisabled, state])

  const services = state.status === 'success' ? state.services : []
  const instances = state.status === 'success' ? state.instances : []
  const active = services.find((svc) => svc.name === selected)
  const activeInstance = instances.find((inst) => inst.service === selected)
  const activeDetail = detail && detail.service === selected ? detail : undefined

  useEffect(() => {
    if (!active) return
    const container = active.template.containers[0]
    setEditImage(container?.image ?? '')
    setEditCommand(
      container?.command?.length ? [...container.command, ...(container.args ?? [])].join(' ') : '',
    )
    setEditEnv(formatEnvText(container?.env))
  }, [active?.name, active?.generation])

  useDashboardEvents({ topics: ['cloudrun'], onEvent: refresh, onResync: refresh, enabled: !isDisabled })

  async function runAction(name: string, action: () => Promise<string>): Promise<void> {
    setBusyAction(name)
    setActionError('')
    setActionMessage('')
    try {
      const message = await action()
      setActionMessage(message)
      refresh()
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Cloud Run operation failed')
    } finally {
      setBusyAction(undefined)
    }
  }

  function handleCreateService(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const serviceId = newServiceId.trim()
    const image = newImage.trim()
    if (!serviceId || !image) {
      setActionError('Service ID and container image are required')
      return
    }
    const command = parseCommandLine(newCommand)
    const env = parseEnvText(newEnv)
    void runAction('create-service', async () => {
      const op = await createCloudRunService({
        serviceId,
        service: {
          template: {
            containers: [
              {
                image,
                ...(command ? { command } : {}),
                ...(env.length > 0 ? { env } : {}),
              },
            ],
          },
        },
      })
      setNewServiceId('')
      setNewCommand('')
      setNewEnv('')
      if (op.response?.name) {
        setSelected(op.response.name)
      }
      return `Created service ${serviceId}`
    })
  }

  function handleUpdateService(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!active) return
    const image = editImage.trim()
    if (!image) {
      setActionError('Container image is required')
      return
    }
    const command = parseCommandLine(editCommand)
    const env = parseEnvText(editEnv)
    void runAction('update-service', async () => {
      await updateCloudRunService(active.name, {
        updateMask: 'template',
        service: {
          template: {
            containers: [
              {
                image,
                ...(command ? { command } : {}),
                ...(env.length > 0 ? { env } : {}),
              },
            ],
          },
        },
      })
      return `Deployed new revision for ${shortName(active.name)}`
    })
  }

  async function handleDeleteService(svc: CloudRunService): Promise<void> {
    const id = shortName(svc.name)
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete Cloud Run service',
        description: `Delete service ${id}, its revisions, and stop any running instance?`,
        target: id,
        confirmLabel: 'Delete',
      }),
    )
    if (!ok) return
    await runAction(`delete-service-${svc.name}`, async () => {
      await deleteCloudRunService(svc.name, id)
      return `Deleted service ${id}`
    })
  }

  async function handleDeleteRevision(svc: CloudRunService, rev: CloudRunRevision): Promise<void> {
    const revId = shortName(rev.name)
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete Cloud Run revision',
        description: `Delete non-latest revision ${revId}?`,
        target: revId,
        confirmLabel: 'Delete',
      }),
    )
    if (!ok) return
    await runAction(`delete-revision-${rev.name}`, async () => {
      await deleteCloudRunRevision(svc.name, revId)
      return `Deleted revision ${revId}`
    })
  }

  async function handleTogglePublicInvoker(svc: CloudRunService): Promise<void> {
    const currentlyPublic = isPublicInvoker(activeDetail?.iamPolicy)
    await runAction(`iam-${svc.name}`, async () => {
      const bindings = currentlyPublic
        ? []
        : [{ role: 'roles/run.invoker', members: ['allUsers'] }]
      await setCloudRunIamPolicy(svc.name, { bindings })
      return currentlyPublic
        ? `Revoked public allUsers invoker on ${shortName(svc.name)}`
        : `Granted public allUsers invoker on ${shortName(svc.name)}`
    })
  }

  if (isDisabled) {
    return (
      <Panel title="Cloud Run">
        <EmptyState
          title="Cloud Run is disabled"
          description="Enable the Cloud Run service in devcloud config to deploy and run local services."
        />
      </Panel>
    )
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Status">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {state.status === 'success'
              ? `${state.statusPayload.status} / ${state.statusPayload.project} / ${state.statusPayload.region}`
              : 'Loading'}
          </span>
          <Button onClick={refresh}>Refresh</Button>
        </div>
        {actionError ? <p className="operation-message error">{actionError}</p> : null}
        {actionMessage ? <p className="operation-message success">{actionMessage}</p> : null}
        {state.status === 'loading' ? (
          <EmptyState title="Loading Cloud Run" description="Reading local services and running instances." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="Cloud Run unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <StatusSummary status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Services">
        <form className="pubsub-action-form" onSubmit={handleCreateService}>
          <label className="compact-filter">
            <span>Service ID</span>
            <input
              aria-label="New Cloud Run service ID"
              onChange={(event) => setNewServiceId(event.target.value)}
              placeholder="hello-svc"
              value={newServiceId}
            />
          </label>
          <label className="compact-filter">
            <span>Image</span>
            <input
              aria-label="New Cloud Run container image"
              onChange={(event) => setNewImage(event.target.value)}
              placeholder="us-docker.pkg.dev/cloudrun/container/hello"
              value={newImage}
            />
          </label>
          <label className="compact-filter">
            <span>Command (optional)</span>
            <input
              aria-label="New Cloud Run container command"
              onChange={(event) => setNewCommand(event.target.value)}
              placeholder="python3 app.py"
              value={newCommand}
            />
          </label>
          <label className="compact-filter wide">
            <span>Environment (KEY=VALUE per line)</span>
            <textarea
              aria-label="New Cloud Run container env"
              onChange={(event) => setNewEnv(event.target.value)}
              placeholder="GREETING=hello"
              rows={2}
              value={newEnv}
            />
          </label>
          <Button disabled={busyAction === 'create-service'} type="submit">
            Create service
          </Button>
        </form>
        {services.length === 0 ? (
          <EmptyState title="No services" description="Services created via the Cloud Run Admin API v2 will appear here." />
        ) : (
          <div className="dynamodb-table-list" aria-label="Cloud Run services">
            {services.map((svc) => {
              const running = instances.some((inst) => inst.service === svc.name)
              return (
                <button
                  type="button"
                  className={`dynamodb-table-row${svc.name === selected ? ' active' : ''}`}
                  aria-pressed={svc.name === selected}
                  key={svc.name}
                  onClick={() => setSelected(svc.name)}
                >
                  <span className="table-row-top">
                    <span className="table-row-name">{shortName(svc.name)}</span>
                    <span className="count-pill">{running ? 'running' : 'idle'}</span>
                  </span>
                  <span className="table-row-meta">{svc.name.split('/services/')[0]}</span>
                  <span className="table-row-tags">
                    <span>gen {svc.generation}</span>
                    <span>{shortName(svc.latestReadyRevision)}</span>
                  </span>
                </button>
              )
            })}
          </div>
        )}
      </Panel>

      {active ? (
        <Panel title={`Service: ${shortName(active.name)}`}>
          <ServiceDetails
            svc={active}
            instance={activeInstance}
            iamPolicy={activeDetail?.iamPolicy}
            busyAction={busyAction}
            onTogglePublicInvoker={() => void handleTogglePublicInvoker(active)}
            onDeleteService={() => void handleDeleteService(active)}
          />

          <h3 className="inspector-heading">Deploy new revision</h3>
          <form className="pubsub-action-form stacked" onSubmit={handleUpdateService}>
            <label className="compact-filter">
              <span>Image</span>
              <input
                aria-label="Update Cloud Run container image"
                onChange={(event) => setEditImage(event.target.value)}
                value={editImage}
              />
            </label>
            <label className="compact-filter">
              <span>Command</span>
              <input
                aria-label="Update Cloud Run container command"
                onChange={(event) => setEditCommand(event.target.value)}
                placeholder="(image entrypoint)"
                value={editCommand}
              />
            </label>
            <label className="compact-filter wide">
              <span>Environment (KEY=VALUE per line)</span>
              <textarea
                aria-label="Update Cloud Run container env"
                onChange={(event) => setEditEnv(event.target.value)}
                rows={2}
                value={editEnv}
              />
            </label>
            <Button disabled={busyAction === 'update-service'} type="submit">
              Deploy revision
            </Button>
          </form>

          <h3 className="inspector-heading">Revisions</h3>
          {activeDetail && activeDetail.revisions.length > 0 ? (
            <div className="dynamodb-table-list" aria-label="Cloud Run revisions">
              {activeDetail.revisions.map((rev) => {
                const isLatest =
                  rev.name === active.latestReadyRevision || rev.name === active.latestCreatedRevision
                return (
                  <section className="dynamodb-table-row" key={rev.name}>
                    <span className="table-row-top">
                      <span className="table-row-name">{shortName(rev.name)}</span>
                      {isLatest ? <span className="count-pill">latest</span> : null}
                    </span>
                    <span className="table-row-meta">{rev.createTime}</span>
                    <span className="table-row-tags">
                      <span>{rev.containers[0]?.image}</span>
                    </span>
                    {!isLatest ? (
                      <div>
                        <Button
                          className="danger"
                          disabled={busyAction === `delete-revision-${rev.name}`}
                          onClick={() => void handleDeleteRevision(active, rev)}
                        >
                          Delete revision
                        </Button>
                      </div>
                    ) : null}
                  </section>
                )
              })}
            </div>
          ) : (
            <p className="inspector-muted">{activeDetail ? 'No revisions.' : 'Loading revisions…'}</p>
          )}
          <h3 className="inspector-heading">Instance logs</h3>
          <pre className="redis-value-pre" aria-label="Cloud Run instance logs">
            {activeDetail && activeDetail.logs.length > 0 ? activeDetail.logs.join('\n') : '(no output yet — send a request to start the instance)'}
          </pre>
        </Panel>
      ) : null}
    </div>
  )
}

function StatusSummary({ status }: { status: CloudRunStatus }): JSX.Element {
  return (
    <dl className="inspector-list">
      <div>
        <dt>Endpoint</dt>
        <dd>
          <code>{status.endpoint}</code>
        </dd>
      </div>
      <div>
        <dt>Default project / region</dt>
        <dd>
          {status.project} / {status.region}
        </dd>
      </div>
      <div>
        <dt>Storage path</dt>
        <dd>
          <code>{status.storagePath}</code>
        </dd>
      </div>
      <div>
        <dt>Services</dt>
        <dd>{status.serviceCount}</dd>
      </div>
      <div>
        <dt>Running instances</dt>
        <dd>{status.instanceCount}</dd>
      </div>
    </dl>
  )
}

function ServiceDetails({
  svc,
  instance,
  iamPolicy,
  busyAction,
  onTogglePublicInvoker,
  onDeleteService,
}: {
  svc: CloudRunService
  instance?: CloudRunInstance
  iamPolicy?: CloudRunIamPolicy
  busyAction?: string
  onTogglePublicInvoker: () => void
  onDeleteService: () => void
}): JSX.Element {
  const container = svc.template.containers[0]
  const publicInvoker = isPublicInvoker(iamPolicy)
  return (
    <>
      <dl className="inspector-list">
        {(svc.urls ?? [svc.uri]).map((url) => (
          <div key={url}>
            <dt>URL</dt>
            <dd>
              <a href={url} target="_blank" rel="noreferrer">
                <code>{url}</code>
              </a>
            </dd>
          </div>
        ))}
        <div>
          <dt>Image</dt>
          <dd>
            <code>{container?.image}</code>
          </dd>
        </div>
        <div>
          <dt>Command</dt>
          <dd>
            <code>{container?.command?.length ? [...container.command, ...(container.args ?? [])].join(' ') : '(image entrypoint)'}</code>
          </dd>
        </div>
        <div>
          <dt>Ingress</dt>
          <dd>{svc.ingress}</dd>
        </div>
        <div>
          <dt>IAM invoker</dt>
          <dd>{publicInvoker ? 'allUsers (public)' : 'authenticated only'}</dd>
        </div>
        <div>
          <dt>Instance</dt>
          <dd>
            {instance
              ? `${instance.mode} · port ${instance.port}${instance.pid ? ` · pid ${instance.pid}` : ''} · ${instance.requestCount} requests`
              : 'not running (starts on first request)'}
          </dd>
        </div>
      </dl>
      <div className="pubsub-action-row">
        <Button disabled={busyAction === `iam-${svc.name}`} onClick={onTogglePublicInvoker}>
          {publicInvoker ? 'Revoke public invoker' : 'Allow public invoker (allUsers)'}
        </Button>
        <Button
          className="danger"
          disabled={busyAction === `delete-service-${svc.name}`}
          onClick={onDeleteService}
        >
          Delete service
        </Button>
      </div>
    </>
  )
}
