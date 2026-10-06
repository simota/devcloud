import { useCallback, useEffect, useState } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import type { DashboardService } from '../dashboard/types'
import { getCloudRunLogs, getCloudRunStatus, listCloudRunInstances, listCloudRunRevisions, listCloudRunServices } from './api'
import type { CloudRunInstance, CloudRunRevision, CloudRunService, CloudRunStatus } from './types'

type CloudRunState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: CloudRunStatus; services: CloudRunService[]; instances: CloudRunInstance[] }
  | { status: 'error'; message: string }

// Tagged with the service it was fetched for, so a slow response for the
// previously selected service is never rendered under another one.
type DetailState = { service: string; revisions: CloudRunRevision[]; logs: string[] } | undefined

type CloudRunDashboardProps = {
  service?: DashboardService
}

function shortName(name: string): string {
  return name.split('/').pop() ?? name
}

export function CloudRunDashboard({ service }: CloudRunDashboardProps): JSX.Element {
  const [state, setState] = useState<CloudRunState>({ status: 'loading' })
  const [selected, setSelected] = useState<string>()
  const [detail, setDetail] = useState<DetailState>()
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      return
    }
    Promise.all([getCloudRunStatus(), listCloudRunServices(), listCloudRunInstances()])
      .then(([statusPayload, servicesPayload, instancesPayload]) => {
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
        setState({ status: 'error', message: error.message })
      })
  }, [isDisabled])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEffect(() => {
    if (!selected || isDisabled) {
      setDetail(undefined)
      return
    }
    let cancelled = false
    Promise.all([listCloudRunRevisions(selected), getCloudRunLogs(selected)])
      .then(([revisions, logs]) => {
        if (!cancelled) {
          setDetail({ service: selected, revisions: revisions.revisions, logs: logs.lines })
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

  useDashboardEvents({ topics: ['cloudrun'], onEvent: refresh, enabled: !isDisabled })

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

  const services = state.status === 'success' ? state.services : []
  const instances = state.status === 'success' ? state.instances : []
  const active = services.find((svc) => svc.name === selected)
  const activeInstance = instances.find((inst) => inst.service === selected)
  const activeDetail = detail && detail.service === selected ? detail : undefined

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
        {state.status === 'loading' ? (
          <EmptyState title="Loading Cloud Run" description="Reading local services and running instances." />
        ) : null}
        {state.status === 'error' ? (
          <EmptyState title="Cloud Run unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <StatusSummary status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Services">
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
          <ServiceDetails svc={active} instance={activeInstance} />
          <h3 className="inspector-heading">Revisions</h3>
          {activeDetail && activeDetail.revisions.length > 0 ? (
            <div className="dynamodb-table-list" aria-label="Cloud Run revisions">
              {activeDetail.revisions.map((rev) => (
                <section className="dynamodb-table-row" key={rev.name}>
                  <span className="table-row-top">
                    <span className="table-row-name">{shortName(rev.name)}</span>
                    {rev.name === active.latestReadyRevision ? <span className="count-pill">latest</span> : null}
                  </span>
                  <span className="table-row-meta">{rev.createTime}</span>
                  <span className="table-row-tags">
                    <span>{rev.containers[0]?.image}</span>
                  </span>
                </section>
              ))}
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

function ServiceDetails({ svc, instance }: { svc: CloudRunService; instance?: CloudRunInstance }): JSX.Element {
  const container = svc.template.containers[0]
  return (
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
        <dt>Instance</dt>
        <dd>
          {instance
            ? `${instance.mode} · port ${instance.port}${instance.pid ? ` · pid ${instance.pid}` : ''} · ${instance.requestCount} requests`
            : 'not running (starts on first request)'}
        </dd>
      </div>
    </dl>
  )
}
