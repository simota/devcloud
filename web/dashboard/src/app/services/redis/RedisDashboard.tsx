import { useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import type { DashboardService } from '../dashboard/types'
import { deleteRedisKey, expireRedisKey, flushRedisDB, getRedisKey, getRedisStatus, listRedisKeys, runRedisCommand } from './api'
import type { RedisCommandResponse, RedisKeyDetail, RedisKeySummary, RedisStatus } from './types'

type RedisState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: RedisStatus; keys: RedisKeySummary[]; nextCursor: number }
  | { status: 'error'; message: string }

type RedisDashboardProps = {
  service?: DashboardService
}

export function RedisDashboard({ service }: RedisDashboardProps): JSX.Element {
  const confirm = useConfirm()
  const [state, setState] = useState<RedisState>({ status: 'loading' })
  const [activeKey, setActiveKey] = useState<string>()
  const [detail, setDetail] = useState<RedisKeyDetail>()
  const [match, setMatch] = useState('*')
  const [message, setMessage] = useState<string>()
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      setState({ status: 'success', statusPayload: disabledStatus(service), keys: [], nextCursor: 0 })
      setActiveKey(undefined)
      setDetail(undefined)
      return
    }
    setState({ status: 'loading' })
    Promise.all([getRedisStatus(), listRedisKeys(0, match)])
      .then(([statusPayload, keysPayload]) => {
        setState({ status: 'success', statusPayload, keys: keysPayload.keys, nextCursor: keysPayload.nextCursor })
        setActiveKey((current) => (current && keysPayload.keys.some((key) => key.key === current) ? current : keysPayload.keys[0]?.key))
      })
      .catch((error: Error) => setState({ status: 'error', message: error.message }))
  }, [isDisabled, match, service])

  const loadMore = useCallback(() => {
    if (state.status !== 'success' || state.nextCursor === 0 || isDisabled) {
      return
    }
    const cursor = state.nextCursor
    listRedisKeys(cursor, match)
      .then((keysPayload) => {
        setState((current) => {
          if (current.status !== 'success') {
            return current
          }
          return {
            ...current,
            keys: [...current.keys, ...keysPayload.keys],
            nextCursor: keysPayload.nextCursor,
          }
        })
      })
      .catch((error: Error) => setMessage(error.message))
  }, [isDisabled, match, state])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEffect(() => {
    if (!activeKey || isDisabled) {
      setDetail(undefined)
      return
    }
    getRedisKey(activeKey)
      .then(setDetail)
      .catch(() => setDetail(undefined))
  }, [activeKey, isDisabled])

  const keys = state.status === 'success' ? state.keys : []
  const statusPayload = state.status === 'success' ? state.statusPayload : undefined

  async function confirmDeleteKey(key: string): Promise<void> {
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete Redis key',
        description: 'This key will be permanently removed from the selected Redis database.',
        target: key,
      }),
    )
    if (!ok) {
      return
    }
    deleteRedisKey(key)
      .then((result) => {
        setMessage(`Deleted ${result.deleted} key`)
        setActiveKey(undefined)
        setDetail(undefined)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  function expireKey(key: string, ttlSeconds: number): void {
    expireRedisKey(key, ttlSeconds)
      .then((result) => {
        setMessage(result.updated ? `Updated TTL for ${key}` : `Key ${key} was not updated`)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  async function confirmFlushDB(): Promise<void> {
    const ok = await confirm(
      dangerConfirm({
        title: 'Flush Redis DB',
        description: 'This removes every key from the selected Redis database. FLUSHALL is never used.',
        target: 'FLUSHDB',
        confirmLabel: 'Flush DB',
      }),
    )
    if (!ok) {
      return
    }
    flushRedisDB()
      .then((result) => {
        setMessage(`Redis FLUSHDB returned ${result.result}`)
        setActiveKey(undefined)
        setDetail(undefined)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  if (isDisabled) {
    return (
      <Panel title="Redis">
        <EmptyState title="Redis is disabled" description="Enable the Redis service in devcloud config to inspect keys and TTLs." />
      </Panel>
    )
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Keys">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">{state.status === 'success' ? `${keys.length} keys` : 'Loading'}</span>
          <Button onClick={refresh}>Refresh</Button>
          <Button disabled={state.status !== 'success' || state.nextCursor === 0} onClick={loadMore}>
            Load more
          </Button>
          <Button className="danger" disabled={keys.length === 0} onClick={confirmFlushDB}>
            Flush DB
          </Button>
        </div>
        {message ? <p className="operation-message">{message}</p> : null}
        <label className="compact-filter">
          <span>Match</span>
          <input aria-label="Match Redis keys" onChange={(event) => setMatch(event.target.value)} type="search" value={match} />
        </label>
        {state.status === 'loading' ? <EmptyState title="Loading Redis" description="Reading Redis key metadata." /> : null}
        {state.status === 'error' ? <EmptyState title="Redis unavailable" description={state.message} actionLabel="Retry" onAction={refresh} /> : null}
        {state.status === 'success' ? <RedisKeyList activeKey={activeKey} keys={keys} onSelectKey={setActiveKey} /> : null}
      </Panel>

      <Panel title="Inspector">
        <RedisInspector detail={detail} onDeleteKey={confirmDeleteKey} onExpireKey={expireKey} status={statusPayload} />
      </Panel>

      <Panel title="Command Runner">
        <RedisCommandRunner />
      </Panel>
    </div>
  )
}

function RedisKeyList({
  activeKey,
  keys,
  onSelectKey,
}: {
  activeKey?: string
  keys: RedisKeySummary[]
  onSelectKey: (key: string) => void
}): JSX.Element {
  if (keys.length === 0) {
    return <EmptyState title="No keys" description="Redis keys created by local clients will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Redis keys">
      {keys.map((item) => (
        <button className={item.key === activeKey ? 'dynamodb-table-row active' : 'dynamodb-table-row'} key={item.key} onClick={() => onSelectKey(item.key)} type="button">
          <span className="table-row-top">
            <span className="table-row-name">{item.key}</span>
            <span className="count-pill">{item.type}</span>
          </span>
          <span className="table-row-meta">TTL {formatTTL(item.ttlSeconds)}</span>
        </button>
      ))}
    </div>
  )
}

function RedisInspector({
  detail,
  onDeleteKey,
  onExpireKey,
  status,
}: {
  detail?: RedisKeyDetail
  onDeleteKey: (key: string) => void
  onExpireKey: (key: string, ttlSeconds: number) => void
  status?: RedisStatus
}): JSX.Element {
  const [ttlText, setTTLText] = useState('60')

  if (!detail) {
    return (
      <div className="dynamodb-inspector">
        <EmptyState title="Inspector" description="Select a key to inspect its type, TTL, and preview." />
        {status ? <StatusSummary status={status} /> : null}
      </div>
    )
  }
  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Key</span>
        <h3>{detail.key}</h3>
        <dl className="inspector-list">
          <div>
            <dt>Type</dt>
            <dd>{detail.type}</dd>
          </div>
          <div>
            <dt>TTL</dt>
            <dd>{formatTTL(detail.ttlSeconds)}</dd>
          </div>
        </dl>
      </section>
      {detail.preview.length === 0 ? <EmptyState title="No preview" description="The key has no previewable value in this slice." /> : null}
      {detail.preview.length > 0 ? <pre className="query-result">{detail.preview.join('\n')}</pre> : null}
      <div className="dynamodb-toolbar">
        <label className="compact-filter small">
          <span>TTL seconds</span>
          <input aria-label="Redis TTL seconds" onChange={(event) => setTTLText(event.target.value)} type="number" value={ttlText} />
        </label>
        <Button disabled={!positiveInteger(ttlText)} onClick={() => onExpireKey(detail.key, Number(ttlText))}>
          Expire
        </Button>
        <Button className="danger" onClick={() => onDeleteKey(detail.key)}>
          Delete
        </Button>
      </div>
      {status ? <StatusSummary status={status} /> : null}
    </div>
  )
}

function StatusSummary({ status }: { status: RedisStatus }): JSX.Element {
  return (
    <section>
      <span className="inspector-label">Connection</span>
      <dl className="inspector-list">
        <div>
          <dt>Mode</dt>
          <dd>{status.mode}</dd>
        </div>
        <div>
          <dt>Address</dt>
          <dd>{status.address}</dd>
        </div>
        <div>
          <dt>Keys</dt>
          <dd>{status.db0Keys}</dd>
        </div>
      </dl>
    </section>
  )
}

function RedisCommandRunner(): JSX.Element {
  const [commandText, setCommandText] = useState('GET key')
  const [result, setResult] = useState<RedisCommandResponse>()
  const [error, setError] = useState<string>()
  const parsed = useMemo(() => parseCommand(commandText), [commandText])

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setError(undefined)
    runRedisCommand(parsed)
      .then(setResult)
      .catch((requestError: Error) => {
        setResult(undefined)
        setError(requestError.message)
      })
  }

  return (
    <form className="dynamodb-inspector" onSubmit={submit}>
      <label className="compact-filter">
        <span>Command</span>
        <textarea aria-label="Redis command" onChange={(event) => setCommandText(event.target.value)} rows={4} value={commandText} />
      </label>
      <Button type="submit">Run</Button>
      {error ? <EmptyState title="Command rejected" description={error} /> : null}
      {result ? <pre className="query-result">{JSON.stringify(result, null, 2)}</pre> : null}
    </form>
  )
}

function disabledStatus(service?: DashboardService): RedisStatus {
  return {
    service: 'redis',
    status: 'disabled',
    running: false,
    mode: 'managed',
    address: service?.endpoint?.replace(/^redis:\/\//, '') ?? '127.0.0.1:6379',
    serverVersion: '',
    connectedClients: 0,
    usedMemoryHuman: '',
    db0Keys: 0,
    storagePath: service?.storagePath ?? '.devcloud/data/redis',
  }
}

function parseCommand(value: string): { command: string; args: string[] } {
  const [command = '', ...args] = value.trim().split(/\s+/)
  return { command, args }
}

function formatTTL(ttlSeconds: number): string {
  if (ttlSeconds === -2) {
    return 'missing'
  }
  if (ttlSeconds === -1) {
    return 'persistent'
  }
  return `${ttlSeconds}s`
}

function positiveInteger(value: string): boolean {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0
}
