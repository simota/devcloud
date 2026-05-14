import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useEventSource } from '../../api/hooks/useEventSource'
import type { DashboardService } from '../dashboard/types'
import {
  deleteRedisKey,
  expireRedisKey,
  flushRedisDB,
  getRedisKey,
  getRedisStatus,
  listRedisKeys,
  runRedisCommand,
} from './api'
import type { RedisCommandResponse, RedisKeyDetail, RedisKeySummary, RedisStatus } from './types'

const COMMAND_HISTORY_LIMIT = 8

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
  const [detailError, setDetailError] = useState<string>()
  const [match, setMatch] = useState('*')
  const [message, setMessage] = useState<RedisMessage>()
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
        setState({
          status: 'success',
          statusPayload,
          keys: keysPayload.keys,
          nextCursor: keysPayload.nextCursor,
        })
        setActiveKey((current) =>
          current && keysPayload.keys.some((key) => key.key === current) ? current : keysPayload.keys[0]?.key,
        )
      })
      .catch((error: Error) => setState({ status: 'error', message: error.message }))
  }, [isDisabled, match, service])

  const loadMore = useCallback(() => {
    if (state.status !== 'success' || state.nextCursor === 0 || isDisabled) {
      return
    }
    listRedisKeys(state.nextCursor, match)
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
      .catch((error: Error) => setMessage({ tone: 'error', text: error.message }))
  }, [isDisabled, match, state])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEventSource({ topics: ['redis'], onEvent: refresh, enabled: !isDisabled })

  useEffect(() => {
    if (!activeKey || isDisabled) {
      setDetail(undefined)
      setDetailError(undefined)
      return
    }
    let cancelled = false
    setDetailError(undefined)
    getRedisKey(activeKey)
      .then((value) => {
        if (!cancelled) {
          setDetail(value)
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setDetail(undefined)
          setDetailError(error.message)
        }
      })
    return () => {
      cancelled = true
    }
  }, [activeKey, isDisabled])

  const keys = state.status === 'success' ? state.keys : []
  const nextCursor = state.status === 'success' ? state.nextCursor : 0
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
        setMessage({ tone: 'success', text: `Deleted ${key} (${result.deleted} key)` })
        setActiveKey(undefined)
        setDetail(undefined)
        refresh()
      })
      .catch((error: Error) => setMessage({ tone: 'error', text: error.message }))
  }

  function applyExpire(key: string, ttlSeconds: number): void {
    expireRedisKey(key, ttlSeconds)
      .then((result) => {
        setMessage({
          tone: result.updated ? 'success' : 'warning',
          text: result.updated
            ? `Set TTL on ${key} to ${ttlSeconds}s`
            : `TTL not updated on ${key} (key may be missing)`,
        })
        getRedisKey(key)
          .then(setDetail)
          .catch(() => {
            /* surfaced via detailError on next load */
          })
        refresh()
      })
      .catch((error: Error) => setMessage({ tone: 'error', text: error.message }))
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
        setMessage({ tone: 'success', text: `FLUSHDB ${result.result}` })
        setActiveKey(undefined)
        setDetail(undefined)
        refresh()
      })
      .catch((error: Error) => setMessage({ tone: 'error', text: error.message }))
  }

  if (isDisabled) {
    return (
      <Panel title="Redis">
        <EmptyState
          title="Redis is disabled"
          description="Enable services.redis.enabled in .devcloud/config.yaml and restart devcloud."
        />
      </Panel>
    )
  }

  return (
    <div className="redis-shell">
      <RedisStatusBar
        status={statusPayload}
        loading={state.status === 'loading'}
        error={state.status === 'error' ? state.message : undefined}
      />

      <div className="redis-workspace">
        <Panel title="Keys">
          <div className="redis-keys-toolbar">
            <KeyCount loading={state.status === 'loading'} keys={keys.length} total={statusPayload?.db0Keys} />
            <div className="redis-toolbar-actions">
              <Button onClick={refresh}>Refresh</Button>
              <Button disabled={state.status !== 'success' || nextCursor === 0} onClick={loadMore}>
                Load more
              </Button>
              <Button className="danger" disabled={keys.length === 0} onClick={confirmFlushDB}>
                Flush DB
              </Button>
            </div>
          </div>

          <label className="redis-match-field">
            <span>Match pattern</span>
            <input
              aria-label="Match Redis keys"
              onChange={(event) => setMatch(event.target.value)}
              placeholder="* | prefix:* | exact-key"
              spellCheck={false}
              type="search"
              value={match}
            />
          </label>

          {message ? <OperationMessage message={message} onDismiss={() => setMessage(undefined)} /> : null}

          {state.status === 'loading' ? (
            <EmptyState title="Loading Redis" description="Reading key metadata via SCAN." />
          ) : null}
          {state.status === 'error' ? (
            <EmptyState
              title="Redis unavailable"
              description={state.message}
              actionLabel="Retry"
              onAction={refresh}
            />
          ) : null}
          {state.status === 'success' ? (
            <RedisKeyList activeKey={activeKey} keys={keys} onSelectKey={setActiveKey} />
          ) : null}
        </Panel>

        <Panel title="Inspector">
          <RedisInspector
            detail={detail}
            error={detailError}
            onDeleteKey={confirmDeleteKey}
            onApplyTTL={applyExpire}
          />
        </Panel>

        <Panel title="Command Runner">
          <RedisCommandRunner />
        </Panel>
      </div>
    </div>
  )
}

type RedisMessage = { tone: 'success' | 'warning' | 'error'; text: string }

function OperationMessage({ message, onDismiss }: { message: RedisMessage; onDismiss: () => void }): JSX.Element {
  return (
    <div className={`redis-message redis-message-${message.tone}`} role="status">
      <span>{message.text}</span>
      <button aria-label="Dismiss" className="redis-message-close" onClick={onDismiss} type="button">
        ×
      </button>
    </div>
  )
}

function KeyCount({
  keys,
  total,
  loading,
}: {
  keys: number
  total: number | undefined
  loading: boolean
}): JSX.Element {
  if (loading) {
    return <span className="redis-keys-count">Loading…</span>
  }
  const totalText = total !== undefined && total >= 0 ? ` / ${total}` : ''
  return (
    <span className="redis-keys-count">
      <strong>{keys}</strong>
      <span className="redis-keys-count-total">{totalText} key{keys === 1 ? '' : 's'}</span>
    </span>
  )
}

function RedisStatusBar({
  status,
  loading,
  error,
}: {
  status: RedisStatus | undefined
  loading: boolean
  error: string | undefined
}): JSX.Element {
  if (loading && !status) {
    return (
      <header className="redis-status-bar redis-status-bar-loading">
        <span>Connecting to Redis…</span>
      </header>
    )
  }
  if (error && !status) {
    return (
      <header className="redis-status-bar redis-status-bar-error" role="alert">
        <span className="redis-status-cell">
          <span className="cell-label">State</span>
          <span className="cell-value">unreachable</span>
        </span>
        <span className="redis-status-cell redis-status-cell-wide">
          <span className="cell-label">Reason</span>
          <span className="cell-value">{error}</span>
        </span>
      </header>
    )
  }
  if (!status) {
    return <header className="redis-status-bar redis-status-bar-loading"><span>—</span></header>
  }
  return (
    <header className="redis-status-bar">
      <span className={`redis-status-cell redis-mode-pill redis-mode-${status.mode || 'unknown'}`}>
        <span className="cell-label">Mode</span>
        <span className="cell-value">{status.mode || 'unknown'}</span>
      </span>
      <span className="redis-status-cell">
        <span className="cell-label">Address</span>
        <code>{status.address || '—'}</code>
      </span>
      <span className="redis-status-cell">
        <span className="cell-label">Server</span>
        <span className="cell-value">{status.serverVersion || '—'}</span>
      </span>
      <span className="redis-status-cell">
        <span className="cell-label">Clients</span>
        <span className="cell-value">{status.connectedClients}</span>
      </span>
      <span className="redis-status-cell">
        <span className="cell-label">Memory</span>
        <span className="cell-value">{status.usedMemoryHuman || '—'}</span>
      </span>
      <span className="redis-status-cell">
        <span className="cell-label">Keys (db0)</span>
        <span className="cell-value">{status.db0Keys}</span>
      </span>
    </header>
  )
}

function RedisKeyList({
  activeKey,
  keys,
  onSelectKey,
}: {
  activeKey: string | undefined
  keys: RedisKeySummary[]
  onSelectKey: (key: string) => void
}): JSX.Element {
  if (keys.length === 0) {
    return (
      <EmptyState
        title="No keys"
        description="Send SET / HSET / LPUSH from a Redis client and refresh."
      />
    )
  }
  return (
    <ul className="redis-key-list" aria-label="Redis keys">
      {keys.map((item) => {
        const active = item.key === activeKey
        return (
          <li key={item.key}>
            <button
              aria-current={active ? 'true' : undefined}
              className={active ? 'redis-key-row active' : 'redis-key-row'}
              onClick={() => onSelectKey(item.key)}
              title={item.key}
              type="button"
            >
              <span className="redis-key-row-top">
                <TypeBadge type={item.type} />
                <TTLBadge ttlSeconds={item.ttlSeconds} compact />
              </span>
              <span className="redis-key-row-name">{item.key}</span>
            </button>
          </li>
        )
      })}
    </ul>
  )
}

function RedisInspector({
  detail,
  error,
  onApplyTTL,
  onDeleteKey,
}: {
  detail: RedisKeyDetail | undefined
  error: string | undefined
  onApplyTTL: (key: string, ttlSeconds: number) => void
  onDeleteKey: (key: string) => void
}): JSX.Element {
  if (error) {
    return <EmptyState title="Key unavailable" description={error} />
  }
  if (!detail) {
    return (
      <EmptyState
        title="Select a key"
        description="Choose a key on the left to inspect its type, TTL, and value."
      />
    )
  }
  return (
    <div className="redis-inspector">
      <InspectorHeader detail={detail} />
      <ValueViewer detail={detail} />
      <InspectorActions detail={detail} onApplyTTL={onApplyTTL} onDeleteKey={onDeleteKey} />
    </div>
  )
}

function InspectorHeader({ detail }: { detail: RedisKeyDetail }): JSX.Element {
  const [copied, setCopied] = useState(false)
  function copy() {
    if (!navigator.clipboard) {
      return
    }
    navigator.clipboard
      .writeText(detail.key)
      .then(() => {
        setCopied(true)
        window.setTimeout(() => setCopied(false), 1500)
      })
      .catch(() => {
        /* clipboard denied; silent */
      })
  }
  return (
    <header className="redis-inspector-header">
      <span className="redis-inspector-eyebrow">Key</span>
      <div className="redis-key-title-row">
        <code className="redis-key-title">{detail.key}</code>
        <button className="redis-copy-button" onClick={copy} title="Copy key" type="button">
          {copied ? 'copied' : 'copy'}
        </button>
      </div>
      <div className="redis-key-badges">
        <TypeBadge type={detail.type} />
        <TTLBadge ttlSeconds={detail.ttlSeconds} />
        <SizeBadge detail={detail} />
      </div>
    </header>
  )
}

function ValueViewer({ detail }: { detail: RedisKeyDetail }): JSX.Element {
  if (!detail.preview || detail.preview.length === 0) {
    return (
      <section className="redis-inspector-section">
        <span className="redis-inspector-eyebrow">Value</span>
        <EmptyState
          title="Empty value"
          description="The key exists but has no previewable content in this slice."
        />
      </section>
    )
  }
  return (
    <section className="redis-inspector-section">
      <header className="redis-section-head">
        <span className="redis-inspector-eyebrow">Value</span>
      </header>
      {renderValueByType(detail)}
    </section>
  )
}

function renderValueByType(detail: RedisKeyDetail): JSX.Element {
  switch (detail.type) {
    case 'string':
      return <StringValueView raw={detail.preview[0] ?? ''} />
    case 'list':
      return <ListValueView items={detail.preview} />
    case 'hash':
      return <HashValueView entries={detail.preview} />
    case 'set':
      return <SetValueView members={detail.preview} />
    case 'zset':
      return <ZSetValueView items={detail.preview} />
    default:
      return <pre className="redis-value-pre">{detail.preview.join('\n')}</pre>
  }
}

function StringValueView({ raw }: { raw: string }): JSX.Element {
  const pretty = useMemo(() => tryFormatJSON(raw), [raw])
  const [mode, setMode] = useState<'pretty' | 'raw'>(pretty ? 'pretty' : 'raw')
  const display = mode === 'pretty' && pretty ? pretty : raw
  return (
    <div className="redis-value-string">
      <div className="redis-value-toolbar">
        <span className="redis-value-meta">{raw.length.toLocaleString()} char{raw.length === 1 ? '' : 's'}</span>
        {pretty ? (
          <div role="tablist" aria-label="Value format" className="redis-segmented">
            <button
              type="button"
              role="tab"
              aria-selected={mode === 'pretty'}
              className={mode === 'pretty' ? 'active' : undefined}
              onClick={() => setMode('pretty')}
            >
              Pretty
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={mode === 'raw'}
              className={mode === 'raw' ? 'active' : undefined}
              onClick={() => setMode('raw')}
            >
              Raw
            </button>
          </div>
        ) : null}
      </div>
      <pre className="redis-value-pre">{display}</pre>
    </div>
  )
}

function ListValueView({ items }: { items: string[] }): JSX.Element {
  return (
    <ol className="redis-list-view">
      {items.map((item, index) => (
        <li key={index}>
          <span className="redis-list-index">{index}</span>
          <code className="redis-list-value">{item}</code>
        </li>
      ))}
    </ol>
  )
}

function HashValueView({ entries }: { entries: string[] }): JSX.Element {
  return (
    <table className="redis-hash-table" aria-label="Hash fields">
      <thead>
        <tr>
          <th scope="col">Field</th>
          <th scope="col">Value</th>
        </tr>
      </thead>
      <tbody>
        {entries.map((entry, index) => {
          const [field, value] = splitFirst(entry, ': ')
          return (
            <tr key={index}>
              <th scope="row"><code>{field}</code></th>
              <td><code>{value}</code></td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function SetValueView({ members }: { members: string[] }): JSX.Element {
  return (
    <ul className="redis-set-view" aria-label="Set members">
      {members.map((member, index) => (
        <li key={index}>
          <code>{member}</code>
        </li>
      ))}
    </ul>
  )
}

function ZSetValueView({ items }: { items: string[] }): JSX.Element {
  return (
    <table className="redis-zset-table" aria-label="Sorted set members">
      <thead>
        <tr>
          <th scope="col">Member</th>
          <th scope="col">Score</th>
        </tr>
      </thead>
      <tbody>
        {items.map((entry, index) => {
          const [member, score] = splitFirst(entry, ': ')
          return (
            <tr key={index}>
              <th scope="row"><code>{member}</code></th>
              <td className="redis-zset-score">{score}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function InspectorActions({
  detail,
  onApplyTTL,
  onDeleteKey,
}: {
  detail: RedisKeyDetail
  onApplyTTL: (key: string, ttlSeconds: number) => void
  onDeleteKey: (key: string) => void
}): JSX.Element {
  const [ttlText, setTtlText] = useState('60')
  const ttlValue = Number(ttlText)
  const ttlValid = Number.isInteger(ttlValue) && ttlValue > 0
  function setExpire(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!ttlValid) {
      return
    }
    onApplyTTL(detail.key, ttlValue)
  }
  function persist() {
    onApplyTTL(detail.key, -1)
  }
  return (
    <section className="redis-inspector-section">
      <header className="redis-section-head">
        <span className="redis-inspector-eyebrow">Actions</span>
      </header>
      <form className="redis-ttl-form" onSubmit={setExpire}>
        <label>
          <span>TTL seconds</span>
          <input
            aria-label="TTL seconds"
            min={1}
            onChange={(event) => setTtlText(event.target.value)}
            type="number"
            value={ttlText}
          />
        </label>
        <Button disabled={!ttlValid} type="submit">
          Set TTL
        </Button>
        <Button className="ghost" onClick={persist} type="button">
          Persist (no TTL)
        </Button>
      </form>
      <div className="redis-danger-row">
        <Button className="danger" onClick={() => onDeleteKey(detail.key)} type="button">
          Delete key
        </Button>
      </div>
    </section>
  )
}

function TypeBadge({ type }: { type: string }): JSX.Element {
  const normalized = (type || 'unknown').toLowerCase()
  return <span className={`redis-badge redis-type-${normalized}`}>{normalized}</span>
}

function TTLBadge({ ttlSeconds, compact }: { ttlSeconds: number; compact?: boolean }): JSX.Element {
  if (ttlSeconds === -2) {
    return <span className="redis-badge redis-ttl-missing">{compact ? '✕' : 'missing'}</span>
  }
  if (ttlSeconds === -1) {
    return <span className="redis-badge redis-ttl-persistent">{compact ? '∞' : 'persistent'}</span>
  }
  return (
    <span className="redis-badge redis-ttl-active" title={`${ttlSeconds}s`}>
      {compact ? `${humanTTL(ttlSeconds)}` : `TTL ${humanTTL(ttlSeconds)}`}
    </span>
  )
}

function SizeBadge({ detail }: { detail: RedisKeyDetail }): JSX.Element | null {
  if (!detail.preview || detail.preview.length === 0) {
    return null
  }
  const size = detail.preview.length
  let label: string
  switch (detail.type) {
    case 'string':
      label = `${(detail.preview[0] ?? '').length} char${(detail.preview[0] ?? '').length === 1 ? '' : 's'}`
      break
    case 'list':
      label = `${size} item${size === 1 ? '' : 's'}`
      break
    case 'hash':
      label = `${size} field${size === 1 ? '' : 's'}`
      break
    case 'set':
      label = `${size} member${size === 1 ? '' : 's'}`
      break
    case 'zset':
      label = `${size} entr${size === 1 ? 'y' : 'ies'}`
      break
    default:
      label = `${size}`
  }
  return <span className="redis-badge redis-size">{label}</span>
}

type CommandHistoryEntry = {
  id: number
  input: string
  outcome: 'success' | 'error'
  detail: string
  rows?: string[]
}

function RedisCommandRunner(): JSX.Element {
  const [commandText, setCommandText] = useState('GET your-key')
  const [history, setHistory] = useState<CommandHistoryEntry[]>([])
  const counter = useRef(0)
  const parsed = useMemo(() => parseCommand(commandText), [commandText])

  function recordEntry(entry: Omit<CommandHistoryEntry, 'id'>) {
    counter.current += 1
    const id = counter.current
    setHistory((current) => [{ id, ...entry }, ...current].slice(0, COMMAND_HISTORY_LIMIT))
  }

  function runParsed(command: { command: string; args: string[] }, originalInput: string) {
    if (!command.command) {
      recordEntry({ input: originalInput, outcome: 'error', detail: 'No command supplied' })
      return
    }
    runRedisCommand(command)
      .then((response: RedisCommandResponse) => {
        recordEntry({
          input: originalInput,
          outcome: 'success',
          detail: `${response.command} (${response.class})`,
          rows: response.rows ?? [],
        })
      })
      .catch((error: Error) => {
        recordEntry({ input: originalInput, outcome: 'error', detail: error.message })
      })
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    runParsed(parsed, commandText)
  }

  function onKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault()
      runParsed(parsed, commandText)
    }
  }

  function rerun(input: string) {
    setCommandText(input)
    runParsed(parseCommand(input), input)
  }

  return (
    <div className="redis-runner">
      <form className="redis-runner-form" onSubmit={submit}>
        <label className="redis-runner-label">
          <span>Command</span>
          <textarea
            aria-label="Redis command"
            onChange={(event) => setCommandText(event.target.value)}
            onKeyDown={onKeyDown}
            rows={3}
            spellCheck={false}
            value={commandText}
          />
        </label>
        <div className="redis-runner-toolbar">
          <span className="redis-runner-hint">
            Allowlisted commands only · <kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>Enter</kbd> to run
          </span>
          <Button type="submit">Run</Button>
        </div>
      </form>

      {history.length === 0 ? (
        <EmptyState
          title="No commands yet"
          description="Try GET, HGETALL, LRANGE, SCAN 0 MATCH * COUNT 50, or TYPE."
        />
      ) : null}

      {history.length > 0 ? (
        <ol className="redis-runner-history" aria-label="Command history">
          {history.map((entry) => (
            <li key={entry.id} className={`redis-runner-entry redis-runner-${entry.outcome}`}>
              <header className="redis-runner-entry-head">
                <code className="redis-runner-input">{entry.input}</code>
                <button
                  className="redis-runner-rerun"
                  onClick={() => rerun(entry.input)}
                  title="Run again"
                  type="button"
                >
                  rerun
                </button>
              </header>
              <p className="redis-runner-detail">
                <span className="redis-runner-tag">{entry.outcome === 'success' ? 'OK' : 'ERR'}</span>
                <span>{entry.detail}</span>
              </p>
              {entry.rows && entry.rows.length > 0 ? (
                <pre className="redis-runner-result">{entry.rows.join('\n')}</pre>
              ) : null}
            </li>
          ))}
        </ol>
      ) : null}
    </div>
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
  const tokens = value.trim().split(/\s+/).filter((token) => token.length > 0)
  const [command = '', ...args] = tokens
  return { command, args }
}

function tryFormatJSON(value: string): string | undefined {
  const trimmed = value.trim()
  if (trimmed === '' || (trimmed[0] !== '{' && trimmed[0] !== '[' && trimmed[0] !== '"')) {
    return undefined
  }
  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2)
  } catch {
    return undefined
  }
}

function splitFirst(value: string, separator: string): [string, string] {
  const index = value.indexOf(separator)
  if (index === -1) {
    return [value, '']
  }
  return [value.slice(0, index), value.slice(index + separator.length)]
}

function humanTTL(seconds: number): string {
  if (seconds < 60) {
    return `${seconds}s`
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m`
  }
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h`
  }
  return `${Math.floor(seconds / 86400)}d`
}
