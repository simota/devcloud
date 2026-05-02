import { useCallback, useEffect, useMemo, useState } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import type { DashboardService } from '../dashboard/types'
import {
  getSQSDeadLetter,
  getSQSStatus,
  listSQSLeases,
  listSQSMessages,
  listSQSQueues,
  purgeSQSQueue,
} from './api'
import type { SQSDeadLetterResponse, SQSLeaseSnapshot, SQSMessageSnapshot, SQSQueueSnapshot, SQSStatus } from './types'

type QueuesState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: SQSStatus; queues: SQSQueueSnapshot[] }
  | { status: 'error'; message: string }

type DetailState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; messages: SQSMessageSnapshot[]; leases: SQSLeaseSnapshot[]; dlq: SQSDeadLetterResponse }
  | { status: 'error'; message: string }

type SQSDashboardProps = {
  service?: DashboardService
}

export function SQSDashboard({ service }: SQSDashboardProps): JSX.Element {
  const [queuesState, setQueuesState] = useState<QueuesState>({ status: 'loading' })
  const [detailState, setDetailState] = useState<DetailState>({ status: 'idle' })
  const [activeQueueName, setActiveQueueName] = useState<string>()
  const [activeMessageIndex, setActiveMessageIndex] = useState(0)
  const [queueFilter, setQueueFilter] = useState('')
  const [messageFilter, setMessageFilter] = useState('')
  const [purgeError, setPurgeError] = useState<string>()
  const isDisabled = service?.status === 'disabled'

  const refreshQueues = useCallback(() => {
    if (isDisabled) {
      setQueuesState({ status: 'success', statusPayload: disabledStatus(service), queues: [] })
      setDetailState({ status: 'idle' })
      setActiveQueueName(undefined)
      return
    }

    setQueuesState({ status: 'loading' })
    Promise.all([getSQSStatus(), listSQSQueues()])
      .then(([statusPayload, { queues }]) => {
        setQueuesState({ status: 'success', statusPayload, queues })
        setActiveQueueName((current) =>
          current && queues.some((queue) => queue.name === current) ? current : queues[0]?.name,
        )
      })
      .catch((error: Error) => {
        setQueuesState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refreshQueues()
  }, [refreshQueues])

  const queues = queuesState.status === 'success' ? queuesState.queues : []
  const activeQueue = queues.find((queue) => queue.name === activeQueueName)

  const refreshDetail = useCallback(() => {
    if (!activeQueueName || isDisabled) {
      setDetailState({ status: 'idle' })
      return
    }
    setDetailState({ status: 'loading' })
    Promise.all([listSQSMessages(activeQueueName), listSQSLeases(activeQueueName), getSQSDeadLetter(activeQueueName)])
      .then(([messages, leases, dlq]) => {
        setActiveMessageIndex(0)
        setDetailState({ status: 'success', messages: messages.messages, leases: leases.leases, dlq })
      })
      .catch((error: Error) => {
        setDetailState({ status: 'error', message: error.message })
      })
  }, [activeQueueName, isDisabled])

  useEffect(() => {
    refreshDetail()
  }, [refreshDetail])

  const filteredQueues = useMemo(() => {
    const query = queueFilter.trim().toLowerCase()
    if (query === '') {
      return queues
    }
    return queues.filter((queue) => queue.name.toLowerCase().includes(query))
  }, [queues, queueFilter])

  const filteredMessages = useMemo(() => {
    const messages = detailState.status === 'success' ? detailState.messages : []
    const query = messageFilter.trim().toLowerCase()
    if (query === '') {
      return messages
    }
    return messages.filter((message) => JSON.stringify(message).toLowerCase().includes(query))
  }, [detailState, messageFilter])

  const selectedMessage = filteredMessages[Math.min(activeMessageIndex, Math.max(filteredMessages.length - 1, 0))]

  if (isDisabled) {
    return (
      <Panel title="SQS">
        <EmptyState title="SQS is disabled" description="Enable the SQS service in devcloud config to inspect queues and messages." />
      </Panel>
    )
  }

  function selectQueue(queueName: string): void {
    setActiveQueueName(queueName)
    setActiveMessageIndex(0)
    setMessageFilter('')
    setPurgeError(undefined)
  }

  function purgeQueue(): void {
    if (!activeQueueName) {
      return
    }
    setPurgeError(undefined)
    purgeSQSQueue(activeQueueName)
      .then(() => {
        refreshQueues()
        refreshDetail()
      })
      .catch((error: Error) => {
        setPurgeError(error.message)
      })
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Queues">
        <div className="dynamodb-toolbar">
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter SQS queues"
              onChange={(event) => setQueueFilter(event.target.value)}
              placeholder="queue name"
              type="search"
              value={queueFilter}
            />
          </label>
          <Button onClick={refreshQueues}>Refresh</Button>
        </div>
        {queuesState.status === 'loading' ? <EmptyState title="Loading queues" description="Reading local SQS queue metadata." /> : null}
        {queuesState.status === 'error' ? (
          <EmptyState title="SQS queues unavailable" description={queuesState.message} actionLabel="Retry" onAction={refreshQueues} />
        ) : null}
        {queuesState.status === 'success' ? (
          <QueueList activeQueueName={activeQueueName} onSelectQueue={selectQueue} queues={filteredQueues} />
        ) : null}
      </Panel>

      <Panel title="Messages">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {activeQueue ? `${filteredMessages.length} shown / ${activeQueue.totalRetainedMessages} retained` : 'Select a queue'}
          </span>
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter SQS messages"
              disabled={!activeQueue}
              onChange={(event) => {
                setActiveMessageIndex(0)
                setMessageFilter(event.target.value)
              }}
              placeholder="body or attribute"
              type="search"
              value={messageFilter}
            />
          </label>
          <Button disabled={!activeQueue} onClick={refreshDetail}>
            Refresh
          </Button>
        </div>
        <MessageBrowser
          activeIndex={activeMessageIndex}
          detailState={detailState}
          messages={filteredMessages}
          onSelectIndex={setActiveMessageIndex}
          queueName={activeQueueName}
        />
      </Panel>

      <Panel title="Inspector">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">{activeQueue ? activeQueue.name : 'No queue selected'}</span>
          <Button className="danger" disabled={!activeQueue} onClick={purgeQueue}>
            Purge
          </Button>
        </div>
        {purgeError ? <p className="inspector-muted">{purgeError}</p> : null}
        <SQSInspector
          detailState={detailState}
          message={selectedMessage}
          queue={activeQueue}
          status={queuesState.status === 'success' ? queuesState.statusPayload : undefined}
        />
      </Panel>
    </div>
  )
}

type QueueListProps = {
  queues: SQSQueueSnapshot[]
  activeQueueName?: string
  onSelectQueue: (queueName: string) => void
}

function QueueList({ activeQueueName, onSelectQueue, queues }: QueueListProps): JSX.Element {
  if (queues.length === 0) {
    return <EmptyState title="No queues" description="Queues created through the SQS API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="SQS queues">
      {queues.map((queue) => (
        <button
          className={queue.name === activeQueueName ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={queue.name}
          onClick={() => onSelectQueue(queue.name)}
        >
          <span className="table-row-top">
            <span className="table-row-name">{queue.name}</span>
            <span className="count-pill">{queue.totalRetainedMessages}</span>
          </span>
          <span className="table-row-meta">{queue.url}</span>
          <span className="table-row-tags">
            <span>{queue.visibleMessages} visible</span>
            <span>{queue.notVisibleMessages} in flight</span>
            <span>{queue.delayedMessages} delayed</span>
          </span>
        </button>
      ))}
    </div>
  )
}

type MessageBrowserProps = {
  activeIndex: number
  detailState: DetailState
  messages: SQSMessageSnapshot[]
  queueName?: string
  onSelectIndex: (index: number) => void
}

function MessageBrowser({ activeIndex, detailState, messages, onSelectIndex, queueName }: MessageBrowserProps): JSX.Element {
  if (!queueName) {
    return <EmptyState title="No queue selected" description="Choose a queue to inspect retained messages." />
  }
  if (detailState.status === 'loading') {
    return <EmptyState title="Loading messages" description={`Reading messages from ${queueName}.`} />
  }
  if (detailState.status === 'error') {
    return <EmptyState title="SQS messages unavailable" description={detailState.message} />
  }
  if (messages.length === 0) {
    return <EmptyState title="No messages" description={`No retained messages in ${queueName} match the current filter.`} />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">State</th>
            <th scope="col">Body</th>
            <th scope="col">Receive</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((message, index) => (
            <tr
              className={index === activeIndex ? 'item-row active' : 'item-row'}
              key={`${message.messageId}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>{message.state}</td>
              <td>
                <MessagePreview message={message} />
              </td>
              <td>{message.receiveCount}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function MessagePreview({ message }: { message: SQSMessageSnapshot }): JSX.Element {
  return (
    <span className="attribute-preview">
      <span className="attribute-chip">{message.body || '(empty body)'}</span>
      {message.messageGroupId ? <span className="attribute-chip">group: {message.messageGroupId}</span> : null}
      {message.sequenceNumber ? <span className="attribute-chip">seq: {message.sequenceNumber}</span> : null}
    </span>
  )
}

type SQSInspectorProps = {
  detailState: DetailState
  message?: SQSMessageSnapshot
  queue?: SQSQueueSnapshot
  status?: SQSStatus
}

function SQSInspector({ detailState, message, queue, status }: SQSInspectorProps): JSX.Element {
  if (!queue) {
    return <EmptyState title="Inspector" description="Queue attributes and selected message JSON will appear here." />
  }

  const leases = detailState.status === 'success' ? detailState.leases : []
  const dlq = detailState.status === 'success' ? detailState.dlq : undefined

  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Queue</span>
        <h3>{queue.name}</h3>
        <dl className="inspector-list">
          <div>
            <dt>Endpoint</dt>
            <dd>
              <code>{status?.endpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Region</dt>
            <dd>{status?.region ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>ARN</dt>
            <dd>
              <code>{queue.arn}</code>
            </dd>
          </div>
          <div>
            <dt>Visibility</dt>
            <dd>{queue.attributes.VisibilityTimeout ?? 'unknown'}s</dd>
          </div>
          <div>
            <dt>Leases</dt>
            <dd>{leases.length}</dd>
          </div>
          <div>
            <dt>DLQ sources</dt>
            <dd>{dlq?.deadLetterSourceQueues.map((source) => source.name).join(', ') || 'none'}</dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Selected message</span>
        {message ? (
          <pre className="mail-preview">{JSON.stringify(message, null, 2)}</pre>
        ) : (
          <p className="inspector-muted">Select a message row to inspect JSON.</p>
        )}
      </section>
    </div>
  )
}

function disabledStatus(service?: DashboardService): SQSStatus {
  return {
    service: 'sqs',
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:9324',
    region: 'us-east-1',
    authMode: 'relaxed',
    storagePath: service?.storagePath ?? '.devcloud/data/sqs',
    queueCount: 0,
  }
}
