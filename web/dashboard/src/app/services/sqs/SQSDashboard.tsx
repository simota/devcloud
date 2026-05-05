import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import type { DashboardService } from '../dashboard/types'
import {
  changeSQSMessageVisibility,
  createSQSQueue,
  deleteSQSMessage,
  getSQSDeadLetter,
  getSQSStatus,
  listSQSLeases,
  listSQSMessages,
  listSQSQueues,
  purgeSQSQueue,
  receiveSQSMessage,
  sendSQSMessage,
} from './api'
import type {
  SQSDeadLetterResponse,
  SQSLeaseSnapshot,
  SQSMessageAttribute,
  SQSMessageSnapshot,
  SQSQueueSnapshot,
  SQSReceivedMessage,
  SQSStatus,
} from './types'

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
  const [newQueueName, setNewQueueName] = useState('')
  const [newQueueKind, setNewQueueKind] = useState<'standard' | 'fifo'>('standard')
  const [newQueueVisibility, setNewQueueVisibility] = useState('30')
  const [newQueueDelay, setNewQueueDelay] = useState('0')
  const [sendBody, setSendBody] = useState('')
  const [sendDelay, setSendDelay] = useState('0')
  const [sendAttributesJSON, setSendAttributesJSON] = useState('')
  const [sendGroupId, setSendGroupId] = useState('')
  const [sendDeduplicationId, setSendDeduplicationId] = useState('')
  const [receiveMaxMessages, setReceiveMaxMessages] = useState('1')
  const [receiveVisibilityTimeout, setReceiveVisibilityTimeout] = useState('30')
  const [receiveWaitTime, setReceiveWaitTime] = useState('0')
  const [receiveAttributeNames, setReceiveAttributeNames] = useState('All')
  const [receiveMessageAttributeNames, setReceiveMessageAttributeNames] = useState('All')
  const [receivedMessages, setReceivedMessages] = useState<SQSReceivedMessage[]>([])
  const [selectedReceiptIndex, setSelectedReceiptIndex] = useState(0)
  const [pastedReceiptHandle, setPastedReceiptHandle] = useState('')
  const [deleteConfirmation, setDeleteConfirmation] = useState('')
  const [visibilityTimeout, setVisibilityTimeout] = useState('0')
  const [visibilityConfirmation, setVisibilityConfirmation] = useState('')
  const [purgeConfirmation, setPurgeConfirmation] = useState('')
  const [operationMessage, setOperationMessage] = useState('')
  const [operationError, setOperationError] = useState('')
  const [busyAction, setBusyAction] = useState<string>()
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
  const selectedReceivedMessage = receivedMessages[Math.min(selectedReceiptIndex, Math.max(receivedMessages.length - 1, 0))]
  const selectedReceiptHandle = pastedReceiptHandle.trim() || selectedReceivedMessage?.ReceiptHandle || ''

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
    setReceivedMessages([])
    setSelectedReceiptIndex(0)
    setPastedReceiptHandle('')
    setDeleteConfirmation('')
    setVisibilityConfirmation('')
    setPurgeConfirmation('')
    setMessageFilter('')
    setPurgeError(undefined)
  }

  function purgeQueue(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeQueueName) {
      return
    }
    if (purgeConfirmation !== activeQueueName) {
      setPurgeError('Type the queue name to confirm purge confirmation')
      return
    }
    setPurgeError(undefined)
    purgeSQSQueue(activeQueueName)
      .then(() => {
        setPurgeConfirmation('')
        refreshQueues()
        refreshDetail()
      })
      .catch((error: Error) => {
        setPurgeError(error.message)
      })
  }

  async function runAction(name: string, action: () => Promise<string>): Promise<void> {
    setBusyAction(name)
    setOperationError('')
    setOperationMessage('')
    try {
      const message = await action()
      setOperationMessage(message)
      refreshQueues()
      refreshDetail()
    } catch (error) {
      setOperationError(error instanceof Error ? error.message : 'SQS operation failed')
    } finally {
      setBusyAction(undefined)
    }
  }

  function handleCreateQueue(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const queueName = newQueueName.trim()
    if (queueName === '') {
      setOperationError('Queue name is required')
      return
    }
    const finalQueueName = newQueueKind === 'fifo' && !queueName.endsWith('.fifo') ? `${queueName}.fifo` : queueName
    void runAction('create-queue', async () => {
      await createSQSQueue({
        QueueName: finalQueueName,
        Attributes: compactStringMap({
          VisibilityTimeout: normalizedNumberString(newQueueVisibility),
          DelaySeconds: normalizedNumberString(newQueueDelay),
          FifoQueue: newQueueKind === 'fifo' ? 'true' : undefined,
          ContentBasedDeduplication: newQueueKind === 'fifo' ? 'true' : undefined,
        }),
        Tags: { source: 'dashboard' },
      })
      setNewQueueName('')
      setActiveQueueName(finalQueueName)
      return `Created queue ${finalQueueName}`
    })
  }

  function handleSendMessage(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeQueue) {
      setOperationError('Queue is required')
      return
    }
    if (sendBody === '') {
      setOperationError('Message body is required')
      return
    }
    const messageAttributes = parseMessageAttributes(sendAttributesJSON)
    if (messageAttributes instanceof Error) {
      setOperationError(messageAttributes.message)
      return
    }
    void runAction('send-message', async () => {
      const response = await sendSQSMessage(activeQueue.name, {
        MessageBody: sendBody,
        DelaySeconds: normalizedOptionalNumber(sendDelay),
        MessageAttributes: messageAttributes,
        MessageGroupId: sendGroupId.trim() || undefined,
        MessageDeduplicationId: sendDeduplicationId.trim() || undefined,
      })
      setSendBody('')
      setSendAttributesJSON('')
      return `Sent message ${response.MessageId}`
    })
  }

  function handleReceiveMessage(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeQueue) {
      setOperationError('Queue is required')
      return
    }
    void runAction('receive-message', async () => {
      const response = await receiveSQSMessage(activeQueue.name, {
        MaxNumberOfMessages: normalizedOptionalNumber(receiveMaxMessages),
        VisibilityTimeout: normalizedNonNegativeOptionalNumber(receiveVisibilityTimeout),
        WaitTimeSeconds: normalizedNonNegativeOptionalNumber(receiveWaitTime),
        AttributeNames: parseNameList(receiveAttributeNames),
        MessageAttributeNames: parseNameList(receiveMessageAttributeNames),
      })
      const messages = response.Messages ?? []
      setReceivedMessages(messages)
      setSelectedReceiptIndex(0)
      setPastedReceiptHandle('')
      setDeleteConfirmation('')
      return messages.length === 1 ? `Received 1 message` : `Received ${messages.length} messages`
    })
  }

  function handleDeleteMessage(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeQueue) {
      setOperationError('Queue is required')
      return
    }
    if (selectedReceiptHandle === '') {
      setOperationError('Receipt handle is required')
      return
    }
    if (deleteConfirmation !== 'delete') {
      setOperationError('Type delete to confirm DeleteMessage')
      return
    }
    void runAction('delete-message', async () => {
      await deleteSQSMessage(activeQueue.name, { ReceiptHandle: selectedReceiptHandle })
      setReceivedMessages((messages) => messages.filter((message) => message.ReceiptHandle !== selectedReceiptHandle))
      setSelectedReceiptIndex(0)
      setPastedReceiptHandle('')
      setDeleteConfirmation('')
      setVisibilityConfirmation('')
      return 'Deleted message for the selected receipt handle'
    })
  }

  function handleChangeVisibility(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeQueue) {
      setOperationError('Queue is required')
      return
    }
    if (selectedReceiptHandle === '') {
      setOperationError('Receipt handle is required')
      return
    }
    if (visibilityConfirmation !== 'visibility') {
      setOperationError('Type visibility to confirm ChangeMessageVisibility')
      return
    }
    const timeout = normalizedNonNegativeOptionalNumber(visibilityTimeout)
    if (typeof timeout !== 'number') {
      setOperationError('Visibility timeout must be a non-negative number')
      return
    }
    void runAction('change-visibility', async () => {
      await changeSQSMessageVisibility(activeQueue.name, {
        ReceiptHandle: selectedReceiptHandle,
        VisibilityTimeout: timeout,
      })
      setVisibilityConfirmation('')
      return `Changed visibility timeout to ${timeout}s`
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
        <form className="pubsub-action-form stacked" onSubmit={handleCreateQueue}>
          <label className="compact-filter">
            <span>Queue name</span>
            <input
              aria-label="New SQS queue name"
              onChange={(event) => setNewQueueName(event.target.value)}
              placeholder={newQueueKind === 'fifo' ? 'jobs.fifo' : 'jobs'}
              value={newQueueName}
            />
          </label>
          <label className="compact-filter small">
            <span>Type</span>
            <select
              aria-label="New SQS queue type"
              onChange={(event) => setNewQueueKind(event.target.value === 'fifo' ? 'fifo' : 'standard')}
              value={newQueueKind}
            >
              <option value="standard">Standard</option>
              <option value="fifo">FIFO</option>
            </select>
          </label>
          <label className="compact-filter small">
            <span>Visibility</span>
            <input
              aria-label="New SQS queue visibility timeout seconds"
              inputMode="numeric"
              onChange={(event) => setNewQueueVisibility(event.target.value)}
              value={newQueueVisibility}
            />
          </label>
          <label className="compact-filter small">
            <span>Delay</span>
            <input
              aria-label="New SQS queue delay seconds"
              inputMode="numeric"
              onChange={(event) => setNewQueueDelay(event.target.value)}
              value={newQueueDelay}
            />
          </label>
          <Button disabled={busyAction === 'create-queue'} type="submit">
            Create
          </Button>
        </form>
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
        <form className="pubsub-action-form stacked" onSubmit={handleSendMessage}>
          <label className="compact-filter wide">
            <span>Message body</span>
            <textarea
              aria-label="SQS message body"
              disabled={!activeQueue}
              onChange={(event) => setSendBody(event.target.value)}
              placeholder="message body"
              rows={3}
              value={sendBody}
            />
          </label>
          <label className="compact-filter small">
            <span>Delay</span>
            <input
              aria-label="SQS message delay seconds"
              disabled={!activeQueue}
              inputMode="numeric"
              onChange={(event) => setSendDelay(event.target.value)}
              value={sendDelay}
            />
          </label>
          <label className="compact-filter">
            <span>Attributes JSON</span>
            <input
              aria-label="SQS message attributes JSON"
              disabled={!activeQueue}
              onChange={(event) => setSendAttributesJSON(event.target.value)}
              placeholder='{"kind":{"DataType":"String","StringValue":"test"}}'
              value={sendAttributesJSON}
            />
          </label>
          <label className="compact-filter small">
            <span>Group</span>
            <input
              aria-label="SQS FIFO message group ID"
              disabled={!activeQueue}
              onChange={(event) => setSendGroupId(event.target.value)}
              placeholder="FIFO only"
              value={sendGroupId}
            />
          </label>
          <label className="compact-filter small">
            <span>Dedup</span>
            <input
              aria-label="SQS FIFO message deduplication ID"
              disabled={!activeQueue}
              onChange={(event) => setSendDeduplicationId(event.target.value)}
              placeholder="optional"
              value={sendDeduplicationId}
            />
          </label>
          <Button disabled={!activeQueue || busyAction === 'send-message'} type="submit">
            Send
          </Button>
        </form>
        <MessageBrowser
          activeIndex={activeMessageIndex}
          detailState={detailState}
          messages={filteredMessages}
          onSelectIndex={setActiveMessageIndex}
          queueName={activeQueueName}
        />
        <form className="pubsub-action-form stacked" onSubmit={handleReceiveMessage}>
          <label className="compact-filter small">
            <span>Max</span>
            <input
              aria-label="SQS receive max messages"
              disabled={!activeQueue}
              inputMode="numeric"
              onChange={(event) => setReceiveMaxMessages(event.target.value)}
              value={receiveMaxMessages}
            />
          </label>
          <label className="compact-filter small">
            <span>Visibility</span>
            <input
              aria-label="SQS receive visibility timeout seconds"
              disabled={!activeQueue}
              inputMode="numeric"
              onChange={(event) => setReceiveVisibilityTimeout(event.target.value)}
              value={receiveVisibilityTimeout}
            />
          </label>
          <label className="compact-filter small">
            <span>Wait</span>
            <input
              aria-label="SQS receive wait time seconds"
              disabled={!activeQueue}
              inputMode="numeric"
              onChange={(event) => setReceiveWaitTime(event.target.value)}
              value={receiveWaitTime}
            />
          </label>
          <label className="compact-filter">
            <span>Attrs</span>
            <input
              aria-label="SQS receive attribute names"
              disabled={!activeQueue}
              onChange={(event) => setReceiveAttributeNames(event.target.value)}
              placeholder="All or comma-separated names"
              value={receiveAttributeNames}
            />
          </label>
          <label className="compact-filter">
            <span>Msg attrs</span>
            <input
              aria-label="SQS receive message attribute names"
              disabled={!activeQueue}
              onChange={(event) => setReceiveMessageAttributeNames(event.target.value)}
              placeholder="All or comma-separated names"
              value={receiveMessageAttributeNames}
            />
          </label>
          <Button disabled={!activeQueue || busyAction === 'receive-message'} type="submit">
            Receive
          </Button>
        </form>
        <ReceivedMessageList
          messages={receivedMessages}
          onSelectIndex={setSelectedReceiptIndex}
          selectedIndex={selectedReceiptIndex}
        />
      </Panel>

      <Panel title="Inspector">
        {operationError ? <p className="operation-message error">{operationError}</p> : null}
        {operationMessage ? <p className="operation-message success">{operationMessage}</p> : null}
        <form className="pubsub-action-form stacked" onSubmit={purgeQueue}>
          <span className="toolbar-count">{activeQueue ? activeQueue.name : 'No queue selected'}</span>
          <label className="compact-filter">
            <span>Purge confirmation</span>
            <input
              aria-label="Confirm SQS purge queue"
              disabled={!activeQueue}
              onChange={(event) => setPurgeConfirmation(event.target.value)}
              placeholder={activeQueue?.name ?? 'queue name'}
              value={purgeConfirmation}
            />
          </label>
          <Button
            className="danger"
            disabled={!activeQueue || purgeConfirmation !== activeQueue.name || busyAction === 'purge-queue'}
            type="submit"
          >
            Purge
          </Button>
        </form>
        {purgeError ? <p className="inspector-muted">{purgeError}</p> : null}
        <form className="pubsub-action-form stacked" onSubmit={handleChangeVisibility}>
          <label className="compact-filter small">
            <span>Visibility timeout</span>
            <input
              aria-label="SQS change visibility timeout seconds"
              disabled={!activeQueue || selectedReceiptHandle === ''}
              inputMode="numeric"
              onChange={(event) => setVisibilityTimeout(event.target.value)}
              value={visibilityTimeout}
            />
          </label>
          <label className="compact-filter small">
            <span>Confirm</span>
            <input
              aria-label="Confirm SQS change message visibility"
              disabled={!activeQueue || selectedReceiptHandle === ''}
              onChange={(event) => setVisibilityConfirmation(event.target.value)}
              placeholder="visibility"
              value={visibilityConfirmation}
            />
          </label>
          <Button
            disabled={
              !activeQueue ||
              selectedReceiptHandle === '' ||
              visibilityConfirmation !== 'visibility' ||
              busyAction === 'change-visibility'
            }
            type="submit"
          >
            Change visibility
          </Button>
        </form>
        <form className="pubsub-action-form stacked" onSubmit={handleDeleteMessage}>
          <label className="compact-filter wide">
            <span>Receipt handle</span>
            <input
              aria-label="SQS delete receipt handle"
              disabled={!activeQueue}
              onChange={(event) => setPastedReceiptHandle(event.target.value)}
              placeholder={selectedReceivedMessage ? maskReceiptHandle(selectedReceivedMessage.ReceiptHandle) : 'paste receipt handle or select received message'}
              value={pastedReceiptHandle}
            />
          </label>
          <label className="compact-filter small">
            <span>Confirm</span>
            <input
              aria-label="Confirm SQS delete message"
              disabled={!activeQueue || selectedReceiptHandle === ''}
              onChange={(event) => setDeleteConfirmation(event.target.value)}
              placeholder="delete"
              value={deleteConfirmation}
            />
          </label>
          <Button
            className="danger"
            disabled={!activeQueue || selectedReceiptHandle === '' || deleteConfirmation !== 'delete' || busyAction === 'delete-message'}
            type="submit"
          >
            Delete
          </Button>
        </form>
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

type ReceivedMessageListProps = {
  messages: SQSReceivedMessage[]
  selectedIndex: number
  onSelectIndex: (index: number) => void
}

function ReceivedMessageList({ messages, onSelectIndex, selectedIndex }: ReceivedMessageListProps): JSX.Element | null {
  if (messages.length === 0) {
    return null
  }

  return (
    <div className="dynamodb-item-table-wrap" aria-label="Received SQS messages">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Use</th>
            <th scope="col">Message</th>
            <th scope="col">Receipt handle</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((message, index) => (
            <tr
              className={index === selectedIndex ? 'item-row active' : 'item-row'}
              key={`${message.MessageId}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>{index === selectedIndex ? 'selected' : 'select'}</td>
              <td>
                <span className="attribute-preview">
                  <span className="attribute-chip">{message.Body || '(empty body)'}</span>
                  <span className="attribute-chip">{message.MessageId}</span>
                </span>
              </td>
              <td>
                <code>{maskReceiptHandle(message.ReceiptHandle)}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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
  const redrivePolicy = parseRedrivePolicy(queue.attributes.RedrivePolicy)
  const redriveAllowPolicy = parseRedriveAllowPolicy(queue.attributes.RedriveAllowPolicy)

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
          <div>
            <dt>DLQ target</dt>
            <dd>{dlq?.deadLetterQueue?.name ?? redrivePolicy?.deadLetterTargetArn ?? 'none'}</dd>
          </div>
          <div>
            <dt>Redrive</dt>
            <dd>
              {redrivePolicy ? `maxReceiveCount ${redrivePolicy.maxReceiveCount}` : 'none'}
              {redriveAllowPolicy ? `, allow ${redriveAllowPolicy.redrivePermission}` : ''}
            </dd>
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

function normalizedNumberString(value: string): string | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined
  }
  return String(parsed)
}

function normalizedOptionalNumber(value: string): number | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined
  }
  return parsed
}

function normalizedNonNegativeOptionalNumber(value: string): number | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined
  }
  return parsed
}

function compactStringMap(input: Record<string, string | undefined>): Record<string, string> {
  return Object.fromEntries(Object.entries(input).filter((entry): entry is [string, string] => typeof entry[1] === 'string'))
}

function parseMessageAttributes(value: string): Record<string, SQSMessageAttribute> | undefined | Error {
  const trimmed = value.trim()
  if (trimmed === '') {
    return undefined
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return new Error('Message attributes JSON is invalid')
  }
  if (!isPlainObject(parsed)) {
    return new Error('Message attributes must be a JSON object')
  }
  for (const [name, attribute] of Object.entries(parsed)) {
    if (!isPlainObject(attribute) || typeof attribute.DataType !== 'string' || attribute.DataType.trim() === '') {
      return new Error(`Message attribute ${name} must include DataType`)
    }
  }
  return parsed as Record<string, SQSMessageAttribute>
}

function parseNameList(value: string): string[] | undefined {
  const names = value
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean)
  return names.length > 0 ? names : undefined
}

function parseRedrivePolicy(value?: string): { deadLetterTargetArn: string; maxReceiveCount: string } | undefined {
  if (!value) {
    return undefined
  }
  try {
    const parsed: unknown = JSON.parse(value)
    if (!isPlainObject(parsed)) {
      return undefined
    }
    const deadLetterTargetArn = parsed.deadLetterTargetArn
    const maxReceiveCount = parsed.maxReceiveCount
    if (typeof deadLetterTargetArn !== 'string') {
      return undefined
    }
    return {
      deadLetterTargetArn,
      maxReceiveCount: typeof maxReceiveCount === 'string' ? maxReceiveCount : String(maxReceiveCount ?? ''),
    }
  } catch {
    return undefined
  }
}

function parseRedriveAllowPolicy(value?: string): { redrivePermission: string } | undefined {
  if (!value) {
    return undefined
  }
  try {
    const parsed: unknown = JSON.parse(value)
    if (!isPlainObject(parsed) || typeof parsed.redrivePermission !== 'string') {
      return undefined
    }
    return { redrivePermission: parsed.redrivePermission }
  } catch {
    return undefined
  }
}

function maskReceiptHandle(value: string): string {
  if (value.length <= 12) {
    return value === '' ? '' : '...'
  }
  return `${value.slice(0, 6)}...${value.slice(-6)}`
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
