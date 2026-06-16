import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
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
import { CreateQueueForm } from './CreateQueueForm'
import {
  compactStringMap,
  disabledStatus,
  normalizedNonNegativeOptionalNumber,
  normalizedNumberString,
  normalizedOptionalNumber,
  parseMessageAttributes,
  parseNameList,
} from './helpers'
import {
  ChangeVisibilityForm,
  DeleteMessageForm,
  PurgeQueueForm,
} from './InspectorOperationForms'
import { MessageBrowser } from './MessageBrowser'
import { ReceiveMessageForm, SendMessageForm } from './MessageOperationForms'
import { QueueList } from './QueueList'
import { ReceivedMessageList } from './ReceivedMessageList'
import { SQSInspector } from './SQSInspector'
import type { DetailState, QueuesState } from './state'
import type { SQSReceivedMessage } from './types'

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

  useDashboardEvents({ topics: ['sqs'], onEvent: refreshQueues, enabled: !isDisabled })

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
        <CreateQueueForm
          busyAction={busyAction}
          newQueueDelay={newQueueDelay}
          newQueueKind={newQueueKind}
          newQueueName={newQueueName}
          newQueueVisibility={newQueueVisibility}
          onSubmit={handleCreateQueue}
          setNewQueueDelay={setNewQueueDelay}
          setNewQueueKind={setNewQueueKind}
          setNewQueueName={setNewQueueName}
          setNewQueueVisibility={setNewQueueVisibility}
        />
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
        <SendMessageForm
          activeQueue={Boolean(activeQueue)}
          busyAction={busyAction}
          onSubmit={handleSendMessage}
          sendAttributesJSON={sendAttributesJSON}
          sendBody={sendBody}
          sendDeduplicationId={sendDeduplicationId}
          sendDelay={sendDelay}
          sendGroupId={sendGroupId}
          setSendAttributesJSON={setSendAttributesJSON}
          setSendBody={setSendBody}
          setSendDeduplicationId={setSendDeduplicationId}
          setSendDelay={setSendDelay}
          setSendGroupId={setSendGroupId}
        />
        <MessageBrowser
          activeIndex={activeMessageIndex}
          detailState={detailState}
          messages={filteredMessages}
          onSelectIndex={setActiveMessageIndex}
          queueName={activeQueueName}
        />
        <ReceiveMessageForm
          activeQueue={Boolean(activeQueue)}
          busyAction={busyAction}
          onSubmit={handleReceiveMessage}
          receiveAttributeNames={receiveAttributeNames}
          receiveMaxMessages={receiveMaxMessages}
          receiveMessageAttributeNames={receiveMessageAttributeNames}
          receiveVisibilityTimeout={receiveVisibilityTimeout}
          receiveWaitTime={receiveWaitTime}
          setReceiveAttributeNames={setReceiveAttributeNames}
          setReceiveMaxMessages={setReceiveMaxMessages}
          setReceiveMessageAttributeNames={setReceiveMessageAttributeNames}
          setReceiveVisibilityTimeout={setReceiveVisibilityTimeout}
          setReceiveWaitTime={setReceiveWaitTime}
        />
        <ReceivedMessageList
          messages={receivedMessages}
          onSelectIndex={setSelectedReceiptIndex}
          selectedIndex={selectedReceiptIndex}
        />
      </Panel>

      <Panel title="Inspector">
        {operationError ? <p className="operation-message error">{operationError}</p> : null}
        {operationMessage ? <p className="operation-message success">{operationMessage}</p> : null}
        <PurgeQueueForm
          activeQueue={activeQueue}
          busyAction={busyAction}
          onSubmit={purgeQueue}
          purgeConfirmation={purgeConfirmation}
          setPurgeConfirmation={setPurgeConfirmation}
        />
        {purgeError ? <p className="inspector-muted">{purgeError}</p> : null}
        <ChangeVisibilityForm
          activeQueue={Boolean(activeQueue)}
          busyAction={busyAction}
          onSubmit={handleChangeVisibility}
          selectedReceiptHandle={selectedReceiptHandle}
          setVisibilityConfirmation={setVisibilityConfirmation}
          setVisibilityTimeout={setVisibilityTimeout}
          visibilityConfirmation={visibilityConfirmation}
          visibilityTimeout={visibilityTimeout}
        />
        <DeleteMessageForm
          activeQueue={Boolean(activeQueue)}
          busyAction={busyAction}
          deleteConfirmation={deleteConfirmation}
          onSubmit={handleDeleteMessage}
          pastedReceiptHandle={pastedReceiptHandle}
          selectedReceiptHandle={selectedReceiptHandle}
          selectedReceivedMessage={selectedReceivedMessage}
          setDeleteConfirmation={setDeleteConfirmation}
          setPastedReceiptHandle={setPastedReceiptHandle}
        />
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
