import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import type { DashboardService } from '../dashboard/types'
import {
  ackPubSubMessages,
  createPubSubSubscription,
  createPubSubTopic,
  getPubSubStatus,
  listPubSubSubscriptions,
  listPubSubTopics,
  publishPubSubMessage,
  pullPubSubMessages,
} from './api'
import type {
  PubSubDeliverySnapshot,
  PubSubReceivedMessage,
  PubSubStatus,
  PubSubSubscriptionSnapshot,
  PubSubTopicSnapshot,
} from './types'

type PubSubState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: PubSubStatus; topics: PubSubTopicSnapshot[]; subscriptions: PubSubSubscriptionSnapshot[] }
  | { status: 'error'; message: string }

type PubSubDashboardProps = {
  service?: DashboardService
}

export function PubSubDashboard({ service }: PubSubDashboardProps): JSX.Element {
  const [state, setState] = useState<PubSubState>({ status: 'loading' })
  const [activeTopicName, setActiveTopicName] = useState<string>()
  const [activeSubscriptionName, setActiveSubscriptionName] = useState<string>()
  const [topicFilter, setTopicFilter] = useState('')
  const [subscriptionFilter, setSubscriptionFilter] = useState('')
  const [newTopicId, setNewTopicId] = useState('')
  const [newSubscriptionId, setNewSubscriptionId] = useState('')
  const [subscriptionTopicId, setSubscriptionTopicId] = useState('')
  const [ackDeadlineSeconds, setAckDeadlineSeconds] = useState('10')
  const [publishText, setPublishText] = useState('')
  const [orderingKey, setOrderingKey] = useState('')
  const [pullMaxMessages, setPullMaxMessages] = useState('1')
  const [pulledMessages, setPulledMessages] = useState<PubSubReceivedMessage[]>([])
  const [selectedAckId, setSelectedAckId] = useState('')
  const [actionMessage, setActionMessage] = useState('')
  const [actionError, setActionError] = useState('')
  const [busyAction, setBusyAction] = useState<string>()
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      setState({ status: 'success', statusPayload: disabledStatus(service), topics: [], subscriptions: [] })
      setActiveSubscriptionName(undefined)
      return
    }

    setState({ status: 'loading' })
    Promise.all([getPubSubStatus(), listPubSubTopics(), listPubSubSubscriptions()])
      .then(([statusPayload, topics, subscriptions]) => {
        setState({
          status: 'success',
          statusPayload,
          topics: topics.topics,
          subscriptions: subscriptions.subscriptions,
        })
        setActiveTopicName((current) => (current && topics.topics.some((topic) => topic.name === current) ? current : topics.topics[0]?.name))
        setSubscriptionTopicId((current) => current || resourceId(topics.topics[0]?.name ?? ''))
        setActiveSubscriptionName((current) =>
          current && subscriptions.subscriptions.some((subscription) => subscription.name === current)
            ? current
            : subscriptions.subscriptions[0]?.name,
        )
      })
      .catch((error: Error) => {
        setState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refresh()
  }, [refresh])

  useDashboardEvents({ topics: ['pubsub'], onEvent: refresh, enabled: !isDisabled })

  const topics = state.status === 'success' ? state.topics : []
  const subscriptions = state.status === 'success' ? state.subscriptions : []
  const activeTopic = topics.find((topic) => topic.name === activeTopicName)
  const activeSubscription = subscriptions.find((subscription) => subscription.name === activeSubscriptionName)

  const filteredTopics = useMemo(() => {
    const query = topicFilter.trim().toLowerCase()
    if (query === '') {
      return topics
    }
    return topics.filter((topic) => topic.name.toLowerCase().includes(query))
  }, [topics, topicFilter])

  const filteredSubscriptions = useMemo(() => {
    const query = subscriptionFilter.trim().toLowerCase()
    if (query === '') {
      return subscriptions
    }
    return subscriptions.filter((subscription) => JSON.stringify(subscription).toLowerCase().includes(query))
  }, [subscriptions, subscriptionFilter])

  async function runAction(name: string, action: () => Promise<string>): Promise<void> {
    setBusyAction(name)
    setActionError('')
    setActionMessage('')
    try {
      const message = await action()
      setActionMessage(message)
      refresh()
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Pub/Sub operation failed')
    } finally {
      setBusyAction(undefined)
    }
  }

  function handleCreateTopic(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const topicId = newTopicId.trim()
    if (topicId === '') {
      setActionError('Topic ID is required')
      return
    }
    void runAction('create-topic', async () => {
      const topic = await createPubSubTopic(topicId)
      setNewTopicId('')
      setActiveTopicName(topic.name)
      setSubscriptionTopicId(resourceId(topic.name))
      return `Created topic ${resourceId(topic.name)}`
    })
  }

  function handleCreateSubscription(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const subscriptionId = newSubscriptionId.trim()
    const topicId = subscriptionTopicId.trim() || resourceId(activeTopic?.name ?? '')
    if (subscriptionId === '' || topicId === '') {
      setActionError('Subscription ID and topic are required')
      return
    }
    void runAction('create-subscription', async () => {
      const subscription = await createPubSubSubscription({
        subscriptionId,
        topicId,
        ackDeadlineSeconds: Number.parseInt(ackDeadlineSeconds, 10) || undefined,
      })
      setNewSubscriptionId('')
      setActiveSubscriptionName(subscription.name)
      return `Created subscription ${resourceId(subscription.name)}`
    })
  }

  function handlePublish(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const topicId = resourceId(activeTopic?.name ?? '')
    if (topicId === '' || publishText === '') {
      setActionError('Topic and message are required')
      return
    }
    void runAction('publish', async () => {
      const response = await publishPubSubMessage({
        topicId,
        data: encodeBase64(publishText),
        attributes: { source: 'dashboard' },
        orderingKey: orderingKey.trim() || undefined,
      })
      setPublishText('')
      setOrderingKey('')
      return `Published ${response.messageIds.length} message`
    })
  }

  function handlePull(): void {
    const subscriptionId = resourceId(activeSubscription?.name ?? '')
    if (subscriptionId === '') {
      setActionError('Subscription is required')
      return
    }
    void runAction('pull', async () => {
      const response = await pullPubSubMessages(subscriptionId, Number.parseInt(pullMaxMessages, 10) || 1)
      const receivedMessages = response.receivedMessages ?? []
      setPulledMessages(receivedMessages)
      setSelectedAckId(receivedMessages[0]?.ackId ?? '')
      return `Pulled ${receivedMessages.length} message`
    })
  }

  function handleAck(): void {
    const subscriptionId = resourceId(activeSubscription?.name ?? '')
    const ackIds = selectedAckId ? [selectedAckId] : pulledMessages.map((message) => message.ackId).filter(Boolean)
    if (subscriptionId === '' || ackIds.length === 0) {
      setActionError('Ack ID is required')
      return
    }
    void runAction('ack', async () => {
      await ackPubSubMessages(subscriptionId, ackIds)
      setPulledMessages((current) => current.filter((message) => !ackIds.includes(message.ackId)))
      setSelectedAckId('')
      return `Acknowledged ${ackIds.length} message`
    })
  }

  if (isDisabled) {
    return (
      <Panel title="Pub/Sub">
        <EmptyState title="Pub/Sub is disabled" description="Enable the Pub/Sub service in devcloud config to inspect topics and subscriptions." />
      </Panel>
    )
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Topics">
        <div className="dynamodb-toolbar">
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter Pub/Sub topics"
              onChange={(event) => setTopicFilter(event.target.value)}
              placeholder="topic name"
              type="search"
              value={topicFilter}
            />
          </label>
          <Button onClick={refresh}>Refresh</Button>
        </div>
        <form className="pubsub-action-form" onSubmit={handleCreateTopic}>
          <label className="compact-filter">
            <span>Topic ID</span>
            <input
              aria-label="New Pub/Sub topic ID"
              onChange={(event) => setNewTopicId(event.target.value)}
              placeholder="orders"
              value={newTopicId}
            />
          </label>
          <Button disabled={busyAction === 'create-topic'} type="submit">
            Create
          </Button>
        </form>
        {state.status === 'loading' ? <EmptyState title="Loading topics" description="Reading local Pub/Sub resource metadata." /> : null}
        {state.status === 'error' ? <EmptyState title="Pub/Sub unavailable" description={state.message} actionLabel="Retry" onAction={refresh} /> : null}
        {state.status === 'success' ? <TopicList activeTopicName={activeTopicName} onSelectTopic={setActiveTopicName} topics={filteredTopics} /> : null}
        <form className="pubsub-action-form stacked" onSubmit={handlePublish}>
          <label className="compact-filter">
            <span>Message</span>
            <input
              aria-label="Pub/Sub publish message"
              disabled={!activeTopic}
              onChange={(event) => setPublishText(event.target.value)}
              placeholder="message body"
              value={publishText}
            />
          </label>
          <label className="compact-filter">
            <span>Ordering key</span>
            <input
              aria-label="Pub/Sub publish ordering key"
              disabled={!activeTopic}
              onChange={(event) => setOrderingKey(event.target.value)}
              placeholder="optional"
              value={orderingKey}
            />
          </label>
          <Button disabled={!activeTopic || busyAction === 'publish'} type="submit">
            Publish
          </Button>
        </form>
      </Panel>

      <Panel title="Subscriptions">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {state.status === 'success' ? `${filteredSubscriptions.length} shown / ${subscriptions.length} subscriptions` : 'Loading'}
          </span>
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter Pub/Sub subscriptions"
              onChange={(event) => setSubscriptionFilter(event.target.value)}
              placeholder="subscription or topic"
              type="search"
              value={subscriptionFilter}
            />
          </label>
        </div>
        <form className="pubsub-action-form" onSubmit={handleCreateSubscription}>
          <label className="compact-filter">
            <span>Subscription ID</span>
            <input
              aria-label="New Pub/Sub subscription ID"
              onChange={(event) => setNewSubscriptionId(event.target.value)}
              placeholder="orders-sub"
              value={newSubscriptionId}
            />
          </label>
          <label className="compact-filter">
            <span>Topic</span>
            <select
              aria-label="Topic for new Pub/Sub subscription"
              onChange={(event) => setSubscriptionTopicId(event.target.value)}
              value={subscriptionTopicId}
            >
              <option value="">Select topic</option>
              {topics.map((topic) => (
                <option key={topic.name} value={resourceId(topic.name)}>
                  {resourceId(topic.name)}
                </option>
              ))}
            </select>
          </label>
          <label className="compact-filter small">
            <span>Ack deadline</span>
            <input
              aria-label="New Pub/Sub subscription ack deadline seconds"
              inputMode="numeric"
              onChange={(event) => setAckDeadlineSeconds(event.target.value)}
              value={ackDeadlineSeconds}
            />
          </label>
          <Button disabled={busyAction === 'create-subscription'} type="submit">
            Create
          </Button>
        </form>
        {state.status === 'success' ? (
          <SubscriptionList
            activeSubscriptionName={activeSubscriptionName}
            onSelectSubscription={setActiveSubscriptionName}
            subscriptions={filteredSubscriptions}
          />
        ) : null}
      </Panel>

      <Panel title="Inspector">
        {actionError ? <p className="operation-message error">{actionError}</p> : null}
        {actionMessage ? <p className="operation-message success">{actionMessage}</p> : null}
        <PubSubInspector
          busyAction={busyAction}
          onAck={handleAck}
          onPull={handlePull}
          pulledMessages={pulledMessages}
          pullMaxMessages={pullMaxMessages}
          selectedAckId={selectedAckId}
          setPullMaxMessages={setPullMaxMessages}
          setSelectedAckId={setSelectedAckId}
          status={state.status === 'success' ? state.statusPayload : undefined}
          subscription={activeSubscription}
          topicCount={topics.length}
        />
      </Panel>
    </div>
  )
}

type TopicListProps = {
  topics: PubSubTopicSnapshot[]
  activeTopicName?: string
  onSelectTopic: (topicName: string) => void
}

function TopicList({ activeTopicName, onSelectTopic, topics }: TopicListProps): JSX.Element {
  if (topics.length === 0) {
    return <EmptyState title="No topics" description="Topics created through Pub/Sub REST or SDK clients will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="Pub/Sub topics">
      {topics.map((topic) => (
        <button
          className={topic.name === activeTopicName ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={topic.name}
          onClick={() => onSelectTopic(topic.name)}
        >
          <span className="table-row-top">
            <span className="table-row-name">{resourceId(topic.name)}</span>
            <span className="count-pill">{topic.subscriptionCount}</span>
          </span>
          <span className="table-row-meta">{topic.name}</span>
          <span className="table-row-tags">
            <span>{topic.subscriptionCount} subscriptions</span>
          </span>
        </button>
      ))}
    </div>
  )
}

type SubscriptionListProps = {
  subscriptions: PubSubSubscriptionSnapshot[]
  activeSubscriptionName?: string
  onSelectSubscription: (subscriptionName: string) => void
}

function SubscriptionList({ activeSubscriptionName, onSelectSubscription, subscriptions }: SubscriptionListProps): JSX.Element {
  if (subscriptions.length === 0) {
    return <EmptyState title="No subscriptions" description="Subscriptions created through Pub/Sub REST or SDK clients will appear here." />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Subscription</th>
            <th scope="col">Backlog</th>
            <th scope="col">In flight</th>
            <th scope="col">Attempts</th>
          </tr>
        </thead>
        <tbody>
          {subscriptions.map((subscription) => (
            <tr
              className={subscription.name === activeSubscriptionName ? 'item-row active' : 'item-row'}
              key={subscription.name}
              onClick={() => onSelectSubscription(subscription.name)}
            >
              <td>
                <span className="attribute-preview">
                  <span className="attribute-chip">{resourceId(subscription.name)}</span>
                  <span className="attribute-chip">{resourceId(subscription.topic)}</span>
                </span>
              </td>
              <td>{subscription.backlogMessages}</td>
              <td>{subscription.inFlightMessages}</td>
              <td>{subscription.maxDeliveryAttemptSeen}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

type PubSubInspectorProps = {
  busyAction?: string
  onAck: () => void
  onPull: () => void
  pulledMessages: PubSubReceivedMessage[]
  pullMaxMessages: string
  selectedAckId: string
  setPullMaxMessages: (value: string) => void
  setSelectedAckId: (value: string) => void
  status?: PubSubStatus
  subscription?: PubSubSubscriptionSnapshot
  topicCount: number
}

function PubSubInspector({
  busyAction,
  onAck,
  onPull,
  pulledMessages,
  pullMaxMessages,
  selectedAckId,
  setPullMaxMessages,
  setSelectedAckId,
  status,
  subscription,
  topicCount,
}: PubSubInspectorProps): JSX.Element {
  if (!subscription) {
    return <EmptyState title="Inspector" description="Subscription backlog, leases, and recent deliveries will appear here." />
  }

  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Service</span>
        <h3>{resourceId(subscription.name)}</h3>
        <dl className="inspector-list">
          <div>
            <dt>REST</dt>
            <dd>
              <code>{status?.restEndpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>gRPC</dt>
            <dd>
              <code>{status?.grpcEndpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Project</dt>
            <dd>{status?.project ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>Topics</dt>
            <dd>{topicCount}</dd>
          </div>
          <div>
            <dt>Ack deadline</dt>
            <dd>{subscription.ackDeadlineSeconds}s</dd>
          </div>
          <div>
            <dt>Retained</dt>
            <dd>{subscription.totalRetainedMessages}</dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Actions</span>
        <div className="pubsub-action-row">
          <label className="compact-filter small">
            <span>Pull max</span>
            <input
              aria-label="Pub/Sub pull max messages"
              inputMode="numeric"
              onChange={(event) => setPullMaxMessages(event.target.value)}
              value={pullMaxMessages}
            />
          </label>
          <Button disabled={busyAction === 'pull'} onClick={onPull}>
            Pull
          </Button>
          <Button disabled={pulledMessages.length === 0 || busyAction === 'ack'} onClick={onAck}>
            Ack
          </Button>
        </div>
        {pulledMessages.length > 0 ? (
          <div className="pulled-message-list">
            {pulledMessages.map((message) => (
              <label className="pulled-message" key={message.ackId}>
                <input
                  checked={selectedAckId === message.ackId}
                  name="pubsub-ack-id"
                  onChange={() => setSelectedAckId(message.ackId)}
                  type="radio"
                />
                <span>
                  <strong>{message.message.messageId ?? 'pulled message'}</strong>
                  <code>{decodeBase64(message.message.data ?? '')}</code>
                </span>
              </label>
            ))}
          </div>
        ) : null}
      </section>
      <section>
        <span className="inspector-label">Recent deliveries</span>
        {subscription.recentDeliveries && subscription.recentDeliveries.length > 0 ? (
          <RecentDeliveryTable deliveries={subscription.recentDeliveries} />
        ) : (
          <p className="inspector-muted">No retained delivery records for this subscription.</p>
        )}
      </section>
    </div>
  )
}

function encodeBase64(value: string): string {
  return window.btoa(unescape(encodeURIComponent(value)))
}

function decodeBase64(value: string): string {
  if (value === '') {
    return ''
  }
  try {
    return decodeURIComponent(escape(window.atob(value)))
  } catch {
    return value
  }
}

function RecentDeliveryTable({ deliveries }: { deliveries: PubSubDeliverySnapshot[] }): JSX.Element {
  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Message</th>
            <th scope="col">State</th>
            <th scope="col">Attempt</th>
            <th scope="col">Ready</th>
          </tr>
        </thead>
        <tbody>
          {deliveries.map((delivery) => (
            <tr className="item-row" key={`${delivery.messageId}-${delivery.deliveryAttempt}-${delivery.state}`}>
              <td>
                <span className="attribute-preview">
                  <span className="attribute-chip">{delivery.messageId}</span>
                  {delivery.orderingKey ? <span className="attribute-chip">{delivery.orderingKey}</span> : null}
                </span>
              </td>
              <td>{delivery.state}</td>
              <td>{delivery.deliveryAttempt}</td>
              <td>{deliveryAvailability(delivery)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function deliveryAvailability(delivery: PubSubDeliverySnapshot): string {
  if (delivery.leaseDeadline) {
    return `lease until ${formatDate(delivery.leaseDeadline)}`
  }
  if (delivery.nextDeliveryTime) {
    return `retry at ${formatDate(delivery.nextDeliveryTime)}`
  }
  return delivery.publishTime ? `published ${formatDate(delivery.publishTime)}` : 'available'
}

function disabledStatus(service?: DashboardService): PubSubStatus {
  return {
    service: 'pubsub',
    status: 'disabled',
    running: false,
    grpcEndpoint: '127.0.0.1:18085',
    restEndpoint: service?.endpoint ?? 'http://127.0.0.1:18086',
    project: 'devcloud',
    storagePath: service?.storagePath ?? '.devcloud/data/pubsub',
    topicCount: 0,
    subscriptionCount: 0,
  }
}

function resourceId(name: string): string {
  return name.split('/').pop() ?? name
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}
