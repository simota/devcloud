import { fetchJSON } from '../../api/client'
import type {
  PubSubPublishResponse,
  PubSubPullResponse,
  PubSubStatus,
  PubSubSubscriptionResponse,
  PubSubSubscriptionSnapshot,
  PubSubSubscriptionsResponse,
  PubSubTopicResponse,
  PubSubTopicSnapshot,
  PubSubTopicsResponse,
} from './types'

export async function getPubSubStatus(): Promise<PubSubStatus> {
  return fetchJSON<PubSubStatus>('/api/pubsub/status')
}

export async function listPubSubTopics(): Promise<PubSubTopicsResponse> {
  return fetchJSON<PubSubTopicsResponse>('/api/pubsub/topics')
}

export async function createPubSubTopic(topicId: string): Promise<PubSubTopicSnapshot> {
  return fetchJSON<PubSubTopicSnapshot>('/api/pubsub/topics', {
    method: 'POST',
    body: { topicId },
  })
}

export async function getPubSubTopic(topicId: string): Promise<PubSubTopicResponse> {
  return fetchJSON<PubSubTopicResponse>(`/api/pubsub/topics/${encodeURIComponent(topicId)}`)
}

export async function listPubSubSubscriptions(): Promise<PubSubSubscriptionsResponse> {
  return fetchJSON<PubSubSubscriptionsResponse>('/api/pubsub/subscriptions')
}

export async function createPubSubSubscription(input: {
  subscriptionId: string
  topicId: string
  ackDeadlineSeconds?: number
}): Promise<PubSubSubscriptionSnapshot> {
  return fetchJSON<PubSubSubscriptionSnapshot>('/api/pubsub/subscriptions', {
    method: 'POST',
    body: input,
  })
}

export async function getPubSubSubscription(subscriptionId: string): Promise<PubSubSubscriptionResponse> {
  return fetchJSON<PubSubSubscriptionResponse>(`/api/pubsub/subscriptions/${encodeURIComponent(subscriptionId)}`)
}

export async function publishPubSubMessage(input: {
  topicId: string
  data: string
  attributes?: Record<string, string>
  orderingKey?: string
}): Promise<PubSubPublishResponse> {
  return fetchJSON<PubSubPublishResponse>(`/api/pubsub/topics/${encodeURIComponent(input.topicId)}/publish`, {
    method: 'POST',
    body: {
      messages: [
        {
          data: input.data,
          attributes: input.attributes,
          orderingKey: input.orderingKey,
        },
      ],
    },
  })
}

export async function pullPubSubMessages(subscriptionId: string, maxMessages: number): Promise<PubSubPullResponse> {
  return fetchJSON<PubSubPullResponse>(`/api/pubsub/subscriptions/${encodeURIComponent(subscriptionId)}/pull`, {
    method: 'POST',
    body: { maxMessages },
  })
}

export async function ackPubSubMessages(subscriptionId: string, ackIds: string[]): Promise<void> {
  await fetchJSON<Record<string, never>>(`/api/pubsub/subscriptions/${encodeURIComponent(subscriptionId)}/ack`, {
    method: 'POST',
    body: { ackIds },
  })
}
