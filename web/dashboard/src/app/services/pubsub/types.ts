export type PubSubStatus = {
  service: string
  status: string
  running: boolean
  grpcEndpoint: string
  restEndpoint: string
  project: string
  storagePath: string
  topicCount: number
  subscriptionCount: number
}

export type PubSubTopicSnapshot = {
  name: string
  subscriptionCount: number
  createdAt?: string
  updatedAt?: string
}

export type PubSubDeliverySnapshot = {
  messageId: string
  publishTime?: string
  orderingKey?: string
  state: string
  leaseDeadline?: string
  nextDeliveryTime?: string
  deliveryAttempt: number
}

export type PubSubSubscriptionSnapshot = {
  name: string
  topic: string
  ackDeadlineSeconds: number
  createdAt?: string
  updatedAt?: string
  enableMessageOrdering?: boolean
  retainAckedMessages?: boolean
  messageRetentionDuration?: string
  expirationPolicy?: Record<string, unknown>
  filter?: string
  deadLetterPolicy?: Record<string, unknown>
  retryPolicy?: Record<string, unknown>
  pushConfig?: Record<string, unknown>
  backlogMessages: number
  inFlightMessages: number
  totalRetainedMessages: number
  maxDeliveryAttemptSeen: number
  recentDeliveries?: PubSubDeliverySnapshot[]
}

export type PubSubTopicsResponse = {
  project: string
  topics: PubSubTopicSnapshot[]
}

export type PubSubTopicResponse = {
  project: string
  topic: PubSubTopicSnapshot
}

export type PubSubSubscriptionsResponse = {
  project: string
  subscriptions: PubSubSubscriptionSnapshot[]
}

export type PubSubSubscriptionResponse = {
  project: string
  subscription: PubSubSubscriptionSnapshot
}

export type PubSubMessage = {
  data?: string
  attributes?: Record<string, string>
  messageId?: string
  publishTime?: string
  orderingKey?: string
}

export type PubSubReceivedMessage = {
  ackId: string
  message: PubSubMessage
  deliveryAttempt?: number
}

export type PubSubPublishResponse = {
  messageIds: string[]
}

export type PubSubPullResponse = {
  receivedMessages?: PubSubReceivedMessage[]
}
