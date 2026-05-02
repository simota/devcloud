export type SQSStatus = {
  service: string
  status: string
  running: boolean
  endpoint: string
  region: string
  authMode: string
  storagePath: string
  queueCount: number
}

export type SQSQueueSnapshot = {
  name: string
  url: string
  arn: string
  attributes: Record<string, string>
  tags?: Record<string, string>
  createdAt: string
  visibleMessages: number
  notVisibleMessages: number
  delayedMessages: number
  totalRetainedMessages: number
}

export type SQSMessageAttribute = {
  DataType: string
  StringValue?: string
  BinaryValue?: string
  StringListValues?: string[]
  BinaryListValues?: string[]
}

export type SQSMessageSnapshot = {
  messageId: string
  body: string
  md5OfMessageBody: string
  attributes?: Record<string, SQSMessageAttribute>
  systemAttributes?: Record<string, SQSMessageAttribute>
  sentAt: string
  availableAt: string
  invisibleUntil?: string
  receiveCount: number
  firstReceiveAt?: string
  state: string
  messageGroupId?: string
  deduplicationId?: string
  sequenceNumber?: string
}

export type SQSLeaseSnapshot = {
  messageId: string
  visibleAfter: string
  receiveCount: number
  receiptHandlePresent: boolean
}

export type SQSQueuesResponse = {
  queues: SQSQueueSnapshot[]
}

export type SQSQueueResponse = {
  queue: SQSQueueSnapshot
}

export type SQSMessagesResponse = {
  queueName: string
  messages: SQSMessageSnapshot[]
}

export type SQSLeasesResponse = {
  queueName: string
  leases: SQSLeaseSnapshot[]
}

export type SQSDeadLetterResponse = {
  queueName: string
  deadLetterQueue?: SQSQueueSnapshot
  deadLetterSourceQueues: SQSQueueSnapshot[]
}
