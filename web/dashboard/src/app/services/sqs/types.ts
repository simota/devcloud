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

export type SQSCreateQueueInput = {
  QueueName: string
  Attributes?: Record<string, string>
  Tags?: Record<string, string>
}

export type SQSCreateQueueResponse = {
  QueueUrl: string
}

export type SQSSendMessageInput = {
  MessageBody: string
  DelaySeconds?: number
  MessageAttributes?: Record<string, SQSMessageAttribute>
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export type SQSSendMessageResponse = {
  MessageId: string
  MD5OfMessageBody: string
  MD5OfMessageAttributes?: string
  MD5OfMessageSystemAttributes?: string
  SequenceNumber?: string
}

export type SQSReceiveMessageInput = {
  MaxNumberOfMessages?: number
  VisibilityTimeout?: number
  WaitTimeSeconds?: number
  AttributeNames?: string[]
  MessageAttributeNames?: string[]
  MessageSystemAttributeNames?: string[]
}

export type SQSReceivedMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfMessageBody: string
  MD5OfMessageAttributes?: string
  MD5OfMessageSystemAttributes?: string
  Body: string
  Attributes?: Record<string, string>
  MessageAttributes?: Record<string, SQSMessageAttribute>
}

export type SQSReceiveMessageResponse = {
  Messages?: SQSReceivedMessage[]
}

export type SQSDeleteMessageInput = {
  ReceiptHandle: string
}

export type SQSChangeMessageVisibilityInput = {
  ReceiptHandle: string
  VisibilityTimeout: number
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
