import type {
  SQSDeadLetterResponse,
  SQSLeaseSnapshot,
  SQSMessageSnapshot,
  SQSQueueSnapshot,
  SQSStatus,
} from './types'

export type QueuesState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: SQSStatus; queues: SQSQueueSnapshot[] }
  | { status: 'error'; message: string }

export type DetailState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; messages: SQSMessageSnapshot[]; leases: SQSLeaseSnapshot[]; dlq: SQSDeadLetterResponse }
  | { status: 'error'; message: string }
