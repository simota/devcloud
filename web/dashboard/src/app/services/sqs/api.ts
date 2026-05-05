import { fetchJSON } from '../../api/client'
import type {
  SQSCreateQueueInput,
  SQSCreateQueueResponse,
  SQSChangeMessageVisibilityInput,
  SQSDeadLetterResponse,
  SQSLeasesResponse,
  SQSMessagesResponse,
  SQSQueueResponse,
  SQSQueuesResponse,
  SQSDeleteMessageInput,
  SQSReceiveMessageInput,
  SQSReceiveMessageResponse,
  SQSSendMessageInput,
  SQSSendMessageResponse,
  SQSStatus,
} from './types'

export async function getSQSStatus(): Promise<SQSStatus> {
  return fetchJSON<SQSStatus>('/api/sqs/status')
}

export async function listSQSQueues(): Promise<SQSQueuesResponse> {
  return fetchJSON<SQSQueuesResponse>('/api/sqs/queues')
}

export async function createSQSQueue(input: SQSCreateQueueInput): Promise<SQSCreateQueueResponse> {
  return fetchJSON<SQSCreateQueueResponse>('/api/sqs/queues', {
    method: 'POST',
    body: { input },
  })
}

export async function getSQSQueue(queueName: string): Promise<SQSQueueResponse> {
  return fetchJSON<SQSQueueResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}`)
}

export async function listSQSMessages(queueName: string): Promise<SQSMessagesResponse> {
  return fetchJSON<SQSMessagesResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}/messages`)
}

export async function sendSQSMessage(queueName: string, input: SQSSendMessageInput): Promise<SQSSendMessageResponse> {
  return fetchJSON<SQSSendMessageResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}/messages`, {
    method: 'POST',
    body: { input },
  })
}

export async function receiveSQSMessage(queueName: string, input: SQSReceiveMessageInput): Promise<SQSReceiveMessageResponse> {
  return fetchJSON<SQSReceiveMessageResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}/receive`, {
    method: 'POST',
    body: { input },
  })
}

export async function deleteSQSMessage(queueName: string, input: SQSDeleteMessageInput): Promise<void> {
  await fetchJSON<Record<string, never>>(`/api/sqs/queues/${encodeURIComponent(queueName)}/delete`, {
    method: 'POST',
    body: { input },
  })
}

export async function changeSQSMessageVisibility(queueName: string, input: SQSChangeMessageVisibilityInput): Promise<void> {
  await fetchJSON<Record<string, never>>(`/api/sqs/queues/${encodeURIComponent(queueName)}/visibility`, {
    method: 'POST',
    body: { input },
  })
}

export async function listSQSLeases(queueName: string): Promise<SQSLeasesResponse> {
  return fetchJSON<SQSLeasesResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}/leases`)
}

export async function getSQSDeadLetter(queueName: string): Promise<SQSDeadLetterResponse> {
  return fetchJSON<SQSDeadLetterResponse>(`/api/sqs/queues/${encodeURIComponent(queueName)}/dlq`)
}

export async function purgeSQSQueue(queueName: string): Promise<void> {
  const response = await fetch(`/api/sqs/queues/${encodeURIComponent(queueName)}/purge`, { method: 'POST' })
  if (!response.ok) {
    const detail = await response.text()
    throw new Error(detail || `Request failed with ${response.status}`)
  }
}
