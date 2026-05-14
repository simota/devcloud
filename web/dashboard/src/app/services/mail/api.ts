import { fetchJSON, fetchNoContent, fetchText } from '../../api/client'
import type { MailMessageSummary } from './types'

export type MailMessagesResponse = {
  messages: MailMessageSummary[]
}

export async function listMailMessages(): Promise<MailMessagesResponse> {
  return fetchJSON<MailMessagesResponse>('/api/messages')
}

export async function getMailMessage(messageID: string): Promise<MailMessageSummary> {
  return fetchJSON<MailMessageSummary>(`/api/messages/${encodeURIComponent(messageID)}`)
}

export async function getMailMessageRaw(messageID: string): Promise<string> {
  return fetchText(`/api/messages/${encodeURIComponent(messageID)}/raw`)
}

export async function deleteAllMailMessages(): Promise<void> {
  return fetchNoContent('/api/messages', { method: 'DELETE' })
}
