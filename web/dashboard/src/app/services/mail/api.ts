import type { MailMessageSummary } from './types'

export type MailMessagesResponse = {
  messages: MailMessageSummary[]
}

export async function listMailMessages(): Promise<MailMessagesResponse> {
  const response = await fetch('/api/messages', { headers: { Accept: 'application/json' } })
  if (!response.ok) {
    throw new Error('Mail messages request failed')
  }
  return (await response.json()) as MailMessagesResponse
}
