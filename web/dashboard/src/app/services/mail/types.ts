export type MailMessageSummary = {
  id: string
  from: string
  to: string[]
  subject: string
  headers?: Record<string, string[]>
  textBody?: string
  htmlBody?: string
  attachments?: MailAttachment[]
  receivedAt: string
  parseError?: string
}

export type MailAttachment = {
  id: string
  fileName: string
  contentType: string
  size: number
}
