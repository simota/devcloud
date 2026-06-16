import { useCallback, useEffect, useMemo, useState } from 'react'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
import type { DashboardService } from '../dashboard/types'
import { deleteAllMailMessages, getMailMessageRaw, listMailMessages } from './api'
import { decodeMimeAddress, decodeMimeEncodedWord } from './mimeDecoder'
import type { MailAttachment, MailMessageSummary } from './types'

type MessagesState =
  | { status: 'loading' }
  | { status: 'success'; messages: MailMessageSummary[] }
  | { status: 'error'; message: string }

type MailDashboardProps = {
  service?: DashboardService
}

export function MailDashboard({ service }: MailDashboardProps): JSX.Element {
  const confirm = useConfirm()
  const [messagesState, setMessagesState] = useState<MessagesState>({ status: 'loading' })
  const [selectedID, setSelectedID] = useState<string>()
  const [filter, setFilter] = useState('')
  const isDisabled = service?.status === 'disabled'

  const refreshMessages = useCallback((): void => {
    if (isDisabled) {
      setMessagesState({ status: 'success', messages: [] })
      return
    }
    setMessagesState({ status: 'loading' })
    listMailMessages()
      .then(({ messages }) => {
        setMessagesState({ status: 'success', messages })
        setSelectedID((current) => current ?? messages[0]?.id)
      })
      .catch((error: Error) => {
        setMessagesState({ status: 'error', message: error.message })
      })
  }, [isDisabled])

  useDashboardEvents({ topics: ['mail'], onEvent: refreshMessages, enabled: !isDisabled })

  async function clearMessages(): Promise<void> {
    if (isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Clear inbox',
        description: 'Delete all stored Mail messages from this local devcloud inbox.',
        target: 'CLEAR INBOX',
        confirmLabel: 'Clear inbox',
      }),
    )
    if (!ok) {
      return
    }
    setMessagesState({ status: 'loading' })
    deleteAllMailMessages()
      .then(() => {
        setSelectedID(undefined)
        setMessagesState({ status: 'success', messages: [] })
      })
      .catch((error: Error) => {
        setMessagesState({ status: 'error', message: error.message })
      })
  }

  useEffect(() => {
    refreshMessages()
  }, [refreshMessages])

  const messages = messagesState.status === 'success' ? messagesState.messages : []
  const filteredMessages = useMemo(() => filterMessages(messages, filter), [messages, filter])
  const selectedMessage =
    filteredMessages.find((message) => message.id === selectedID) ??
    filteredMessages[0] ??
    (filter.trim() === '' ? messages[0] : undefined)

  if (isDisabled) {
    return (
      <Panel title="Mail">
        <EmptyState
          title="Mail is disabled"
          description="Enable services.mail.enabled in .devcloud/config.yaml to inspect received messages."
        />
      </Panel>
    )
  }

  return (
    <div className="mail-shell">
      <Panel title="Inbox">
        <div className="mail-inbox-toolbar">
          <label className="mail-inbox-filter">
            <span>Filter</span>
            <input
              aria-label="Filter messages"
              onChange={(event) => setFilter(event.target.value)}
              placeholder="sender, recipient, subject"
              type="search"
              value={filter}
            />
          </label>
          <div className="mail-inbox-actions">
            <Button onClick={refreshMessages}>Refresh</Button>
            <Button className="danger" disabled={messages.length === 0} onClick={clearMessages}>
              Clear all
            </Button>
          </div>
        </div>
        {messagesState.status === 'loading' ? (
          <EmptyState title="Loading messages" description="Reading the local Mail inbox." />
        ) : null}
        {messagesState.status === 'error' ? (
          <EmptyState
            title="Mail messages unavailable"
            description={messagesState.message}
            actionLabel="Retry"
            onAction={refreshMessages}
          />
        ) : null}
        {messagesState.status === 'success' ? (
          <MailMessageList
            messages={filteredMessages}
            selectedID={selectedMessage?.id}
            totalMessages={messages.length}
            onSelectMessage={setSelectedID}
          />
        ) : null}
      </Panel>

      <Panel title="Message">
        <MailMessageInspector message={selectedMessage} />
      </Panel>
    </div>
  )
}

type MailMessageListProps = {
  messages: MailMessageSummary[]
  selectedID: string | undefined
  totalMessages: number
  onSelectMessage: (messageID: string) => void
}

function MailMessageList({
  messages,
  selectedID,
  totalMessages,
  onSelectMessage,
}: MailMessageListProps): JSX.Element {
  if (totalMessages === 0) {
    return (
      <EmptyState
        title="Inbox is empty"
        description="Send mail to localhost:11025 (relaxed SMTP AUTH) and refresh."
      />
    )
  }
  if (messages.length === 0) {
    return (
      <EmptyState
        title="No matches"
        description="Try a different filter or clear the search."
      />
    )
  }

  return (
    <ul className="mail-inbox-list" aria-label="Messages">
      {messages.map((message) => {
        const active = message.id === selectedID
        const subject = decodeMimeEncodedWord(message.subject) || '(No subject)'
        const fromDisplay = decodeMimeAddress(message.from) || '(unknown sender)'
        const snippet = messageSnippet(message)
        return (
          <li key={message.id}>
            <button
              aria-current={active ? 'true' : undefined}
              className={active ? 'mail-inbox-row active' : 'mail-inbox-row'}
              onClick={() => onSelectMessage(message.id)}
              type="button"
            >
              <span className="mail-inbox-row-top">
                <span className="mail-inbox-sender">{fromDisplay}</span>
                <time className="mail-inbox-time" dateTime={message.receivedAt} title={formatAbsoluteDate(message.receivedAt)}>
                  {formatRelativeDate(message.receivedAt)}
                </time>
              </span>
              <span className="mail-inbox-subject">{subject}</span>
              {snippet ? <span className="mail-inbox-snippet">{snippet}</span> : null}
            </button>
          </li>
        )
      })}
    </ul>
  )
}

type MailMessageInspectorProps = {
  message: MailMessageSummary | undefined
}

type InspectorTab = 'preview' | 'attachments' | 'raw'

function MailMessageInspector({ message }: MailMessageInspectorProps): JSX.Element {
  const [activeTab, setActiveTab] = useState<InspectorTab>('preview')
  const [showHeaders, setShowHeaders] = useState(false)

  useEffect(() => {
    setActiveTab('preview')
    setShowHeaders(false)
  }, [message?.id])

  if (!message) {
    return (
      <EmptyState
        title="Select a message"
        description="Messages accepted via SMTP appear here. Pick a row on the left to inspect."
      />
    )
  }

  const subject = decodeMimeEncodedWord(message.subject) || '(No subject)'
  const fromDisplay = decodeMimeAddress(message.from)
  const toList = message.to.map(decodeMimeAddress).filter(Boolean)
  const ccList = headerAddresses(message.headers, 'Cc')
  const bccList = headerAddresses(message.headers, 'Bcc')
  const replyTo = headerAddresses(message.headers, 'Reply-To')
  const attachments = message.attachments ?? []
  const previewBody = message.textBody || message.htmlBody || message.parseError || ''
  const hasBody = previewBody.trim().length > 0

  const tabs: { id: InspectorTab; label: string; count?: number }[] = [
    { id: 'preview', label: 'Preview' },
    { id: 'attachments', label: 'Attachments', count: attachments.length },
    { id: 'raw', label: 'Raw' },
  ]

  return (
    <article className="mail-inspector">
      <header className="mail-inspector-header">
        <h1 className="mail-subject">{subject}</h1>
        <dl className="mail-recipients">
          <RecipientRow label="From" values={fromDisplay ? [fromDisplay] : ['(unknown sender)']} />
          <RecipientRow label="To" values={toList.length > 0 ? toList : ['(no recipients)']} />
          {ccList.length > 0 ? <RecipientRow label="Cc" values={ccList} /> : null}
          {bccList.length > 0 ? <RecipientRow label="Bcc" values={bccList} /> : null}
          {replyTo.length > 0 ? <RecipientRow label="Reply-To" values={replyTo} /> : null}
          <div className="mail-recipients-row">
            <dt>Date</dt>
            <dd>
              <time dateTime={message.receivedAt} title={formatAbsoluteDate(message.receivedAt)}>
                {formatAbsoluteDate(message.receivedAt)}
              </time>
              <span className="mail-recipients-aux">· {formatRelativeDate(message.receivedAt)}</span>
            </dd>
          </div>
        </dl>
      </header>

      <nav className="mail-tabs" role="tablist" aria-label="Message view">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            role="tab"
            aria-selected={activeTab === tab.id}
            className={activeTab === tab.id ? 'mail-tab active' : 'mail-tab'}
            onClick={() => setActiveTab(tab.id)}
            type="button"
          >
            {tab.label}
            {tab.count !== undefined && tab.count > 0 ? (
              <span className="mail-tab-count">{tab.count}</span>
            ) : null}
          </button>
        ))}
      </nav>

      {activeTab === 'preview' ? (
        <PreviewTab body={previewBody} hasBody={hasBody} parseError={message.parseError} />
      ) : null}
      {activeTab === 'attachments' ? <AttachmentsTab attachments={attachments} /> : null}
      {activeTab === 'raw' ? <RawTab messageID={message.id} /> : null}

      <details
        className="mail-headers"
        open={showHeaders}
        onToggle={(event) => setShowHeaders((event.target as HTMLDetailsElement).open)}
      >
        <summary>
          <span>Headers</span>
          <span className="mail-headers-count">{Object.keys(message.headers ?? {}).length}</span>
        </summary>
        <HeadersTable headers={message.headers ?? {}} />
      </details>
    </article>
  )
}

function RecipientRow({ label, values }: { label: string; values: string[] }): JSX.Element {
  return (
    <div className="mail-recipients-row">
      <dt>{label}</dt>
      <dd>{values.join(', ')}</dd>
    </div>
  )
}

function PreviewTab({
  body,
  hasBody,
  parseError,
}: {
  body: string
  hasBody: boolean
  parseError: string | undefined
}): JSX.Element {
  if (!hasBody && !parseError) {
    return (
      <EmptyState
        title="No body"
        description="The message contains no decoded text content."
      />
    )
  }
  if (parseError && !hasBody) {
    return (
      <div className="mail-preview-error">
        <span className="mail-eyebrow">Parse error</span>
        <p>{parseError}</p>
      </div>
    )
  }
  return (
    <div className="mail-preview" lang="auto">
      {renderLinkedText(body)}
    </div>
  )
}

function AttachmentsTab({ attachments }: { attachments: MailAttachment[] }): JSX.Element {
  if (attachments.length === 0) {
    return (
      <EmptyState
        title="No attachments"
        description="This message has no parsed attachments."
      />
    )
  }
  return (
    <ul className="mail-attachment-list" aria-label="Attachments">
      {attachments.map((attachment) => (
        <li key={attachment.id} className="mail-attachment-item">
          <span className="mail-attachment-icon" aria-hidden="true">
            {attachmentIcon(attachment.contentType)}
          </span>
          <div className="mail-attachment-body">
            <span className="mail-attachment-name">{attachment.fileName || attachment.id}</span>
            <span className="mail-attachment-meta">
              {attachment.contentType || 'application/octet-stream'} · {formatBytes(attachment.size)}
            </span>
          </div>
        </li>
      ))}
    </ul>
  )
}

function RawTab({ messageID }: { messageID: string }): JSX.Element {
  const [rawState, setRawState] = useState<
    { status: 'loading' } | { status: 'success'; raw: string } | { status: 'error'; message: string }
  >({ status: 'loading' })

  useEffect(() => {
    let cancelled = false
    setRawState({ status: 'loading' })
    getMailMessageRaw(messageID)
      .then((raw) => {
        if (!cancelled) {
          setRawState({ status: 'success', raw })
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setRawState({ status: 'error', message: error.message })
        }
      })
    return () => {
      cancelled = true
    }
  }, [messageID])

  if (rawState.status === 'loading') {
    return <EmptyState title="Loading raw source" description="Reading the stored RFC 822 message." />
  }
  if (rawState.status === 'error') {
    return <EmptyState title="Raw source unavailable" description={rawState.message} />
  }

  const raw = rawState.raw
  function copyRaw() {
    if (!navigator.clipboard) {
      return
    }
    navigator.clipboard.writeText(raw).catch(() => {
      /* clipboard denied */
    })
  }

  return (
    <div className="mail-raw">
      <div className="mail-raw-toolbar">
        <span className="mail-raw-meta">{raw.length.toLocaleString()} bytes</span>
        <button className="mail-raw-copy" onClick={copyRaw} type="button">
          Copy
        </button>
      </div>
      <pre className="mail-raw-pre">{raw || '(empty)'}</pre>
    </div>
  )
}

function HeadersTable({ headers }: { headers: Record<string, string[]> }): JSX.Element {
  const entries = Object.entries(headers).sort(([left], [right]) => left.localeCompare(right))
  if (entries.length === 0) {
    return <p className="mail-headers-empty">No parsed headers.</p>
  }
  return (
    <table className="mail-headers-table">
      <tbody>
        {entries.map(([key, values]) => (
          <tr key={key}>
            <th scope="row">{key}</th>
            <td>{values.map(decodeMimeEncodedWord).join(', ')}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function filterMessages(messages: MailMessageSummary[], filter: string): MailMessageSummary[] {
  const query = filter.trim().toLowerCase()
  if (query === '') {
    return messages
  }
  return messages.filter((message) => {
    const haystack = [
      decodeMimeEncodedWord(message.subject),
      decodeMimeAddress(message.from),
      ...message.to.map(decodeMimeAddress),
      messageSnippet(message),
    ]
    return haystack.join(' ').toLowerCase().includes(query)
  })
}

function messageSnippet(message: MailMessageSummary): string {
  const source = message.textBody || message.htmlBody || message.parseError || ''
  return source.replace(/\s+/g, ' ').trim()
}

function headerAddresses(
  headers: Record<string, string[]> | undefined,
  field: string,
): string[] {
  if (!headers) {
    return []
  }
  const lower = field.toLowerCase()
  for (const [key, values] of Object.entries(headers)) {
    if (key.toLowerCase() === lower) {
      return values
        .flatMap((value) => splitAddressList(value))
        .map(decodeMimeAddress)
        .filter(Boolean)
    }
  }
  return []
}

function splitAddressList(value: string): string[] {
  return value
    .split(/,(?![^<]*>)/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
}

function renderLinkedText(text: string): JSX.Element[] {
  const nodes: JSX.Element[] = []
  const pattern = /(https?:\/\/[^\s<>"']+)|((?:[a-zA-Z0-9._%+-]+)@(?:[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}))/g
  let lastIndex = 0
  let key = 0
  let match: RegExpExecArray | null = pattern.exec(text)
  while (match !== null) {
    const beforeText = text.slice(lastIndex, match.index)
    if (beforeText) {
      key += 1
      nodes.push(<span key={`t-${key}`}>{beforeText}</span>)
    }
    key += 1
    if (match[1]) {
      nodes.push(
        <a key={`l-${key}`} href={match[1]} rel="noopener noreferrer" target="_blank">
          {match[1]}
        </a>,
      )
    } else {
      nodes.push(
        <a key={`m-${key}`} href={`mailto:${match[2]}`}>
          {match[2]}
        </a>,
      )
    }
    lastIndex = pattern.lastIndex
    match = pattern.exec(text)
  }
  const tail = text.slice(lastIndex)
  if (tail) {
    key += 1
    nodes.push(<span key={`t-${key}`}>{tail}</span>)
  }
  return nodes
}

function attachmentIcon(contentType: string | undefined): string {
  if (!contentType) {
    return '📎'
  }
  const lower = contentType.toLowerCase()
  if (lower.startsWith('image/')) {
    return '🖼'
  }
  if (lower === 'application/pdf') {
    return '📄'
  }
  if (lower.startsWith('audio/')) {
    return '🎵'
  }
  if (lower.startsWith('video/')) {
    return '🎬'
  }
  if (lower.includes('zip') || lower.includes('tar') || lower.includes('gzip')) {
    return '🗜'
  }
  if (lower.startsWith('text/')) {
    return '📝'
  }
  return '📎'
}

function formatAbsoluteDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  return date.toLocaleString()
}

function formatRelativeDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  const diffMs = Date.now() - date.getTime()
  if (diffMs < 0) {
    return 'in the future'
  }
  const seconds = Math.floor(diffMs / 1000)
  if (seconds < 60) {
    return 'just now'
  }
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) {
    return `${minutes}m ago`
  }
  const hours = Math.floor(minutes / 60)
  if (hours < 24) {
    return `${hours}h ago`
  }
  const days = Math.floor(hours / 24)
  if (days < 7) {
    return `${days}d ago`
  }
  return date.toLocaleDateString()
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) {
    return 'unknown'
  }
  if (value < 1024) {
    return `${value} B`
  }
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = value / 1024
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`
}
