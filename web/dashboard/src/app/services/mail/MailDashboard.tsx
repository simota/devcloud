import { useEffect, useMemo, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import { Tabs } from '../../../ui/Tabs'
import { Dialog } from '../../../ui/Dialog'
import type { DashboardService } from '../dashboard/types'
import { deleteAllMailMessages, getMailMessageRaw, listMailMessages } from './api'
import type { MailMessageSummary } from './types'

type MessagesState =
  | { status: 'loading' }
  | { status: 'success'; messages: MailMessageSummary[] }
  | { status: 'error'; message: string }

type MailDashboardProps = {
  service?: DashboardService
}

export function MailDashboard({ service }: MailDashboardProps): JSX.Element {
  const [messagesState, setMessagesState] = useState<MessagesState>({ status: 'loading' })
  const [selectedID, setSelectedID] = useState<string>()
  const [filter, setFilter] = useState('')
  const [clearDialogOpen, setClearDialogOpen] = useState(false)
  const isDisabled = service?.status === 'disabled'

  function refreshMessages(): void {
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
  }

  function clearMessages(): void {
    if (isDisabled) {
      return
    }
    setClearDialogOpen(true)
  }

  function confirmClearMessages(): void {
    setClearDialogOpen(false)
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
  }, [isDisabled])

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
          description="Enable the Mail service in devcloud config to inspect received messages."
        />
        <a className="compat-link" href="/mail">
          Open current Mail dashboard
        </a>
      </Panel>
    )
  }

  return (
    <div className="mail-workspace">
      <Panel title="Inbox">
        <div className="mail-toolbar">
          <label className="mail-filter">
            <span>Filter</span>
            <input
              aria-label="Filter messages"
              onChange={(event) => setFilter(event.target.value)}
              placeholder="sender, recipient, subject"
              type="search"
              value={filter}
            />
          </label>
          <Button onClick={refreshMessages}>Refresh</Button>
          <Button className="danger" onClick={clearMessages}>
            Clear all
          </Button>
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
        <a className="compat-link" href="/mail">
          Open current Mail dashboard
        </a>
      </Panel>
      {clearDialogOpen ? (
        <Dialog title="Clear inbox" onClose={() => setClearDialogOpen(false)}>
          <p className="dialog-copy">Delete all stored Mail messages from this local devcloud inbox?</p>
          <div className="dialog-actions">
            <Button onClick={() => setClearDialogOpen(false)}>Cancel</Button>
            <Button className="danger" onClick={confirmClearMessages}>
              Clear all
            </Button>
          </div>
        </Dialog>
      ) : null}
    </div>
  )
}

type MailMessageListProps = {
  messages: MailMessageSummary[]
  selectedID?: string
  totalMessages: number
  onSelectMessage: (messageID: string) => void
}

function MailMessageList({ messages, selectedID, totalMessages, onSelectMessage }: MailMessageListProps): JSX.Element {
  if (totalMessages === 0) {
    return <EmptyState title="No messages" description="Send mail to localhost:1025 and refresh the inbox." />
  }

  if (messages.length === 0) {
    return <EmptyState title="No matching messages" description="Adjust the filter to show messages in the local inbox." />
  }

  return (
    <div className="mail-list" aria-label="Messages">
      {messages.map((message) => (
        <button
          className={message.id === selectedID ? 'mail-row active' : 'mail-row'}
          key={message.id}
          onClick={() => onSelectMessage(message.id)}
        >
          <span className="mail-row-top">
            <span className="mail-from">{message.from || '(unknown sender)'}</span>
            <span>{formatDate(message.receivedAt)}</span>
          </span>
          <span className="mail-subject">{message.subject || '(No subject)'}</span>
          <span className="mail-snippet">{messageSnippet(message) || message.to.join(', ') || message.id}</span>
        </button>
      ))}
    </div>
  )
}

type MailMessageInspectorProps = {
  message?: MailMessageSummary
}

function MailMessageInspector({ message }: MailMessageInspectorProps): JSX.Element {
  const [activeTab, setActiveTab] = useState('preview')

  if (!message) {
    return <EmptyState title="Inbox is waiting" description="Messages accepted by SMTP appear here." />
  }

  const headerEntries = Object.entries(message.headers ?? {}).sort(([left], [right]) => left.localeCompare(right))

  return (
    <div className="mail-inspector">
      <div>
        <h3>{message.subject || '(No subject)'}</h3>
        <p>
          {message.from || '(unknown sender)'} to {message.to.join(', ') || '(no recipients)'} -{' '}
          {formatDate(message.receivedAt)}
        </p>
      </div>

      <Tabs
        activeID={activeTab}
        items={[
          { id: 'preview', label: 'Preview' },
          { id: 'raw', label: 'Raw' },
        ]}
        onChange={setActiveTab}
      />

      {activeTab === 'raw' ? (
        <MailRawSource messageID={message.id} />
      ) : (
        <div>
          <span className="inspector-label">Preview</span>
          <pre className="mail-preview">
            {message.textBody || message.htmlBody || message.parseError || '(No preview body)'}
          </pre>
        </div>
      )}

      {message.attachments && message.attachments.length > 0 ? (
        <div>
          <span className="inspector-label">Attachments</span>
          <ul className="attachment-list">
            {message.attachments.map((attachment) => (
              <li key={attachment.id}>
                <span>{attachment.fileName || attachment.id}</span>
                <span>
                  {attachment.contentType || 'application/octet-stream'} - {formatBytes(attachment.size)}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div>
        <span className="inspector-label">Headers</span>
        {headerEntries.length === 0 ? (
          <p className="inspector-muted">No parsed headers.</p>
        ) : (
          <dl className="metadata-list">
            {headerEntries.map(([key, values]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{values.join(', ')}</dd>
              </div>
            ))}
          </dl>
        )}
      </div>
    </div>
  )
}

type MailRawSourceProps = {
  messageID: string
}

function MailRawSource({ messageID }: MailRawSourceProps): JSX.Element {
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

  return (
    <div>
      <span className="inspector-label">Raw source</span>
      <pre className="mail-preview">{rawState.raw || '(No raw source)'}</pre>
    </div>
  )
}

function filterMessages(messages: MailMessageSummary[], filter: string): MailMessageSummary[] {
  const query = filter.trim().toLowerCase()
  if (query === '') {
    return messages
  }
  return messages.filter((message) =>
    [message.subject, message.from, ...message.to, messageSnippet(message)].join(' ').toLowerCase().includes(query),
  )
}

function messageSnippet(message: MailMessageSummary): string {
  return (message.textBody || message.htmlBody || message.parseError || '').replace(/\s+/g, ' ').trim()
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  return date.toLocaleString()
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
