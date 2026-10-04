import { useEffect, useState, type MutableRefObject } from 'react'
import type { MessageSummary } from '../api'
import { addressLabel, formatBytes, formatRelativeDate, fullAddress } from '../format'

export function MessageList({ messages, selectedID, highlightedIDs, buttons, onSelect }: {
  messages: MessageSummary[]
  selectedID?: string
  highlightedIDs: string[]
  buttons: MutableRefObject<Map<string, HTMLButtonElement>>
  onSelect: (id: string) => void
}): JSX.Element {
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30000)
    return () => window.clearInterval(timer)
  }, [])
  const tabStop = messages.some((message) => message.id === selectedID) ? selectedID : messages[0]?.id
  return (
    <ul className="mail-inbox-list" aria-label="Message list">
      {messages.map((message) => (
        <li key={message.id}>
          <button
            aria-current={message.id === selectedID ? 'true' : undefined}
            className={`mail-inbox-row${message.id === selectedID ? ' active' : ''}${highlightedIDs.includes(message.id) ? ' new-message' : ''}`}
            data-message-id={message.id}
            onClick={() => onSelect(message.id)}
            ref={(element) => { if (element) buttons.current.set(message.id, element); else buttons.current.delete(message.id) }}
            tabIndex={message.id === tabStop ? 0 : -1}
            type="button"
          >
            <span className="mail-inbox-row-top">
              <span className="mail-inbox-sender" title={fullAddress(message.from)}>{addressLabel(message.from)}</span>
              <time className="mail-inbox-time" dateTime={message.receivedAt} title={message.receivedAt}>
                {formatRelativeDate(message.receivedAt, now)}
              </time>
            </span>
            <span className="mail-inbox-row-top">
              <span className="mail-inbox-subject">{message.subject || '(No subject)'}</span>
              <span className="mail-inbox-meta">
                {message.attachmentCount > 0 ? (
                  <span aria-label={`${message.attachmentCount} attachments`}><span aria-hidden="true">📎</span> {message.attachmentCount}</span>
                ) : null}
                <span>{formatBytes(message.size)}</span>
              </span>
            </span>
            <span className="mail-inbox-snippet">{message.snippet.replace(/\s+/g, ' ').trim() || '\u00a0'}</span>
          </button>
        </li>
      ))}
    </ul>
  )
}
