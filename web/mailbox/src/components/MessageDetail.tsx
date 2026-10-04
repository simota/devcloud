import { useEffect, useState, type ReactNode } from 'react'
import {
  ApiError, attachmentURL, downloadURL, errorMessage, getMessage, getRaw, messageURL,
  type Attachment, type MessageDetail as Detail,
} from '../api'
import { formatAbsoluteDate, formatBytes, fullAddress } from '../format'
import { Button, EmptyState, Loading, Tabs, type TabItem } from './ui'

type Remote<T> = { status: 'loading' } | { status: 'success'; data: T } | { status: 'error'; message: string; missing?: boolean }
type ContentTab = 'html' | 'text' | 'source' | 'attachments'

export function MessageDetail({ id, onDelete }: { id: string; onDelete: (id: string) => void }): JSX.Element {
  const [state, setState] = useState<Remote<Detail>>({ status: 'loading' })
  const [retry, setRetry] = useState(0)
  useEffect(() => {
    const controller = new AbortController()
    setState({ status: 'loading' })
    getMessage(id, controller.signal).then((data) => {
      if (!controller.signal.aborted) setState({ status: 'success', data })
    }).catch((failure: unknown) => {
      if (!controller.signal.aborted) setState({ status: 'error', message: errorMessage(failure), missing: failure instanceof ApiError && failure.status === 404 })
    })
    return () => controller.abort()
  }, [id, retry])
  if (state.status === 'loading') return <Loading detail />
  if (state.status === 'error') return (
    <div role="alert">
      <EmptyState
        title={state.missing ? 'Message not found' : 'Failed to load message'}
        description={state.missing ? 'The requested message may have been deleted.' : state.message}
        actionLabel="Retry"
        onAction={() => setRetry((value) => value + 1)}
      />
    </div>
  )
  return <MessageInspector key={state.data.id} message={state.data} onDelete={onDelete} />
}

function MessageInspector({ message, onDelete }: { message: Detail; onDelete: (id: string) => void }): JSX.Element {
  const [activeTab, setActiveTab] = useState<ContentTab>(message.hasHtml ? 'html' : 'text')
  const [raw, setRaw] = useState<Remote<string>>({ status: 'loading' })
  const [retryRaw, setRetryRaw] = useState(0)
  const [copyState, setCopyState] = useState('')
  const [copying, setCopying] = useState(false)
  useEffect(() => {
    const controller = new AbortController()
    setRaw({ status: 'loading' })
    getRaw(message.id, controller.signal).then((data) => {
      if (!controller.signal.aborted) setRaw({ status: 'success', data })
    }).catch((failure: unknown) => {
      if (!controller.signal.aborted) setRaw({ status: 'error', message: errorMessage(failure) })
    })
    return () => controller.abort()
  }, [message.id, retryRaw])

  async function copyRaw(): Promise<void> {
    if (raw.status !== 'success') return
    setCopyState('')
    if (!navigator.clipboard) {
      setCopyState('Clipboard is unavailable. Open Source to select and copy the message.')
      return
    }
    setCopying(true)
    try {
      await navigator.clipboard.writeText(raw.data)
      setCopyState('Copied to clipboard')
    } catch {
      setCopyState('Could not copy. Allow clipboard access or copy the text from Source.')
    } finally {
      setCopying(false)
    }
  }

  const tabs: TabItem<ContentTab>[] = [
    { id: 'html', label: 'HTML', disabled: !message.hasHtml },
    { id: 'text', label: 'Plain Text' },
    { id: 'source', label: 'Source' },
    { id: 'attachments', label: `Attachments (${message.attachments.length})` },
  ]
  const bcc = message.headers.filter(([name]) => name.toLowerCase() === 'bcc').map(([, value]) => value)
  const date = message.date || message.receivedAt
  return (
    <article className="mail-inspector">
      <header className="mail-inspector-header">
        <h1 className="mail-subject">{message.subject || '(No subject)'}</h1>
        <div className="mail-detail-actions">
          <a className="button" href={downloadURL(message.id)} download>Download .eml</a>
          <Button disabled={raw.status !== 'success' || copying} onClick={() => void copyRaw()}>Copy raw</Button>
          <Button className="danger" onClick={() => onDelete(message.id)}>Delete</Button>
        </div>
        <div aria-live="polite" className={copyState ? 'mail-copy-status' : 'sr-only'}>{copyState}</div>
        <dl className="mail-recipients">
          <RecipientRow label="From" values={[fullAddress(message.from)]} />
          <RecipientRow label="To" values={message.to.map(fullAddress)} />
          {message.cc.length > 0 ? <RecipientRow label="Cc" values={message.cc.map(fullAddress)} /> : null}
          {bcc.length > 0 ? <RecipientRow label="Bcc" values={bcc} /> : null}
          <div className="mail-recipients-row"><dt>Date</dt><dd><time dateTime={date} title={date}>{formatAbsoluteDate(date)}</time></dd></div>
          <RecipientRow label="Subject" values={[message.subject || '(No subject)']} />
        </dl>
      </header>
      {message.warnings.length > 0 ? (
        <div className="mail-preview-error" role="status">
          <strong>Some content could not be decoded. View Source for the original message.</strong>
          <ul>{message.warnings.map((warning, index) => <li key={index}>{warning}</li>)}</ul>
        </div>
      ) : null}
      <Tabs items={tabs} activeID={activeTab} onChange={setActiveTab} />
      {tabs.map((tab) => (
        <div
          role="tabpanel"
          id={`tabpanel-${tab.id}`}
          aria-labelledby={`tab-${tab.id}`}
          hidden={activeTab !== tab.id}
          tabIndex={0}
          className="mail-tabpanel"
          key={tab.id}
        >
          {activeTab === tab.id && tab.id === 'html' && message.hasHtml ? (
            <iframe
              className="mail-html-frame"
              src={messageURL(message.id, '/html')}
              title="Message HTML body"
              sandbox="allow-popups allow-popups-to-escape-sandbox"
              referrerPolicy="no-referrer"
            />
          ) : null}
          {activeTab === tab.id && tab.id === 'text' ? (
            message.text.trim() ? <div className="mail-preview">{renderLinkedText(message.text)}</div> : <NoBody />
          ) : null}
          {activeTab === tab.id && tab.id === 'source' ? (
            raw.status === 'loading' ? <Loading detail /> : raw.status === 'error' ? (
              <div role="alert"><EmptyState title="Failed to load source" description={raw.message} actionLabel="Retry" onAction={() => setRetryRaw((value) => value + 1)} /></div>
            ) : <pre className="mail-raw-pre">{raw.data}</pre>
          ) : null}
          {activeTab === tab.id && tab.id === 'attachments' ? <Attachments messageID={message.id} attachments={message.attachments} /> : null}
        </div>
      ))}
      <details className="mail-headers">
        <summary>Headers ({message.headers.length})</summary>
        <table className="mail-headers-table" aria-label="Message headers"><tbody>
          {message.headers.map(([name, value], index) => <tr key={index}><th scope="row">{name}</th><td>{value}</td></tr>)}
        </tbody></table>
      </details>
    </article>
  )
}

function RecipientRow({ label, values }: { label: string; values: string[] }): JSX.Element {
  return <div className="mail-recipients-row"><dt>{label}</dt><dd>{values.join(', ') || '—'}</dd></div>
}

function NoBody(): JSX.Element {
  return <EmptyState title="No message body" description="This message contains no readable text or HTML body." />
}

function Attachments({ messageID, attachments }: { messageID: string; attachments: Attachment[] }): JSX.Element {
  if (attachments.length === 0) return <EmptyState title="No attachments" description="This message contains no file attachments." />
  return (
    <ul className="mail-attachment-list" aria-label="Attachments list">
      {attachments.map((attachment) => (
        <li className="mail-attachment-item" key={attachment.index}>
          <span className="mail-attachment-icon" aria-hidden="true">📎</span>
          <div className="mail-attachment-body">
            <bdi className="mail-attachment-name" title={attachment.filename}>{attachment.filename}</bdi>
            <span className="mail-attachment-meta">{attachment.contentType} · {formatBytes(attachment.size)}</span>
          </div>
          <a className="button" href={attachmentURL(messageID, attachment.index)} download={attachment.filename}>Download<span className="sr-only"> <bdi>{attachment.filename}</bdi></span></a>
        </li>
      ))}
    </ul>
  )
}

// Candidate recognition is followed by URL parsing and an explicit protocol allowlist.
export function renderLinkedText(text: string): ReactNode[] {
  const nodes: ReactNode[] = []
  const pattern = /\b(?:https?:\/\/|mailto:)[^\s<>"']+|\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/gi
  let lastIndex = 0
  for (const match of text.matchAll(pattern)) {
    const index = match.index ?? 0
    const candidate = match[0].replace(/[.,;!?]+$/, '')
    nodes.push(text.slice(lastIndex, index))
    let url: URL | undefined
    try {
      url = new URL(/^(?:https?:\/\/|mailto:)/i.test(candidate) ? candidate : `mailto:${candidate}`)
    } catch { /* Invalid URLs remain plain text. */ }
    if (url && ['http:', 'https:', 'mailto:'].includes(url.protocol)) {
      nodes.push(<a key={index} href={url.href} target="_blank" rel="noopener noreferrer">{candidate}</a>)
    } else nodes.push(candidate)
    nodes.push(match[0].slice(candidate.length))
    lastIndex = index + match[0].length
  }
  nodes.push(text.slice(lastIndex))
  return nodes
}
