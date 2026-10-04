import { useCallback, useEffect, useRef, useState } from 'react'
import { clearMessages, deleteMessage, errorMessage } from './api'
import { MessageDetail } from './components/MessageDetail'
import { MessageList } from './components/MessageList'
import { Button, Confirm, EmptyState, Loading, Panel } from './components/ui'
import { useDebouncedValue, useEventStream, useMessages } from './hooks'

type DeleteIntent = { kind: 'message'; id: string } | { kind: 'all' }

export function App(): JSX.Element {
  const [query, setQuery] = useState('')
  const debouncedQuery = useDebouncedValue(query, 300)
  const inbox = useMessages(debouncedQuery)
  const [selectedID, setSelectedID] = useState<string>()
  const [mobileDetail, setMobileDetail] = useState(false)
  const [intent, setIntent] = useState<DeleteIntent>()
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState<string>()
  const searchRef = useRef<HTMLInputElement>(null)
  const detailRef = useRef<HTMLElement>(null)
  const inboxRef = useRef<HTMLElement>(null)
  const rowButtons = useRef(new Map<string, HTMLButtonElement>())
  const mutationInFlight = useRef(false)
  const reconnecting = useEventStream({
    onOpen: () => { void inbox.refresh() },
    onMessage: (id) => { void inbox.refresh({ notify: true, newMessageID: id }) },
  })

  useEffect(() => {
    setSelectedID(undefined)
    setMobileDetail(false)
  }, [debouncedQuery])

  function focusRow(id?: string): void {
    window.requestAnimationFrame(() => {
      const row = id ? rowButtons.current.get(id) : undefined
      if (row) {
        row.focus({ preventScroll: true })
        row.scrollIntoView({ block: 'nearest' })
      } else searchRef.current?.focus()
    })
  }

  const openMessage = useCallback((id: string): void => {
    setSelectedID(id)
    setMobileDetail(true)
    if (window.matchMedia('(max-width: 767px)').matches) {
      window.requestAnimationFrame(() => detailRef.current?.focus())
    }
  }, [])

  function backToInbox(): void {
    setMobileDetail(false)
    focusRow(selectedID)
  }

  function requestDelete(id: string): void {
    setDeleteError(undefined)
    setIntent({ kind: 'message', id })
  }

  function clearSearch(): void {
    setQuery('')
    searchRef.current?.focus()
  }

  async function performDelete(): Promise<void> {
    if (!intent || mutationInFlight.current) return
    const target = intent
    mutationInFlight.current = true
    setDeleting(true)
    setDeleteError(undefined)
    inbox.setAnnouncement('')
    const index = target.kind === 'message' ? inbox.items.findIndex((item) => item.id === target.id) : -1
    const adjacent = inbox.items[index + 1]?.id ?? inbox.items[index - 1]?.id
    try {
      if (target.kind === 'all') await clearMessages()
      else await deleteMessage(target.id)
      if (target.kind === 'all') {
        setSelectedID(undefined)
        setMobileDetail(false)
      } else if (selectedID === target.id) setSelectedID(undefined)
      const result = await inbox.refresh({ reset: target.kind === 'all' })
      let nextID: string | undefined
      if (target.kind === 'message') {
        nextID = result
          ? result.items.find((item) => item.id === adjacent)?.id ?? result.items[Math.max(0, index)]?.id ?? result.items.at(-1)?.id
          : adjacent
        setSelectedID(nextID)
        if (!nextID) setMobileDetail(false)
      }
      inbox.setAnnouncement(target.kind === 'all' ? 'Inbox cleared' : 'Message deleted')
      setIntent(undefined)
      window.requestAnimationFrame(() => {
        if (target.kind === 'all' || !nextID) searchRef.current?.focus()
        else if (window.matchMedia('(max-width: 767px)').matches) detailRef.current?.focus()
        else focusRow(nextID)
      })
    } catch (failure: unknown) {
      setDeleteError(errorMessage(failure))
    } finally {
      mutationInFlight.current = false
      setDeleting(false)
    }
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent): void {
      if (event.defaultPrevented || event.isComposing || event.altKey || event.ctrlKey || event.metaKey || intent || deleting) return
      const target = event.target instanceof HTMLElement ? event.target : null
      const editing = target?.matches('input, textarea, select') || target?.isContentEditable
      if (event.key === 'Escape') {
        if (query || editing) {
          event.preventDefault()
          setQuery('')
          target?.blur()
        } else if (mobileDetail && window.matchMedia('(max-width: 767px)').matches) {
          event.preventDefault()
          setMobileDetail(false)
          focusRow(selectedID)
        }
        return
      }
      if (editing) return
      if (event.key === '/') {
        event.preventDefault()
        setMobileDetail(false)
        window.requestAnimationFrame(() => searchRef.current?.focus())
        return
      }
      if (event.key === 'j' || event.key === 'ArrowDown' || event.key === 'k' || event.key === 'ArrowUp') {
        if (target?.closest('[role="tablist"]')) return
        const direction = event.key === 'j' || event.key === 'ArrowDown' ? 1 : -1
        const index = inbox.items.findIndex((item) => item.id === selectedID)
        const nextIndex = index < 0 ? (direction > 0 ? 0 : inbox.items.length - 1) : Math.max(0, Math.min(inbox.items.length - 1, index + direction))
        const next = inbox.items[nextIndex]
        if (!next) return
        event.preventDefault()
        setSelectedID(next.id)
        if (target && inboxRef.current?.contains(target)) focusRow(next.id)
      } else if (event.key === 'Enter') {
        const row = target?.closest<HTMLElement>('[data-message-id]')
        if (target?.closest('button, a, summary, [role="tab"]') && !row) return
        const id = row?.dataset.messageId ?? selectedID
        if (!id) return
        event.preventDefault()
        setSelectedID(id)
        setMobileDetail(true)
        window.requestAnimationFrame(() => detailRef.current?.focus())
      } else if ((event.key === 'Delete' || event.key === 'Backspace') && selectedID) {
        event.preventDefault()
        setDeleteError(undefined)
        setIntent({ kind: 'message', id: selectedID })
      }
    }
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, [inbox.items, selectedID, query, mobileDetail, intent, deleting])

  return (
    <div className="mail-app">
      <header className="mail-app-header">
        <strong><span className="mail-brand-mark" aria-hidden="true">✉</span> devcloud mail</strong>
        <span className="mail-app-count">Inbox · {inbox.total.toLocaleString()}</span>
      </header>
      {reconnecting ? <div className="mail-connection-banner" role="status">Reconnecting to server...</div> : null}
      <div className={`mail-shell${mobileDetail ? ' showing-detail' : ''}`}>
        <Panel className="mail-inbox-pane">
          <section className="mail-inbox" ref={inboxRef} aria-label="Inbox">
            <div className="mail-inbox-toolbar">
              <label className="mail-inbox-filter" htmlFor="mail-search"><span>Search</span></label>
              <div className="mail-search-field">
                <input
                  id="mail-search"
                  ref={searchRef}
                  aria-label="Filter messages"
                  placeholder="Search messages..."
                  type="search"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
                {query ? <Button aria-label="Clear search" className="mail-search-clear" onClick={clearSearch}>×</Button> : null}
              </div>
              <div className="mail-inbox-actions">
                <Button disabled={inbox.loading || deleting} onClick={() => void inbox.refresh()}>Refresh</Button>
                <Button className="danger" disabled={inbox.total === 0 || deleting} onClick={() => { setDeleteError(undefined); setIntent({ kind: 'all' }) }}>Clear all</Button>
                {inbox.loading && inbox.items.length > 0 ? <span role="status" className="mail-refresh-status">Refreshing...</span> : null}
              </div>
            </div>
            <div className="mail-inbox-content" aria-busy={inbox.loading || inbox.loadingMore}>
              {inbox.error ? (
                <div className="mail-list-error" role="alert">
                  <EmptyState title="Failed to load messages" description={inbox.error} actionLabel="Retry" onAction={() => void inbox.refresh()} />
                </div>
              ) : null}
              {inbox.loading && inbox.items.length === 0 ? <Loading /> : null}
              {!inbox.loading && !inbox.error && inbox.items.length === 0 ? (
                <div className="mail-list-empty">
                  <EmptyState
                    title={debouncedQuery ? 'No matches found' : 'Inbox is empty'}
                    description={debouncedQuery ? `No messages match "${debouncedQuery}". Clear the filter or try another query.` : 'Send mail via SMTP to localhost:1025 to inspect messages.'}
                    actionLabel={debouncedQuery ? 'Clear search' : undefined}
                    onAction={debouncedQuery ? clearSearch : undefined}
                  />
                </div>
              ) : null}
              {inbox.items.length > 0 ? <MessageList messages={inbox.items} selectedID={selectedID} highlightedIDs={inbox.highlightedIDs} buttons={rowButtons} onSelect={openMessage} /> : null}
              {inbox.moreError ? <div className="mail-list-error" role="alert"><EmptyState title="Failed to load messages" description={inbox.moreError} actionLabel="Retry" onAction={() => void inbox.loadMore()} /></div> : null}
              {inbox.hasMore ? (
                <div className="mail-load-more"><Button disabled={inbox.loading || inbox.loadingMore} onClick={() => void inbox.loadMore()}>
                  {inbox.loadingMore ? 'Loading...' : `Load more (${inbox.items.length} of ${inbox.total})`}
                </Button></div>
              ) : null}
            </div>
          </section>
        </Panel>
        <main className="mail-detail-pane" ref={detailRef} aria-label="Message details" tabIndex={-1}>
          {mobileDetail ? <div className="mail-back-toolbar"><Button onClick={backToInbox}>← Back to inbox</Button></div> : null}
          <div className="mail-detail-body">
            {selectedID ? <MessageDetail key={selectedID} id={selectedID} onDelete={requestDelete} /> : (
              <EmptyState title="No message selected" description="Select a message from the list on the left to read its content." />
            )}
          </div>
        </main>
      </div>
      <div aria-live="polite" aria-atomic="true" className="sr-only">{inbox.announcement}</div>
      {intent ? <Confirm clearAll={intent.kind === 'all'} busy={deleting} error={deleteError} onCancel={() => setIntent(undefined)} onConfirm={() => void performDelete()} /> : null}
    </div>
  )
}
