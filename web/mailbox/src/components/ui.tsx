import {
  useEffect, useId, useRef,
  type ButtonHTMLAttributes, type KeyboardEvent, type ReactNode,
} from 'react'

// Local copies of dashboard primitives, adapted for the standalone mailbox.
export function Button({ children, className, type = 'button', ...props }: ButtonHTMLAttributes<HTMLButtonElement>): JSX.Element {
  return <button className={className ? `button ${className}` : 'button'} type={type} {...props}>{children}</button>
}

export function EmptyState({ title, description, actionLabel, onAction }: {
  title: string; description: string; actionLabel?: string; onAction?: () => void
}): JSX.Element {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{description}</p>
      {actionLabel && onAction ? <Button onClick={onAction}>{actionLabel}</Button> : null}
    </div>
  )
}

export function Panel({ children, className = '' }: { children: ReactNode; className?: string }): JSX.Element {
  return <section className={`panel ${className}`}>{children}</section>
}

export function Loading({ detail = false }: { detail?: boolean }): JSX.Element {
  return (
    <div className={`mail-loading ${detail ? 'detail' : ''}`} role="status">
      <span className="sr-only">{detail ? 'Loading message' : 'Loading messages'}</span>
      <div aria-hidden="true">
        {Array.from({ length: detail ? 4 : 8 }, (_, index) => (
          <div className="mail-skeleton" key={index}><span /><span /></div>
        ))}
      </div>
    </div>
  )
}

export function Dialog({ title, description, children, busy, onClose }: {
  title: string; description: string; children: ReactNode; busy: boolean; onClose: () => void
}): JSX.Element {
  const titleID = useId()
  const descriptionID = useId()
  const dialogRef = useRef<HTMLDialogElement>(null)
  useEffect(() => {
    const dialog = dialogRef.current
    const previousFocus = document.activeElement
    dialog?.showModal()
    return () => {
      dialog?.close()
      if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus()
    }
  }, [])
  return (
    <dialog
      ref={dialogRef}
      aria-labelledby={titleID}
      aria-describedby={descriptionID}
      aria-modal="true"
      aria-busy={busy}
      className="confirm-dialog tone-danger"
      role="alertdialog"
      onCancel={(event) => { event.preventDefault(); if (!busy) onClose() }}
      onClick={(event) => {
        if (event.target !== event.currentTarget || busy) return
        const bounds = event.currentTarget.getBoundingClientRect()
        if (event.clientX < bounds.left || event.clientX > bounds.right ||
          event.clientY < bounds.top || event.clientY > bounds.bottom) onClose()
      }}
    >
      <h2 className="confirm-title" id={titleID}>{title}</h2>
      <p className="confirm-description" id={descriptionID}>{description}</p>
      {children}
    </dialog>
  )
}

export function Confirm({ clearAll, busy, error, onCancel, onConfirm }: {
  clearAll: boolean; busy: boolean; error?: string; onCancel: () => void; onConfirm: () => void
}): JSX.Element {
  return (
    <Dialog
      title={clearAll ? 'Clear inbox' : 'Delete message'}
      description={clearAll ? 'Delete all stored messages? This action cannot be undone.' : 'Are you sure you want to delete this message?'}
      busy={busy}
      onClose={onCancel}
    >
      {error ? <p className="mail-action-error" role="alert">{error}</p> : null}
      <div className="confirm-actions">
        <Button autoFocus disabled={busy} onClick={onCancel}>Cancel</Button>
        <Button className="danger primary" disabled={busy} onClick={onConfirm}>
          {busy ? 'Deleting...' : clearAll ? 'Clear inbox' : 'Delete'}
        </Button>
      </div>
    </Dialog>
  )
}

export type TabItem<T extends string> = { id: T; label: string; disabled?: boolean }

export function Tabs<T extends string>({ items, activeID, onChange }: {
  items: TabItem<T>[]; activeID: T; onChange: (id: T) => void
}): JSX.Element {
  const buttons = useRef(new Map<T, HTMLButtonElement>())
  function onKeyDown(event: KeyboardEvent<HTMLButtonElement>, id: T): void {
    const enabled = items.filter((item) => !item.disabled)
    const index = enabled.findIndex((item) => item.id === id)
    let next: TabItem<T> | undefined
    if (event.key === 'ArrowRight') next = enabled[(index + 1) % enabled.length]
    else if (event.key === 'ArrowLeft') next = enabled[(index + enabled.length - 1) % enabled.length]
    else if (event.key === 'Home') next = enabled[0]
    else if (event.key === 'End') next = enabled[enabled.length - 1]
    if (!next) return
    event.preventDefault()
    event.stopPropagation()
    onChange(next.id)
    buttons.current.get(next.id)?.focus()
  }
  return (
    <div className="mail-tabs" role="tablist" aria-label="Message content tabs">
      {items.map((item) => (
        <button
          aria-selected={item.id === activeID}
          aria-controls={`tabpanel-${item.id}`}
          id={`tab-${item.id}`}
          className={item.id === activeID ? 'mail-tab active' : 'mail-tab'}
          key={item.id}
          disabled={item.disabled}
          onClick={() => onChange(item.id)}
          onKeyDown={(event) => onKeyDown(event, item.id)}
          ref={(element) => { if (element) buttons.current.set(item.id, element); else buttons.current.delete(item.id) }}
          role="tab"
          tabIndex={item.id === activeID ? 0 : -1}
          type="button"
        >{item.label}</button>
      ))}
    </div>
  )
}
