import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useId,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { Button } from './Button'

export type ConfirmOptions = {
  /** Heading text shown at the top of the dialog. */
  title: string
  /** Body copy explaining the consequence of the action. */
  description?: ReactNode
  /** Optional secondary line shown just under the description (e.g. the target name). */
  detail?: ReactNode
  /** Label for the confirm action button. Defaults to "Confirm". */
  confirmLabel?: string
  /** Label for the cancel action button. Defaults to "Cancel". */
  cancelLabel?: string
  /** Visual tone of the confirm button. Defaults to "default". */
  tone?: 'default' | 'danger'
  /**
   * When set, the confirm button is disabled until the user types this exact
   * phrase, matching the previous `window.prompt`-based safety check.
   */
  requirePhrase?: string
  /** Hint shown above the type-to-confirm input. */
  requirePhraseLabel?: ReactNode
}

type PendingConfirm = {
  options: ConfirmOptions
  resolve: (result: boolean) => void
}

type ConfirmContextValue = (options: ConfirmOptions) => Promise<boolean>

const ConfirmContext = createContext<ConfirmContextValue | null>(null)

type ConfirmProviderProps = {
  children: ReactNode
}

export function ConfirmProvider({ children }: ConfirmProviderProps): JSX.Element {
  const [pending, setPending] = useState<PendingConfirm | null>(null)

  const confirm = useCallback<ConfirmContextValue>((options) => {
    return new Promise<boolean>((resolve) => {
      setPending({ options, resolve })
    })
  }, [])

  const handleClose = useCallback(
    (result: boolean) => {
      setPending((current) => {
        current?.resolve(result)
        return null
      })
    },
    [],
  )

  return (
    <ConfirmContext.Provider value={confirm}>
      {children}
      {pending ? (
        <ConfirmDialog
          options={pending.options}
          onCancel={() => handleClose(false)}
          onConfirm={() => handleClose(true)}
        />
      ) : null}
    </ConfirmContext.Provider>
  )
}

export function useConfirm(): ConfirmContextValue {
  const ctx = useContext(ConfirmContext)
  if (!ctx) {
    throw new Error('useConfirm must be called inside <ConfirmProvider>')
  }
  return ctx
}

type ConfirmDialogProps = {
  options: ConfirmOptions
  onCancel: () => void
  onConfirm: () => void
}

function ConfirmDialog({ options, onCancel, onConfirm }: ConfirmDialogProps): JSX.Element {
  const titleId = useId()
  const descriptionId = useId()
  const phraseId = useId()
  const confirmLabel = options.confirmLabel ?? 'Confirm'
  const cancelLabel = options.cancelLabel ?? 'Cancel'
  const tone = options.tone ?? 'default'
  const requirePhrase = options.requirePhrase ?? ''
  const [typed, setTyped] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const phraseSatisfied = requirePhrase === '' || typed === requirePhrase

  useEffect(() => {
    setTyped('')
  }, [options])

  useEffect(() => {
    if (requirePhrase !== '' && inputRef.current) {
      inputRef.current.focus()
    }
  }, [requirePhrase])

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent): void {
      if (event.key === 'Escape') {
        event.preventDefault()
        onCancel()
      } else if (event.key === 'Enter' && phraseSatisfied) {
        const target = event.target as HTMLElement | null
        if (target?.tagName === 'TEXTAREA') {
          return
        }
        event.preventDefault()
        onConfirm()
      }
    }
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, [onCancel, onConfirm, phraseSatisfied])

  function handleBackdropClick(event: React.MouseEvent<HTMLDivElement>): void {
    if (event.target === event.currentTarget) {
      onCancel()
    }
  }

  return (
    <div className="confirm-backdrop" onMouseDown={handleBackdropClick} role="presentation">
      <section
        aria-describedby={options.description || options.detail ? descriptionId : undefined}
        aria-labelledby={titleId}
        aria-modal="true"
        className={`confirm-dialog tone-${tone}`}
        role="alertdialog"
      >
        <h2 className="confirm-title" id={titleId}>
          {options.title}
        </h2>
        {options.description || options.detail ? (
          <div className="confirm-body" id={descriptionId}>
            {options.description ? <p className="confirm-description">{options.description}</p> : null}
            {options.detail ? <p className="confirm-detail">{options.detail}</p> : null}
          </div>
        ) : null}
        {requirePhrase !== '' ? (
          <div className="confirm-phrase">
            <label htmlFor={phraseId}>
              {options.requirePhraseLabel ?? (
                <>
                  Type <code>{requirePhrase}</code> to confirm
                </>
              )}
            </label>
            <input
              autoComplete="off"
              autoCorrect="off"
              className="confirm-phrase-input"
              id={phraseId}
              onChange={(event) => setTyped(event.target.value)}
              ref={inputRef}
              spellCheck={false}
              type="text"
              value={typed}
            />
          </div>
        ) : null}
        <div className="confirm-actions">
          <Button onClick={onCancel} type="button">
            {cancelLabel}
          </Button>
          <Button
            autoFocus={requirePhrase === ''}
            className={tone === 'danger' ? 'danger primary' : 'primary'}
            disabled={!phraseSatisfied}
            onClick={onConfirm}
            type="button"
          >
            {confirmLabel}
          </Button>
        </div>
      </section>
    </div>
  )
}

/** Convenience helper that builds a danger-toned ConfirmOptions where the user
 *  must type the target name (the previous `window.prompt` safety pattern). */
export function dangerConfirm(params: {
  title: string
  description?: ReactNode
  target: string
  confirmLabel?: string
}): ConfirmOptions {
  return {
    title: params.title,
    description: params.description,
    detail: (
      <>
        Target: <code>{params.target}</code>
      </>
    ),
    confirmLabel: params.confirmLabel ?? 'Delete',
    tone: 'danger',
    requirePhrase: params.target,
  }
}
