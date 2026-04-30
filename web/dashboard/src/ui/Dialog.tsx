import type { ReactNode } from 'react'
import { Button } from './Button'

type DialogProps = {
  title: string
  children: ReactNode
  onClose: () => void
}

export function Dialog({ title, children, onClose }: DialogProps): JSX.Element {
  return (
    <div className="dialog-backdrop" role="presentation">
      <section aria-modal="true" className="dialog" role="dialog">
        <header className="dialog-head">
          <h2>{title}</h2>
          <Button aria-label="Close dialog" onClick={onClose}>
            Close
          </Button>
        </header>
        <div>{children}</div>
      </section>
    </div>
  )
}
