import type { ReactNode } from 'react'

type PanelProps = {
  title?: string
  children: ReactNode
}

export function Panel({ title, children }: PanelProps): JSX.Element {
  return (
    <section className="panel">
      {title ? (
        <header className="panel-head">
          <h2>{title}</h2>
        </header>
      ) : null}
      <div className="panel-body">{children}</div>
    </section>
  )
}
