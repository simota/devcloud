import { Button } from './Button'

type EmptyStateProps = {
  title: string
  description: string
  actionLabel?: string
  onAction?: () => void
}

export function EmptyState({ title, description, actionLabel, onAction }: EmptyStateProps): JSX.Element {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{description}</p>
      {actionLabel && onAction ? <Button onClick={onAction}>{actionLabel}</Button> : null}
    </div>
  )
}
