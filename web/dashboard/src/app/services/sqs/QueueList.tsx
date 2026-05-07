import { EmptyState } from '../../../ui/EmptyState'
import type { SQSQueueSnapshot } from './types'

type QueueListProps = {
  queues: SQSQueueSnapshot[]
  activeQueueName?: string
  onSelectQueue: (queueName: string) => void
}

export function QueueList({ activeQueueName, onSelectQueue, queues }: QueueListProps): JSX.Element {
  if (queues.length === 0) {
    return <EmptyState title="No queues" description="Queues created through the SQS API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="SQS queues">
      {queues.map((queue) => (
        <button
          className={queue.name === activeQueueName ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={queue.name}
          onClick={() => onSelectQueue(queue.name)}
        >
          <span className="table-row-top">
            <span className="table-row-name">{queue.name}</span>
            <span className="count-pill">{queue.totalRetainedMessages}</span>
          </span>
          <span className="table-row-meta">{queue.url}</span>
          <span className="table-row-tags">
            <span>{queue.visibleMessages} visible</span>
            <span>{queue.notVisibleMessages} in flight</span>
            <span>{queue.delayedMessages} delayed</span>
          </span>
        </button>
      ))}
    </div>
  )
}
