import { EmptyState } from '../../../ui/EmptyState'
import type { DetailState } from './state'
import type { SQSMessageSnapshot } from './types'

type MessageBrowserProps = {
  activeIndex: number
  detailState: DetailState
  messages: SQSMessageSnapshot[]
  queueName?: string
  onSelectIndex: (index: number) => void
}

export function MessageBrowser({ activeIndex, detailState, messages, onSelectIndex, queueName }: MessageBrowserProps): JSX.Element {
  if (!queueName) {
    return <EmptyState title="No queue selected" description="Choose a queue to inspect retained messages." />
  }
  if (detailState.status === 'loading') {
    return <EmptyState title="Loading messages" description={`Reading messages from ${queueName}.`} />
  }
  if (detailState.status === 'error') {
    return <EmptyState title="SQS messages unavailable" description={detailState.message} />
  }
  if (messages.length === 0) {
    return <EmptyState title="No messages" description={`No retained messages in ${queueName} match the current filter.`} />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">State</th>
            <th scope="col">Body</th>
            <th scope="col">Receive</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((message, index) => (
            <tr
              className={index === activeIndex ? 'item-row active' : 'item-row'}
              key={`${message.messageId}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>{message.state}</td>
              <td>
                <MessagePreview message={message} />
              </td>
              <td>{message.receiveCount}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function MessagePreview({ message }: { message: SQSMessageSnapshot }): JSX.Element {
  return (
    <span className="attribute-preview">
      <span className="attribute-chip">{message.body || '(empty body)'}</span>
      {message.messageGroupId ? <span className="attribute-chip">group: {message.messageGroupId}</span> : null}
      {message.sequenceNumber ? <span className="attribute-chip">seq: {message.sequenceNumber}</span> : null}
    </span>
  )
}
