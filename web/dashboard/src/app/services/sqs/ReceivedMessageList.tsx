import { maskReceiptHandle } from './helpers'
import type { SQSReceivedMessage } from './types'

type ReceivedMessageListProps = {
  messages: SQSReceivedMessage[]
  selectedIndex: number
  onSelectIndex: (index: number) => void
}

export function ReceivedMessageList({ messages, onSelectIndex, selectedIndex }: ReceivedMessageListProps): JSX.Element | null {
  if (messages.length === 0) {
    return null
  }

  return (
    <div className="dynamodb-item-table-wrap" aria-label="Received SQS messages">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Use</th>
            <th scope="col">Message</th>
            <th scope="col">Receipt handle</th>
          </tr>
        </thead>
        <tbody>
          {messages.map((message, index) => (
            <tr
              className={index === selectedIndex ? 'item-row active' : 'item-row'}
              key={`${message.MessageId}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>{index === selectedIndex ? 'selected' : 'select'}</td>
              <td>
                <span className="attribute-preview">
                  <span className="attribute-chip">{message.Body || '(empty body)'}</span>
                  <span className="attribute-chip">{message.MessageId}</span>
                </span>
              </td>
              <td>
                <code>{maskReceiptHandle(message.ReceiptHandle)}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
