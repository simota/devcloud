import { EmptyState } from '../../../ui/EmptyState'
import { parseRedriveAllowPolicy, parseRedrivePolicy } from './helpers'
import type { DetailState } from './state'
import type { SQSMessageSnapshot, SQSQueueSnapshot, SQSStatus } from './types'

type SQSInspectorProps = {
  detailState: DetailState
  message?: SQSMessageSnapshot
  queue?: SQSQueueSnapshot
  status?: SQSStatus
}

export function SQSInspector({ detailState, message, queue, status }: SQSInspectorProps): JSX.Element {
  if (!queue) {
    return <EmptyState title="Inspector" description="Queue attributes and selected message JSON will appear here." />
  }

  const leases = detailState.status === 'success' ? detailState.leases : []
  const dlq = detailState.status === 'success' ? detailState.dlq : undefined
  const redrivePolicy = parseRedrivePolicy(queue.attributes.RedrivePolicy)
  const redriveAllowPolicy = parseRedriveAllowPolicy(queue.attributes.RedriveAllowPolicy)

  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Queue</span>
        <h3>{queue.name}</h3>
        <dl className="inspector-list">
          <div>
            <dt>Endpoint</dt>
            <dd>
              <code>{status?.endpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Region</dt>
            <dd>{status?.region ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>ARN</dt>
            <dd>
              <code>{queue.arn}</code>
            </dd>
          </div>
          <div>
            <dt>Visibility</dt>
            <dd>{queue.attributes.VisibilityTimeout ?? 'unknown'}s</dd>
          </div>
          <div>
            <dt>Leases</dt>
            <dd>{leases.length}</dd>
          </div>
          <div>
            <dt>DLQ sources</dt>
            <dd>{dlq?.deadLetterSourceQueues.map((source) => source.name).join(', ') || 'none'}</dd>
          </div>
          <div>
            <dt>DLQ target</dt>
            <dd>{dlq?.deadLetterQueue?.name ?? redrivePolicy?.deadLetterTargetArn ?? 'none'}</dd>
          </div>
          <div>
            <dt>Redrive</dt>
            <dd>
              {redrivePolicy ? `maxReceiveCount ${redrivePolicy.maxReceiveCount}` : 'none'}
              {redriveAllowPolicy ? `, allow ${redriveAllowPolicy.redrivePermission}` : ''}
            </dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Selected message</span>
        {message ? (
          <pre className="mail-preview">{JSON.stringify(message, null, 2)}</pre>
        ) : (
          <p className="inspector-muted">Select a message row to inspect JSON.</p>
        )}
      </section>
    </div>
  )
}
