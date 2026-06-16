import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'

type CreateQueueFormProps = {
  busyAction?: string
  newQueueName: string
  newQueueKind: 'standard' | 'fifo'
  newQueueVisibility: string
  newQueueDelay: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setNewQueueName: (value: string) => void
  setNewQueueKind: (value: 'standard' | 'fifo') => void
  setNewQueueVisibility: (value: string) => void
  setNewQueueDelay: (value: string) => void
}

export function CreateQueueForm({
  busyAction,
  newQueueName,
  newQueueKind,
  newQueueVisibility,
  newQueueDelay,
  onSubmit,
  setNewQueueName,
  setNewQueueKind,
  setNewQueueVisibility,
  setNewQueueDelay,
}: CreateQueueFormProps): JSX.Element {
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <label className="compact-filter">
        <span>Queue name</span>
        <input
          aria-label="New SQS queue name"
          onChange={(event) => setNewQueueName(event.target.value)}
          placeholder={newQueueKind === 'fifo' ? 'jobs.fifo' : 'jobs'}
          value={newQueueName}
        />
      </label>
      <label className="compact-filter small">
        <span>Type</span>
        <select
          aria-label="New SQS queue type"
          onChange={(event) => setNewQueueKind(event.target.value === 'fifo' ? 'fifo' : 'standard')}
          value={newQueueKind}
        >
          <option value="standard">Standard</option>
          <option value="fifo">FIFO</option>
        </select>
      </label>
      <label className="compact-filter small">
        <span>Visibility</span>
        <input
          aria-label="New SQS queue visibility timeout seconds"
          inputMode="numeric"
          onChange={(event) => setNewQueueVisibility(event.target.value)}
          value={newQueueVisibility}
        />
      </label>
      <label className="compact-filter small">
        <span>Delay</span>
        <input
          aria-label="New SQS queue delay seconds"
          inputMode="numeric"
          onChange={(event) => setNewQueueDelay(event.target.value)}
          value={newQueueDelay}
        />
      </label>
      <Button disabled={busyAction === 'create-queue'} type="submit">
        Create
      </Button>
    </form>
  )
}
