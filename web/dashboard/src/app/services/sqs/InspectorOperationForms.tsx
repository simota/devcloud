import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { maskReceiptHandle } from './helpers'
import type { SQSQueueSnapshot, SQSReceivedMessage } from './types'

type PurgeQueueFormProps = {
  activeQueue?: SQSQueueSnapshot
  busyAction?: string
  purgeConfirmation: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setPurgeConfirmation: (value: string) => void
}

export function PurgeQueueForm({
  activeQueue,
  busyAction,
  purgeConfirmation,
  onSubmit,
  setPurgeConfirmation,
}: PurgeQueueFormProps): JSX.Element {
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <span className="toolbar-count">{activeQueue ? activeQueue.name : 'No queue selected'}</span>
      <label className="compact-filter">
        <span>Purge confirmation</span>
        <input
          aria-label="Confirm SQS purge queue"
          disabled={!activeQueue}
          onChange={(event) => setPurgeConfirmation(event.target.value)}
          placeholder={activeQueue?.name ?? 'queue name'}
          value={purgeConfirmation}
        />
      </label>
      <Button
        className="danger"
        disabled={!activeQueue || purgeConfirmation !== activeQueue.name || busyAction === 'purge-queue'}
        type="submit"
      >
        Purge
      </Button>
    </form>
  )
}

type ChangeVisibilityFormProps = {
  activeQueue: boolean
  busyAction?: string
  selectedReceiptHandle: string
  visibilityTimeout: string
  visibilityConfirmation: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setVisibilityTimeout: (value: string) => void
  setVisibilityConfirmation: (value: string) => void
}

export function ChangeVisibilityForm({
  activeQueue,
  busyAction,
  selectedReceiptHandle,
  visibilityTimeout,
  visibilityConfirmation,
  onSubmit,
  setVisibilityTimeout,
  setVisibilityConfirmation,
}: ChangeVisibilityFormProps): JSX.Element {
  const handleDisabled = !activeQueue || selectedReceiptHandle === ''
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <label className="compact-filter small">
        <span>Visibility timeout</span>
        <input
          aria-label="SQS change visibility timeout seconds"
          disabled={handleDisabled}
          inputMode="numeric"
          onChange={(event) => setVisibilityTimeout(event.target.value)}
          value={visibilityTimeout}
        />
      </label>
      <label className="compact-filter small">
        <span>Confirm</span>
        <input
          aria-label="Confirm SQS change message visibility"
          disabled={handleDisabled}
          onChange={(event) => setVisibilityConfirmation(event.target.value)}
          placeholder="visibility"
          value={visibilityConfirmation}
        />
      </label>
      <Button
        disabled={
          handleDisabled ||
          visibilityConfirmation !== 'visibility' ||
          busyAction === 'change-visibility'
        }
        type="submit"
      >
        Change visibility
      </Button>
    </form>
  )
}

type DeleteMessageFormProps = {
  activeQueue: boolean
  busyAction?: string
  selectedReceivedMessage?: SQSReceivedMessage
  selectedReceiptHandle: string
  pastedReceiptHandle: string
  deleteConfirmation: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setPastedReceiptHandle: (value: string) => void
  setDeleteConfirmation: (value: string) => void
}

export function DeleteMessageForm({
  activeQueue,
  busyAction,
  selectedReceivedMessage,
  selectedReceiptHandle,
  pastedReceiptHandle,
  deleteConfirmation,
  onSubmit,
  setPastedReceiptHandle,
  setDeleteConfirmation,
}: DeleteMessageFormProps): JSX.Element {
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <label className="compact-filter wide">
        <span>Receipt handle</span>
        <input
          aria-label="SQS delete receipt handle"
          disabled={!activeQueue}
          onChange={(event) => setPastedReceiptHandle(event.target.value)}
          placeholder={selectedReceivedMessage ? maskReceiptHandle(selectedReceivedMessage.ReceiptHandle) : 'paste receipt handle or select received message'}
          value={pastedReceiptHandle}
        />
      </label>
      <label className="compact-filter small">
        <span>Confirm</span>
        <input
          aria-label="Confirm SQS delete message"
          disabled={!activeQueue || selectedReceiptHandle === ''}
          onChange={(event) => setDeleteConfirmation(event.target.value)}
          placeholder="delete"
          value={deleteConfirmation}
        />
      </label>
      <Button
        className="danger"
        disabled={!activeQueue || selectedReceiptHandle === '' || deleteConfirmation !== 'delete' || busyAction === 'delete-message'}
        type="submit"
      >
        Delete
      </Button>
    </form>
  )
}
