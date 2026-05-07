import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'

type SendMessageFormProps = {
  activeQueue: boolean
  busyAction?: string
  sendBody: string
  sendDelay: string
  sendAttributesJSON: string
  sendGroupId: string
  sendDeduplicationId: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setSendBody: (value: string) => void
  setSendDelay: (value: string) => void
  setSendAttributesJSON: (value: string) => void
  setSendGroupId: (value: string) => void
  setSendDeduplicationId: (value: string) => void
}

export function SendMessageForm({
  activeQueue,
  busyAction,
  sendBody,
  sendDelay,
  sendAttributesJSON,
  sendGroupId,
  sendDeduplicationId,
  onSubmit,
  setSendBody,
  setSendDelay,
  setSendAttributesJSON,
  setSendGroupId,
  setSendDeduplicationId,
}: SendMessageFormProps): JSX.Element {
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <label className="compact-filter wide">
        <span>Message body</span>
        <textarea
          aria-label="SQS message body"
          disabled={!activeQueue}
          onChange={(event) => setSendBody(event.target.value)}
          placeholder="message body"
          rows={3}
          value={sendBody}
        />
      </label>
      <label className="compact-filter small">
        <span>Delay</span>
        <input
          aria-label="SQS message delay seconds"
          disabled={!activeQueue}
          inputMode="numeric"
          onChange={(event) => setSendDelay(event.target.value)}
          value={sendDelay}
        />
      </label>
      <label className="compact-filter">
        <span>Attributes JSON</span>
        <input
          aria-label="SQS message attributes JSON"
          disabled={!activeQueue}
          onChange={(event) => setSendAttributesJSON(event.target.value)}
          placeholder='{"kind":{"DataType":"String","StringValue":"test"}}'
          value={sendAttributesJSON}
        />
      </label>
      <label className="compact-filter small">
        <span>Group</span>
        <input
          aria-label="SQS FIFO message group ID"
          disabled={!activeQueue}
          onChange={(event) => setSendGroupId(event.target.value)}
          placeholder="FIFO only"
          value={sendGroupId}
        />
      </label>
      <label className="compact-filter small">
        <span>Dedup</span>
        <input
          aria-label="SQS FIFO message deduplication ID"
          disabled={!activeQueue}
          onChange={(event) => setSendDeduplicationId(event.target.value)}
          placeholder="optional"
          value={sendDeduplicationId}
        />
      </label>
      <Button disabled={!activeQueue || busyAction === 'send-message'} type="submit">
        Send
      </Button>
    </form>
  )
}

type ReceiveMessageFormProps = {
  activeQueue: boolean
  busyAction?: string
  receiveMaxMessages: string
  receiveVisibilityTimeout: string
  receiveWaitTime: string
  receiveAttributeNames: string
  receiveMessageAttributeNames: string
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setReceiveMaxMessages: (value: string) => void
  setReceiveVisibilityTimeout: (value: string) => void
  setReceiveWaitTime: (value: string) => void
  setReceiveAttributeNames: (value: string) => void
  setReceiveMessageAttributeNames: (value: string) => void
}

export function ReceiveMessageForm({
  activeQueue,
  busyAction,
  receiveMaxMessages,
  receiveVisibilityTimeout,
  receiveWaitTime,
  receiveAttributeNames,
  receiveMessageAttributeNames,
  onSubmit,
  setReceiveMaxMessages,
  setReceiveVisibilityTimeout,
  setReceiveWaitTime,
  setReceiveAttributeNames,
  setReceiveMessageAttributeNames,
}: ReceiveMessageFormProps): JSX.Element {
  return (
    <form className="pubsub-action-form stacked" onSubmit={onSubmit}>
      <label className="compact-filter small">
        <span>Max</span>
        <input
          aria-label="SQS receive max messages"
          disabled={!activeQueue}
          inputMode="numeric"
          onChange={(event) => setReceiveMaxMessages(event.target.value)}
          value={receiveMaxMessages}
        />
      </label>
      <label className="compact-filter small">
        <span>Visibility</span>
        <input
          aria-label="SQS receive visibility timeout seconds"
          disabled={!activeQueue}
          inputMode="numeric"
          onChange={(event) => setReceiveVisibilityTimeout(event.target.value)}
          value={receiveVisibilityTimeout}
        />
      </label>
      <label className="compact-filter small">
        <span>Wait</span>
        <input
          aria-label="SQS receive wait time seconds"
          disabled={!activeQueue}
          inputMode="numeric"
          onChange={(event) => setReceiveWaitTime(event.target.value)}
          value={receiveWaitTime}
        />
      </label>
      <label className="compact-filter">
        <span>Attrs</span>
        <input
          aria-label="SQS receive attribute names"
          disabled={!activeQueue}
          onChange={(event) => setReceiveAttributeNames(event.target.value)}
          placeholder="All or comma-separated names"
          value={receiveAttributeNames}
        />
      </label>
      <label className="compact-filter">
        <span>Msg attrs</span>
        <input
          aria-label="SQS receive message attribute names"
          disabled={!activeQueue}
          onChange={(event) => setReceiveMessageAttributeNames(event.target.value)}
          placeholder="All or comma-separated names"
          value={receiveMessageAttributeNames}
        />
      </label>
      <Button disabled={!activeQueue || busyAction === 'receive-message'} type="submit">
        Receive
      </Button>
    </form>
  )
}
