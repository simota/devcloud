import { Button } from '../../ui/Button'
import type { UseNotifications } from '../api/hooks/useNotifications'

type NotificationsToggleProps = {
  notifications: UseNotifications
}

export function NotificationsToggle({ notifications }: NotificationsToggleProps): JSX.Element | null {
  const { permission, enabled, setEnabled, requestPermission } = notifications

  if (permission === 'unsupported') {
    return null
  }

  if (permission === 'default') {
    return (
      <Button
        className="notifications-toggle"
        onClick={() => {
          void requestPermission()
        }}
        title="Show desktop notifications when local services emit events"
      >
        Enable notifications
      </Button>
    )
  }

  if (permission === 'denied') {
    return (
      <Button
        className="notifications-toggle is-disabled"
        disabled
        title="Notifications are blocked. Allow them in your browser site settings to enable."
      >
        Notifications blocked
      </Button>
    )
  }

  // permission === 'granted'
  return (
    <Button
      aria-pressed={enabled}
      className={enabled ? 'notifications-toggle is-on' : 'notifications-toggle'}
      onClick={() => setEnabled(!enabled)}
      title={enabled ? 'Click to mute desktop notifications' : 'Click to enable desktop notifications'}
    >
      {enabled ? 'Notifications: on' : 'Notifications: off'}
    </Button>
  )
}
