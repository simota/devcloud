import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'

export function MailDashboard(): JSX.Element {
  return (
    <Panel title="Mail">
      <div className="placeholder-grid">
        <EmptyState title="Mail React page pending" description="The static Mail dashboard remains available during migration." />
        <a className="compat-link" href="/mail">
          Open current Mail dashboard
        </a>
      </div>
    </Panel>
  )
}
