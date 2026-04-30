import { Panel } from '../../../ui/Panel'
import { BucketSidebar } from './BucketSidebar'
import { ObjectBrowser } from './ObjectBrowser'
import { ObjectInspector } from './ObjectInspector'

export function S3Dashboard(): JSX.Element {
  return (
    <div className="s3-placeholder">
      <Panel title="Buckets">
        <BucketSidebar />
      </Panel>
      <Panel title="Object browser">
        <ObjectBrowser />
      </Panel>
      <Panel title="Inspector">
        <ObjectInspector />
      </Panel>
      <a className="compat-link" href="/s3">
        Open current S3 dashboard
      </a>
    </div>
  )
}
