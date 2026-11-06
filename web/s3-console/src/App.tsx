import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from 'react'
import {
  createBucket,
  deleteBucket,
  deleteObject,
  getInfo,
  listBuckets,
  listObjects,
  MAX_UPLOAD_BYTES,
  objectUrl,
  uploadObject,
  type Bucket,
  type Info,
  type S3Object,
} from './api'
import { baseName, formatDate, formatSize, groupByFolder } from './format'

function readLocation() {
  const params = new URLSearchParams(window.location.search)
  return { bucket: params.get('bucket') ?? '', prefix: params.get('prefix') ?? '' }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : 'Request failed'
}

export function App() {
  const [info, setInfo] = useState<Info | null>(null)
  const [buckets, setBuckets] = useState<Bucket[]>([])
  const [objects, setObjects] = useState<S3Object[]>([])
  const [location, setLocation] = useState(readLocation)
  const [newBucket, setNewBucket] = useState('')
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)
  const [error, setError] = useState('')
  const [status, setStatus] = useState('')
  const [busy, setBusy] = useState(false)
  const fileInput = useRef<HTMLInputElement>(null)
  const { bucket, prefix } = location
  const shownLocation = useRef(location)
  shownLocation.current = location
  const latestListing = useRef(0)

  const navigate = useCallback((next: { bucket: string; prefix: string }) => {
    const params = new URLSearchParams()
    if (next.bucket) params.set('bucket', next.bucket)
    if (next.prefix) params.set('prefix', next.prefix)
    const query = params.toString()
    window.history.pushState(null, '', query ? `/?${query}` : '/')
    setPendingDelete(null)
    setLocation(next)
  }, [])

  useEffect(() => {
    const onPopState = () => {
      setPendingDelete(null)
      setLocation(readLocation())
    }
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  const refreshBuckets = useCallback(async () => {
    setBuckets(await listBuckets())
  }, [])

  // Delete acts on the bucket on screen, so a listing from any other location or
  // an older request must never be shown under it.
  const refreshObjects = useCallback(async () => {
    if (shownLocation.current.bucket !== bucket || shownLocation.current.prefix !== prefix) return
    const request = ++latestListing.current
    try {
      const next = bucket ? await listObjects(bucket, prefix) : []
      if (request === latestListing.current) setObjects(next)
    } catch (e) {
      if (request === latestListing.current) throw e
    }
  }, [bucket, prefix])

  useEffect(() => {
    getInfo().then(setInfo, (e) => setError(message(e)))
    refreshBuckets().catch((e) => setError(message(e)))
  }, [refreshBuckets])

  useEffect(() => {
    setError('')
    setObjects([])
    refreshObjects().catch((e) => {
      setObjects([])
      setError(message(e))
    })
  }, [refreshObjects])

  const run = useCallback(async (done: string, action: () => Promise<void>) => {
    setBusy(true)
    setError('')
    setStatus('')
    try {
      await action()
      setStatus(done)
    } catch (e) {
      setError(message(e))
    } finally {
      setBusy(false)
      setPendingDelete(null)
    }
  }, [])

  const submitBucket = (event: FormEvent) => {
    event.preventDefault()
    const name = newBucket.trim()
    if (!name) return
    void run(`Created bucket ${name}`, async () => {
      await createBucket(name)
      setNewBucket('')
      await refreshBuckets()
      navigate({ bucket: name, prefix: '' })
    })
  }

  const removeBucket = () =>
    run(`Deleted bucket ${bucket}`, async () => {
      await deleteBucket(bucket)
      navigate({ bucket: '', prefix: '' })
      await refreshBuckets()
    })

  const removeObject = (key: string) =>
    run(`Deleted ${key}`, async () => {
      await deleteObject(bucket, key)
      await Promise.all([refreshObjects(), refreshBuckets()])
    })

  const upload = (files: FileList) => {
    const selected = [...files]
    const tooLarge = selected.find((file) => file.size > MAX_UPLOAD_BYTES)
    if (tooLarge) {
      setError(`${tooLarge.name} exceeds the ${formatSize(MAX_UPLOAD_BYTES)} request limit.`)
      return
    }
    void run(`Uploaded ${selected.length} file(s)`, async () => {
      for (const file of selected) await uploadObject(bucket, prefix + file.name, file)
      await Promise.all([refreshObjects(), refreshBuckets()])
    })
  }

  const listing = useMemo(() => groupByFolder(objects, prefix), [objects, prefix])
  const crumbs = useMemo(() => {
    const parts = prefix.split('/').filter(Boolean)
    return parts.map((part, index) => ({ label: part, prefix: `${parts.slice(0, index + 1).join('/')}/` }))
  }, [prefix])
  const isEmpty = listing.folders.length === 0 && listing.objects.length === 0

  return (
    <div className="app">
      <header className="topbar">
        <h1>devcloud S3 console</h1>
        {info && (
          <span className="meta">
            {info.region} · {info.authMode} auth
          </span>
        )}
      </header>
      <div className="layout">
        <nav className="sidebar" aria-label="Buckets">
          <form className="new-bucket" onSubmit={submitBucket}>
            <label htmlFor="new-bucket">New bucket</label>
            <div className="row">
              <input
                id="new-bucket"
                value={newBucket}
                onChange={(event) => setNewBucket(event.target.value)}
                placeholder="my-bucket"
                autoComplete="off"
                spellCheck={false}
              />
              <button className="button primary" disabled={busy || !newBucket.trim()}>Create</button>
            </div>
          </form>
          <ul className="bucket-list">
            {buckets.map((item) => (
              <li key={item.name}>
                <button
                  className={item.name === bucket ? 'bucket active' : 'bucket'}
                  aria-current={item.name === bucket ? 'true' : undefined}
                  onClick={() => navigate({ bucket: item.name, prefix: '' })}
                >
                  <span className="bucket-name">{item.name}</span>
                  <span className="meta">{item.objectCount}</span>
                </button>
              </li>
            ))}
          </ul>
          {buckets.length === 0 && <p className="meta empty-note">No buckets yet.</p>}
        </nav>
        <main className="content">
          <div aria-live="polite">
            {error && <p className="banner error" role="alert">{error}</p>}
            {!error && status && <p className="banner">{status}</p>}
          </div>
          {!bucket && <p className="placeholder">Select or create a bucket.</p>}
          {bucket && (
            <>
              <div className="toolbar">
                <nav className="crumbs" aria-label="Path">
                  <button className="link" onClick={() => navigate({ bucket, prefix: '' })}>{bucket}</button>
                  {crumbs.map((crumb) => (
                    <span key={crumb.prefix}>
                      <span className="separator">/</span>
                      <button className="link" onClick={() => navigate({ bucket, prefix: crumb.prefix })}>{crumb.label}</button>
                    </span>
                  ))}
                </nav>
                <div className="row">
                  <input
                    ref={fileInput}
                    type="file"
                    multiple
                    hidden
                    aria-label="Files to upload"
                    onChange={(event) => {
                      if (event.target.files?.length) upload(event.target.files)
                      event.target.value = ''
                    }}
                  />
                  <button className="button primary" disabled={busy} onClick={() => fileInput.current?.click()}>Upload</button>
                  {pendingDelete === bucket ? (
                    <>
                      <button className="button danger primary" disabled={busy} onClick={() => void removeBucket()}>Confirm delete bucket</button>
                      <button className="button" onClick={() => setPendingDelete(null)}>Cancel</button>
                    </>
                  ) : (
                    <button className="button danger" disabled={busy} onClick={() => setPendingDelete(bucket)}>Delete bucket</button>
                  )}
                </div>
              </div>
              {isEmpty ? (
                <p className="placeholder">This location is empty.</p>
              ) : (
                <table className="objects">
                  <thead>
                    <tr>
                      <th scope="col">Name</th>
                      <th scope="col" className="numeric">Size</th>
                      <th scope="col">Type</th>
                      <th scope="col">Last modified</th>
                      <th scope="col"><span className="sr-only">Actions</span></th>
                    </tr>
                  </thead>
                  <tbody>
                    {listing.folders.map((folder) => (
                      <tr key={folder}>
                        <td colSpan={5}>
                          <button className="link" onClick={() => navigate({ bucket, prefix: folder })}>{baseName(folder)}/</button>
                        </td>
                      </tr>
                    ))}
                    {listing.objects.map((object) => (
                      <tr key={object.key}>
                        <td className="name">{baseName(object.key)}</td>
                        <td className="numeric">{formatSize(object.size)}</td>
                        <td className="meta">{object.contentType}</td>
                        <td className="meta">{formatDate(object.lastModified)}</td>
                        <td className="actions">
                          {pendingDelete === object.key ? (
                            <>
                              <button className="button danger primary" disabled={busy} onClick={() => void removeObject(object.key)}>Confirm</button>
                              <button className="button" onClick={() => setPendingDelete(null)}>Cancel</button>
                            </>
                          ) : (
                            <>
                              <a className="button" href={objectUrl(bucket, object.key)} download>Download</a>
                              <button className="button danger" disabled={busy} onClick={() => setPendingDelete(object.key)}>Delete</button>
                            </>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}
        </main>
      </div>
    </div>
  )
}
