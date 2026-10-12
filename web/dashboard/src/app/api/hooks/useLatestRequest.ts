import { useCallback, useEffect, useRef } from 'react'

/**
 * Start a new snapshot request and return a predicate for its callbacks. Only
 * the latest request may publish a result or an error, including after an
 * event refresh overtakes the initial load. Unmounting invalidates all work.
 * Use a separate instance for independently refreshed state.
 */
export function useLatestRequest(): () => () => boolean {
  const version = useRef(0)
  const mounted = useRef(true)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
      version.current += 1
    }
  }, [])

  return useCallback(() => {
    const requestVersion = ++version.current
    return () => mounted.current && version.current === requestVersion
  }, [])
}
