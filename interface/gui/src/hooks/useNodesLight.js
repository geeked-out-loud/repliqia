import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * useNodesLight Hook
 * 
 * Light polling of GET /nodes every 10 seconds.
 * Stops polling when page is hidden.
 * Used as fallback for stale node state.
 * 
 * Returns { nodes, loading, refresh }
 */
export function useNodesLight() {
  const POLL_INTERVAL_MS = 10000

  const [nodes, setNodes] = useState([])
  const [loading, setLoading] = useState(true)
  const pollTimeout = useRef(null)
  const isMounted = useRef(false)

  const fetch_nodes = useCallback(async () => {
    try {
      const response = await fetch('/nodes', {
        cache: 'no-store',
      })
      if (!response.ok) throw new Error('Failed to fetch nodes')
      const data = await response.json()
      
      if (isMounted.current) {
        setNodes(Array.isArray(data) ? data : [])
        setLoading(false)
      }
    } catch (err) {
      // Silently fail, keep previous state
      if (isMounted.current) {
        setLoading(false)
      }
    }
  }, [])
  
  useEffect(() => {
    isMounted.current = true

    // Check if page is visible
    const isVisible = !document.hidden

    if (!isVisible) {
      // Page hidden, don't poll
      return () => {
        isMounted.current = false
      }
    }

    const scheduleNextPoll = (delay) => {
      if (!isMounted.current) {
        return
      }

      if (pollTimeout.current) {
        clearTimeout(pollTimeout.current)
      }

      pollTimeout.current = setTimeout(async () => {
        await fetch_nodes()
        scheduleNextPoll(POLL_INTERVAL_MS)
      }, delay)
    }

    // Initial fetch
    fetch_nodes().finally(() => {
      scheduleNextPoll(POLL_INTERVAL_MS)
    })

    // Listen for visibility changes
    const onVisibilityChange = () => {
      if (!isMounted.current) {
        return
      }

      if (document.hidden) {
        // Page hidden, clear timeout
        if (pollTimeout.current) {
          clearTimeout(pollTimeout.current)
          pollTimeout.current = null
        }
      } else {
        // Page visible again, resume polling immediately
        fetch_nodes().finally(() => {
          scheduleNextPoll(POLL_INTERVAL_MS)
        })
      }
    }

    document.addEventListener('visibilitychange', onVisibilityChange)

    return () => {
      isMounted.current = false

      if (pollTimeout.current) {
        clearTimeout(pollTimeout.current)
      }

      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [fetch_nodes])

  const refresh = useCallback(async () => {
    await fetch_nodes()
  }, [fetch_nodes])

  return { nodes, loading, refresh }
}
