import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * useEventLog Hook
 * 
 * Light polling of GET /nodes/{nodeId}/events every 20 seconds
 * Stops polling when page is hidden.
 * 
 * Returns { events, loading, refresh }
 */
export function useEventLog(nodeId) {
  const [events, setEvents] = useState([])
  const [loading, setLoading] = useState(true)
  const pollTimeout = useRef(null)
  const isMounted = useRef(false)

  const fetch_events = useCallback(async () => {
    if (!nodeId) {
      setEvents([])
      return
    }

    try {
      const response = await fetch(`/nodes/${nodeId}/events`, {
        cache: 'no-store',
      })
      if (!response.ok) throw new Error('Failed to fetch events')
      const data = await response.json()
      
      if (isMounted.current) {
        setEvents(Array.isArray(data.events) ? data.events : [])
        setLoading(false)
      }
    } catch (err) {
      // Silently fail, keep previous state
      if (isMounted.current) {
        setLoading(false)
      }
    }
  }, [nodeId])
  
  useEffect(() => {
    isMounted.current = true

    if (!nodeId) {
      setEvents([])
      setLoading(false)
      return () => {
        isMounted.current = false
      }
    }

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
        await fetch_events()
        scheduleNextPoll(20000)  // 20s polling interval
      }, delay)
    }

    // Initial fetch
    fetch_events().finally(() => {
      scheduleNextPoll(20000)  // Schedule next poll in 20s
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
        fetch_events().finally(() => {
          scheduleNextPoll(20000)
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
  }, [nodeId, fetch_events])

  const refresh = useCallback(async () => {
    await fetch_events()
  }, [fetch_events])

  return { events, loading, refresh }
}
