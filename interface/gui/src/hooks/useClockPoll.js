import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * useClockPoll Hook
 * 
 * Light polling of /proxy/{nodeId}/node/clock every 5 seconds
 * Stops polling when page is hidden.
 * Useful for seeing vector clock advances in real-time.
 * 
 * Returns { clock, loading, refresh }
 */
export function useClockPoll(nodeId) {
  const [clock, setClock] = useState(null)
  const [loading, setLoading] = useState(true)
  const pollTimeout = useRef(null)
  const isMounted = useRef(false)

  const fetch_clock = useCallback(async () => {
    if (!nodeId) {
      setClock(null)
      return
    }

    try {
      const response = await fetch(`/proxy/${nodeId}/node/clock`, {
        cache: 'no-store',
      })
      if (!response.ok) throw new Error('Failed to fetch clock')
      const data = await response.json()
      
      if (isMounted.current) {
        setClock(data)
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
      setClock(null)
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
        await fetch_clock()
        scheduleNextPoll(5000)  // 5s polling interval
      }, delay)
    }

    // Initial fetch
    fetch_clock().finally(() => {
      scheduleNextPoll(5000)  // Schedule next poll in 5s
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
        fetch_clock().finally(() => {
          scheduleNextPoll(5000)
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
  }, [nodeId, fetch_clock])

  const refresh = useCallback(async () => {
    await fetch_clock()
  }, [fetch_clock])

  return { clock, loading, refresh }
}
