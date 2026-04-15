import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * useNodes Hook
 * 
 * Polls GET /nodes every 2000ms
 * Returns { nodes, loading, refresh }
 */
export function useNodes() {
  const [nodes, setNodes] = useState([])
  const [loading, setLoading] = useState(true)
  const pollTimeout = useRef(null)
  const requestInFlight = useRef(false)
  const abortController = useRef(null)
  const isMounted = useRef(false)
  const hasRecentError = useRef(false)

  const fetch_nodes = useCallback(async () => {
    if (requestInFlight.current) {
      return
    }

    requestInFlight.current = true
    const controller = new AbortController()
    abortController.current = controller

    try {
      const response = await fetch('/nodes', {
        signal: controller.signal,
        cache: 'no-store',
      })
      if (!response.ok) throw new Error('Failed to fetch nodes')
      const data = await response.json()
      setNodes(Array.isArray(data) ? data : [])
      hasRecentError.current = false
    } catch (err) {
      if (err?.name !== 'AbortError') {
        hasRecentError.current = true
      }
      // Silently fail, keep previous state
    } finally {
      requestInFlight.current = false
      if (abortController.current === controller) {
        abortController.current = null
      }

      if (isMounted.current) {
        setLoading(false)
      }
    }
  }, [])
  
  useEffect(() => {
    isMounted.current = true

    const pollDelay = () => {
      if (document.hidden) {
        return 10000
      }

      return hasRecentError.current ? 5000 : 2500
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
        scheduleNextPoll(pollDelay())
      }, delay)
    }

    fetch_nodes().finally(() => {
      scheduleNextPoll(pollDelay())
    })

    const onVisibilityChange = () => {
      if (!isMounted.current) {
        return
      }

      if (!document.hidden) {
        fetch_nodes()
      }

      scheduleNextPoll(pollDelay())
    }

    document.addEventListener('visibilitychange', onVisibilityChange)
    
    return () => {
      isMounted.current = false

      if (pollTimeout.current) {
        clearTimeout(pollTimeout.current)
        pollTimeout.current = null
      }

      if (abortController.current) {
        abortController.current.abort()
        abortController.current = null
      }

      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [fetch_nodes])
  
  return {
    nodes,
    loading,
    refresh: fetch_nodes,
  }
}
