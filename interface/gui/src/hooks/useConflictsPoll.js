import { useCallback, useEffect, useRef, useState } from 'react'

function normalizeConflicts(payload) {
  if (!payload) {
    return []
  }

  // Canonical backend response: { conflict_count, conflicts: [{ key, versions: [...] }] }
  if (Array.isArray(payload.conflicts)) {
    return payload.conflicts
      .filter((entry) => entry && typeof entry === 'object' && typeof entry.key === 'string')
      .map((entry) => {
        const versions = Array.isArray(entry.versions) ? entry.versions : []
        return {
          key: entry.key,
          versions,
          conflict_count: typeof entry.conflict_count === 'number' ? entry.conflict_count : versions.length,
        }
      })
  }

  // Backward compatibility: { key: [siblings...] }
  if (typeof payload === 'object' && !Array.isArray(payload)) {
    return Object.entries(payload)
      .filter(([, siblings]) => Array.isArray(siblings))
      .map(([key, siblings]) => ({
        key,
        versions: siblings,
        conflict_count: siblings.length,
      }))
  }

  return []
}

/**
 * useConflictsPoll Hook
 * 
 * Light polling of /proxy/{nodeId}/conflicts every 5 seconds
 * Stops polling when page is hidden.
 * Useful for detecting conflicts without manual refresh.
 * 
 * Returns { conflicts, loading, refresh }
 */
export function useConflictsPoll(nodeId) {
  const [conflicts, setConflicts] = useState([])
  const [loading, setLoading] = useState(true)
  const pollTimeout = useRef(null)
  const isMounted = useRef(false)

  const fetch_conflicts = useCallback(async () => {
    if (!nodeId) {
      setConflicts([])
      return
    }

    try {
      const response = await fetch(`/proxy/${nodeId}/conflicts`, {
        cache: 'no-store',
      })
      if (!response.ok) throw new Error('Failed to fetch conflicts')
      const data = await response.json()
      
      if (isMounted.current) {
        setConflicts(normalizeConflicts(data))
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
      setConflicts([])
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
        await fetch_conflicts()
        scheduleNextPoll(5000)  // 5s polling interval
      }, delay)
    }

    // Initial fetch
    fetch_conflicts().finally(() => {
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
        fetch_conflicts().finally(() => {
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
  }, [nodeId, fetch_conflicts])

  const refresh = useCallback(async () => {
    await fetch_conflicts()
  }, [fetch_conflicts])

  return { conflicts, loading, refresh }
}
