import { useEffect, useRef, useState } from 'react'

/**
 * useWebSocket Hook
 * 
 * Connects to WebSocket at ws://localhost:5000/ws
 * Returns { events, connected } where events is array (newest first, max 100)
 * Auto-reconnects after 2s on disconnect
 */
export function useWebSocket() {
  const [events, setEvents] = useState([])
  const [connected, setConnected] = useState(false)
  const ws = useRef(null)
  const reconnectTimeout = useRef(null)
  const shouldReconnect = useRef(true)
  
  useEffect(() => {
    shouldReconnect.current = true

    const connect = () => {
      if (!shouldReconnect.current) {
        return
      }

      try {
        const wsUrl = 'ws://localhost:5000/ws'
        console.log(`[WebSocket] Attempting to connect to ${wsUrl}`)

        const socket = new WebSocket(wsUrl)
        ws.current = socket

        socket.onopen = () => {
          // Ignore stale sockets (React StrictMode may briefly create/cleanup one).
          if (socket !== ws.current || !shouldReconnect.current) {
            socket.close(1000, 'stale-socket')
            return
          }

          console.log('[WebSocket] Connected successfully')
          setConnected(true)
          if (reconnectTimeout.current) {
            clearTimeout(reconnectTimeout.current)
            reconnectTimeout.current = null
          }
        }

        socket.onmessage = (event) => {
          if (socket !== ws.current || !shouldReconnect.current) {
            return
          }

          try {
            const data = JSON.parse(event.data)
            setEvents(prev => {
              const updated = [data, ...prev]
              return updated.length > 100 ? updated.slice(0, 100) : updated
            })
          } catch (err) {
            // Ignore parse errors
          }
        }

        socket.onclose = (event) => {
          const isIntentional = !shouldReconnect.current || socket !== ws.current
          if (isIntentional) {
            return
          }

          console.log(`[WebSocket] Closed. Code: ${event.code}, Reason: ${event.reason}`)
          setConnected(false)
          reconnectTimeout.current = setTimeout(connect, 2000)
        }

        socket.onerror = (error) => {
          const isIntentional = !shouldReconnect.current || socket !== ws.current
          if (isIntentional) {
            return
          }

          console.error('[WebSocket] Error:', error)
          setConnected(false)
        }
      } catch (err) {
        if (!shouldReconnect.current) {
          return
        }

        console.error('[WebSocket] Connection error:', err)
        setConnected(false)
        reconnectTimeout.current = setTimeout(connect, 2000)
      }
    }
    
    connect()
    
    return () => {
      shouldReconnect.current = false

      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current)
        reconnectTimeout.current = null
      }

      const socket = ws.current
      ws.current = null

      if (socket && socket.readyState === WebSocket.OPEN) {
        socket.close(1000, 'cleanup')
      }
    }
  }, [])
  
  return { events, connected }
}
