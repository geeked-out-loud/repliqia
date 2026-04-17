import { Activity, Server, ServerOff } from 'lucide-react'

/**
 * EventLog Component
 * 
 * Props: { events, connected }
 * 
 * Displays real-time stream of operations and node lifecycle events (newest first).
 */
export default function EventLog({ events = [], connected = false }) {
  const isOperationEvent = (event) => {
    return (
      event?.type === 'operation' ||
      event?.type === 'operation_completed' ||
      event?.event_type === 'operation_completed'
    )
  }

  const getMethodColor = (method) => {
    switch (method) {
      case 'PUT':
        return 'bg-emerald-500/20 text-emerald-400'
      case 'GET':
        return 'bg-zinc-600/20 text-zinc-400'
      case 'DELETE':
        return 'bg-rose-500/20 text-rose-400'
      case 'POST':
        return 'bg-amber-500/20 text-amber-400'
      case 'SYNC':
        return 'bg-indigo-500/20 text-indigo-400'
      default:
        return 'bg-zinc-600/20 text-zinc-400'
    }
  }
  
  const getStatusColor = (status) => {
    if (status === 200 || status === 201) return 'text-emerald-400'
    if (status === 503) return 'text-zinc-500'
    if (status >= 400) return 'text-rose-400'
    return 'text-zinc-400'
  }
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Activity className="w-4 h-4 text-zinc-500" />
          <div className="text-xs text-zinc-500 uppercase tracking-widest">
            event log
          </div>
        </div>
        
        {/* Connection status dot */}
        {connected && (
          <div className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
        )}
      </div>
      
      {/* Event list */}
      <div className="h-48 overflow-y-auto space-y-1">
        {events.length === 0 ? (
          <div className="text-center text-zinc-600 text-xs py-8">
            Waiting for events...
          </div>
        ) : (
          events.map((event, idx) => {
            
            // Node lifecycle events
            if (event.type === 'node_online') {
              return (
                <div
                  key={idx}
                  className="flex items-center gap-2 px-2 py-1.5 text-xs text-emerald-400 bg-emerald-500/5 rounded border border-emerald-500/10"
                >
                  <Server className="w-3 h-3 shrink-0" />
                  <span className="font-mono truncate">
                    Node {event.node_id} came online
                  </span>
                  <span className="text-zinc-600 ml-auto shrink-0">
                    {event.timestamp}
                  </span>
                </div>
              )
            }
            
            if (event.type === 'node_offline') {
              return (
                <div
                  key={idx}
                  className="flex items-center gap-2 px-2 py-1.5 text-xs text-zinc-500 bg-zinc-800/50 rounded border border-zinc-700"
                >
                  <ServerOff className="w-3 h-3 shrink-0" />
                  <span className="font-mono truncate">
                    Node {event.node_id} went offline
                  </span>
                  <span className="text-zinc-600 ml-auto shrink-0">
                    {event.timestamp}
                  </span>
                </div>
              )
            }
            
            // Operation events
            if (isOperationEvent(event)) {
              const isSuccess = event.status_code >= 200 && event.status_code < 300
              const bgClass = isSuccess ? 'bg-zinc-800/30' : 'bg-rose-500/10 border-rose-500/20'
              const borderClass = isSuccess ? 'border-zinc-700' : 'border-rose-500/20'
              
              return (
                <div
                  key={idx}
                  className={`flex items-center gap-2 px-2 py-1.5 text-xs rounded border ${bgClass} ${borderClass}`}
                >
                  {/* Timestamp */}
                  <span className="font-mono text-zinc-600 w-16 shrink-0">
                    {event.timestamp}
                  </span>
                  
                  {/* Node badge */}
                  <span className="font-mono text-xs px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 shrink-0">
                    {event.node_id}
                  </span>
                  
                  {/* Method badge */}
                  <span className={`font-mono text-xs px-1.5 py-0.5 rounded shrink-0 ${getMethodColor(event.method)}`}>
                    {event.method}
                  </span>
                  
                  {/* Path */}
                  <span className={`font-mono text-xs truncate ${isSuccess ? 'text-zinc-400' : 'text-rose-400'}`}>
                    {event.path}
                  </span>
                  
                  {/* Clock (if available) */}
                  {event.clock && (
                    <span className="text-zinc-500 text-xs shrink-0">
                      {JSON.stringify(event.clock).slice(0, 20)}...
                    </span>
                  )}
                  
                  {/* Status code */}
                  <span className={`font-mono text-xs font-bold shrink-0 ${getStatusColor(event.status_code)}`}>
                    {event.status_code}
                  </span>
                </div>
              )
            }
            
            // System events
            if (event.type === 'system') {
              return (
                <div
                  key={idx}
                  className="flex items-center gap-2 px-2 py-1.5 text-xs text-zinc-500"
                >
                  <span className="font-mono truncate">
                    {event.message}
                  </span>
                  <span className="text-zinc-600 ml-auto shrink-0">
                    {event.timestamp}
                  </span>
                </div>
              )
            }
            
            // Generic event
            return (
              <div
                key={idx}
                className="flex items-center gap-2 px-2 py-1.5 text-xs text-zinc-600"
              >
                <span className="font-mono truncate">
                  {event.type}
                </span>
                {event.timestamp && (
                  <span className="text-zinc-600 ml-auto shrink-0">
                    {event.timestamp}
                  </span>
                )}
              </div>
            )
          })
        )}
      </div>
    </div>
  )
}
