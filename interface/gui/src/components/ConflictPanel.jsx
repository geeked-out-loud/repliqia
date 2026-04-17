import { useEffect, useState } from 'react'
import { GitMerge } from 'lucide-react'
import { useConflictsPoll } from '../hooks/useConflictsPoll'

/**
 * ConflictPanel Component
 * 
 * Props: { nodes, events }
 * 
 * Displays all conflicting keys and their sibling values.
 * Auto-polls conflicts every 5s to detect updates in real-time.
 * Triggers immediate refresh when SYNC operations complete.
 */
export default function ConflictPanel({ nodes = [], events = [] }) {
  const isOperationEvent = (event) => {
    return (
      event?.type === 'operation' ||
      event?.type === 'operation_completed' ||
      event?.event_type === 'operation_completed'
    )
  }

  const onlineNodes = nodes.filter(n => n.status === 'online')
  const [selectedNode, setSelectedNode] = useState(onlineNodes[0]?.node_id || null)
  
  // Auto-poll conflicts every 5s
  const { conflicts, loading, refresh } = useConflictsPoll(selectedNode)
  
  // Trigger refresh when SYNC operations complete (conflict detection)
  useEffect(() => {
    const latestEvent = events[0]
    if (isOperationEvent(latestEvent) && latestEvent?.method === 'SYNC') {
      // Refresh on SYNC completion (success or failure)
      refresh()
    }
  }, [events, refresh])
  
  useEffect(() => {
    // Update selectedNode if current one goes offline
    if (onlineNodes.length > 0 && !selectedNode) {
      setSelectedNode(onlineNodes[0].node_id)
    } else if (selectedNode && !onlineNodes.find(n => n.node_id === selectedNode)) {
      setSelectedNode(onlineNodes[0]?.node_id || null)
    }
  }, [onlineNodes, selectedNode])
  
  const conflictEntries = Array.isArray(conflicts) ? conflicts : []
  const hasConflicts = conflictEntries.length > 0
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header label with icon */}
      <div className="flex items-center gap-2 mb-3">
        <GitMerge className="w-4 h-4 text-zinc-500" />
        <div className="text-xs text-zinc-500 uppercase tracking-widest">
          conflicts
        </div>
      </div>
      
      {/* Node selector */}
      {onlineNodes.length > 0 && (
        <div className="mb-3">
          <select
            value={selectedNode || ''}
            onChange={(e) => setSelectedNode(e.target.value)}
            className="w-full bg-zinc-800 border border-zinc-700 rounded px-2 py-1.5 text-xs font-mono text-zinc-100 focus:outline-none focus:border-indigo-500"
          >
            {onlineNodes.map(node => (
              <option key={node.node_id} value={node.node_id}>
                Node {node.node_id} (Port {node.port})
              </option>
            ))}
          </select>
        </div>
      )}
      
      {/* Content */}
      {loading ? (
        <div className="text-center text-zinc-500 text-xs py-4">
          Loading...
        </div>
      ) : !hasConflicts ? (
        <div className="text-center py-6">
          <div className="text-zinc-500 text-sm">
            ✓ No conflicts
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {conflictEntries.map((conflict) => {
            const key = conflict?.key || 'unknown-key'
            const siblings = Array.isArray(conflict?.versions) ? conflict.versions : []
            return (
            <div key={key}>
              {/* Key name */}
              <div className="font-mono text-zinc-300 text-sm mb-2 flex items-center justify-between gap-2">
                <span>{key}</span>
                <span className="text-[10px] text-zinc-500 uppercase tracking-wider">
                  {siblings.length} sibling{siblings.length === 1 ? '' : 's'}
                </span>
              </div>
              
              {/* Siblings */}
              <div className="flex gap-2 overflow-x-auto pb-2">
                {(siblings || []).map((sibling, idx) => (
                  <div
                    key={idx}
                    className="flex-shrink-0 bg-zinc-800 rounded-lg p-3 border border-rose-500/30 w-48"
                  >
                    {/* Value */}
                    <div className="font-mono text-zinc-100 text-sm break-all mb-1">
                      {typeof sibling.value === 'string'
                        ? sibling.value
                        : JSON.stringify(sibling.value)
                      }
                    </div>
                    
                    {/* Clock */}
                    <div className="font-mono text-rose-400 text-xs">
                      {typeof sibling.clock === 'object'
                        ? JSON.stringify(sibling.clock)
                        : String(sibling.clock)
                      }
                    </div>
                    
                    {/* Author (if available) */}
                    {sibling.author && (
                      <div className="text-zinc-500 text-xs mt-1">
                        by {sibling.author}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )})}
        </div>
      )}
    </div>
  )
}
