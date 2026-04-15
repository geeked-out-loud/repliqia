import { useState, useEffect } from 'react'
import { useClockPoll } from '../hooks/useClockPoll'
import { proxyGet } from '../api'

/**
 * VectorClockViz Component
 * 
 * Props: { nodes, selectedKey }
 * 
 * Shows two views:
 * 1. Node clocks (auto-updates every 5s)
 * 2. Key-specific causality (fetches clock for inspected key)
 * 
 * Displays vector clock values to show causality and concurrency.
 */
export default function VectorClockViz({ nodes = [], selectedKey = '', events = [] }) {
  const isOperationEvent = (event) => {
    return (
      event?.type === 'operation' ||
      event?.type === 'operation_completed' ||
      event?.event_type === 'operation_completed'
    )
  }

  const [key, setKey] = useState(selectedKey)
  const [keyClocks, setKeyClocks] = useState({})
  const [loading, setLoading] = useState(false)
  const [selectedNode, setSelectedNode] = useState(nodes[0]?.node_id || null)
  
  const onlineNodes = nodes.filter(n => n.status === 'online')
  
  // Use auto-polling for selected node's clock
  useEffect(() => {
    if (onlineNodes.length > 0 && !selectedNode) {
      setSelectedNode(onlineNodes[0].node_id)
    }
  }, [onlineNodes, selectedNode])
  
  const { clock: nodeClockData, loading: clockLoading, refresh: refreshClock } = useClockPoll(selectedNode)
  
  // Trigger refresh when PUT/DELETE operations complete on the selected node
  useEffect(() => {
    const latestEvent = events[0]
    if (isOperationEvent(latestEvent) && latestEvent?.node_id === selectedNode) {
      if (latestEvent?.method === 'PUT' || latestEvent?.method === 'DELETE') {
        refreshClock()
      }
    }
  }, [events, selectedNode, refreshClock])
  
  // Parse node clock from response
  const nodeClock = nodeClockData?.vector_clock || nodeClockData || {}
  
  // Fetch clocks for a specific key whenever key changes
  useEffect(() => {
    if (!key.trim() || onlineNodes.length === 0) {
      setKeyClocks({})
      return
    }
    
    const fetchClocks = async () => {
      setLoading(true)
      const newClocks = {}
      
      for (const node of onlineNodes) {
        try {
          const response = await fetch(`/proxy/${node.node_id}/kvstore/${key.trim()}`, {
            cache: 'no-store',
          })
          if (response.ok) {
            const data = await response.json()
            newClocks[node.node_id] = data.clock || {}
          }
        } catch (err) {
          newClocks[node.node_id] = {}
        }
      }
      
      setKeyClocks(newClocks)
      setLoading(false)
    }
    
    fetchClocks()
  }, [key, onlineNodes.length])
  
  // Collect all node IDs that appear in any clock (for key-based view)
  const allClockNodes = new Set()
  Object.values(keyClocks).forEach(clock => {
    Object.keys(clock || {}).forEach(nodeId => allClockNodes.add(nodeId))
  })
  const clockNodeIds = Array.from(allClockNodes).sort()
  
  // Detect concurrent updates in key clocks
  let hasConcurrency = false
  if (clockNodeIds.length > 0) {
    for (const nodeId of clockNodeIds) {
      const values = new Set()
      for (const nodeClock of Object.values(keyClocks)) {
        values.add(nodeClock?.[nodeId] ?? 0)
      }
      if (values.size > 1) {
        hasConcurrency = true
        break
      }
    }
  }
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header label */}
      <div className="text-xs text-zinc-500 uppercase tracking-widest mb-3">
        vector clocks
      </div>
      
      {/* Node selector for live clock view */}
      <div className="mb-4 p-3 bg-zinc-800/30 rounded border border-zinc-700">
        <div className="text-xs text-zinc-400 mb-2">Live Node Clock (updates every 5s)</div>
        <div className="flex items-center gap-2 mb-2">
          <select
            value={selectedNode || ''}
            onChange={(e) => setSelectedNode(e.target.value)}
            className="flex-1 bg-zinc-800 border border-zinc-700 rounded px-2 py-1.5 text-sm font-mono text-zinc-100 focus:outline-none focus:border-indigo-500"
          >
            {onlineNodes.map(node => (
              <option key={node.node_id} value={node.node_id}>
                Node {node.node_id} (Port {node.port})
              </option>
            ))}
          </select>
        </div>
        {clockLoading ? (
          <div className="text-xs text-zinc-500">Loading...</div>
        ) : (
          <div className="font-mono text-xs text-zinc-300 p-2 bg-zinc-900 rounded">
            {JSON.stringify(nodeClock, null, 2).split('\n').slice(0, 4).join('\n')}...
          </div>
        )}
      </div>
      
      {/* Key inspector for causality */}
      <div className="border-t border-zinc-700 pt-4">
        <div className="text-xs text-zinc-400 mb-2">Inspect Key Causality</div>
      <div className="mb-4">
        <input
          type="text"
          placeholder="Enter key to inspect..."
          className="w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm font-mono text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
      </div>
      
      {/* Table or empty state */}
      {!key.trim() ? (
        <div className="text-center text-zinc-500 text-xs py-4">
          Enter a key to inspect vector clocks
        </div>
      ) : loading ? (
        <div className="text-center text-zinc-500 text-xs py-4">
          Loading...
        </div>
      ) : clockNodeIds.length === 0 ? (
        <div className="text-center text-zinc-500 text-xs py-4">
          No data found for this key
        </div>
      ) : (
        <>
          {/* Table */}
          <div className="overflow-x-auto mb-3">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr>
                  <td className="border border-zinc-700 bg-zinc-800 px-2 py-1.5 font-mono text-zinc-400">
                    Node
                  </td>
                  {clockNodeIds.map(nodeId => (
                    <td
                      key={nodeId}
                      className="border border-zinc-700 bg-zinc-800 px-2 py-1.5 text-center font-mono text-zinc-400"
                    >
                      {nodeId}
                    </td>
                  ))}
                </tr>
              </thead>
              <tbody>
                {onlineNodes.map(node => (
                  <tr key={node.node_id}>
                    <td className="border border-zinc-700 bg-zinc-800/50 px-2 py-1.5 font-mono text-zinc-300 font-bold">
                      {node.node_id}
                    </td>
                    {clockNodeIds.map(clockNodeId => {
                      const value = keyClocks[node.node_id]?.[clockNodeId] ?? 0
                      
                      // Check if this clock value differs from others
                      const otherValues = onlineNodes
                        .filter(n => n.node_id !== node.node_id)
                        .map(n => keyClocks[n.node_id]?.[clockNodeId] ?? 0)
                      const isDifferent = otherValues.some(v => v !== value)
                      
                      return (
                        <td
                          key={`${node.node_id}-${clockNodeId}`}
                          className={`border border-zinc-700 bg-zinc-800/50 px-2 py-1.5 text-center font-mono ${
                            isDifferent
                              ? 'text-amber-400 font-bold'
                              : 'text-zinc-300'
                          }`}
                        >
                          {value}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {/* Concurrency indicator */}
          <div className="pt-2 border-t border-zinc-700">
            <div className={`text-xs font-mono flex items-center gap-1 ${
              hasConcurrency
                ? 'text-amber-400'
                : 'text-emerald-400'
            }`}>
              {hasConcurrency ? '⚠' : '✓'}
              {hasConcurrency
                ? ' concurrent updates detected'
                : ' causal ordering intact'
              }
            </div>
          </div>
        </>
      )}
      </div>
    </div>
  )
}
