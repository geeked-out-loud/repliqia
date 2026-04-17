import { Server, ServerOff, PowerOff, RotateCcw } from 'lucide-react'

/**
 * NodeCard Component
 * 
 * Props: { node, onStop, onRestart, onSelect, selected }
 * 
 * Displays a single node with status, port, and controls.
 * Node shape: { node_id, port, status: "online"|"offline" }
 */
export default function NodeCard({
  node,
  onStop,
  onRestart,
  onSelect,
  selected = false,
}) {
  if (!node) return null
  
  const isOnline = node.status === 'online'
  const containerClasses = `
    rounded-xl border bg-zinc-800 p-4 cursor-pointer 
    transition-all duration-200
    ${selected 
      ? 'border-indigo-500 shadow-lg shadow-indigo-500/10' 
      : isOnline
        ? 'border-zinc-700 hover:border-zinc-500'
        : 'border-zinc-800 opacity-60'
    }
  `
  
  return (
    <div className={containerClasses} onClick={() => onSelect?.(node.node_id)}>
      
      {/* Top row: icon, node_id, port */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-start gap-2">
          {isOnline ? (
            <Server className="w-4 h-4 text-emerald-400 mt-0.5" />
          ) : (
            <ServerOff className="w-4 h-4 text-zinc-600 mt-0.5" />
          )}
          <div>
            <div className="font-mono font-bold text-zinc-100 text-sm">
              {node.node_id}
            </div>
            <div className="text-zinc-500 text-xs">
              :{node.port}
            </div>
          </div>
        </div>
        
        {/* Status dot */}
        {isOnline && (
          <div className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
        )}
      </div>
      
      {/* Bottom row: action buttons */}
      <div className="flex gap-2">
        {isOnline ? (
          <button
            className="flex-1 flex items-center justify-center gap-1 px-2 py-1.5 rounded text-xs font-mono bg-transparent hover:bg-rose-500/10 text-rose-400 hover:text-rose-300 transition-colors"
            onClick={(e) => {
              e.stopPropagation()
              onStop?.(node.node_id)
            }}
            title="Stop node"
          >
            <PowerOff className="w-3 h-3" />
            Stop
          </button>
        ) : (
          <button
            className="flex-1 flex items-center justify-center gap-1 px-2 py-1.5 rounded text-xs font-mono bg-transparent hover:bg-emerald-500/10 text-emerald-400 hover:text-emerald-300 transition-colors"
            onClick={(e) => {
              e.stopPropagation()
              onRestart?.(node.node_id)
            }}
            title="Restart node"
          >
            <RotateCcw className="w-3 h-3" />
            Start
          </button>
        )}
      </div>
      
    </div>
  )
}
