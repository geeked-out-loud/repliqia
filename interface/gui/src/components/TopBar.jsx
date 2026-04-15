/**
 * TopBar Component
 * 
 * Props: { nodeCount, onClearAll, clearingAll }
 * 
 * Displays project title, subtitle, and global clear-all action.
 */
export default function TopBar({ nodeCount = 0, onClearAll, clearingAll = false }) {
  return (
    <div className="bg-zinc-950 border-b border-zinc-800 px-6 py-3">
      <div className="flex items-center justify-between">
        
        {/* Left: Title */}
        <div>
          <div className="font-mono font-bold text-indigo-400 tracking-widest text-sm">
            REPLIQIA
          </div>
          <div className="text-zinc-600 text-xs">
            distributed kv store
          </div>
        </div>
        
        {/* Right: Status indicators */}
        <div className="flex items-center gap-6">
          
          {/* Node count badge */}
          <div className="flex items-center gap-2">
            <div className="text-zinc-400 text-xs uppercase tracking-widest">
              Nodes
            </div>
            <div className="bg-indigo-500/20 text-indigo-400 px-2.5 py-1 rounded-full text-xs font-mono font-bold">
              {nodeCount}
            </div>
          </div>
          
          <button
            type="button"
            onClick={onClearAll}
            disabled={clearingAll}
            className="px-3 py-1.5 rounded bg-rose-500/30 text-rose-300 hover:bg-rose-500/20 disabled:opacity-50 disabled:cursor-not-allowed text-xs font-mono transition-colors"
            title="Stop all nodes and wipe backend state"
          >
            {clearingAll ? 'Clearing...' : 'Clear'}
          </button>
        </div>
        
      </div>
    </div>
  )
}
