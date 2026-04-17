import { useState } from 'react'
import { Plus } from 'lucide-react'
import NodeCard from './NodeCard'

/**
 * NodeGrid Component
 * 
 * Props: { nodes, selectedNode, onSelect, onStop, onRestart, onAddNode }
 * 
 * Displays a grid of NodeCards and provides an "add node" button.
 */
export default function NodeGrid({
  nodes = [],
  selectedNode = null,
  onSelect,
  onStop,
  onRestart,
  onAddNode,
}) {
  const [showAddForm, setShowAddForm] = useState(false)
  const [newNodeId, setNewNodeId] = useState('')
  
  const handleAddSubmit = (e) => {
    e.preventDefault()
    const id = newNodeId.trim().toUpperCase()
    
    if (id && id.length === 1 && /^[A-Z]$/.test(id)) {
      onAddNode?.(id)
      setNewNodeId('')
      setShowAddForm(false)
    }
  }
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header label */}
      <div className="text-xs text-zinc-500 uppercase tracking-widest mb-3">
        nodes
      </div>
      
      {/* Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
        
        {/* Node cards */}
        {nodes.map(node => (
          <NodeCard
            key={node.node_id}
            node={node}
            selected={selectedNode === node.node_id}
            onSelect={onSelect}
            onStop={onStop}
            onRestart={onRestart}
          />
        ))}
        
        {/* Add node button */}
        {!showAddForm && (
          <button
            className="rounded-xl border-2 border-dashed border-zinc-700 bg-zinc-800/50 p-4 cursor-pointer transition-all duration-200 hover:border-zinc-500 hover:bg-zinc-800 flex items-center justify-center"
            onClick={() => setShowAddForm(true)}
            title="Add new node"
          >
            <Plus className="w-5 h-5 text-zinc-600 hover:text-zinc-400" />
          </button>
        )}
        
        {/* Add form */}
        {showAddForm && (
          <form
            className="rounded-xl border border-zinc-700 bg-zinc-800 p-4 flex flex-col gap-2"
            onSubmit={handleAddSubmit}
          >
            <input
              type="text"
              placeholder="A"
              maxLength="1"
              className="font-mono text-sm bg-zinc-700 border border-zinc-600 rounded px-2 py-1 text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              value={newNodeId}
              onChange={(e) => setNewNodeId(e.target.value.toUpperCase())}
              autoFocus
            />
            <div className="flex gap-2">
              <button
                type="submit"
                className="flex-1 px-2 py-1 rounded text-xs font-mono bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
              >
                Start
              </button>
              <button
                type="button"
                className="flex-1 px-2 py-1 rounded text-xs font-mono bg-zinc-700 hover:bg-zinc-600 text-zinc-300 transition-colors"
                onClick={() => {
                  setShowAddForm(false)
                  setNewNodeId('')
                }}
              >
                Cancel
              </button>
            </div>
          </form>
        )}
        
      </div>
    </div>
  )
}
