import { useState } from 'react'
import { Loader2 } from 'lucide-react'
import {
  proxyPut,
  proxyGet,
  proxyDelete,
  proxySync,
} from '../api'

/**
 * OperationPanel Component
 * 
 * Props: { nodes, onResult, onOperationComplete }
 * 
 * Provides a form to execute operations on nodes (PUT, GET, DELETE, SYNC).
 * Calls onOperationComplete after operations finish for cascading refreshes.
 */
export default function OperationPanel({ nodes = [], onResult, onOperationComplete }) {
  const [selectedNode, setSelectedNode] = useState('')
  const [operation, setOperation] = useState('GET')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [peerNode, setPeerNode] = useState('')
  const [loading, setLoading] = useState(false)
  const [lastExecution, setLastExecution] = useState(null)
  
  const onlineNodes = nodes.filter(n => n.status === 'online')
  const otherNodes = onlineNodes.filter(n => n.node_id !== selectedNode)
  
  const handleExecute = async (e) => {
    e.preventDefault()

    const trimmedKey = key.trim()

    if (!selectedNode) return
    if (operation !== 'SYNC' && !trimmedKey) return
    
    setLoading(true)
    let result
    
    try {
      switch (operation) {
        case 'PUT': {
          let parsedValue = value.trim()
          if (!parsedValue) parsedValue = '{}'
          try {
            parsedValue = JSON.parse(parsedValue)
          } catch {
            // Keep as string if not valid JSON
          }
          result = await proxyPut(selectedNode, trimmedKey, parsedValue)
          break
        }
        case 'GET':
          result = await proxyGet(selectedNode, trimmedKey)
          break
        case 'DELETE':
          result = await proxyDelete(selectedNode, trimmedKey)
          break
        case 'SYNC':
          if (!peerNode) {
            onResult({ data: null, error: 'Select peer node' })
            setLoading(false)
            return
          }
          result = await proxySync(selectedNode, peerNode, trimmedKey || null)
          break
        default:
          result = { data: null, error: 'Unknown operation' }
      }
    } catch (err) {
      result = { data: null, error: err.message }
    } finally {
      setLoading(false)
    }
    
    setLastExecution({
      operation,
      node: selectedNode,
      key: trimmedKey || null,
      peerNode: operation === 'SYNC' ? peerNode : null,
      result,
      at: new Date().toISOString(),
    })

    onResult?.(result)
    onOperationComplete?.()  // Trigger cascading refreshes
  }

  const renderResultBody = () => {
    if (!lastExecution) {
      return null
    }

    const { result } = lastExecution

    if (result?.error) {
      return (
        <div className="text-rose-300 text-xs font-mono break-words">
          {result.error}
        </div>
      )
    }

    if (result?.data == null) {
      return (
        <div className="text-emerald-300 text-xs font-mono">
          Success (no response body)
        </div>
      )
    }

    return (
      <pre className="mt-2 max-h-48 overflow-auto rounded bg-zinc-950 p-2 text-[11px] leading-5 text-zinc-200">
        {JSON.stringify(result.data, null, 2)}
      </pre>
    )
  }
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header label */}
      <div className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
        operation
      </div>
      
      {/* Form */}
      <form className="space-y-4" onSubmit={handleExecute}>
        
        {/* Node selector */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1 font-mono">
            Node
          </label>
          <select
            className="w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm text-zinc-100 font-mono focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
            value={selectedNode}
            onChange={(e) => setSelectedNode(e.target.value)}
          >
            <option value="">Select node...</option>
            {onlineNodes.map(node => (
              <option key={node.node_id} value={node.node_id}>
                Node {node.node_id} (:{node.port})
              </option>
            ))}
          </select>
        </div>
        
        {/* Operation selector */}
        <div>
          <label className="block text-xs text-zinc-400 mb-2 font-mono">
            Operation
          </label>
          <div className="flex gap-2">
            {['PUT', 'GET', 'DELETE', 'SYNC'].map(op => (
              <button
                key={op}
                type="button"
                className={`flex-1 px-3 py-2 rounded text-xs font-mono transition-colors ${
                  operation === op
                    ? 'bg-indigo-600 text-white'
                    : 'bg-zinc-800 border border-zinc-700 text-zinc-400 hover:border-zinc-500'
                }`}
                onClick={() => setOperation(op)}
              >
                {op}
              </button>
            ))}
          </div>
        </div>
        
        {/* Key input */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1 font-mono">
            Key {operation === 'SYNC' ? '(optional)' : ''}
          </label>
          <input
            type="text"
            placeholder={operation === 'SYNC' ? 'Leave blank to sync all keys' : 'user:1'}
            className="w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm font-mono text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
            value={key}
            onChange={(e) => setKey(e.target.value)}
          />
        </div>
        
        {/* Value input (PUT only) */}
        {operation === 'PUT' && (
          <div>
            <label className="block text-xs text-zinc-400 mb-1 font-mono">
              Value (JSON)
            </label>
            <textarea
              placeholder='{"name":"Alice"}'
              className="w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm font-mono text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors resize-none"
              rows="3"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          </div>
        )}
        
        {/* Peer node selector (SYNC only) */}
        {operation === 'SYNC' && (
          <div>
            <label className="block text-xs text-zinc-400 mb-1 font-mono">
              Peer Node
            </label>
            <select
              className="w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-sm text-zinc-100 font-mono focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
              value={peerNode}
              onChange={(e) => setPeerNode(e.target.value)}
            >
              <option value="">Select peer...</option>
              {otherNodes.map(node => (
                <option key={node.node_id} value={node.node_id}>
                  Node {node.node_id} (:{node.port})
                </option>
              ))}
            </select>
          </div>
        )}
        
        {/* Fire button */}
        <button
          type="submit"
          disabled={loading}
          className="w-full bg-indigo-600 hover:bg-indigo-500 disabled:bg-indigo-600/50 disabled:cursor-not-allowed text-white rounded-lg px-4 py-2 transition-colors flex items-center justify-center gap-2 font-mono text-sm"
        >
          {loading && <Loader2 className="w-4 h-4 animate-spin" />}
          {loading ? 'Executing...' : 'Fire'}
        </button>

        {/* Latest operation result */}
        {lastExecution && (
          <div className={`rounded-lg border p-3 ${
            lastExecution.result?.error
              ? 'border-rose-500/40 bg-rose-500/10'
              : 'border-emerald-500/30 bg-emerald-500/10'
          }`}>
            <div className="text-[11px] font-mono text-zinc-300">
              {lastExecution.operation} on Node {lastExecution.node}
              {lastExecution.key ? ` key=${lastExecution.key}` : ''}
              {lastExecution.peerNode ? ` peer=${lastExecution.peerNode}` : ''}
            </div>
            {renderResultBody()}
          </div>
        )}
        
      </form>
    </div>
  )
}
