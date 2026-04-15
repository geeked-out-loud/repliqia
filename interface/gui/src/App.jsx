import { useState, useEffect } from 'react'
import { useWebSocket } from './hooks/useWebSocket'
import { useNodesLight } from './hooks/useNodesLight'
import {
  startNode,
  stopNode,
  restartNode,
  resetDemo,
} from './api'

import TopBar from './components/TopBar'
import NodeGrid from './components/NodeGrid'
import OperationPanel from './components/OperationPanel'
import VectorClockViz from './components/VectorClockViz'
import ConflictPanel from './components/ConflictPanel'
import EventLog from './components/EventLog'
import DemoRunner from './components/DemoRunner'

/**
 * App Component
 * 
 * Main application shell. Manages top-level state and layout.
 */
export default function App() {
  const [selectedNode, setSelectedNode] = useState(null)
  const [lastResult, setLastResult] = useState(null)
  const [selectedKey, setSelectedKey] = useState('')
  const [clearingAll, setClearingAll] = useState(false)
  
  const { events: wsEvents, connected } = useWebSocket()
  const { nodes, refresh: refreshNodes } = useNodesLight()
  
  // Events from WebSocket when connected, otherwise empty
  const events = connected ? wsEvents : []
  
  // Auto-select first online node if none selected
  useEffect(() => {
    const onlineNodes = (nodes || []).filter(n => n.status === 'online')
    if (onlineNodes.length > 0 && !selectedNode) {
      setSelectedNode(onlineNodes[0].node_id)
    } else if (selectedNode && !nodes?.find(n => n.node_id === selectedNode)) {
      setSelectedNode(null)
    }
  }, [nodes, selectedNode])

  // WebSocket events are newest-first; refresh nodes immediately on lifecycle updates.
  useEffect(() => {
    const latestEvent = events[0]
    if (latestEvent?.type === 'node_online' || latestEvent?.type === 'node_offline') {
      refreshNodes()
    }
  }, [events, refreshNodes])
  
  const handleAddNode = async (node_id) => {
    await startNode(node_id)
    await refreshNodes()
  }
  
  const handleStopNode = async (node_id) => {
    await stopNode(node_id)
    await refreshNodes()
  }
  
  const handleRestartNode = async (node_id) => {
    await restartNode(node_id)
    await refreshNodes()
  }
  
  const handleSelectNode = (node_id) => {
    setSelectedNode(node_id)
  }
  
  const handleOperationResult = (result) => {
    setLastResult(result)
  }

  const handleClearAll = async () => {
    if (clearingAll) {
      return
    }

    const confirmed = window.confirm(
      'This will stop all nodes and wipe backend data, vector clocks, and storage. Continue?'
    )

    if (!confirmed) {
      return
    }

    setClearingAll(true)

    try {
      const result = await resetDemo()

      if (result?.error) {
        setLastResult({ data: null, error: `Clear all failed: ${result.error}` })
      } else {
        setSelectedNode(null)
        setSelectedKey('')
        setLastResult({ data: result?.data ?? { reset: true }, error: null })
      }
    } catch (err) {
      setLastResult({
        data: null,
        error: `Clear all failed: ${err.message || String(err)}`,
      })
    } finally {
      await refreshNodes()
      setClearingAll(false)
    }
  }
  
  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 flex flex-col">
      
      {/* Top bar */}
      <TopBar
        nodeCount={nodes.length}
        onClearAll={handleClearAll}
        clearingAll={clearingAll}
      />
      
      {/* Main content */}
      <main className="flex-1 p-4 grid grid-cols-1 lg:grid-cols-3 gap-4 overflow-auto">
        
        {/* Left column */}
        <div className="lg:col-span-1 flex flex-col gap-4">
          
          {/* Node Grid */}
          <NodeGrid
            nodes={nodes}
            selectedNode={selectedNode}
            onSelect={handleSelectNode}
            onStop={handleStopNode}
            onRestart={handleRestartNode}
            onAddNode={handleAddNode}
          />
          
          {/* Operation Panel */}
          <OperationPanel
            nodes={nodes}
            onResult={handleOperationResult}
          />
          
        </div>
        
        {/* Right column */}
        <div className="lg:col-span-2 flex flex-col gap-4">
          
          {/* Vector Clock Viz */}
          <VectorClockViz
            nodes={nodes}
            selectedKey={selectedKey}
            events={events}
          />
          
          {/* Bottom section: Conflicts & Demo */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <ConflictPanel nodes={nodes} events={events} />
            <DemoRunner onEventsGenerated={handleOperationResult} />
          </div>
          
        </div>
        
      </main>
      
      {/* Event log strip */}
      <div className="border-t border-zinc-800 px-4 py-2 bg-zinc-950">
        <EventLog events={events} connected={connected} />
      </div>
      
    </div>
  )
}
