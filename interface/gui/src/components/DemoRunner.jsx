import { useState, useEffect, useRef } from 'react'
import {
  ChevronLeft,
  ChevronRight,
  Play,
  Pause,
  FastForward,
  RotateCcw,
} from 'lucide-react'
import { scenarios, scenarioNames } from '../scenarios'
import { resetDemo } from '../api'

/**
 * DemoRunner Component
 * 
 * Props: { onEventsGenerated }
 * 
 * Manages demo scenario playback with step-by-step execution.
 */
export default function DemoRunner({ onEventsGenerated }) {
  const [activeScenario, setActiveScenario] = useState(null)
  const [currentStep, setCurrentStep] = useState(0)
  const [isAutoPlaying, setIsAutoPlaying] = useState(false)
  const [speed, setSpeed] = useState(1) // 1x or 2x
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null) // New: error state
  const autoPlayTimer = useRef(null)
  
  const scenario = activeScenario ? scenarios[activeScenario] : null
  const totalSteps = scenario ? scenario.length : 0
  const isComplete = scenario ? currentStep >= totalSteps : false
  const step = !isComplete ? scenario?.[currentStep] : null
  const progressPct = totalSteps > 0 ? Math.min((currentStep / totalSteps) * 100, 100) : 0
  
  // Auto-play effect
  useEffect(() => {
    if (!isAutoPlaying || !scenario || loading) {
      return
    }

    if (currentStep >= totalSteps) {
      setIsAutoPlaying(false)
      return
    }
    
    const delay = speed === 2 ? 1000 : 2000
    autoPlayTimer.current = setTimeout(() => {
      advanceStep()
    }, delay)
    
    return () => {
      if (autoPlayTimer.current) {
        clearTimeout(autoPlayTimer.current)
      }
    }
  }, [isAutoPlaying, currentStep, scenario, speed, totalSteps, loading])
  
  const selectScenario = (name) => {
    setActiveScenario(name)
    setCurrentStep(0)
    setIsAutoPlaying(false)
    setError(null)  // Clear error
    setSpeed(1)
  }
  
  const advanceStep = async () => {
    if (!scenario || loading || currentStep >= totalSteps) {
      setIsAutoPlaying(false)
      return
    }

    const activeStepData = scenario[currentStep]
    
    setLoading(true)
    setError(null)  // Clear previous errors

    try {
      let result = null

      if (activeStepData?.action) {
        result = await activeStepData.action()
      }

      if (result?.error) {
        throw new Error(result.error)
      }

      if (onEventsGenerated && result?.data) {
        onEventsGenerated(result.data)
      }

      const nextIndex = currentStep + 1
      setCurrentStep(nextIndex)

      if (nextIndex >= totalSteps) {
        setIsAutoPlaying(false)
      }
    } catch (err) {
      // Capture and display error
      setError({
        message: err.message || String(err),
        step: currentStep + 1,
        stepName: activeStepData?.narration || 'Unknown',
      })
      setIsAutoPlaying(false)  // Stop auto-play on error
    } finally {
      setLoading(false)
    }
  }
  
  const goBack = () => {
    setIsAutoPlaying(false)
    setCurrentStep(prev => Math.max(0, prev - 1))
  }
  
  const executeStep = async () => {
    await advanceStep()
  }
  
  const handleReset = async () => {
    setLoading(true)
    setError(null)  // Clear error on reset
    try {
      await resetDemo()
      setActiveScenario(null)
      setCurrentStep(0)
      setIsAutoPlaying(false)
    } catch (err) {
      // Show error but allow recovery
      setError({
        message: 'Failed to reset demo: ' + (err.message || String(err)),
        step: -1,
        stepName: 'Reset',
      })
    } finally {
      setLoading(false)
    }
  }
  
  return (
    <div className="rounded-xl border border-zinc-700 bg-zinc-900 p-4">
      
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="text-xs text-zinc-500 uppercase tracking-widest">
          demo runner
        </div>
        {activeScenario && (
          <button
            className="text-xs text-zinc-600 hover:text-zinc-400 transition-colors"
            onClick={handleReset}
            disabled={loading}
            title="Reset all nodes and data"
          >
            <RotateCcw className="w-3 h-3" />
          </button>
        )}
      </div>
      
      {/* Scenario selector buttons */}
      <div className="flex gap-2 mb-4">
        {Object.keys(scenarios).map(key => (
          <button
            key={key}
            className={`flex-1 px-3 py-2 rounded text-xs font-mono transition-colors ${
              activeScenario === key
                ? 'border-indigo-500 text-indigo-400 bg-indigo-500/10 border'
                : 'border border-zinc-700 text-zinc-400 hover:border-zinc-500'
            }`}
            onClick={() => selectScenario(key)}
          >
            {scenarioNames[key]}
          </button>
        ))}
      </div>
      
      {/* Scenario content */}
      {!activeScenario ? (
        <div className="text-center text-zinc-500 text-xs py-4">
          Select a scenario to begin
        </div>
      ) : (
        <>
          {/* Error banner */}
          {error && (
            <div className="mb-4 p-3 bg-rose-500/10 border border-rose-500/30 rounded-lg">
              <div className="text-rose-400 text-xs font-mono font-bold mb-1">
                ✗ Error at Step {error.step}
              </div>
              <div className="text-rose-300 text-xs mb-2">
                {error.stepName}
              </div>
              <div className="text-rose-200 text-xs font-mono">
                {error.message}
              </div>
              <button
                onClick={() => setError(null)}
                className="text-xs text-rose-400 hover:text-rose-300 mt-2 underline"
              >
                Dismiss
              </button>
            </div>
          )}
          
          {/* Progress bar */}
          <div className="mb-3">
            <div className="w-full h-1 bg-zinc-800 rounded overflow-hidden">
              <div
                className="h-full bg-indigo-500 transition-all duration-300"
                style={{
                  width: `${progressPct}%`,
                }}
              />
            </div>
            <div className="text-xs text-zinc-500 mt-1 text-right font-mono">
              {isComplete
                ? `Completed ${totalSteps} of ${totalSteps}`
                : `Step ${currentStep + 1} of ${totalSteps}`}
            </div>
          </div>
          
          {/* Narration box */}
          <div className="bg-zinc-800 rounded-lg p-4 font-sans text-zinc-300 text-sm leading-relaxed border border-zinc-700 min-h-16 mb-4">
            {isComplete
              ? 'Scenario complete. Choose another scenario or reset to run again.'
              : (step?.narration || '...')}
          </div>
          
          {/* Controls row */}
          <div className="flex gap-2 mb-4">
            {/* Back button */}
            <button
              className="p-2 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-400 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              onClick={goBack}
              disabled={currentStep === 0 || loading}
              title="Previous step"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            
            {/* Play/Pause */}
            <button
              className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 rounded text-xs font-mono transition-colors ${
                isAutoPlaying
                  ? 'bg-amber-600 hover:bg-amber-500 text-white'
                  : 'bg-zinc-800 hover:bg-zinc-700 text-zinc-400'
              }`}
              onClick={() => setIsAutoPlaying(!isAutoPlaying)}
              disabled={loading || isComplete}
              title={isAutoPlaying ? 'Pause' : 'Auto-play'}
            >
              {isAutoPlaying ? (
                <>
                  <Pause className="w-3 h-3" />
                  Pause
                </>
              ) : (
                <>
                  <Play className="w-3 h-3" />
                  Play
                </>
              )}
            </button>
            
            {/* Next button */}
            <button
              className="p-2 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-400 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              onClick={advanceStep}
              disabled={currentStep >= totalSteps || loading}
              title="Next step"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
            
            {/* 2x speed button */}
            <button
              className={`p-2 rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${
                speed === 2
                  ? 'bg-indigo-600 hover:bg-indigo-500 text-white'
                  : 'bg-zinc-800 hover:bg-zinc-700 text-zinc-400'
              }`}
              onClick={() => setSpeed(speed === 1 ? 2 : 1)}
              disabled={!isAutoPlaying || loading}
              title="2x speed"
            >
              <FastForward className="w-4 h-4" />
            </button>
          </div>
          
          {/* Step execution button (optional) */}
          <button
            className="w-full px-3 py-2 rounded text-xs font-mono bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            onClick={executeStep}
            disabled={loading || !step?.action || isComplete}
            title="Execute current step"
          >
            {loading
              ? 'Executing...'
              : (isComplete ? 'Scenario Complete' : 'Execute Step')}
          </button>
        </>
      )}
    </div>
  )
}
