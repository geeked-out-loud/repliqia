import React from 'react'

export class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null, errorInfo: null }
  }

  componentDidCatch(error, errorInfo) {
    console.error('ErrorBoundary caught:', error, errorInfo)
    this.setState({
      error: error,
      errorInfo: errorInfo
    })
  }

  render() {
    if (this.state.errorInfo) {
      return (
        <div className="min-h-screen bg-red-950 text-red-100 flex items-center justify-center p-4">
          <div className="max-w-2xl">
            <h1 className="text-2xl font-bold mb-4">React Error</h1>
            <div className="bg-red-900 border border-red-700 rounded p-4 mb-4">
              <p className="font-mono text-sm whitespace-pre-wrap">
                {this.state.error?.toString()}
              </p>
            </div>
            <div className="bg-red-900 border border-red-700 rounded p-4">
              <p className="font-mono text-xs whitespace-pre-wrap">
                {this.state.errorInfo.componentStack}
              </p>
            </div>
            <button 
              onClick={() => window.location.reload()}
              className="mt-4 px-4 py-2 bg-red-700 hover:bg-red-600 rounded"
            >
              Reload
            </button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
