import { createRoot } from 'react-dom/client'
import './main.css'
import App from './App'
import { ErrorBoundary } from './components/ErrorBoundary'

createRoot(document.getElementById('app')).render(
  <ErrorBoundary>
    <App />
  </ErrorBoundary>
)
