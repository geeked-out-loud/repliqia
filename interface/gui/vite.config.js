import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/ws': { target: 'ws://localhost:5000', ws: true, changeOrigin: true },
      '/nodes': 'http://localhost:5000',
      '/proxy': 'http://localhost:5000',
      '/demo': 'http://localhost:5000',
    }
  }
})
