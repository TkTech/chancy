import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../dist',
    // Since our dist directory is outside the vite root,
    // it doesn't empty by default.
    emptyOutDir: true,
  }
})
