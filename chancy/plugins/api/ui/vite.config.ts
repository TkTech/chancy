import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { visualizer } from "rollup-plugin-visualizer";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), visualizer()],
  build: {
    outDir: '../dist',
    // Since our dist directory is outside the vite root,
    // it doesn't empty by default.
    emptyOutDir: true,
  }
})
