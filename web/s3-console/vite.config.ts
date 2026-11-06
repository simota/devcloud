import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const consoleOrigin = process.env.DEVCLOUD_S3_CONSOLE_ORIGIN ?? 'http://127.0.0.1:9001'

export default defineConfig({
  base: '/',
  build: {
    emptyOutDir: true,
    outDir: '../../services/s3/assets/console',
  },
  plugins: [react()],
  server: {
    proxy: {
      '/api': consoleOrigin,
    },
  },
})
