import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const dashboardOrigin = process.env.DEVCLOUD_DASHBOARD_ORIGIN ?? 'http://127.0.0.1:8025'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': dashboardOrigin,
    },
  },
})
