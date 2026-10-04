import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const mailboxOrigin = process.env.DEVCLOUD_MAILBOX_ORIGIN ?? 'http://127.0.0.1:8025'

export default defineConfig({
  base: '/',
  build: {
    emptyOutDir: true,
    outDir: '../../services/mailbox/assets/ui',
  },
  plugins: [react()],
  server: {
    proxy: {
      '/api': mailboxOrigin,
    },
  },
})
