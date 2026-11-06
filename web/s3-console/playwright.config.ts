import { defineConfig } from '@playwright/test'

// Black-box suite against the real Docker image; see e2e/harness.ts.
export default defineConfig({
  testDir: 'e2e',
  outputDir: 'dist/e2e/pw',
  workers: 1,
  retries: 0,
  timeout: 600_000,
})
