// Walks every use case in one continuous page recording, for the release demo.
import * as fs from 'node:fs/promises'
import { test } from '@playwright/test'
import {
  OUT,
  Recorder,
  buildImage,
  openRecordedPage,
  removeImage,
  saveVideo,
  serveWebOrigin,
  port,
} from './harness'
import {
  browserUsesCors,
  consoleManagesObjects,
  existingGateStillPasses,
  imageServesBothPorts,
  sdkSignsOverSocket,
} from './usecases'

test('demo walks every use case', async ({ browser }) => {
  await buildImage()
  const { context, page, clockStart } = await openRecordedPage(browser)
  const panel = await serveWebOrigin(port('panel'))
  const recorders: Recorder[] = []

  const run = async (useCase: string, title: string, flow: (recorder: Recorder) => Promise<void>) => {
    await page.goto(panel.url)
    await page.evaluate((text) => {
      const heading = document.getElementById('title')
      if (heading) heading.textContent = text
    }, `${useCase}: ${title}`)
    await page.waitForTimeout(1_500)
    const recorder = new Recorder(useCase, page, clockStart)
    recorders.push(recorder)
    await flow(recorder)
    recorder.endedAt = recorder.now()
  }

  let video
  try {
    await run('UC-001', 'CORS from a browser', (recorder) => browserUsesCors(recorder, page))
    await run('UC-002', 'Signed requests from an SDK', sdkSignsOverSocket)
    await run('UC-003', 'Web console', (recorder) => consoleManagesObjects(recorder, page))
    await run('UC-004', 'Starting the image', imageServesBothPorts)
    await run('UC-005', 'Existing S3 acceptance gate', existingGateStillPasses)
  } finally {
    video = await saveVideo(context, page, 'demo')
    await panel.close()
    await removeImage()
  }

  await fs.mkdir(`${OUT}/demo`, { recursive: true })
  await fs.copyFile(video.file, `${OUT}/demo/screen.webm`)
  await fs.writeFile(`${OUT}/demo/demo.json`, JSON.stringify({
    run_token: process.env.VERIFY_SCAN_RUN_TOKEN ?? 'local',
    video: `${OUT}/demo/screen.webm`,
    use_cases: recorders.map((recorder) => ({
      use_case: recorder.useCase,
      correlation_id: recorder.correlationId,
      systems: recorder.systems,
      start_s: recorder.startedAt,
      end_s: Math.min(recorder.endedAt, video.duration),
      checkpoints: recorder.checkpoints.map(({ id, correlation_id, actual, interval }) => ({
        id,
        correlation_id,
        actual,
        start_s: interval?.[0] ?? recorder.startedAt,
        end_s: Math.min(interval?.[1] ?? recorder.endedAt, video.duration),
      })),
    })),
  }, null, 2))
})
