// One test per use case: `npx playwright test image.spec.ts --grep "<name>"`.
import { test } from '@playwright/test'
import {
  Recorder,
  buildImage,
  openRecordedPage,
  removeImage,
  saveVideo,
  writeScenario,
} from './harness'
import {
  browserUsesCors,
  consoleManagesObjects,
  existingGateStillPasses,
  imageServesBothPorts,
  sdkSignsOverSocket,
} from './usecases'

test.beforeAll(async () => {
  test.setTimeout(600_000)
  await buildImage()
})

test.afterAll(removeImage)

test('browser uses cors', async ({ browser }) => {
  const { context, page, clockStart } = await openRecordedPage(browser)
  const recorder = new Recorder('UC-001', page, clockStart)
  let video
  try {
    await browserUsesCors(recorder, page)
  } finally {
    video = await saveVideo(context, page, 'UC-001')
    await writeScenario(recorder, video)
  }
})

test('sdk signs over socket', async () => {
  const recorder = new Recorder('UC-002', null, Date.now())
  try {
    await sdkSignsOverSocket(recorder)
  } finally {
    await writeScenario(recorder)
  }
})

test('console manages objects', async ({ browser }) => {
  const { context, page, clockStart } = await openRecordedPage(browser)
  const recorder = new Recorder('UC-003', page, clockStart)
  let video
  try {
    await consoleManagesObjects(recorder, page)
  } finally {
    video = await saveVideo(context, page, 'UC-003')
    await writeScenario(recorder, video)
  }
})

test('image serves both ports', async () => {
  const recorder = new Recorder('UC-004', null, Date.now())
  try {
    await imageServesBothPorts(recorder)
  } finally {
    await writeScenario(recorder)
  }
})

test('existing gate still passes', async () => {
  const recorder = new Recorder('UC-005', null, Date.now())
  try {
    await existingGateStillPasses(recorder)
  } finally {
    await writeScenario(recorder)
  }
})
