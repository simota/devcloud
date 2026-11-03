import { fetchJSON } from '../../api/client'
import type { STSCallerIdentity, STSStatus } from './types'

export async function getSTSStatus(): Promise<STSStatus> {
  return fetchJSON<STSStatus>('/api/sts/status')
}

export async function getSTSCallerIdentity(): Promise<STSCallerIdentity> {
  return fetchJSON<STSCallerIdentity>('/api/sts/caller-identity')
}
