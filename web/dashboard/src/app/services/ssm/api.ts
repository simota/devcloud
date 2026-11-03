import { fetchJSON } from '../../api/client'
import type { SSMParametersResponse, SSMStatus } from './types'

export async function getSSMStatus(): Promise<SSMStatus> {
  return fetchJSON<SSMStatus>('/api/ssm/status')
}

export async function listSSMParameters(): Promise<SSMParametersResponse> {
  return fetchJSON<SSMParametersResponse>('/api/ssm/parameters')
}
