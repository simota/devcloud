import { fetchJSON } from '../../api/client'
import type { LambdaFunctionsResponse, LambdaInvocationsResponse, LambdaInvokeResult, LambdaStatus } from './types'

export async function getLambdaStatus(): Promise<LambdaStatus> {
  return fetchJSON<LambdaStatus>('/api/lambda/status')
}

export async function listLambdaFunctions(): Promise<LambdaFunctionsResponse> {
  return fetchJSON<LambdaFunctionsResponse>('/api/lambda/functions')
}

export async function listLambdaInvocations(): Promise<LambdaInvocationsResponse> {
  return fetchJSON<LambdaInvocationsResponse>('/api/lambda/invocations')
}

export async function invokeLambdaFunction(name: string, event: string): Promise<LambdaInvokeResult> {
  return fetchJSON<LambdaInvokeResult>(`/api/lambda/functions/${encodeURIComponent(name)}/invoke`, {
    method: 'POST',
    rawBody: event,
    headers: { 'Content-Type': 'application/json' },
    timeoutMs: 910_000,
  })
}
