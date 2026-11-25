import { fetchJSON } from '../../api/client'
import type {
  ApplicationAutoScalingScalableTargetsResponse,
  ApplicationAutoScalingScalingPoliciesResponse,
  ApplicationAutoScalingScheduledActionsResponse,
  ApplicationAutoScalingStatus,
  DeleteScalingPolicyInput,
  DeleteScheduledActionInput,
  DeregisterScalableTargetInput,
  PutScalingPolicyInput,
  PutScalingPolicyResponse,
  PutScheduledActionInput,
  RegisterScalableTargetInput,
} from './types'

export async function getApplicationAutoScalingStatus(): Promise<ApplicationAutoScalingStatus> {
  return fetchJSON<ApplicationAutoScalingStatus>('/api/applicationautoscaling/status')
}

export async function listApplicationAutoScalingScalableTargets(): Promise<ApplicationAutoScalingScalableTargetsResponse> {
  return fetchJSON<ApplicationAutoScalingScalableTargetsResponse>('/api/applicationautoscaling/scalable-targets')
}

export async function listApplicationAutoScalingScalingPolicies(): Promise<ApplicationAutoScalingScalingPoliciesResponse> {
  return fetchJSON<ApplicationAutoScalingScalingPoliciesResponse>('/api/applicationautoscaling/scaling-policies')
}

export async function listApplicationAutoScalingScheduledActions(): Promise<ApplicationAutoScalingScheduledActionsResponse> {
  return fetchJSON<ApplicationAutoScalingScheduledActionsResponse>('/api/applicationautoscaling/scheduled-actions')
}

export async function registerApplicationAutoScalingScalableTarget(
  input: RegisterScalableTargetInput,
): Promise<Record<string, never>> {
  return fetchJSON<Record<string, never>>('/api/applicationautoscaling/scalable-targets', {
    method: 'POST',
    body: { input },
  })
}

export async function deregisterApplicationAutoScalingScalableTarget(
  input: DeregisterScalableTargetInput,
  confirmation: string,
): Promise<Record<string, never>> {
  return fetchJSON<Record<string, never>>('/api/applicationautoscaling/scalable-targets/deregister', {
    method: 'POST',
    body: { input, confirmation },
  })
}

export async function putApplicationAutoScalingScalingPolicy(
  input: PutScalingPolicyInput,
): Promise<PutScalingPolicyResponse> {
  return fetchJSON<PutScalingPolicyResponse>('/api/applicationautoscaling/scaling-policies', {
    method: 'POST',
    body: { input },
  })
}

export async function deleteApplicationAutoScalingScalingPolicy(
  input: DeleteScalingPolicyInput,
  confirmation: string,
): Promise<Record<string, never>> {
  return fetchJSON<Record<string, never>>('/api/applicationautoscaling/scaling-policies/delete', {
    method: 'POST',
    body: { input, confirmation },
  })
}

export async function putApplicationAutoScalingScheduledAction(
  input: PutScheduledActionInput,
): Promise<Record<string, never>> {
  return fetchJSON<Record<string, never>>('/api/applicationautoscaling/scheduled-actions', {
    method: 'POST',
    body: { input },
  })
}

export async function deleteApplicationAutoScalingScheduledAction(
  input: DeleteScheduledActionInput,
  confirmation: string,
): Promise<Record<string, never>> {
  return fetchJSON<Record<string, never>>('/api/applicationautoscaling/scheduled-actions/delete', {
    method: 'POST',
    body: { input, confirmation },
  })
}
