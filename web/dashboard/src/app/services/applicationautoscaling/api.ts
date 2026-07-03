import { fetchJSON } from '../../api/client'
import type {
  ApplicationAutoScalingScalableTargetsResponse,
  ApplicationAutoScalingScalingPoliciesResponse,
  ApplicationAutoScalingScheduledActionsResponse,
  ApplicationAutoScalingStatus,
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
