export type ApplicationAutoScalingStatus = {
  service: string
  status: string
  running: boolean
  endpoint: string
  region: string
  storagePath: string
  scalableTargetCount: number
  scalingPolicyCount: number
  scheduledActionCount: number
}

export type ApplicationAutoScalingSuspendedState = {
  DynamicScalingInSuspended?: boolean
  DynamicScalingOutSuspended?: boolean
  ScheduledScalingSuspended?: boolean
}

export type ApplicationAutoScalingScalableTarget = {
  ServiceNamespace: string
  ResourceId: string
  ScalableDimension: string
  MinCapacity: number
  MaxCapacity: number
  RoleARN?: string
  SuspendedState?: ApplicationAutoScalingSuspendedState
  CreationTime: string
}

export type ApplicationAutoScalingScalingPolicy = {
  PolicyARN: string
  PolicyName: string
  ServiceNamespace: string
  ResourceId: string
  ScalableDimension: string
  PolicyType: string
  StepScalingPolicyConfiguration?: unknown
  TargetTrackingScalingPolicyConfiguration?: unknown
  Alarms: unknown[]
  CreationTime: string
}

export type ApplicationAutoScalingScheduledAction = {
  ScheduledActionName: string
  ServiceNamespace: string
  ResourceId: string
  ScalableDimension?: string
  Schedule: string
  Timezone?: string
  StartTime?: string
  EndTime?: string
  ScalableTargetAction?: unknown
  CreationTime: string
}

export type ApplicationAutoScalingScalableTargetsResponse = {
  scalableTargets: ApplicationAutoScalingScalableTarget[]
}

export type ApplicationAutoScalingScalingPoliciesResponse = {
  scalingPolicies: ApplicationAutoScalingScalingPolicy[]
}

export type ApplicationAutoScalingScheduledActionsResponse = {
  scheduledActions: ApplicationAutoScalingScheduledAction[]
}
