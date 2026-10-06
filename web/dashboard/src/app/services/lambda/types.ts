export type LambdaStatus = {
  service: string
  status: string
  running: boolean
  endpoint: string
  region: string
  storagePath: string
  functionCount: number
  invocationCount: number
}

export type LambdaFunction = {
  FunctionName: string
  FunctionArn: string
  Runtime: string
  Handler: string
  Role: string
  Description: string
  Timeout: number
  MemorySize: number
  CodeSize: number
  CodeSha256: string
  LastModified: string
  State: string
  Environment?: { Variables: Record<string, string> }
}

export type LambdaInvocation = {
  requestId: string
  functionName: string
  invocationType: string
  status: string
  durationMs: number
  startedAt: string
  logTail: string
}

export type LambdaFunctionsResponse = {
  functions: LambdaFunction[]
}

export type LambdaInvocationsResponse = {
  invocations: LambdaInvocation[]
}

export type LambdaInvokeResult = {
  statusCode: number
  functionError: string | null
  payload: string
  log: string
}
