export type SSMStatus = {
  service: 'ssm'
  status: string
  running: boolean
  endpoint: string
  region: string
  storagePath: string
  parameterCount: number
}

// Metadata from DescribeParameters. Values are never sent to the dashboard.
export type SSMParameter = {
  Name: string
  ARN: string
  Type: 'String' | 'StringList' | 'SecureString'
  Version: number
  LastModifiedDate: number
  Tier: string
  DataType: string
  Description?: string
  KeyId?: string
}

export type SSMParametersResponse = {
  parameters: SSMParameter[]
}
