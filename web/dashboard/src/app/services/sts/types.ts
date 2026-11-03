export type STSStatus = {
  service: 'sts'
  status: string
  running: boolean
  endpoint: string
  region: string
}

export type STSCallerIdentity = {
  account: string
  arn: string
  userId: string
}
