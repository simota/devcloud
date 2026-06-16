export type RedisStatus = {
  service: string
  status: string
  running: boolean
  mode: string
  address: string
  serverVersion: string
  connectedClients: number
  usedMemoryHuman: string
  currentDB: number
  databaseCount: number
  currentDBKeys: number
  storagePath: string
}

export type RedisKeySummary = {
  key: string
  type: string
  ttlSeconds: number
}

export type RedisKeysResponse = {
  cursor: number
  nextCursor: number
  keys: RedisKeySummary[]
}

export type RedisKeyDetail = {
  key: string
  type: string
  ttlSeconds: number
  preview: string[]
}

export type RedisCommandRequest = {
  command: string
  args: string[]
}

export type RedisCommandResponse = {
  command: string
  class: string
  rows: string[]
}
