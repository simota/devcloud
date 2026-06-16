import { fetchJSON } from '../../api/client'
import type { RedisCommandRequest, RedisCommandResponse, RedisKeyDetail, RedisKeysResponse, RedisStatus } from './types'

export async function getRedisStatus(): Promise<RedisStatus> {
  return fetchJSON<RedisStatus>('/api/redis/status')
}

export async function listRedisKeys(cursor = 0, match = '*', count = 100): Promise<RedisKeysResponse> {
  const params = new URLSearchParams({ cursor: String(cursor), match, count: String(count) })
  return fetchJSON<RedisKeysResponse>(`/api/redis/keys?${params.toString()}`)
}

export async function getRedisKey(key: string): Promise<RedisKeyDetail> {
  return fetchJSON<RedisKeyDetail>(`/api/redis/keys/${encodeURIComponent(key)}`)
}

export async function runRedisCommand(input: RedisCommandRequest): Promise<RedisCommandResponse> {
  return fetchJSON<RedisCommandResponse>('/api/redis/command', {
    method: 'POST',
    body: input,
  })
}

export async function deleteRedisKey(key: string): Promise<{ deleted: number }> {
  return fetchJSON<{ deleted: number }>(`/api/redis/keys/${encodeURIComponent(key)}`, { method: 'DELETE' })
}

export async function expireRedisKey(key: string, ttlSeconds: number): Promise<{ updated: boolean }> {
  return fetchJSON<{ updated: boolean }>(`/api/redis/keys/${encodeURIComponent(key)}/expire`, {
    method: 'POST',
    body: { ttlSeconds },
  })
}

export async function flushRedisDB(): Promise<{ result: string }> {
  return fetchJSON<{ result: string }>('/api/redis/keys?confirm=FLUSHDB', { method: 'DELETE' })
}

export async function selectRedisDB(db: number): Promise<{ currentDB: number }> {
  return fetchJSON<{ currentDB: number }>('/api/redis/select-db', {
    method: 'POST',
    body: { db },
  })
}
