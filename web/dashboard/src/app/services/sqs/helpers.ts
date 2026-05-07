import type { DashboardService } from '../dashboard/types'
import type { SQSMessageAttribute, SQSStatus } from './types'

export function disabledStatus(service?: DashboardService): SQSStatus {
  return {
    service: 'sqs',
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:9324',
    region: 'us-east-1',
    authMode: 'relaxed',
    storagePath: service?.storagePath ?? '.devcloud/data/sqs',
    queueCount: 0,
  }
}

export function normalizedNumberString(value: string): string | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined
  }
  return String(parsed)
}

export function normalizedOptionalNumber(value: string): number | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined
  }
  return parsed
}

export function normalizedNonNegativeOptionalNumber(value: string): number | undefined {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined
  }
  return parsed
}

export function compactStringMap(input: Record<string, string | undefined>): Record<string, string> {
  return Object.fromEntries(Object.entries(input).filter((entry): entry is [string, string] => typeof entry[1] === 'string'))
}

export function parseMessageAttributes(value: string): Record<string, SQSMessageAttribute> | undefined | Error {
  const trimmed = value.trim()
  if (trimmed === '') {
    return undefined
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return new Error('Message attributes JSON is invalid')
  }
  if (!isPlainObject(parsed)) {
    return new Error('Message attributes must be a JSON object')
  }
  for (const [name, attribute] of Object.entries(parsed)) {
    if (!isPlainObject(attribute) || typeof attribute.DataType !== 'string' || attribute.DataType.trim() === '') {
      return new Error(`Message attribute ${name} must include DataType`)
    }
  }
  return parsed as Record<string, SQSMessageAttribute>
}

export function parseNameList(value: string): string[] | undefined {
  const names = value
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean)
  return names.length > 0 ? names : undefined
}

export function parseRedrivePolicy(value?: string): { deadLetterTargetArn: string; maxReceiveCount: string } | undefined {
  if (!value) {
    return undefined
  }
  try {
    const parsed: unknown = JSON.parse(value)
    if (!isPlainObject(parsed)) {
      return undefined
    }
    const deadLetterTargetArn = parsed.deadLetterTargetArn
    const maxReceiveCount = parsed.maxReceiveCount
    if (typeof deadLetterTargetArn !== 'string') {
      return undefined
    }
    return {
      deadLetterTargetArn,
      maxReceiveCount: typeof maxReceiveCount === 'string' ? maxReceiveCount : String(maxReceiveCount ?? ''),
    }
  } catch {
    return undefined
  }
}

export function parseRedriveAllowPolicy(value?: string): { redrivePermission: string } | undefined {
  if (!value) {
    return undefined
  }
  try {
    const parsed: unknown = JSON.parse(value)
    if (!isPlainObject(parsed) || typeof parsed.redrivePermission !== 'string') {
      return undefined
    }
    return { redrivePermission: parsed.redrivePermission }
  } catch {
    return undefined
  }
}

export function maskReceiptHandle(value: string): string {
  if (value.length <= 12) {
    return value === '' ? '' : '...'
  }
  return `${value.slice(0, 6)}...${value.slice(-6)}`
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
