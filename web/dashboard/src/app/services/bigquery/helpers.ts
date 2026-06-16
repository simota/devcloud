import type { DashboardService } from '../dashboard/types'
import type {
  BigQueryInsertAllRow,
  BigQueryJob,
  BigQuerySchema,
  BigQueryStatus,
} from './types'

export function disabledStatus(service?: DashboardService): BigQueryStatus {
  return {
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:19050',
    project: 'devcloud',
    location: 'US',
    authMode: 'relaxed',
    storagePath: service?.storagePath ?? '.devcloud/data/bigquery',
    datasetCount: 0,
    jobCount: 0,
  }
}

export function schemaLabel(schema: { fields?: Array<{ name: string; type?: string; mode?: string }> }): string {
  const fields = schema.fields ?? []
  if (fields.length === 0) {
    return 'No schema fields'
  }
  return fields.map((field) => `${field.name} ${field.type ?? 'STRING'} ${field.mode ?? 'NULLABLE'}`).join(' / ')
}

export function recentJobs(jobs: BigQueryJob[]): BigQueryJob[] {
  return [...jobs].sort((left, right) => jobCreationTime(right) - jobCreationTime(left)).slice(0, 8)
}

export function jobCreationTime(job: BigQueryJob): number {
  const raw = job.job?.statistics?.creationTime
  if (!raw) {
    return 0
  }
  const millis = Number.parseInt(raw, 10)
  return Number.isFinite(millis) ? millis : 0
}

export function jobMetadataLabel(job: BigQueryJob): string {
  const query = job.job?.statistics?.query
  const totalRows = query?.totalRows ?? '0'
  const dryRun = query?.dryRun ? 'dry run' : 'executed'
  const cache = query?.cacheHit ? 'cache hit' : 'cache miss'
  return `${dryRun} / ${totalRows} rows / ${cache}`
}

export function sanitizedJobDetail(job: BigQueryJob): Record<string, unknown> {
  const detail = (job.job ?? job) as Record<string, unknown>
  const cloned = JSON.parse(JSON.stringify(detail)) as Record<string, unknown>
  const configuration = cloned.configuration
  if (isRecord(configuration) && isRecord(configuration.query)) {
    delete configuration.query.queryParameters
  }
  return cloned
}

export function bigQueryRowObject(
  fields: Array<{ name: string }>,
  row?: { f: Array<{ v: unknown }> },
): Record<string, unknown> {
  if (!row) {
    return {}
  }
  return row.f.reduce<Record<string, unknown>>((record, cell, index) => {
    record[fields[index]?.name ?? `column_${index + 1}`] = cell.v
    return record
  }, {})
}

export function parseJSONRecord<T extends Record<string, unknown>>(source: string, label: string): T | Error {
  try {
    const parsed = JSON.parse(source) as unknown
    if (!isRecord(parsed)) {
      return new Error(`${label} must be a JSON object.`)
    }
    return parsed as T
  } catch {
    return new Error(`${label} has a JSON validation error.`)
  }
}

export function parseSchemaFields(source: string): BigQuerySchema | Error {
  const fields = source
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [name, type = 'STRING', mode] = line.split(':').map((part) => part.trim())
      if (!name) {
        return new Error('Schema field names are required.')
      }
      return {
        name,
        type: type || 'STRING',
        mode: mode || undefined,
      }
    })

  const error = fields.find((field): field is Error => field instanceof Error)
  if (error) {
    return error
  }
  return { fields }
}

export function parseInsertRows(source: string, insertId?: string): BigQueryInsertAllRow[] | Error {
  let parsed: unknown
  try {
    parsed = JSON.parse(source) as unknown
  } catch {
    return new Error('Row JSON has a JSON validation error.')
  }

  if (Array.isArray(parsed)) {
    const rows: BigQueryInsertAllRow[] = []
    for (const [index, row] of parsed.entries()) {
      if (!isRecord(row)) {
        return new Error(`Row ${index + 1} must be a JSON object.`)
      }
      rows.push({ json: row })
    }
    return rows
  }

  if (!isRecord(parsed)) {
    return new Error('Row JSON must be a JSON object or an array of JSON objects.')
  }
  return [{ insertId: insertId || undefined, json: parsed }]
}

export function readNestedString(record: Record<string, unknown>, path: string[]): string | undefined {
  let current: unknown = record
  for (const segment of path) {
    if (!isRecord(current)) {
      return undefined
    }
    current = current[segment]
  }
  return typeof current === 'string' && current.trim() !== '' ? current.trim() : undefined
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null'
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}
