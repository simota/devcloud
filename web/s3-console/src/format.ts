import type { S3Object } from './api'

export interface Listing {
  folders: string[]
  objects: S3Object[]
}

/** Groups a flat prefix listing into the immediate folders and objects below `prefix`. */
export function groupByFolder(objects: S3Object[], prefix: string): Listing {
  const folders = new Set<string>()
  const direct: S3Object[] = []
  for (const object of objects) {
    const slash = object.key.indexOf('/', prefix.length)
    if (slash === -1) direct.push(object)
    else folders.add(object.key.slice(0, slash + 1))
  }
  return { folders: [...folders].sort(), objects: direct }
}

export function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let value = bytes
  let unit = -1
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit += 1
  }
  return `${value.toFixed(1)} ${units[unit]}`
}

export function formatDate(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

export function baseName(path: string): string {
  const trimmed = path.endsWith('/') ? path.slice(0, -1) : path
  return trimmed.slice(trimmed.lastIndexOf('/') + 1)
}
