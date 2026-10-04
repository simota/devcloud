import type { MailAddress } from './api'

export function addressLabel(from: MailAddress): string {
  return from.name || from.address || '(unknown sender)'
}

export function fullAddress(from: MailAddress): string {
  return from.name && from.address ? `${from.name} <${from.address}>` : addressLabel(from)
}

export function formatAbsoluteDate(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

export function formatRelativeDate(value: string, now = Date.now()): string {
  const time = new Date(value).getTime()
  if (Number.isNaN(time)) return value
  const minutes = Math.floor(Math.max(0, now - time) / 60000)
  if (minutes === 0) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

export function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = value / 1024
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`
}
