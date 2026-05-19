import type { ReactNode } from 'react'

const TOKEN_PATTERN =
  /("(?:\\.|[^"\\])*")(\s*:)?|(\b(?:true|false|null)\b)|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g

export function highlightJSON(source: string): ReactNode[] {
  const nodes: ReactNode[] = []
  let lastIndex = 0
  let keyCounter = 0

  TOKEN_PATTERN.lastIndex = 0
  let match: RegExpExecArray | null
  while ((match = TOKEN_PATTERN.exec(source)) !== null) {
    const [whole, stringLit, colonTail, keyword, number] = match
    const start = match.index

    if (start > lastIndex) {
      nodes.push(source.slice(lastIndex, start))
    }

    if (stringLit !== undefined) {
      const isKey = colonTail !== undefined
      nodes.push(
        <span className={isKey ? 'json-token-key' : 'json-token-string'} key={`t${keyCounter++}`}>
          {stringLit}
        </span>,
      )
      if (colonTail) {
        nodes.push(colonTail)
      }
    } else if (keyword !== undefined) {
      const cls = keyword === 'null' ? 'json-token-null' : 'json-token-bool'
      nodes.push(
        <span className={cls} key={`t${keyCounter++}`}>
          {keyword}
        </span>,
      )
    } else if (number !== undefined) {
      nodes.push(
        <span className="json-token-number" key={`t${keyCounter++}`}>
          {number}
        </span>,
      )
    } else {
      nodes.push(whole)
    }

    lastIndex = start + whole.length
  }

  if (lastIndex < source.length) {
    nodes.push(source.slice(lastIndex))
  }

  return nodes
}

export function tryPrettyJSON(value: string): string | undefined {
  const trimmed = value.trim()
  if (trimmed === '' || (trimmed[0] !== '{' && trimmed[0] !== '[' && trimmed[0] !== '"')) {
    return undefined
  }
  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2)
  } catch {
    return undefined
  }
}
