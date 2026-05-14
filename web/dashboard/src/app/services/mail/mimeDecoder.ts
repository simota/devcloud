// RFC 2047 MIME encoded-word decoder. Inputs like
//   =?utf-8?B?44GT44KT44Gr44Gh44Gv?=
// or
//   =?utf-8?Q?Hello=20world?=
// are decoded back to readable text. Anything that doesn't match an
// encoded-word is returned untouched, so callers can pass arbitrary
// header values through this safely.

export function decodeMimeEncodedWord(input: string): string {
  if (!input || input.indexOf('=?') === -1) {
    return input
  }
  return input.replace(
    /=\?([^?]+)\?([BbQq])\?([^?]*)\?=(\s+(?==\?))?/g,
    (match, charset: string, encoding: string, text: string) => {
      try {
        const bytes = encoding.toUpperCase() === 'B' ? decodeBase64Bytes(text) : decodeQEncodedBytes(text)
        const decoder = new TextDecoder(normalizeCharset(charset))
        return decoder.decode(bytes)
      } catch {
        return match
      }
    },
  )
}

export function decodeMimeAddress(value: string): string {
  if (!value) {
    return ''
  }
  return decodeMimeEncodedWord(value).trim()
}

function decodeBase64Bytes(text: string): Uint8Array {
  const binary = atob(text.replace(/\s+/g, ''))
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

function decodeQEncodedBytes(text: string): Uint8Array {
  const normalized = text.replace(/_/g, ' ')
  const out: number[] = []
  for (let i = 0; i < normalized.length; i += 1) {
    const ch = normalized[i]
    if (ch === '=' && i + 2 < normalized.length) {
      const hex = normalized.slice(i + 1, i + 3)
      const code = parseInt(hex, 16)
      if (!Number.isNaN(code)) {
        out.push(code)
        i += 2
        continue
      }
    }
    out.push(normalized.charCodeAt(i))
  }
  return new Uint8Array(out)
}

function normalizeCharset(charset: string): string {
  const lower = charset.trim().toLowerCase()
  if (lower === 'unknown-8bit' || lower === '') {
    return 'utf-8'
  }
  return lower
}
