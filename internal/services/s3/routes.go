package s3

import "strings"

func parsePathStyle(path string) (bucket string, key string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "", "", false
	}
	bucket, key, _ = strings.Cut(trimmed, "/")
	if bucket == "" {
		return "", "", false
	}
	return bucket, key, true
}

func parseVirtualHostStyle(host string, path string) (bucket string, key string, ok bool) {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return "", "", false
	}
	if withoutPort, _, found := strings.Cut(host, ":"); found {
		host = withoutPort
	}
	if !strings.HasSuffix(host, ".localhost") {
		return "", "", false
	}
	bucket = strings.TrimSuffix(host, ".localhost")
	if bucket == "" || strings.Contains(bucket, ".") {
		return "", "", false
	}
	key = strings.TrimPrefix(path, "/")
	return bucket, key, true
}
