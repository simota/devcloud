package s3

import "strings"

// virtualHostLocalSuffixes lists the trailing host segments under which a
// virtual-hosted-style request (<bucket>.<suffix>) is treated as local.
// `.localhost` is the AWS SDK-friendly default; `.127.0.0.1` and `.0.0.0.0`
// cover the common cases where an SDK or test harness sets a literal-IP Host
// header without going through DNS.
var virtualHostLocalSuffixes = []string{
	".localhost",
	".127.0.0.1",
	".0.0.0.0",
}

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
	var trimmed string
	for _, suffix := range virtualHostLocalSuffixes {
		if strings.HasSuffix(host, suffix) {
			trimmed = strings.TrimSuffix(host, suffix)
			break
		}
	}
	if trimmed == "" || strings.Contains(trimmed, ".") {
		return "", "", false
	}
	key = strings.TrimPrefix(path, "/")
	return trimmed, key, true
}
