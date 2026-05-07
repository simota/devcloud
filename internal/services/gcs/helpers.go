package gcs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func bucketNameFromPath(escapedPath string) (string, bool) {
	escapedName := strings.TrimPrefix(escapedPath, "/storage/v1/b/")
	if escapedName == "" || strings.Contains(escapedName, "/") {
		return "", false
	}
	name, err := url.PathUnescape(escapedName)
	if err != nil || name == "" {
		return "", false
	}
	return name, true
}

func bucketAndSuffixFromPath(escapedPath string, prefix string) (bucket string, suffix string, ok bool) {
	trimmed := strings.TrimPrefix(escapedPath, prefix)
	if trimmed == "" || trimmed == escapedPath {
		return "", "", false
	}
	escapedBucket, suffix, hasSuffix := strings.Cut(trimmed, "/")
	bucket, err := url.PathUnescape(escapedBucket)
	if err != nil || bucket == "" {
		return "", "", false
	}
	if !hasSuffix {
		return bucket, "", true
	}
	return bucket, suffix, true
}

func gcsUserMetadataFromHeaders(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-goog-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-goog-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func mergeMetadata(base map[string]string, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	metadata := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		metadata[key] = value
	}
	for key, value := range override {
		metadata[key] = value
	}
	return metadata
}

func parseHTTPRange(header string, size int64) (start int64, end int64, partial bool, err error) {
	if header == "" {
		if size == 0 {
			return 0, -1, false, nil
		}
		return 0, size - 1, false, nil
	}
	if size == 0 {
		return 0, 0, false, fmt.Errorf("empty object has no satisfiable range")
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	left, right, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, fmt.Errorf("invalid range")
	}
	if left == "" {
		suffix, err := strconv.ParseInt(right, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, nil
	}
	start, err = strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, fmt.Errorf("invalid range start")
	}
	if right == "" {
		return start, size - 1, true, nil
	}
	end, err = strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range end")
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true, nil
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, reason string, message string) {
	writeJSON(w, status, errorResponse{
		Error: errorBody{
			Code:    status,
			Message: message,
			Errors: []errorItem{{
				Domain:  "global",
				Reason:  reason,
				Message: message,
			}},
		},
	})
}
