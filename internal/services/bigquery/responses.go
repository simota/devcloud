package bigquery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func hasChildren(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) > 0
}

func datasetETag(t time.Time) string {
	return fmt.Sprintf("\"%d\"", t.UnixNano())
}

func unixMillisString(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixMilli())
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func boolPtr(value bool) *bool {
	return &value
}

func defaultIAMPolicy() iamPolicy {
	return iamPolicy{
		Version:  1,
		ETag:     datasetETag(time.Unix(0, 0).UTC()),
		Bindings: []iamBinding{},
	}
}

func normalizeIAMPolicy(policy iamPolicy) iamPolicy {
	if policy.Version == 0 {
		policy.Version = 1
	}
	if policy.ETag == "" {
		policy.ETag = datasetETag(time.Now().UTC())
	}
	if policy.Bindings == nil {
		policy.Bindings = []iamBinding{}
	}
	return policy
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
			Status: statusText(status),
		},
	})
}

func statusText(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ALREADY_EXISTS"
	case http.StatusMethodNotAllowed:
		return "METHOD_NOT_ALLOWED"
	default:
		return strings.ToUpper(strings.ReplaceAll(http.StatusText(status), " ", "_"))
	}
}
