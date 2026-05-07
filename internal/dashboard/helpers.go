package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func downloadFilename(key string) string {
	name := key
	if index := strings.LastIndex(name, "/"); index >= 0 {
		name = name[index+1:]
	}
	if name == "" {
		return "object"
	}
	return strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			return '_'
		}
		return r
	}, name)
}

func dashboardPathParts(escapedPath string, prefix string) ([]string, error) {
	suffix := strings.TrimPrefix(escapedPath, prefix)
	if suffix == escapedPath {
		return nil, nil
	}
	rawParts := strings.Split(strings.Trim(suffix, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		if raw == "" {
			continue
		}
		part, err := url.PathUnescape(raw)
		if err != nil {
			return nil, err
		}
		if part == "." || part == ".." || strings.ContainsAny(part, `/\`) {
			return nil, errors.New("invalid path segment")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func positiveLimitFromRequest(w http.ResponseWriter, r *http.Request, fallback int) (int, bool) {
	limit := fallback
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return 0, false
		}
		limit = parsed
	}
	return limit, true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
