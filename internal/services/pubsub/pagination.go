package pubsub

import (
	"net/http"
	"strconv"
)

func parseListPagination(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	query := r.URL.Query()
	pageSize := 0
	if raw := query.Get("pageSize"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be non-negative")
			return 0, 0, false
		}
		pageSize = parsed
	}
	start := 0
	if raw := query.Get("pageToken"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageToken is invalid")
			return 0, 0, false
		}
		start = parsed
	}
	return start, pageSize, true
}

func pageBounds(total int, start int, pageSize int) (int, string) {
	if start > total {
		start = total
	}
	end := total
	if pageSize > 0 && start+pageSize < total {
		end = start + pageSize
		return end, strconv.Itoa(end)
	}
	return end, ""
}

func copyStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	copied := make(map[string]string, len(value))
	for k, v := range value {
		copied[k] = v
	}
	return copied
}

func copyAnyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	copied := make(map[string]any, len(value))
	for k, v := range value {
		copied[k] = v
	}
	return copied
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
