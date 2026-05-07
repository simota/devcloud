package redshift

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSONError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]any{
		"__type":  code,
		"message": message,
	})
}

func writeDataAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeDataAPIError(w http.ResponseWriter, status int, code string, message string) {
	writeDataAPIJSON(w, status, map[string]any{
		"__type":  code,
		"message": message,
	})
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeDataAPIResultFormat(value string) (string, error) {
	if value == "" {
		return "JSON", nil
	}
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch normalized {
	case "JSON", "CSV":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported ResultFormat %q", value)
	}
}

func metadataPatternMatches(value string, pattern string) bool {
	if pattern == "" {
		return true
	}
	return sqlLikeMatch(strings.ToLower(value), strings.ToLower(pattern))
}

func sqlLikeMatch(value string, pattern string) bool {
	valueRunes := []rune(value)
	patternRunes := []rune(pattern)
	memo := map[[2]int]bool{}
	seen := map[[2]int]bool{}
	var match func(int, int) bool
	match = func(valueIndex int, patternIndex int) bool {
		key := [2]int{valueIndex, patternIndex}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		if patternIndex == len(patternRunes) {
			memo[key] = valueIndex == len(valueRunes)
			return memo[key]
		}
		switch patternRunes[patternIndex] {
		case '%':
			memo[key] = match(valueIndex, patternIndex+1) ||
				(valueIndex < len(valueRunes) && match(valueIndex+1, patternIndex))
		case '_':
			memo[key] = valueIndex < len(valueRunes) && match(valueIndex+1, patternIndex+1)
		default:
			memo[key] = valueIndex < len(valueRunes) &&
				valueRunes[valueIndex] == patternRunes[patternIndex] &&
				match(valueIndex+1, patternIndex+1)
		}
		return memo[key]
	}
	return match(0, 0)
}

func paginateStrings(values []string, maxResults int, nextToken string) ([]string, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []string{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateTableMembers(values []tableMember, maxResults int, nextToken string) ([]tableMember, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []tableMember{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateColumnMetadata(values []columnMetadata, maxResults int, nextToken string) ([]columnMetadata, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []columnMetadata{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateRows(values [][]string, maxResults int, nextToken string) ([][]string, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return [][]string{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginationStart(nextToken string) (int, error) {
	if nextToken == "" {
		return 0, nil
	}
	start, err := strconv.Atoi(nextToken)
	if err != nil || start < 0 {
		return 0, errors.New("NextToken is invalid")
	}
	return start, nil
}

func paginationEnd(start int, total int, maxResults int) int {
	if maxResults <= 0 || start+maxResults > total {
		return total
	}
	return start + maxResults
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func hostFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "127.0.0.1"
	}
	return host
}

func portFromAddr(addr string, fallback int) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fallback
	}
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseCredentialDurationSeconds(value string) int {
	const defaultDurationSeconds = 900
	if value == "" {
		return defaultDurationSeconds
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultDurationSeconds
	}
	return parsed
}

func parseTagMembers(values map[string][]string) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		key := strings.TrimSpace(firstFormValue(values, fmt.Sprintf("Tags.member.%d.Key", i)))
		value := firstFormValue(values, fmt.Sprintf("Tags.member.%d.Value", i))
		if key == "" && value == "" {
			break
		}
		if key == "" {
			continue
		}
		tags = append(tags, Tag{Key: key, Value: value})
	}
	return tags
}

func parseTagKeyMembers(values map[string][]string) []string {
	var keys []string
	for i := 1; ; i++ {
		key := strings.TrimSpace(firstFormValue(values, fmt.Sprintf("TagKeys.member.%d", i)))
		if key == "" {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func firstFormValue(values map[string][]string, key string) string {
	if len(values[key]) == 0 {
		return ""
	}
	return values[key][0]
}

func mergeTags(existing []Tag, updates []Tag) []Tag {
	byKey := make(map[string]string, len(existing)+len(updates))
	for _, tag := range existing {
		byKey[tag.Key] = tag.Value
	}
	for _, tag := range updates {
		byKey[tag.Key] = tag.Value
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, Tag{Key: key, Value: byKey[key]})
	}
	return result
}

func deleteTags(existing []Tag, keys []string) []Tag {
	remove := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		remove[key] = struct{}{}
	}
	result := make([]Tag, 0, len(existing))
	for _, tag := range existing {
		if _, ok := remove[tag.Key]; ok {
			continue
		}
		result = append(result, tag)
	}
	return result
}
