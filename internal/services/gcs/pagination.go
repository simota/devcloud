package gcs

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

func paginationWindow(query url.Values, total int) (int, int, string, error) {
	start := 0
	if token := strings.TrimSpace(query.Get("pageToken")); token != "" {
		offset, err := strconv.Atoi(token)
		if err != nil || offset < 0 {
			return 0, 0, "", fmt.Errorf("invalid pageToken")
		}
		if offset > total {
			offset = total
		}
		start = offset
	}

	limit := total - start
	if value := strings.TrimSpace(query.Get("maxResults")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, 0, "", fmt.Errorf("invalid maxResults")
		}
		if parsed < limit {
			limit = parsed
		}
	}

	end := start + limit
	nextToken := ""
	if end < total {
		nextToken = strconv.Itoa(end)
	}
	return start, end, nextToken, nil
}

type objectListEntry struct {
	name   string
	object *objectResource
	prefix string
}

func paginateObjectsResponse(query url.Values, response objectsListResponse) (objectsListResponse, error) {
	entries := make([]objectListEntry, 0, len(response.Items)+len(response.Prefixes))
	for i := range response.Items {
		entries = append(entries, objectListEntry{name: response.Items[i].Name, object: &response.Items[i]})
	}
	for _, prefix := range response.Prefixes {
		entries = append(entries, objectListEntry{name: prefix, prefix: prefix})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	start, end, nextToken, err := paginationWindow(query, len(entries))
	if err != nil {
		return objectsListResponse{}, err
	}
	paginated := objectsListResponse{
		Kind:          response.Kind,
		Items:         []objectResource{},
		Prefixes:      []string{},
		NextPageToken: nextToken,
	}
	for _, entry := range entries[start:end] {
		if entry.object != nil {
			paginated.Items = append(paginated.Items, *entry.object)
			continue
		}
		paginated.Prefixes = append(paginated.Prefixes, entry.prefix)
	}
	if len(paginated.Items) == 0 {
		paginated.Items = nil
	}
	if len(paginated.Prefixes) == 0 {
		paginated.Prefixes = nil
	}
	return paginated, nil
}
