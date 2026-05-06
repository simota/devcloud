package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	snapshots := make([]snapshotResource, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		if resourceProject(snapshot.Name) == project && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot.public())
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(snapshots), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	project, snapshotID, ok := snapshotNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := snapshotName(project, snapshotID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(snapshotID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid snapshot name")
		return
	}
	switch r.Method {
	case http.MethodPut:
		var request struct {
			Subscription string `json:"subscription"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if !validFullSubscriptionName(request.Subscription) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.snapshots[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "snapshot already exists")
			return
		}
		subscription, found := s.subscriptions[request.Subscription]
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		snapshot := snapshotResource{
			Name:         name,
			Topic:        subscription.Topic,
			Subscription: subscription.Name,
			ExpireTime:   s.now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339Nano),
			Deliveries:   snapshotDeliveries(s.deliveries[subscription.Name]),
		}
		s.snapshots[name] = snapshot
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.snapshots, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, snapshot.public())
	case http.MethodGet:
		s.mu.Lock()
		snapshot, found := s.snapshots[name]
		s.mu.Unlock()
		if !found || snapshotExpired(snapshot, s.now().UTC()) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
			return
		}
		writeJSON(w, http.StatusOK, snapshot.public())
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.snapshots[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
			return
		}
		delete(s.snapshots, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func snapshotName(project string, snapshotID string) string {
	return fmt.Sprintf("projects/%s/snapshots/%s", project, snapshotID)
}

func (snapshot snapshotResource) public() snapshotResource {
	snapshot.Deliveries = nil
	return snapshot
}

func (schema schemaResource) public(view string) schemaResource {
	if view == "BASIC" {
		schema.Definition = ""
	}
	return schema
}

func snapshotExpired(snapshot snapshotResource, now time.Time) bool {
	if strings.TrimSpace(snapshot.ExpireTime) == "" {
		return false
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, snapshot.ExpireTime)
	if err != nil {
		expiresAt, err = time.Parse(time.RFC3339, snapshot.ExpireTime)
	}
	return err == nil && !expiresAt.After(now)
}

func snapshotDeliveries(deliveries []deliveryRecord) []deliveryRecord {
	if len(deliveries) == 0 {
		return nil
	}
	copied := make([]deliveryRecord, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Acked {
			continue
		}
		delivery.AckID = ""
		delivery.LeaseDeadline = time.Time{}
		delivery.NextDeliveryTime = time.Time{}
		copied = append(copied, delivery)
	}
	return copied
}
