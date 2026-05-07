package pubsub

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRESTSnapshotCRUDListTopicSnapshotsAndSeek(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 30,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmVwbGF5","attributes":{"secret":"hidden"}}]
	}`)

	createSnapshot := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/snapshots/orders-snapshot", `{
		"subscription":"projects/devcloud/subscriptions/orders-sub"
	}`)
	if createSnapshot.Code != http.StatusOK {
		t.Fatalf("create snapshot status = %d, want %d: %s", createSnapshot.Code, http.StatusOK, createSnapshot.Body.String())
	}
	var snapshot snapshotResource
	if err := json.NewDecoder(createSnapshot.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Name != "projects/devcloud/snapshots/orders-snapshot" || snapshot.Topic != "projects/devcloud/topics/orders" || snapshot.Subscription != "projects/devcloud/subscriptions/orders-sub" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	for _, forbidden := range []string{"ackId", "cmVwbGF5", "secret"} {
		if strings.Contains(createSnapshot.Body.String(), forbidden) {
			t.Fatalf("snapshot response leaked %q: %s", forbidden, createSnapshot.Body.String())
		}
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var pulled struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}
	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}

	seek := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"snapshot":"projects/devcloud/snapshots/orders-snapshot"
	}`)
	if seek.Code != http.StatusOK {
		t.Fatalf("seek status = %d, want %d: %s", seek.Code, http.StatusOK, seek.Body.String())
	}
	replayed := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replayed pull status = %d, want %d: %s", replayed.Code, http.StatusOK, replayed.Body.String())
	}
	if !strings.Contains(replayed.Body.String(), "cmVwbGF5") {
		t.Fatalf("seek did not replay snapshot message: %s", replayed.Body.String())
	}

	listSnapshots := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/snapshots", "")
	if listSnapshots.Code != http.StatusOK || !strings.Contains(listSnapshots.Body.String(), "orders-snapshot") {
		t.Fatalf("list snapshots = status %d body %s", listSnapshots.Code, listSnapshots.Body.String())
	}
	topicSnapshots := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/snapshots", "")
	if topicSnapshots.Code != http.StatusOK || !strings.Contains(topicSnapshots.Body.String(), "projects/devcloud/snapshots/orders-snapshot") {
		t.Fatalf("topic snapshots = status %d body %s", topicSnapshots.Code, topicSnapshots.Body.String())
	}

	deleteSnapshot := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/snapshots/orders-snapshot", "")
	if deleteSnapshot.Code != http.StatusNoContent {
		t.Fatalf("delete snapshot status = %d, want %d: %s", deleteSnapshot.Code, http.StatusNoContent, deleteSnapshot.Body.String())
	}
	getDeleted := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/snapshots/orders-snapshot", "")
	if getDeleted.Code != http.StatusNotFound {
		t.Fatalf("get deleted snapshot status = %d, want %d", getDeleted.Code, http.StatusNotFound)
	}
}

func TestRESTExpiredSnapshotsAreHiddenFromListGetAndSeek(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZXhwaXJlZA=="}]}`)

	createSnapshot := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/snapshots/orders-snapshot", `{
		"subscription":"projects/devcloud/subscriptions/orders-sub"
	}`)
	if createSnapshot.Code != http.StatusOK {
		t.Fatalf("create snapshot status = %d, want %d: %s", createSnapshot.Code, http.StatusOK, createSnapshot.Body.String())
	}

	now = now.Add(8 * 24 * time.Hour)
	listSnapshots := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/snapshots", "")
	if listSnapshots.Code != http.StatusOK {
		t.Fatalf("list snapshots status = %d, want %d: %s", listSnapshots.Code, http.StatusOK, listSnapshots.Body.String())
	}
	if strings.Contains(listSnapshots.Body.String(), "orders-snapshot") {
		t.Fatalf("expired snapshot should not be listed: %s", listSnapshots.Body.String())
	}
	topicSnapshots := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/snapshots", "")
	if topicSnapshots.Code != http.StatusOK {
		t.Fatalf("topic snapshots status = %d, want %d: %s", topicSnapshots.Code, http.StatusOK, topicSnapshots.Body.String())
	}
	if strings.Contains(topicSnapshots.Body.String(), "orders-snapshot") {
		t.Fatalf("expired topic snapshot should not be listed: %s", topicSnapshots.Body.String())
	}
	getSnapshot := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/snapshots/orders-snapshot", "")
	if getSnapshot.Code != http.StatusNotFound {
		t.Fatalf("get expired snapshot status = %d, want %d: %s", getSnapshot.Code, http.StatusNotFound, getSnapshot.Body.String())
	}
	seek := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"snapshot":"projects/devcloud/snapshots/orders-snapshot"
	}`)
	if seek.Code != http.StatusNotFound {
		t.Fatalf("seek expired snapshot status = %d, want %d: %s", seek.Code, http.StatusNotFound, seek.Body.String())
	}
}

func TestRESTSeekByTimeReplaysRetainedMessagesAfterTimestamp(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 30,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"YmVmb3Jl"}]}`)
	now = now.Add(10 * time.Second)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"YWZ0ZXI="}]}`)

	seek := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"time":"2026-05-02T12:00:05Z"
	}`)
	if seek.Code != http.StatusOK {
		t.Fatalf("seek status = %d, want %d: %s", seek.Code, http.StatusOK, seek.Body.String())
	}
	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":10}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	var pulled struct {
		ReceivedMessages []struct {
			Message struct {
				Data string `json:"data"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].Message.Data != "YWZ0ZXI=" {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}
}

func TestRESTSeekRejectsInvalidTimeRequests(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)

	invalidTime := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{"time":"not-a-time"}`)
	if invalidTime.Code != http.StatusBadRequest {
		t.Fatalf("invalid time status = %d, want %d: %s", invalidTime.Code, http.StatusBadRequest, invalidTime.Body.String())
	}
	bothFields := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"snapshot":"projects/devcloud/snapshots/orders-snapshot",
		"time":"2026-05-02T12:00:00Z"
	}`)
	if bothFields.Code != http.StatusBadRequest {
		t.Fatalf("both fields status = %d, want %d: %s", bothFields.Code, http.StatusBadRequest, bothFields.Body.String())
	}
}

func TestRESTSnapshotsPersistAcrossReload(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cGVyc2lzdGVkLXNuYXA="}]}`)
	createSnapshot := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/snapshots/orders-snapshot", `{
		"subscription":"projects/devcloud/subscriptions/orders-sub"
	}`)
	if createSnapshot.Code != http.StatusOK {
		t.Fatalf("create snapshot status = %d, want %d: %s", createSnapshot.Code, http.StatusOK, createSnapshot.Body.String())
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	getSnapshot := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/snapshots/orders-snapshot", "")
	if getSnapshot.Code != http.StatusOK {
		t.Fatalf("reloaded snapshot status = %d, want %d: %s", getSnapshot.Code, http.StatusOK, getSnapshot.Body.String())
	}
	seek := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"snapshot":"projects/devcloud/snapshots/orders-snapshot"
	}`)
	if seek.Code != http.StatusOK {
		t.Fatalf("reloaded seek status = %d, want %d: %s", seek.Code, http.StatusOK, seek.Body.String())
	}
	pull := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("reloaded pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "cGVyc2lzdGVkLXNuYXA=") {
		t.Fatalf("reloaded snapshot did not replay message: %s", pull.Body.String())
	}
}
