package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

func TestBucketNotificationMetadataEndpointsPersist(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><QueueConfiguration><Id>docs-created</Id><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put notification status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?notification", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get notification status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed NotificationConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode notification config: %v", err)
	}
	if len(parsed.QueueConfigurations) != 1 || parsed.QueueConfigurations[0].ID != "docs-created" || parsed.QueueConfigurations[0].Queue == "" || len(parsed.QueueConfigurations[0].Events) != 1 {
		t.Fatalf("notification config = %#v", parsed)
	}
	if rules := parsed.QueueConfigurations[0].Filter.S3Key.Rules; len(rules) != 2 || rules[0].Name != "prefix" || rules[0].Value != "docs/" || rules[1].Name != "suffix" || rules[1].Value != ".txt" {
		t.Fatalf("notification filter rules = %#v", parsed.QueueConfigurations[0].Filter.S3Key.Rules)
	}
}

func TestBucketNotificationEventBridgeMetadataPersists(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><EventBridgeConfiguration /></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put notification status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?notification", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get notification status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed NotificationConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode notification config: %v", err)
	}
	if parsed.EventBridgeConfiguration == nil {
		t.Fatalf("event bridge configuration was not persisted: %#v", parsed)
	}
}

func TestBucketNotificationRecordsMatchingObjectEvents(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	config := `<NotificationConfiguration><TopicConfiguration><Topic>arn:aws:sns:us-east-1:000000000000:local</Topic><Event>s3:ObjectCreated:Put</Event><Event>s3:ObjectRemoved:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></TopicConfiguration></NotificationConfiguration>`
	if putNotification := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config)); putNotification.Code != http.StatusOK {
		t.Fatalf("put notification status = %d; body=%s", putNotification.Code, putNotification.Body.String())
	}

	if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("body")); put.Code != http.StatusOK {
		t.Fatalf("put matching object status = %d; body=%s", put.Code, put.Body.String())
	}
	if putIgnored := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.bin", strings.NewReader("body")); putIgnored.Code != http.StatusOK {
		t.Fatalf("put ignored object status = %d; body=%s", putIgnored.Code, putIgnored.Body.String())
	}
	if deleteObject := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt", nil); deleteObject.Code != http.StatusNoContent {
		t.Fatalf("delete matching object status = %d; body=%s", deleteObject.Code, deleteObject.Body.String())
	}

	events, ok, err := store.ListNotificationEvents(context.Background(), "demo-bucket")
	if err != nil {
		t.Fatalf("list notification events: %v", err)
	}
	if !ok {
		t.Fatal("bucket missing when listing notification events")
	}
	if len(events) != 2 {
		t.Fatalf("notification event count = %d, want 2: %#v", len(events), events)
	}
	if events[0].EventName != "s3:ObjectCreated:Put" || events[0].Key != "docs/readme.txt" || events[0].ETag == "" || events[0].EventID == "" {
		t.Fatalf("created event = %#v", events[0])
	}
	if events[1].EventName != "s3:ObjectRemoved:Delete" || events[1].Key != "docs/readme.txt" {
		t.Fatalf("removed event = %#v", events[1])
	}
}

func TestBucketNotificationRejectsUnsupportedEvent(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ReducedRedundancyLostObject</Event></QueueConfiguration></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusBadRequest {
		t.Fatalf("unsupported notification status = %d, want %d; body=%s", put.Code, http.StatusBadRequest, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported notification error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("unsupported notification code = %q, want InvalidArgument", parsed.Code)
	}
}

