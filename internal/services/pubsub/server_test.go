package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRESTReadiness(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["service"] != "pubsub" || body["status"] != "running" || body["protocol"] != "rest" {
		t.Fatalf("body = %#v", body)
	}
}

func TestServerDefaultsPubSubListenerAddresses(t *testing.T) {
	server := NewServer(Config{})

	if server.config.GRPCAddr != "127.0.0.1:8085" {
		t.Fatalf("GRPCAddr = %q, want %q", server.config.GRPCAddr, "127.0.0.1:8085")
	}
	if server.config.RESTAddr != "127.0.0.1:8086" {
		t.Fatalf("RESTAddr = %q, want %q", server.config.RESTAddr, "127.0.0.1:8086")
	}
	if server.config.EnablePush {
		t.Fatalf("EnablePush default = true, want false")
	}
}

func TestGRPCReadiness(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["service"] != "pubsub" || body["status"] != "running" || body["protocol"] != "grpc" {
		t.Fatalf("body = %#v", body)
	}
}

func TestListenerCountMatchesRESTConfiguration(t *testing.T) {
	restDisabled := NewServer(Config{Project: "devcloud", RESTEnabled: false})
	if got := restDisabled.listenerCount(); got != 1 {
		t.Fatalf("listenerCount() with REST disabled = %d, want 1", got)
	}

	restEnabled := NewServer(Config{Project: "devcloud", RESTEnabled: true})
	if got := restEnabled.listenerCount(); got != 2 {
		t.Fatalf("listenerCount() with REST enabled = %d, want 2", got)
	}
}

func TestGRPCReadinessReportsStoreLoadFailureSafely(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	if err := os.WriteFile(storagePath, []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub store: %v", err)
	}
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "pubsub resource store unavailable") {
		t.Fatalf("body = %s", body)
	}
	if strings.Contains(body, storagePath) {
		t.Fatalf("readiness leaked storage path: %s", body)
	}
}

func TestGRPCHealthDoesNotRequireStoreReadiness(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	if err := os.WriteFile(storagePath, []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub store: %v", err)
	}
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestGRPCReadinessRejectsUnsupportedMethod(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET" {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func newPubSubGRPCTestClients(t *testing.T, config Config) (*Server, context.Context, pubsubpb.PublisherClient, pubsubpb.SubscriberClient) {
	t.Helper()
	server := NewServer(config)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return server, ctx, pubsubpb.NewPublisherClient(conn), pubsubpb.NewSubscriberClient(conn)
}

func TestGRPCPublisherSubscriberWorkflow(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)

	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if topic.GetName() != "projects/devcloud/topics/orders" {
		t.Fatalf("topic name = %q", topic.GetName())
	}

	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/orders-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if subscription.GetTopic() != topic.GetName() {
		t.Fatalf("subscription topic = %q", subscription.GetTopic())
	}

	publish, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{
			Data:       []byte("hello over grpc"),
			Attributes: map[string]string{"source": "grpc-test"},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(publish.GetMessageIds()) != 1 {
		t.Fatalf("message ids = %#v", publish.GetMessageIds())
	}

	pull, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: subscription.GetName(),
		MaxMessages:  1,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", pull.GetReceivedMessages())
	}
	received := pull.GetReceivedMessages()[0]
	if string(received.GetMessage().GetData()) != "hello over grpc" {
		t.Fatalf("message data = %q", received.GetMessage().GetData())
	}
	if received.GetAckId() == "" {
		t.Fatalf("ack id is empty")
	}

	if _, err := subscriber.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subscription.GetName(),
		AckIds:       []string{received.GetAckId()},
	}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	empty, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull after ack: %v", err)
	}
	if len(empty.GetReceivedMessages()) != 0 {
		t.Fatalf("received after ack = %#v", empty.GetReceivedMessages())
	}
}

func TestGRPCListTopicsAndSubscriptionsPaginate(t *testing.T) {
	_, ctx, publisher, subscriber := newPubSubGRPCTestClients(t, Config{Project: "devcloud"})
	topicNames := []string{
		"projects/devcloud/topics/alpha",
		"projects/devcloud/topics/bravo",
		"projects/devcloud/topics/charlie",
	}
	for _, name := range topicNames {
		if _, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
			t.Fatalf("CreateTopic %s: %v", name, err)
		}
	}
	subscriptions := []struct {
		name  string
		topic string
	}{
		{"projects/devcloud/subscriptions/orders-a", topicNames[0]},
		{"projects/devcloud/subscriptions/orders-b", topicNames[0]},
		{"projects/devcloud/subscriptions/orders-c", topicNames[1]},
	}
	for _, subscription := range subscriptions {
		if _, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subscription.name, Topic: subscription.topic}); err != nil {
			t.Fatalf("CreateSubscription %s: %v", subscription.name, err)
		}
	}

	topics, err := publisher.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/devcloud", PageSize: 2})
	if err != nil {
		t.Fatalf("ListTopics first page: %v", err)
	}
	if len(topics.GetTopics()) != 2 || topics.GetTopics()[0].GetName() != topicNames[0] || topics.GetNextPageToken() == "" {
		t.Fatalf("topics first page = %#v", topics)
	}
	nextTopics, err := publisher.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/devcloud", PageToken: topics.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListTopics second page: %v", err)
	}
	if len(nextTopics.GetTopics()) != 1 || nextTopics.GetTopics()[0].GetName() != topicNames[2] {
		t.Fatalf("topics second page = %#v", nextTopics)
	}
	if _, err := publisher.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/devcloud", PageToken: "not-an-offset"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListTopics invalid token error = %v, want InvalidArgument", err)
	}

	listedSubscriptions, err := subscriber.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/devcloud", PageSize: 2})
	if err != nil {
		t.Fatalf("ListSubscriptions first page: %v", err)
	}
	if len(listedSubscriptions.GetSubscriptions()) != 2 || listedSubscriptions.GetSubscriptions()[0].GetName() != subscriptions[0].name || listedSubscriptions.GetNextPageToken() == "" {
		t.Fatalf("subscriptions first page = %#v", listedSubscriptions)
	}
	topicSubscriptions, err := publisher.ListTopicSubscriptions(ctx, &pubsubpb.ListTopicSubscriptionsRequest{Topic: topicNames[0], PageSize: 1})
	if err != nil {
		t.Fatalf("ListTopicSubscriptions first page: %v", err)
	}
	if len(topicSubscriptions.GetSubscriptions()) != 1 || topicSubscriptions.GetSubscriptions()[0] != subscriptions[0].name || topicSubscriptions.GetNextPageToken() == "" {
		t.Fatalf("topic subscriptions first page = %#v", topicSubscriptions)
	}
	nextTopicSubscriptions, err := publisher.ListTopicSubscriptions(ctx, &pubsubpb.ListTopicSubscriptionsRequest{
		Topic:     topicNames[0],
		PageToken: topicSubscriptions.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListTopicSubscriptions second page: %v", err)
	}
	if len(nextTopicSubscriptions.GetSubscriptions()) != 1 || nextTopicSubscriptions.GetSubscriptions()[0] != subscriptions[1].name {
		t.Fatalf("topic subscriptions second page = %#v", nextTopicSubscriptions)
	}
}

func TestGRPCModifyAckDeadlineExtendsReleasesAndRejectsInvalidDeadlines(t *testing.T) {
	server, ctx, publisher, subscriber := newPubSubGRPCTestClients(t, Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     5,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/modify-deadline"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/modify-deadline-sub",
		Topic: topic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("extend")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1})
	if err != nil {
		t.Fatalf("Pull first: %v", err)
	}
	if len(first.GetReceivedMessages()) != 1 {
		t.Fatalf("first pull = %#v", first.GetReceivedMessages())
	}
	ackID := first.GetReceivedMessages()[0].GetAckId()
	if _, err := subscriber.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subscription.GetName(),
		AckIds:             []string{ackID},
		AckDeadlineSeconds: 5,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline extend: %v", err)
	}

	now = now.Add(3 * time.Second)
	stillLeased, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1, ReturnImmediately: true})
	if err != nil {
		t.Fatalf("Pull while extended: %v", err)
	}
	if len(stillLeased.GetReceivedMessages()) != 0 {
		t.Fatalf("extended lease should hide message, got %#v", stillLeased.GetReceivedMessages())
	}
	now = now.Add(3 * time.Second)
	redelivery, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1})
	if err != nil {
		t.Fatalf("Pull redelivery: %v", err)
	}
	if len(redelivery.GetReceivedMessages()) != 1 || redelivery.GetReceivedMessages()[0].GetDeliveryAttempt() != 2 {
		t.Fatalf("redelivery = %#v", redelivery.GetReceivedMessages())
	}
	if _, err := subscriber.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       subscription.GetName(),
		AckIds:             []string{redelivery.GetReceivedMessages()[0].GetAckId()},
		AckDeadlineSeconds: 0,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline release: %v", err)
	}
	released, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1})
	if err != nil {
		t.Fatalf("Pull released: %v", err)
	}
	if len(released.GetReceivedMessages()) != 1 || released.GetReceivedMessages()[0].GetDeliveryAttempt() != 3 {
		t.Fatalf("released redelivery = %#v", released.GetReceivedMessages())
	}
	if _, err := subscriber.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: subscription.GetName(), AckDeadlineSeconds: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative deadline error = %v, want InvalidArgument", err)
	}
	if _, err := subscriber.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: subscription.GetName(), AckDeadlineSeconds: 6}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized deadline error = %v, want InvalidArgument", err)
	}
}

func TestReleaseAckIDLockedClearsOnlyMatchingUnackedDelivery(t *testing.T) {
	leaseDeadline := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	nextDeliveryTime := leaseDeadline.Add(time.Second)
	deliveries := []deliveryRecord{
		{MessageID: "message-1", AckID: "ack-1", LeaseDeadline: leaseDeadline, NextDeliveryTime: nextDeliveryTime, DeliveryAttempt: 1},
		{MessageID: "message-2", AckID: "ack-2", LeaseDeadline: leaseDeadline, NextDeliveryTime: nextDeliveryTime, DeliveryAttempt: 1, Acked: true},
	}

	releaseAckIDLocked(deliveries, "ack-1")
	releaseAckIDLocked(deliveries, "ack-2")

	if deliveries[0].AckID != "" || !deliveries[0].LeaseDeadline.IsZero() || !deliveries[0].NextDeliveryTime.IsZero() {
		t.Fatalf("matching unacked delivery was not released: %#v", deliveries[0])
	}
	if deliveries[1].AckID != "ack-2" || deliveries[1].LeaseDeadline.IsZero() || deliveries[1].NextDeliveryTime.IsZero() {
		t.Fatalf("acked delivery should remain unchanged: %#v", deliveries[1])
	}
}

func TestGRPCCreateSubscriptionAndSnapshotAssignNames(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/assigned-names"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription without name: %v", err)
	}
	if subscription.GetName() != "projects/devcloud/subscriptions/devcloud-auto-sub-1" {
		t.Fatalf("assigned subscription name = %q", subscription.GetName())
	}

	snapshot, err := subscriber.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Subscription: subscription.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot without name: %v", err)
	}
	if snapshot.GetName() != "projects/devcloud/snapshots/devcloud-auto-snapshot-1" {
		t.Fatalf("assigned snapshot name = %q", snapshot.GetName())
	}
	if snapshot.GetTopic() != topic.GetName() {
		t.Fatalf("snapshot topic = %q", snapshot.GetTopic())
	}
}

func TestGRPCUpdateTopicSubscriptionAndSnapshotMetadata(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 10})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/update-orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	updatedTopic, err := publisher.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic: &pubsubpb.Topic{
			Name:                     topic.GetName(),
			Labels:                   map[string]string{"env": "local"},
			MessageRetentionDuration: durationpb.New(24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "message_retention_duration"}},
	})
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if updatedTopic.GetLabels()["env"] != "local" || updatedTopic.GetMessageRetentionDuration().AsDuration() != 24*time.Hour {
		t.Fatalf("updated topic = %#v", updatedTopic)
	}

	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/update-orders-sub",
		Topic: topic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	updatedSubscription, err := subscriber.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{
			Name:                  subscription.GetName(),
			Labels:                map[string]string{"team": "compat"},
			AckDeadlineSeconds:    20,
			EnableMessageOrdering: true,
			RetryPolicy: &pubsubpb.RetryPolicy{
				MinimumBackoff: durationpb.New(2 * time.Second),
				MaximumBackoff: durationpb.New(10 * time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "ack_deadline_seconds", "enable_message_ordering", "retry_policy"}},
	})
	if err != nil {
		t.Fatalf("UpdateSubscription: %v", err)
	}
	if updatedSubscription.GetLabels()["team"] != "compat" || updatedSubscription.GetAckDeadlineSeconds() != 20 || !updatedSubscription.GetEnableMessageOrdering() {
		t.Fatalf("updated subscription = %#v", updatedSubscription)
	}
	if updatedSubscription.GetRetryPolicy().GetMinimumBackoff().AsDuration() != 2*time.Second {
		t.Fatalf("updated retry policy = %#v", updatedSubscription.GetRetryPolicy())
	}

	snapshot, err := subscriber.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name:         "projects/devcloud/snapshots/update-orders-snapshot",
		Subscription: subscription.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	updatedSnapshot, err := subscriber.UpdateSnapshot(ctx, &pubsubpb.UpdateSnapshotRequest{
		Snapshot:   &pubsubpb.Snapshot{Name: snapshot.GetName(), Labels: map[string]string{"purpose": "seek"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateSnapshot: %v", err)
	}
	if updatedSnapshot.GetLabels()["purpose"] != "seek" {
		t.Fatalf("updated snapshot = %#v", updatedSnapshot)
	}
}

func TestPubSubGoClientCompatibilitySmoke(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", "bufnet")

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)

	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/client-smoke"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/client-smoke-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	published, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{
			Data:       []byte("devcloud pubsub client smoke"),
			Attributes: map[string]string{"source": "go-client"},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(published.GetMessageIds()) != 1 {
		t.Fatalf("message ids = %#v", published.GetMessageIds())
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
		MaxOutstandingBytes:      1024 * 1024,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv streaming pull response: %v", err)
	}
	if len(response.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", response.GetReceivedMessages())
	}
	received := response.GetReceivedMessages()[0]
	if string(received.GetMessage().GetData()) != "devcloud pubsub client smoke" {
		t.Fatalf("message data = %q", received.GetMessage().GetData())
	}
	if received.GetMessage().GetAttributes()["source"] != "go-client" {
		t.Fatalf("received attributes = %#v", received.GetMessage().GetAttributes())
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		AckIds: []string{received.GetAckId()},
	}); err != nil {
		t.Fatalf("send streaming ack: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	deleted, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull after streaming ack: %v", err)
	}
	if len(deleted.GetReceivedMessages()) != 0 {
		t.Fatalf("received after streaming ack = %#v", deleted.GetReceivedMessages())
	}
}

func TestGRPCStreamingPullReceivesAndAcknowledges(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-orders-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{
			Data:       []byte("streaming hello"),
			Attributes: map[string]string{"source": "streaming-test"},
		}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
		MaxOutstandingBytes:      1024 * 1024,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv streaming pull response: %v", err)
	}
	if len(response.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", response.GetReceivedMessages())
	}
	received := response.GetReceivedMessages()[0]
	if string(received.GetMessage().GetData()) != "streaming hello" {
		t.Fatalf("message data = %q", received.GetMessage().GetData())
	}
	if received.GetAckId() == "" {
		t.Fatalf("ack id is empty")
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		AckIds: []string{received.GetAckId()},
	}); err != nil {
		t.Fatalf("send ack: %v", err)
	}
	for {
		snapshot := server.Snapshot()
		if len(snapshot.Subscriptions) == 1 && snapshot.Subscriptions[0].TotalRetainedMessages == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("streaming ack was not applied before context deadline")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	empty, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull after streaming ack: %v", err)
	}
	if len(empty.GetReceivedMessages()) != 0 {
		t.Fatalf("received after streaming ack = %#v", empty.GetReceivedMessages())
	}
}

func TestGRPCStreamingPullCanBeDisabled(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", StreamingPullDisabled: true})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	stream, err := pubsubpb.NewSubscriberClient(conn).StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/devcloud/subscriptions/disabled-stream",
		StreamAckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("StreamingPull disabled error = %v, want %s", err, codes.Unimplemented)
	}
}

func TestGRPCStreamingPullHonorsMaxOutstandingMessages(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-flow"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-flow-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("first")},
			{Data: []byte("second")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	firstResponse, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv first streaming pull response: %v", err)
	}
	if len(firstResponse.GetReceivedMessages()) != 1 || string(firstResponse.GetReceivedMessages()[0].GetMessage().GetData()) != "first" {
		t.Fatalf("first response = %#v", firstResponse.GetReceivedMessages())
	}

	secondResponses := make(chan *pubsubpb.StreamingPullResponse, 1)
	secondErrs := make(chan error, 1)
	go func() {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			secondErrs <- recvErr
			return
		}
		secondResponses <- response
	}()

	select {
	case response := <-secondResponses:
		t.Fatalf("received second response before ack: %#v", response.GetReceivedMessages())
	case err := <-secondErrs:
		t.Fatalf("recv second before ack: %v", err)
	case <-time.After(80 * time.Millisecond):
	}

	if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{firstResponse.GetReceivedMessages()[0].GetAckId()}}); err != nil {
		t.Fatalf("send streaming ack: %v", err)
	}
	select {
	case response := <-secondResponses:
		if len(response.GetReceivedMessages()) != 1 || string(response.GetReceivedMessages()[0].GetMessage().GetData()) != "second" {
			t.Fatalf("second response = %#v", response.GetReceivedMessages())
		}
	case err := <-secondErrs:
		t.Fatalf("recv second after ack: %v", err)
	case <-ctx.Done():
		t.Fatalf("second response was not delivered after ack: %v", ctx.Err())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func TestGRPCStreamingPullByteFlowDoesNotChargeUnsentAttempts(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-byte-flow"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-byte-flow-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("a")},
			{Data: []byte(strings.Repeat("b", 128))},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   2,
		MaxOutstandingBytes:      16,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	firstResponse, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv first streaming pull response: %v", err)
	}
	if len(firstResponse.GetReceivedMessages()) != 1 || string(firstResponse.GetReceivedMessages()[0].GetMessage().GetData()) != "a" {
		t.Fatalf("first response = %#v", firstResponse.GetReceivedMessages())
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{firstResponse.GetReceivedMessages()[0].GetAckId()}}); err != nil {
		t.Fatalf("send first ack: %v", err)
	}
	secondResponse, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv second streaming pull response: %v", err)
	}
	if len(secondResponse.GetReceivedMessages()) != 1 {
		t.Fatalf("second response = %#v", secondResponse.GetReceivedMessages())
	}
	second := secondResponse.GetReceivedMessages()[0]
	if string(second.GetMessage().GetData()) != strings.Repeat("b", 128) {
		t.Fatalf("second data = %q", second.GetMessage().GetData())
	}
	if second.GetDeliveryAttempt() != 1 {
		t.Fatalf("second delivery attempt = %d, want 1", second.GetDeliveryAttempt())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func TestGRPCStreamingPullModifyDeadlineReleasesMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-release"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-release-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("release me")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv streaming pull response: %v", err)
	}
	if len(response.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", response.GetReceivedMessages())
	}
	ackID := response.GetReceivedMessages()[0].GetAckId()
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		ModifyDeadlineAckIds:  []string{ackID},
		ModifyDeadlineSeconds: []int32{0},
	}); err != nil {
		t.Fatalf("send modify deadline: %v", err)
	}
	redelivery, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv redelivery after streaming modify deadline: %v", err)
	}
	if len(redelivery.GetReceivedMessages()) != 1 {
		t.Fatalf("redelivered messages = %#v", redelivery.GetReceivedMessages())
	}
	if string(redelivery.GetReceivedMessages()[0].GetMessage().GetData()) != "release me" {
		t.Fatalf("redelivered message data = %q", redelivery.GetReceivedMessages()[0].GetMessage().GetData())
	}
	if redelivery.GetReceivedMessages()[0].GetAckId() == ackID {
		t.Fatalf("redelivery reused ack id %q", ackID)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func TestGRPCStreamingPullRedeliversAfterStreamAckDeadlineExpires(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 1})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-deadline"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-deadline-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 1,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("deadline redelivery")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 1,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv first streaming pull response: %v", err)
	}
	if len(first.GetReceivedMessages()) != 1 {
		t.Fatalf("first received messages = %#v", first.GetReceivedMessages())
	}
	firstAckID := first.GetReceivedMessages()[0].GetAckId()
	now = now.Add(2 * time.Second)

	redelivery, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv redelivery after stream ack deadline: %v", err)
	}
	if len(redelivery.GetReceivedMessages()) != 1 {
		t.Fatalf("redelivered messages = %#v", redelivery.GetReceivedMessages())
	}
	received := redelivery.GetReceivedMessages()[0]
	if string(received.GetMessage().GetData()) != "deadline redelivery" {
		t.Fatalf("redelivered message data = %q", received.GetMessage().GetData())
	}
	if received.GetAckId() == "" || received.GetAckId() == firstAckID {
		t.Fatalf("redelivery ack id = %q, first ack id = %q", received.GetAckId(), firstAckID)
	}
	if received.GetDeliveryAttempt() != 2 {
		t.Fatalf("redelivery attempt = %d, want 2", received.GetDeliveryAttempt())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func TestGRPCStreamingPullCancellationReturnsPromptly(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-cancel"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-cancel-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	streamCtx, stopStream := context.WithCancel(ctx)
	stream, err := subscriber.StreamingPull(streamCtx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, recvErr := stream.Recv()
		done <- recvErr
	}()
	stopStream()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Recv after cancel error = nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("StreamingPull Recv did not return promptly after cancellation")
	}
}

func TestGRPCStreamingPullCloseSendReleasesOutstandingMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-release-on-close"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-release-on-close-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("release on close")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv streaming pull response: %v", err)
	}
	if len(response.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", response.GetReceivedMessages())
	}
	streamAckID := response.GetReceivedMessages()[0].GetAckId()
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	for {
		pull, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
			Subscription:      subscription.GetName(),
			MaxMessages:       1,
			ReturnImmediately: true,
		})
		if err != nil {
			t.Fatalf("Pull after stream close: %v", err)
		}
		if len(pull.GetReceivedMessages()) == 1 {
			if pull.GetReceivedMessages()[0].GetAckId() == streamAckID {
				t.Fatalf("redelivery reused ack id %q", streamAckID)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("outstanding message was not released before context deadline")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestGRPCStreamingPullRespectsOrderingKeyGate(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-ordering"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                  "projects/devcloud/subscriptions/streaming-ordering-sub",
		Topic:                 topic.GetName(),
		AckDeadlineSeconds:    30,
		EnableMessageOrdering: true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{
			{Data: []byte("first"), OrderingKey: "customer-1"},
			{Data: []byte("second"), OrderingKey: "customer-1"},
			{Data: []byte("other"), OrderingKey: "customer-2"},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 30,
		MaxOutstandingMessages:   2,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv streaming pull response: %v", err)
	}
	if len(response.GetReceivedMessages()) != 2 {
		t.Fatalf("received messages = %#v", response.GetReceivedMessages())
	}
	first := response.GetReceivedMessages()[0]
	other := response.GetReceivedMessages()[1]
	if string(first.GetMessage().GetData()) != "first" || first.GetMessage().GetOrderingKey() != "customer-1" {
		t.Fatalf("first ordered message = %#v", first.GetMessage())
	}
	if string(other.GetMessage().GetData()) != "other" || other.GetMessage().GetOrderingKey() != "customer-2" {
		t.Fatalf("other ordered message = %#v", other.GetMessage())
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{first.GetAckId()}}); err != nil {
		t.Fatalf("send first ack: %v", err)
	}
	next, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv second ordering-key message: %v", err)
	}
	if len(next.GetReceivedMessages()) != 1 {
		t.Fatalf("next received messages = %#v", next.GetReceivedMessages())
	}
	if string(next.GetReceivedMessages()[0].GetMessage().GetData()) != "second" || next.GetReceivedMessages()[0].GetMessage().GetOrderingKey() != "customer-1" {
		t.Fatalf("second ordered message = %#v", next.GetReceivedMessages()[0].GetMessage())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func TestGRPCStreamingPullDeadLetterPolicyTransfersAfterMaxDeliveryAttempts(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 1})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-dlq"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	dlqTopic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/streaming-dlq-target"})
	if err != nil {
		t.Fatalf("CreateTopic dlq: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/streaming-dlq-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 1,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.GetName(),
			MaxDeliveryAttempts: 5,
		},
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	dlqSubscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/streaming-dlq-target-sub",
		Topic: dlqTopic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription dlq: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{
			Data:        []byte("to dlq"),
			Attributes:  map[string]string{"kind": "retry"},
			OrderingKey: "customer-1",
		}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream, err := subscriber.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull: %v", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subscription.GetName(),
		StreamAckDeadlineSeconds: 1,
		MaxOutstandingMessages:   1,
	}); err != nil {
		t.Fatalf("send initial streaming pull request: %v", err)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv attempt %d: %v", attempt, err)
		}
		if len(response.GetReceivedMessages()) != 1 || response.GetReceivedMessages()[0].GetDeliveryAttempt() != int32(attempt) {
			t.Fatalf("attempt %d received messages = %#v", attempt, response.GetReceivedMessages())
		}
		if err := stream.Send(&pubsubpb.StreamingPullRequest{
			ModifyDeadlineAckIds:  []string{response.GetReceivedMessages()[0].GetAckId()},
			ModifyDeadlineSeconds: []int32{0},
		}); err != nil {
			t.Fatalf("release attempt %d: %v", attempt, err)
		}
	}

	for {
		snapshot := server.Snapshot()
		for _, subscription := range snapshot.Subscriptions {
			if subscription.Name == dlqSubscription.GetName() && subscription.TotalRetainedMessages == 1 {
				goto dlqReady
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("dead-letter transfer was not applied before context deadline")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
dlqReady:
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	dlqPull, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      dlqSubscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull dlq: %v", err)
	}
	if len(dlqPull.GetReceivedMessages()) != 1 {
		t.Fatalf("dlq received messages = %#v", dlqPull.GetReceivedMessages())
	}
	message := dlqPull.GetReceivedMessages()[0].GetMessage()
	if string(message.GetData()) != "to dlq" || message.GetAttributes()["kind"] != "retry" || message.GetOrderingKey() != "customer-1" {
		t.Fatalf("dlq message = %#v", message)
	}
}

func TestGRPCRejectsMissingDeadLetterTopic(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/grpc-missing-dlq"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err = subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/grpc-missing-dlq-sub",
		Topic: topic.GetName(),
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     "projects/devcloud/topics/grpc-missing-dlq-target",
			MaxDeliveryAttempts: 5,
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("CreateSubscription missing DLQ error = %v, want %s", err, codes.NotFound)
	}

	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/grpc-existing-sub",
		Topic: topic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription existing: %v", err)
	}
	_, err = subscriber.UpdateSubscription(ctx, &pubsubpb.UpdateSubscriptionRequest{
		Subscription: &pubsubpb.Subscription{
			Name: subscription.GetName(),
			DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
				DeadLetterTopic:     "projects/devcloud/topics/grpc-missing-dlq-target",
				MaxDeliveryAttempts: 5,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dead_letter_policy"}},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("UpdateSubscription missing DLQ error = %v, want %s", err, codes.NotFound)
	}
}

func TestPubSubGoClientEmulatorPublishReceiveAckAndCleanup(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 10})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	t.Setenv("PUBSUB_EMULATOR_HOST", "bufnet")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	endpoint := os.Getenv("PUBSUB_EMULATOR_HOST")
	if endpoint == "" {
		t.Fatalf("PUBSUB_EMULATOR_HOST is empty")
	}
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial emulator: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)

	const googlePubSubGoClientPackage = "cloud.google.com/go/pubsub"
	_ = googlePubSubGoClientPackage
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/client-smoke-topic"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	t.Cleanup(func() {
		_, _ = publisher.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: topic.GetName()})
	})
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/devcloud/subscriptions/client-smoke-sub",
		Topic:              topic.GetName(),
		AckDeadlineSeconds: 10,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	t.Cleanup(func() {
		_, _ = subscriber.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: subscription.GetName()})
	})

	publish, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{
			Data:       []byte("sdk smoke"),
			Attributes: map[string]string{"source": "go-client"},
		}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(publish.GetMessageIds()) != 1 {
		t.Fatalf("message ids = %#v", publish.GetMessageIds())
	}
	pull, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("received messages = %#v", pull.GetReceivedMessages())
	}
	received := pull.GetReceivedMessages()[0]
	if string(received.GetMessage().GetData()) != "sdk smoke" || received.GetMessage().GetAttributes()["source"] != "go-client" || received.GetAckId() == "" {
		t.Fatalf("received message = %#v", received)
	}
	if _, err := subscriber.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subscription.GetName(),
		AckIds:       []string{received.GetAckId()},
	}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if _, err := subscriber.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: subscription.GetName()}); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if _, err := publisher.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic.GetName()}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := subscriber.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subscription.GetName()}); err == nil {
		t.Fatalf("GetSubscription after delete error = nil")
	}
	if _, err := publisher.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: topic.GetName()}); err == nil {
		t.Fatalf("GetTopic after delete error = nil")
	}
}

func TestGRPCSnapshotCRUDAndSeek(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/snapshot-orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/snapshot-orders-sub",
		Topic: topic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("snapshot replay")}},
	}); err != nil {
		t.Fatalf("Publish before snapshot: %v", err)
	}
	snapshot, err := subscriber.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name:         "projects/devcloud/snapshots/orders-snapshot-grpc",
		Subscription: subscription.GetName(),
		Labels:       map[string]string{"kind": "test"},
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snapshot.GetName() != "projects/devcloud/snapshots/orders-snapshot-grpc" || snapshot.GetTopic() != topic.GetName() || snapshot.GetLabels()["kind"] != "test" || snapshot.GetExpireTime() == nil {
		t.Fatalf("snapshot metadata mismatch")
	}
	if _, err := subscriber.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snapshot.GetName()}); err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	list, err := subscriber.ListSnapshots(ctx, &pubsubpb.ListSnapshotsRequest{Project: "projects/devcloud"})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(list.GetSnapshots()) != 1 || list.GetSnapshots()[0].GetName() != snapshot.GetName() {
		t.Fatalf("ListSnapshots returned unexpected metadata")
	}
	topicSnapshots, err := publisher.ListTopicSnapshots(ctx, &pubsubpb.ListTopicSnapshotsRequest{Topic: topic.GetName()})
	if err != nil {
		t.Fatalf("ListTopicSnapshots: %v", err)
	}
	if len(topicSnapshots.GetSnapshots()) != 1 || topicSnapshots.GetSnapshots()[0] != snapshot.GetName() {
		t.Fatalf("ListTopicSnapshots returned %#v", topicSnapshots.GetSnapshots())
	}

	pull, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull before seek: %v", err)
	}
	if len(pull.GetReceivedMessages()) != 1 {
		t.Fatalf("Pull before seek returned unexpected count")
	}
	if _, err := subscriber.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: subscription.GetName(),
		AckIds:       []string{pull.GetReceivedMessages()[0].GetAckId()},
	}); err != nil {
		t.Fatalf("Acknowledge before seek: %v", err)
	}
	if _, err := subscriber.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: subscription.GetName(),
		Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: snapshot.GetName()},
	}); err != nil {
		t.Fatalf("Seek by snapshot: %v", err)
	}
	replayed, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       1,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull after snapshot seek: %v", err)
	}
	if len(replayed.GetReceivedMessages()) != 1 {
		t.Fatalf("Pull after snapshot seek returned unexpected count")
	}

	now = now.Add(10 * time.Second)
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("after seek time")}},
	}); err != nil {
		t.Fatalf("Publish after snapshot: %v", err)
	}
	if _, err := subscriber.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: subscription.GetName(),
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(now.Add(-time.Second))},
	}); err != nil {
		t.Fatalf("Seek by time: %v", err)
	}
	afterTime, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{
		Subscription:      subscription.GetName(),
		MaxMessages:       10,
		ReturnImmediately: true,
	})
	if err != nil {
		t.Fatalf("Pull after time seek: %v", err)
	}
	if len(afterTime.GetReceivedMessages()) != 1 || string(afterTime.GetReceivedMessages()[0].GetMessage().GetData()) != "after seek time" {
		t.Fatalf("Pull after time seek returned unexpected messages")
	}
	if _, err := subscriber.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: snapshot.GetName()}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
}

func TestGRPCSchemaServiceCRUDAndValidateMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := pubsubpb.NewSchemaServiceClient(conn)
	created, err := client.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
		Parent:   "projects/devcloud",
		SchemaId: "order-event",
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"OrderEvent","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	if created.GetName() != "projects/devcloud/schemas/order-event" || created.GetType() != pubsubpb.Schema_AVRO || created.GetRevisionId() != "1" {
		t.Fatalf("created schema = %#v", created)
	}

	basic, err := client.GetSchema(ctx, &pubsubpb.GetSchemaRequest{
		Name: created.GetName(),
		View: pubsubpb.SchemaView_BASIC,
	})
	if err != nil {
		t.Fatalf("GetSchema BASIC: %v", err)
	}
	if basic.GetDefinition() != "" {
		t.Fatalf("BASIC schema leaked definition")
	}
	list, err := client.ListSchemas(ctx, &pubsubpb.ListSchemasRequest{
		Parent:   "projects/devcloud",
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("ListSchemas: %v", err)
	}
	if len(list.GetSchemas()) != 1 || list.GetSchemas()[0].GetName() != created.GetName() || list.GetSchemas()[0].GetDefinition() != "" {
		t.Fatalf("schema list = %#v", list.GetSchemas())
	}
	if _, err := client.ValidateSchema(ctx, &pubsubpb.ValidateSchemaRequest{
		Parent: "projects/devcloud",
		Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_PROTOCOL_BUFFER, Definition: "message OrderEvent {}"},
	}); err != nil {
		t.Fatalf("ValidateSchema: %v", err)
	}
	if _, err := client.ValidateSchema(ctx, &pubsubpb.ValidateSchemaRequest{
		Parent: "projects/devcloud",
		Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: `not-json`},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ValidateSchema invalid AVRO code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent:     "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Name{Name: created.GetName()},
		Message:    []byte(`{}`),
		Encoding:   pubsubpb.Encoding_JSON,
	}); err != nil {
		t.Fatalf("ValidateMessage existing schema: %v", err)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent:     "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Name{Name: created.GetName()},
		Message:    []byte(`not-json`),
		Encoding:   pubsubpb.Encoding_JSON,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ValidateMessage invalid JSON code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent: "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Schema{Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
			Definition: "message OrderEvent {}",
		}},
		Message:  []byte("order"),
		Encoding: pubsubpb.Encoding_BINARY,
	}); err != nil {
		t.Fatalf("ValidateMessage inline schema: %v", err)
	}
	committed, err := client.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
		Name: created.GetName(),
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"OrderEventV2","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CommitSchema: %v", err)
	}
	if committed.GetRevisionId() != "2" || committed.GetDefinition() == created.GetDefinition() || committed.GetRevisionCreateTime() == nil {
		t.Fatalf("committed schema = %#v", committed)
	}
	if _, err := client.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
		Name: created.GetName(),
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `["not","an","object"]`,
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CommitSchema invalid AVRO code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	revisions, err := client.ListSchemaRevisions(ctx, &pubsubpb.ListSchemaRevisionsRequest{
		Name: created.GetName(),
		View: pubsubpb.SchemaView_FULL,
	})
	if err != nil {
		t.Fatalf("ListSchemaRevisions: %v", err)
	}
	if len(revisions.GetSchemas()) != 2 || revisions.GetSchemas()[0].GetRevisionId() != "1" || revisions.GetSchemas()[1].GetRevisionId() != "2" {
		t.Fatalf("schema revisions = %#v", revisions.GetSchemas())
	}
	rolledBack, err := client.RollbackSchema(ctx, &pubsubpb.RollbackSchemaRequest{
		Name:       created.GetName(),
		RevisionId: "1",
	})
	if err != nil {
		t.Fatalf("RollbackSchema: %v", err)
	}
	if rolledBack.GetRevisionId() != "3" || rolledBack.GetDefinition() != created.GetDefinition() {
		t.Fatalf("rolled back schema = %#v", rolledBack)
	}
	afterDelete, err := client.DeleteSchemaRevision(ctx, &pubsubpb.DeleteSchemaRevisionRequest{
		Name: created.GetName() + "@2",
	})
	if err != nil {
		t.Fatalf("DeleteSchemaRevision: %v", err)
	}
	if afterDelete.GetRevisionId() != "3" {
		t.Fatalf("schema after deleting revision = %#v", afterDelete)
	}
	if _, err := client.DeleteSchema(ctx, &pubsubpb.DeleteSchemaRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}
	if _, err := client.GetSchema(ctx, &pubsubpb.GetSchemaRequest{Name: created.GetName()}); err == nil {
		t.Fatalf("GetSchema after delete error = nil")
	}
}

func TestGRPCTopicSchemaSettingsRoundTripAndPublishValidation(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	schemaClient := pubsubpb.NewSchemaServiceClient(conn)
	schema, err := schemaClient.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
		Parent:   "projects/devcloud",
		SchemaId: "topic-schema",
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"TopicSchema","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	publisher := pubsubpb.NewPublisherClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{
		Name: "projects/devcloud/topics/schema-topic",
		SchemaSettings: &pubsubpb.SchemaSettings{
			Schema:          schema.GetName(),
			Encoding:        pubsubpb.Encoding_JSON,
			FirstRevisionId: "1",
			LastRevisionId:  "1",
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if topic.GetSchemaSettings().GetSchema() != schema.GetName() || topic.GetSchemaSettings().GetEncoding() != pubsubpb.Encoding_JSON || topic.GetSchemaSettings().GetFirstRevisionId() != "1" || topic.GetSchemaSettings().GetLastRevisionId() != "1" {
		t.Fatalf("created topic schema settings = %#v", topic.GetSchemaSettings())
	}

	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("not-json")}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Publish invalid JSON code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte(`{"ok":true}`)}},
	}); err != nil {
		t.Fatalf("Publish valid JSON: %v", err)
	}

	updated, err := publisher.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic: &pubsubpb.Topic{
			Name: topic.GetName(),
			SchemaSettings: &pubsubpb.SchemaSettings{
				Schema:   schema.GetName(),
				Encoding: pubsubpb.Encoding_BINARY,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schema_settings"}},
	})
	if err != nil {
		t.Fatalf("UpdateTopic schema settings: %v", err)
	}
	if updated.GetSchemaSettings().GetEncoding() != pubsubpb.Encoding_BINARY {
		t.Fatalf("updated topic schema settings = %#v", updated.GetSchemaSettings())
	}
}

func TestGRPCPushSubscriptionDeliveryUsesPushAndRetryPolicy(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 2})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/grpc-push"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/grpc-push-sub",
		Topic: topic.GetName(),
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: "http://127.0.0.1/push",
			Attributes:   map[string]string{"x-goog-version": "v1"},
		},
		RetryPolicy: &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(5 * time.Second),
			MaximumBackoff: durationpb.New(10 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if subscription.GetPushConfig().GetPushEndpoint() != "http://127.0.0.1/push" || subscription.GetPushConfig().GetAttributes()["x-goog-version"] != "v1" {
		t.Fatalf("created push config = %#v", subscription.GetPushConfig())
	}
	if subscription.GetRetryPolicy().GetMinimumBackoff().AsDuration() != 5*time.Second || subscription.GetRetryPolicy().GetMaximumBackoff().AsDuration() != 10*time.Second {
		t.Fatalf("created retry policy = %#v", subscription.GetRetryPolicy())
	}
	got, err := subscriber.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subscription.GetName()})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetPushConfig().GetPushEndpoint() != "http://127.0.0.1/push" || got.GetRetryPolicy().GetMinimumBackoff().AsDuration() != 5*time.Second {
		t.Fatalf("GetSubscription returned %#v", got)
	}

	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("grpc push")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	attempts := 0
	responseStatus := http.StatusInternalServerError
	var pushed struct {
		Subscription    string `json:"subscription"`
		DeliveryAttempt int    `json:"deliveryAttempt"`
		Message         struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"message"`
	}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		attempts++
		if r.URL.String() != "http://127.0.0.1/push" {
			t.Errorf("push endpoint = %s", r.URL.String())
		}
		if err := json.NewDecoder(r.Body).Decode(&pushed); err != nil {
			t.Errorf("decode push body: %v", err)
		}
		return &http.Response{StatusCode: responseStatus, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})}
	delivered, err := server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("deliverPush: %v", err)
	}
	if !delivered || attempts != 1 {
		t.Fatalf("first push delivered=%v attempts=%d", delivered, attempts)
	}
	if pushed.Subscription != subscription.GetName() || pushed.DeliveryAttempt != 1 || pushed.Message.DeliveryAttempt != 1 {
		t.Fatalf("first push metadata = %#v", pushed)
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || len(snapshot.Subscriptions[0].RecentDeliveries) != 1 {
		t.Fatalf("snapshot after failed grpc push = %#v", snapshot.Subscriptions)
	}
	recent := snapshot.Subscriptions[0].RecentDeliveries[0]
	if recent.State != "delayed" || recent.NextDeliveryTime != now.Add(5*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("grpc retry state = %#v", recent)
	}
	if delivered, err := server.deliverPush(context.Background(), client); err != nil || delivered {
		t.Fatalf("push before retry backoff delivered=%v err=%v", delivered, err)
	}
	now = now.Add(5 * time.Second)
	responseStatus = http.StatusNoContent
	delivered, err = server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("retry deliverPush: %v", err)
	}
	if !delivered || attempts != 2 {
		t.Fatalf("retry push delivered=%v attempts=%d", delivered, attempts)
	}
	if pushed.DeliveryAttempt != 2 || pushed.Message.DeliveryAttempt != 2 {
		t.Fatalf("retry push metadata = %#v", pushed)
	}
	snapshot = server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("snapshot after successful grpc push = %#v", snapshot.Subscriptions)
	}
}

func TestGRPCModifyPushConfigAndDetachSubscription(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	publisher := pubsubpb.NewPublisherClient(conn)
	subscriber := pubsubpb.NewSubscriberClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/devcloud/topics/grpc-modify-push"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subscription, err := subscriber.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  "projects/devcloud/subscriptions/grpc-modify-push-sub",
		Topic: topic.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("detach backlog")}},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	snapshot, err := subscriber.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name:         "projects/devcloud/snapshots/grpc-detach-snapshot",
		Subscription: subscription.GetName(),
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if _, err := subscriber.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{
		Subscription: subscription.GetName(),
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: "http://127.0.0.1/push",
			Attributes:   map[string]string{"x-goog-version": "v1"},
		},
	}); err != nil {
		t.Fatalf("ModifyPushConfig set: %v", err)
	}
	withPush, err := subscriber.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subscription.GetName()})
	if err != nil {
		t.Fatalf("GetSubscription with push: %v", err)
	}
	if withPush.GetPushConfig().GetPushEndpoint() != "http://127.0.0.1/push" {
		t.Fatalf("push endpoint = %q", withPush.GetPushConfig().GetPushEndpoint())
	}
	if _, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1, ReturnImmediately: true}); err == nil {
		t.Fatalf("Pull with push config error = nil")
	}
	if _, err := subscriber.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{Subscription: subscription.GetName()}); err != nil {
		t.Fatalf("ModifyPushConfig clear: %v", err)
	}
	withoutPush, err := subscriber.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subscription.GetName()})
	if err != nil {
		t.Fatalf("GetSubscription after clear: %v", err)
	}
	if withoutPush.GetPushConfig() != nil {
		t.Fatalf("push config after clear = %#v", withoutPush.GetPushConfig())
	}

	if _, err := publisher.DetachSubscription(ctx, &pubsubpb.DetachSubscriptionRequest{Subscription: subscription.GetName()}); err != nil {
		t.Fatalf("DetachSubscription: %v", err)
	}
	detached, err := subscriber.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subscription.GetName()})
	if err != nil {
		t.Fatalf("GetSubscription after detach: %v", err)
	}
	if !detached.GetDetached() {
		t.Fatalf("detached flag = false")
	}
	if _, err := subscriber.Pull(ctx, &pubsubpb.PullRequest{Subscription: subscription.GetName(), MaxMessages: 1, ReturnImmediately: true}); err == nil {
		t.Fatalf("Pull after detach error = nil")
	}
	if _, err := subscriber.GetSnapshot(ctx, &pubsubpb.GetSnapshotRequest{Snapshot: snapshot.GetName()}); err == nil {
		t.Fatalf("GetSnapshot after detach error = nil")
	}
}

func TestRESTListsEmptyTopics(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var body struct {
		Topics []any `json:"topics"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Topics == nil || len(body.Topics) != 0 {
		t.Fatalf("topics = %#v, want empty array", body.Topics)
	}
}

func TestRESTRejectsUnsupportedTopicMethod(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/devcloud/topics", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET" {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func TestRESTAuthModes(t *testing.T) {
	relaxed := NewServer(Config{Project: "devcloud", AuthMode: "relaxed"})
	if rec := performPubSubRequest(relaxed, http.MethodGet, "/v1/projects/devcloud/topics", ""); rec.Code != http.StatusOK {
		t.Fatalf("relaxed status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	oauthRelaxed := NewServer(Config{Project: "devcloud", AuthMode: "oauth-relaxed"})
	if rec := performPubSubRequest(oauthRelaxed, http.MethodGet, "/v1/projects/devcloud/topics", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("oauth-relaxed missing bearer status = %d, want %d: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	req.Header.Set("Authorization", "Bearer local-token")
	oauthAuthorized := httptest.NewRecorder()
	oauthRelaxed.ServeHTTP(oauthAuthorized, req)
	if oauthAuthorized.Code != http.StatusOK {
		t.Fatalf("oauth-relaxed bearer status = %d, want %d: %s", oauthAuthorized.Code, http.StatusOK, oauthAuthorized.Body.String())
	}

	strict := NewServer(Config{Project: "devcloud", AuthMode: "strict", BearerToken: "expected-token"})
	wrongReq := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	wrongReq.Header.Set("Authorization", "Bearer wrong-token")
	wrongToken := httptest.NewRecorder()
	strict.ServeHTTP(wrongToken, wrongReq)
	if wrongToken.Code != http.StatusUnauthorized {
		t.Fatalf("strict wrong token status = %d, want %d: %s", wrongToken.Code, http.StatusUnauthorized, wrongToken.Body.String())
	}
	if strings.Contains(wrongToken.Body.String(), "expected-token") || strings.Contains(wrongToken.Body.String(), "wrong-token") {
		t.Fatalf("auth error leaked token material: %s", wrongToken.Body.String())
	}
	if got := wrongToken.Header().Get("WWW-Authenticate"); got != `Bearer realm="devcloud-pubsub"` {
		t.Fatalf("WWW-Authenticate = %q", got)
	}

	authorizedReq := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	authorizedReq.Header.Set("Authorization", "Bearer expected-token")
	authorized := httptest.NewRecorder()
	strict.ServeHTTP(authorized, authorizedReq)
	if authorized.Code != http.StatusOK {
		t.Fatalf("strict authorized status = %d, want %d: %s", authorized.Code, http.StatusOK, authorized.Body.String())
	}
}

func TestRESTRejectsInvalidProjectNames(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "list topics", method: http.MethodGet, path: "/v1/projects/bad!/topics"},
		{name: "get topic", method: http.MethodGet, path: "/v1/projects/bad!/topics/orders"},
		{name: "topic subscriptions", method: http.MethodGet, path: "/v1/projects/bad!/topics/orders/subscriptions"},
		{name: "list subscriptions", method: http.MethodGet, path: "/v1/projects/bad!/subscriptions"},
		{name: "get subscription", method: http.MethodGet, path: "/v1/projects/bad!/subscriptions/orders-sub"},
		{name: "pull", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:pull", body: `{"maxMessages":1}`},
		{name: "acknowledge", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:acknowledge", body: `{"ackIds":["ack-1"]}`},
		{name: "modify ack deadline", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:modifyAckDeadline", body: `{"ackIds":["ack-1"],"ackDeadlineSeconds":0}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performPubSubRequest(server, tt.method, tt.path, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "INVALID_ARGUMENT") {
				t.Fatalf("error body = %s", rec.Body.String())
			}
		})
	}
}

func TestRESTTopicCRUD(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(create.Body).Decode(&topic); err != nil {
		t.Fatalf("decode topic: %v", err)
	}
	if topic.Name != "projects/devcloud/topics/orders" {
		t.Fatalf("topic name = %q", topic.Name)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", get.Code, http.StatusOK)
	}

	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	var listed struct {
		Topics []topicResource `json:"topics"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode topics: %v", err)
	}
	if len(listed.Topics) != 1 || listed.Topics[0].Name != topic.Name {
		t.Fatalf("topics = %#v", listed.Topics)
	}
}

func TestRESTResourcePathsDecodeEscapedNames(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	createTopic := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders%2Bpriority", "{}")
	if createTopic.Code != http.StatusOK {
		t.Fatalf("create topic status = %d, want %d: %s", createTopic.Code, http.StatusOK, createTopic.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(createTopic.Body).Decode(&topic); err != nil {
		t.Fatalf("decode topic: %v", err)
	}
	if topic.Name != "projects/devcloud/topics/orders+priority" {
		t.Fatalf("topic name = %q", topic.Name)
	}

	createSubscription := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders%2Bpriority-sub", `{
		"topic":"projects/devcloud/topics/orders+priority"
	}`)
	if createSubscription.Code != http.StatusOK {
		t.Fatalf("create subscription status = %d, want %d: %s", createSubscription.Code, http.StatusOK, createSubscription.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(createSubscription.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.Name != "projects/devcloud/subscriptions/orders+priority-sub" || subscription.Topic != topic.Name {
		t.Fatalf("subscription = %#v", subscription)
	}

	getTopic := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders%2Bpriority", "")
	if getTopic.Code != http.StatusOK {
		t.Fatalf("get topic status = %d, want %d: %s", getTopic.Code, http.StatusOK, getTopic.Body.String())
	}
}

func TestRESTTopicMetadataAndPatch(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"labels":{"env":"local"},
		"messageRetentionDuration":"600s",
		"schemaSettings":{"schema":"projects/devcloud/schemas/order-event","encoding":"JSON"},
		"kmsKeyName":"projects/devcloud/locations/global/keyRings/local/cryptoKeys/orders"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created topicResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created topic: %v", err)
	}
	if created.Labels["env"] != "local" || created.MessageRetentionDuration != "600s" || created.SchemaSettings["encoding"] != "JSON" || created.KMSKeyName == "" {
		t.Fatalf("created topic metadata = %#v", created)
	}

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=labels,message_retention_duration,schema_settings", `{
		"labels":{"env":"test","owner":"pubsub"},
		"messageRetentionDuration":"1200s",
		"schemaSettings":{"schema":"projects/devcloud/schemas/order-event","encoding":"BINARY"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(get.Body).Decode(&topic); err != nil {
		t.Fatalf("decode patched topic: %v", err)
	}
	if topic.Labels["env"] != "test" || topic.Labels["owner"] != "pubsub" || topic.MessageRetentionDuration != "1200s" || topic.SchemaSettings["encoding"] != "BINARY" {
		t.Fatalf("patched topic metadata = %#v", topic)
	}
	if topic.KMSKeyName == "" {
		t.Fatalf("patch without kmsKeyName mask should preserve kmsKeyName: %#v", topic)
	}
}

func TestRESTTopicTimestampsPersistAndPatchUpdatesOnlyUpdatedAt(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	server.now = func() time.Time { return now }

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created topicResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created topic: %v", err)
	}
	if created.CreatedAt != "2026-05-02T12:00:00Z" || created.UpdatedAt != created.CreatedAt {
		t.Fatalf("created timestamps = createdAt %q updatedAt %q", created.CreatedAt, created.UpdatedAt)
	}

	now = now.Add(5 * time.Minute)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=labels", `{
		"labels":{"env":"test"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var patched topicResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched topic: %v", err)
	}
	if patched.CreatedAt != created.CreatedAt || patched.UpdatedAt != "2026-05-02T12:05:00Z" {
		t.Fatalf("patched timestamps = createdAt %q updatedAt %q", patched.CreatedAt, patched.UpdatedAt)
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted topicResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted topic: %v", err)
	}
	if persisted.CreatedAt != created.CreatedAt || persisted.UpdatedAt != patched.UpdatedAt {
		t.Fatalf("persisted timestamps = createdAt %q updatedAt %q", persisted.CreatedAt, persisted.UpdatedAt)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Topics) != 1 || snapshot.Topics[0].CreatedAt != created.CreatedAt || snapshot.Topics[0].UpdatedAt != patched.UpdatedAt {
		t.Fatalf("snapshot topics = %#v", snapshot.Topics)
	}
}

func TestRESTRejectsInvalidTopicMetadata(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	invalidRetention := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"messageRetentionDuration":"soon"
	}`)
	if invalidRetention.Code != http.StatusBadRequest {
		t.Fatalf("invalid retention status = %d, want %d: %s", invalidRetention.Code, http.StatusBadRequest, invalidRetention.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	invalidSchema := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders", `{
		"schemaSettings":{"schema":"projects/devcloud/topics/not-a-schema"}
	}`)
	if invalidSchema.Code != http.StatusBadRequest {
		t.Fatalf("invalid schema status = %d, want %d: %s", invalidSchema.Code, http.StatusBadRequest, invalidSchema.Body.String())
	}

	unsupportedMask := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=expirationPolicy", `{
		"labels":{"env":"test"}
	}`)
	if unsupportedMask.Code != http.StatusBadRequest {
		t.Fatalf("unsupported mask status = %d, want %d: %s", unsupportedMask.Code, http.StatusBadRequest, unsupportedMask.Body.String())
	}
}

func TestRESTListTopicsPaginates(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/alpha", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/bravo", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/charlie", "{}")

	firstPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageSize=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d: %s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first struct {
		Topics        []topicResource `json:"topics"`
		NextPageToken string          `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Topics) != 2 || first.Topics[0].Name != "projects/devcloud/topics/alpha" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageSize=2&pageToken="+first.NextPageToken, "")
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d: %s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second struct {
		Topics        []topicResource `json:"topics"`
		NextPageToken string          `json:"nextPageToken"`
	}
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Topics) != 1 || second.Topics[0].Name != "projects/devcloud/topics/charlie" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestRESTListTopicsRejectsInvalidPagination(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	rec := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageToken=not-an-offset", "")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("error body = %s", rec.Body.String())
	}
}

func TestRESTRejectsDuplicateTopicCreate(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	first := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want %d: %s", first.Code, http.StatusOK, first.Body.String())
	}
	duplicate := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}
	if !strings.Contains(duplicate.Body.String(), "ALREADY_EXISTS") {
		t.Fatalf("duplicate error body = %s", duplicate.Body.String())
	}
}

func TestRESTSubscriptionCRUDAndTopicSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"labels":{"env":"local"},
		"ackDeadlineSeconds":2,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true,
		"messageRetentionDuration":"1200s",
		"expirationPolicy":{"ttl":"86400s"},
		"filter":"attributes.kind=\"test\"",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5},
		"retryPolicy":{"minimumBackoff":"1s","maximumBackoff":"10s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(create.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.Name != "projects/devcloud/subscriptions/orders-sub" || subscription.Topic != "projects/devcloud/topics/orders" {
		t.Fatalf("subscription = %#v", subscription)
	}
	if subscription.Labels["env"] != "local" {
		t.Fatalf("subscription labels = %#v", subscription.Labels)
	}
	if !subscription.EnableMessageOrdering || !subscription.EnableExactlyOnceDelivery || !subscription.RetainAckedMessages || subscription.MessageRetentionDuration != "1200s" || subscription.ExpirationPolicy == nil || subscription.DeadLetterPolicy == nil || subscription.RetryPolicy == nil || subscription.PushConfig == nil {
		t.Fatalf("advanced metadata not preserved: %#v", subscription)
	}
	if subscription.Filter != `attributes.kind="test"` {
		t.Fatalf("filter = %q, want attributes.kind=\"test\"", subscription.Filter)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", get.Code, http.StatusOK)
	}

	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	var listed struct {
		Subscriptions []subscriptionResource `json:"subscriptions"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode subscriptions: %v", err)
	}
	if len(listed.Subscriptions) != 1 || listed.Subscriptions[0].Name != subscription.Name {
		t.Fatalf("subscriptions = %#v", listed.Subscriptions)
	}

	topicSubscriptions := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions", "")
	if topicSubscriptions.Code != http.StatusOK {
		t.Fatalf("topic subscriptions status = %d, want %d", topicSubscriptions.Code, http.StatusOK)
	}
	var topicSubs struct {
		Subscriptions []string `json:"subscriptions"`
	}
	if err := json.NewDecoder(topicSubscriptions.Body).Decode(&topicSubs); err != nil {
		t.Fatalf("decode topic subscriptions: %v", err)
	}
	if len(topicSubs.Subscriptions) != 1 || topicSubs.Subscriptions[0] != subscription.Name {
		t.Fatalf("topic subscriptions = %#v", topicSubs.Subscriptions)
	}

	blockedDelete := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if blockedDelete.Code != http.StatusBadRequest {
		t.Fatalf("blocked topic delete status = %d, want %d", blockedDelete.Code, http.StatusBadRequest)
	}

	deleteSubscription := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if deleteSubscription.Code != http.StatusNoContent {
		t.Fatalf("delete subscription status = %d, want %d", deleteSubscription.Code, http.StatusNoContent)
	}
	deleteTopic := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if deleteTopic.Code != http.StatusNoContent {
		t.Fatalf("delete topic status = %d, want %d", deleteTopic.Code, http.StatusNoContent)
	}
}

func TestRESTSubscriptionTimestampsPersistAndPatchUpdatesOnlyUpdatedAt(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created subscriptionResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created subscription: %v", err)
	}
	if created.CreatedAt != "2026-05-02T12:00:00Z" || created.UpdatedAt != created.CreatedAt {
		t.Fatalf("created timestamps = createdAt %q updatedAt %q", created.CreatedAt, created.UpdatedAt)
	}

	now = now.Add(5 * time.Minute)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=labels", `{
		"labels":{"env":"test"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var patched subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched subscription: %v", err)
	}
	if patched.CreatedAt != created.CreatedAt || patched.UpdatedAt != "2026-05-02T12:05:00Z" {
		t.Fatalf("patched timestamps = createdAt %q updatedAt %q", patched.CreatedAt, patched.UpdatedAt)
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted subscription: %v", err)
	}
	if persisted.CreatedAt != created.CreatedAt || persisted.UpdatedAt != patched.UpdatedAt {
		t.Fatalf("persisted timestamps = createdAt %q updatedAt %q", persisted.CreatedAt, persisted.UpdatedAt)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].CreatedAt != created.CreatedAt || snapshot.Subscriptions[0].UpdatedAt != patched.UpdatedAt {
		t.Fatalf("snapshot subscriptions = %#v", snapshot.Subscriptions)
	}
}

func TestRESTListSubscriptionsPaginates(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-c", `{"topic":"projects/devcloud/topics/orders"}`)

	firstPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions?pageSize=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d: %s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first struct {
		Subscriptions []subscriptionResource `json:"subscriptions"`
		NextPageToken string                 `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Subscriptions) != 2 || first.Subscriptions[0].Name != "projects/devcloud/subscriptions/orders-a" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	topicPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions?pageSize=1&pageToken=1", "")
	if topicPage.Code != http.StatusOK {
		t.Fatalf("topic page status = %d, want %d: %s", topicPage.Code, http.StatusOK, topicPage.Body.String())
	}
	var topicSubscriptions struct {
		Subscriptions []string `json:"subscriptions"`
		NextPageToken string   `json:"nextPageToken"`
	}
	if err := json.NewDecoder(topicPage.Body).Decode(&topicSubscriptions); err != nil {
		t.Fatalf("decode topic subscriptions: %v", err)
	}
	if len(topicSubscriptions.Subscriptions) != 1 || topicSubscriptions.Subscriptions[0] != "projects/devcloud/subscriptions/orders-b" || topicSubscriptions.NextPageToken == "" {
		t.Fatalf("topic subscriptions page = %#v", topicSubscriptions)
	}
}

func TestRESTRejectsDuplicateSubscriptionCreate(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	body := `{"topic":"projects/devcloud/topics/orders"}`

	first := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want %d: %s", first.Code, http.StatusOK, first.Body.String())
	}
	duplicate := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", body)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}
	if !strings.Contains(duplicate.Body.String(), "ALREADY_EXISTS") {
		t.Fatalf("duplicate error body = %s", duplicate.Body.String())
	}
}

func TestRESTPatchSubscriptionUpdatesMetadata(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     60,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=labels,ackDeadlineSeconds,enableMessageOrdering,enableExactlyOnceDelivery,retainAckedMessages,messageRetentionDuration,expirationPolicy,filter,deadLetterPolicy,retryPolicy,pushConfig", `{
		"name":"projects/devcloud/subscriptions/orders-sub",
		"labels":{"env":"test","owner":"pubsub"},
		"ackDeadlineSeconds":30,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true,
		"messageRetentionDuration":"1800s",
		"expirationPolicy":{"ttl":"172800s"},
		"filter":"attributes.kind=\"priority\"",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5},
		"retryPolicy":{"minimumBackoff":"2s","maximumBackoff":"20s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.AckDeadlineSeconds != 30 || !subscription.EnableMessageOrdering || !subscription.EnableExactlyOnceDelivery || !subscription.RetainAckedMessages || subscription.MessageRetentionDuration != "1800s" || subscription.Filter != `attributes.kind="priority"` {
		t.Fatalf("subscription patch did not update core fields: %#v", subscription)
	}
	if subscription.Labels["env"] != "test" || subscription.Labels["owner"] != "pubsub" {
		t.Fatalf("subscription patch did not update labels: %#v", subscription.Labels)
	}
	if subscription.ExpirationPolicy == nil || subscription.DeadLetterPolicy == nil || subscription.RetryPolicy == nil || subscription.PushConfig == nil {
		t.Fatalf("subscription patch did not preserve advanced metadata: %#v", subscription)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted subscription: %v", err)
	}
	if persisted.Labels["owner"] != "pubsub" || persisted.AckDeadlineSeconds != 30 || !persisted.EnableMessageOrdering || !persisted.EnableExactlyOnceDelivery || !persisted.RetainAckedMessages || persisted.MessageRetentionDuration != "1800s" || persisted.ExpirationPolicy == nil || persisted.Filter != `attributes.kind="priority"` || persisted.PushConfig == nil {
		t.Fatalf("persisted subscription = %#v", persisted)
	}
}

func TestRESTPatchSubscriptionAcceptsWrappedBodyAndSnakeCaseMask(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     60,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true
	}`)

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"subscription":{
			"name":"projects/devcloud/subscriptions/orders-sub",
			"ackDeadlineSeconds":10,
			"enableMessageOrdering":false,
			"enableExactlyOnceDelivery":false,
			"retainAckedMessages":false
		},
		"updateMask":{"paths":["ack_deadline_seconds","enable_message_ordering","enable_exactly_once_delivery","retain_acked_messages"]}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.AckDeadlineSeconds != 10 || subscription.EnableMessageOrdering || subscription.EnableExactlyOnceDelivery || subscription.RetainAckedMessages {
		t.Fatalf("subscription = %#v", subscription)
	}
}

func TestRESTPatchSubscriptionRejectsUnsafeUpdates(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     5,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/other", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	tooLarge := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{"ackDeadlineSeconds":6}`)
	if tooLarge.Code != http.StatusBadRequest {
		t.Fatalf("too large status = %d, want %d: %s", tooLarge.Code, http.StatusBadRequest, tooLarge.Body.String())
	}
	changeTopic := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/other"}`)
	if changeTopic.Code != http.StatusBadRequest {
		t.Fatalf("change topic status = %d, want %d: %s", changeTopic.Code, http.StatusBadRequest, changeTopic.Body.String())
	}
	unsupportedMask := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=expirationPolicy.foo", `{"expirationPolicy":{"ttl":"3600s"}}`)
	if unsupportedMask.Code != http.StatusBadRequest {
		t.Fatalf("unsupported mask status = %d, want %d: %s", unsupportedMask.Code, http.StatusBadRequest, unsupportedMask.Body.String())
	}
	invalidRetention := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=messageRetentionDuration", `{"messageRetentionDuration":"soon"}`)
	if invalidRetention.Code != http.StatusBadRequest {
		t.Fatalf("invalid retention status = %d, want %d: %s", invalidRetention.Code, http.StatusBadRequest, invalidRetention.Body.String())
	}
	invalidExpiration := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=expirationPolicy", `{"expirationPolicy":{"ttl":"later"}}`)
	if invalidExpiration.Code != http.StatusBadRequest {
		t.Fatalf("invalid expiration status = %d, want %d: %s", invalidExpiration.Code, http.StatusBadRequest, invalidExpiration.Body.String())
	}
}

func TestRESTSubscriptionFilterAppliesToPublishedMessages(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/priority-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind=\"priority\""
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/all-orders", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"cHJpb3JpdHk=","attributes":{"kind":"priority"}},
			{"data":"bm9ybWFs","attributes":{"kind":"normal"}}
		]
	}`)

	filteredPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/priority-orders:pull", `{"maxMessages":10}`)
	if filteredPull.Code != http.StatusOK {
		t.Fatalf("filtered pull status = %d, want %d: %s", filteredPull.Code, http.StatusOK, filteredPull.Body.String())
	}
	if !strings.Contains(filteredPull.Body.String(), "cHJpb3JpdHk=") || strings.Contains(filteredPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("filtered subscription received wrong messages: %s", filteredPull.Body.String())
	}

	unfilteredPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/all-orders:pull", `{"maxMessages":10}`)
	if unfilteredPull.Code != http.StatusOK {
		t.Fatalf("unfiltered pull status = %d, want %d: %s", unfilteredPull.Code, http.StatusOK, unfilteredPull.Body.String())
	}
	if !strings.Contains(unfilteredPull.Body.String(), "cHJpb3JpdHk=") || !strings.Contains(unfilteredPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("unfiltered subscription did not receive both messages: %s", unfilteredPull.Body.String())
	}
}

func TestRESTSubscriptionFilterSupportsPrefixAndInequality(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/prefix-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"hasPrefix(attributes.kind, \"priority\")"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/non-debug-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind != \"debug\""
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"cHJpb3JpdHktZmFzdA==","attributes":{"kind":"priority-fast"}},
			{"data":"ZGVidWc=","attributes":{"kind":"debug"}},
			{"data":"bm9ybWFs","attributes":{"kind":"normal"}}
		]
	}`)

	prefixPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/prefix-orders:pull", `{"maxMessages":10}`)
	if prefixPull.Code != http.StatusOK {
		t.Fatalf("prefix pull status = %d, want %d: %s", prefixPull.Code, http.StatusOK, prefixPull.Body.String())
	}
	if !strings.Contains(prefixPull.Body.String(), "cHJpb3JpdHktZmFzdA==") || strings.Contains(prefixPull.Body.String(), "ZGVidWc=") || strings.Contains(prefixPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("prefix subscription received wrong messages: %s", prefixPull.Body.String())
	}

	inequalityPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/non-debug-orders:pull", `{"maxMessages":10}`)
	if inequalityPull.Code != http.StatusOK {
		t.Fatalf("inequality pull status = %d, want %d: %s", inequalityPull.Code, http.StatusOK, inequalityPull.Body.String())
	}
	if !strings.Contains(inequalityPull.Body.String(), "cHJpb3JpdHktZmFzdA==") || !strings.Contains(inequalityPull.Body.String(), "bm9ybWFs") || strings.Contains(inequalityPull.Body.String(), "ZGVidWc=") {
		t.Fatalf("inequality subscription received wrong messages: %s", inequalityPull.Body.String())
	}
}

func TestRESTRejectsUnsupportedSubscriptionFilter(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind:\"priority\""
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=filter", `{
		"filter":"attributes.kind =~ \"priority\""
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}
}

func TestRESTModifyPushConfigUpdatesAndClearsMetadata(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/old"}
	}`)

	update := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/new","attributes":{"x-goog-version":"v1"}}
	}`)
	if update.Code != http.StatusOK {
		t.Fatalf("update status = %d, want %d: %s", update.Code, http.StatusOK, update.Body.String())
	}
	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.PushConfig == nil || subscription.PushConfig["pushEndpoint"] != "http://127.0.0.1:65535/new" {
		t.Fatalf("pushConfig after update = %#v", subscription.PushConfig)
	}

	clear := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{}`)
	if clear.Code != http.StatusOK {
		t.Fatalf("clear status = %d, want %d: %s", clear.Code, http.StatusOK, clear.Body.String())
	}
	get = performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	var cleared subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&cleared); err != nil {
		t.Fatalf("decode subscription after clear: %v", err)
	}
	if cleared.PushConfig != nil {
		t.Fatalf("pushConfig after clear = %#v, want nil", cleared.PushConfig)
	}
}

func TestSnapshotRedactsPushConfigSensitiveFields(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{
			"pushEndpoint":"http://127.0.0.1:65535/push",
			"attributes":{"authorization":"Bearer hidden"},
			"oidcToken":{"serviceAccountEmail":"local@example.test","audience":"secret-audience"}
		}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-push", "")
	if !strings.Contains(get.Body.String(), "secret-audience") {
		t.Fatalf("resource response should preserve push config metadata for REST compatibility: %s", get.Body.String())
	}

	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %#v", snapshot.Subscriptions)
	}
	pushConfig := snapshot.Subscriptions[0].PushConfig
	if pushConfig["pushEndpoint"] != "http://127.0.0.1:65535/push" {
		t.Fatalf("snapshot pushConfig = %#v", pushConfig)
	}
	data, err := json.Marshal(pushConfig)
	if err != nil {
		t.Fatalf("marshal pushConfig: %v", err)
	}
	for _, forbidden := range []string{"authorization", "Bearer hidden", "oidcToken", "secret-audience", "local@example.test"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("snapshot pushConfig leaked %q: %s", forbidden, data)
		}
	}
}

func TestRESTRejectsInvalidPushConfigEndpoint(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"ftp://127.0.0.1/push"}
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}
	if !strings.Contains(create.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("create error body = %s", create.Body.String())
	}

	createPull := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	if createPull.Code != http.StatusOK {
		t.Fatalf("create pull subscription status = %d, want %d: %s", createPull.Code, http.StatusOK, createPull.Body.String())
	}
	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{
		"pushConfig":{"pushEndpoint":"http://user:pass@127.0.0.1:65535/push"}
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}
	if !strings.Contains(modify.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("modify error body = %s", modify.Body.String())
	}
}

func TestRESTPullRejectsPushSubscriptionWithoutLeakingMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create push subscription status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cHVzaC1vbmx5","attributes":{"token":"hidden"}}]
	}`)

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-push:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusBadRequest {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusBadRequest, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "FAILED_PRECONDITION") {
		t.Fatalf("pull error body = %s", pull.Body.String())
	}
	for _, forbidden := range []string{"cHVzaC1vbmx5", "hidden", "ackId"} {
		if strings.Contains(pull.Body.String(), forbidden) {
			t.Fatalf("push subscription pull leaked %q: %s", forbidden, pull.Body.String())
		}
	}
}

func TestPushDeliveryAcksSuccessfulHTTPResponse(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	received := make(chan struct {
		Subscription string `json:"subscription"`
		Message      struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
		} `json:"message"`
	}, 1)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		if r.Method != http.MethodPost {
			t.Errorf("push method = %s, want POST", r.Method)
		}
		var body struct {
			Subscription string `json:"subscription"`
			Message      struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				MessageID   string            `json:"messageId"`
				PublishTime string            `json:"publishTime"`
			} `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode push body: %v", err)
			return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
		}
		received <- body
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1/push"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cHVzaA==","attributes":{"kind":"local"}}]
	}`)

	delivered, err := server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("deliverPush: %v", err)
	}
	if !delivered {
		t.Fatalf("deliverPush delivered = false")
	}
	select {
	case body := <-received:
		if body.Subscription != "projects/devcloud/subscriptions/orders-push" || body.Message.Data != "cHVzaA==" || body.Message.Attributes["kind"] != "local" || body.Message.MessageID == "" || body.Message.PublishTime == "" {
			t.Fatalf("push body = %#v", body)
		}
	case <-time.After(time.Second):
		t.Fatalf("push endpoint did not receive delivery")
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("successful push was not acked: %#v", snapshot.Subscriptions)
	}
}

func TestPushRetrySchedulesFailedHTTPResponse(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 2})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	statusCode := http.StatusInternalServerError
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		attempts++
		return &http.Response{StatusCode: statusCode, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"5s","maximumBackoff":"10s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1/push"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnk="}]
	}`)

	delivered, err := server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("first deliverPush: %v", err)
	}
	if !delivered || attempts != 1 {
		t.Fatalf("first push delivered=%v attempts=%d", delivered, attempts)
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || len(snapshot.Subscriptions[0].RecentDeliveries) != 1 {
		t.Fatalf("snapshot after failed push = %#v", snapshot.Subscriptions)
	}
	recent := snapshot.Subscriptions[0].RecentDeliveries[0]
	if recent.State != "delayed" || recent.DeliveryAttempt != 1 || recent.NextDeliveryTime != now.Add(5*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("retry delivery summary = %#v", recent)
	}
	delivered, err = server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("early retry deliverPush: %v", err)
	}
	if delivered || attempts != 1 {
		t.Fatalf("early retry delivered=%v attempts=%d", delivered, attempts)
	}

	statusCode = http.StatusOK
	now = now.Add(5 * time.Second)
	delivered, err = server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("retry deliverPush: %v", err)
	}
	if !delivered || attempts != 2 {
		t.Fatalf("retry push delivered=%v attempts=%d", delivered, attempts)
	}
	snapshot = server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("successful retry was not acked: %#v", snapshot.Subscriptions)
	}
}

func TestRESTDetachSubscriptionDropsBacklogAndBlocksPull(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"ZGV0YWNoZWQ=","attributes":{"secret":"hidden"}}]
	}`)

	detach := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:detach", `{}`)
	if detach.Code != http.StatusOK {
		t.Fatalf("detach status = %d, want %d: %s", detach.Code, http.StatusOK, detach.Body.String())
	}
	for _, forbidden := range []string{"ZGV0YWNoZWQ=", "secret", "hidden"} {
		if strings.Contains(detach.Body.String(), forbidden) {
			t.Fatalf("detach response leaked %q: %s", forbidden, detach.Body.String())
		}
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if !subscription.Detached {
		t.Fatalf("subscription detached = false: %#v", subscription)
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusBadRequest {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusBadRequest, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "FAILED_PRECONDITION") {
		t.Fatalf("pull error body = %s", pull.Body.String())
	}
	if strings.Contains(pull.Body.String(), "ZGV0YWNoZWQ=") || strings.Contains(pull.Body.String(), "hidden") {
		t.Fatalf("detached pull leaked retained message: %s", pull.Body.String())
	}

	topicSubscriptions := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions", "")
	if topicSubscriptions.Code != http.StatusOK {
		t.Fatalf("topic subscriptions status = %d, want %d: %s", topicSubscriptions.Code, http.StatusOK, topicSubscriptions.Body.String())
	}
	if strings.Contains(topicSubscriptions.Body.String(), "orders-sub") {
		t.Fatalf("detached subscription should not be listed as attached: %s", topicSubscriptions.Body.String())
	}

	deleteTopic := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if deleteTopic.Code != http.StatusNoContent {
		t.Fatalf("delete topic status = %d, want %d: %s", deleteTopic.Code, http.StatusNoContent, deleteTopic.Body.String())
	}
}

func TestRESTRejectsInvalidDeadLetterPolicy(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	missingTopic := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/missing-dlq-topic", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"maxDeliveryAttempts":5}
	}`)
	if missingTopic.Code != http.StatusBadRequest {
		t.Fatalf("missing deadLetterTopic status = %d, want %d: %s", missingTopic.Code, http.StatusBadRequest, missingTopic.Body.String())
	}

	tooSmall := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/too-small-dlq", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":4}
	}`)
	if tooSmall.Code != http.StatusBadRequest {
		t.Fatalf("too small maxDeliveryAttempts status = %d, want %d: %s", tooSmall.Code, http.StatusBadRequest, tooSmall.Body.String())
	}

	missingTarget := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/missing-dlq-target", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	if missingTarget.Code != http.StatusNotFound {
		t.Fatalf("missing dead-letter topic status = %d, want %d: %s", missingTarget.Code, http.StatusNotFound, missingTarget.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=deadLetterPolicy", `{
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":101}
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch invalid deadLetterPolicy status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}

	missingPatchTarget := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=deadLetterPolicy", `{
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	if missingPatchTarget.Code != http.StatusNotFound {
		t.Fatalf("patch missing dead-letter topic status = %d, want %d: %s", missingPatchTarget.Code, http.StatusNotFound, missingPatchTarget.Body.String())
	}
}

func TestRESTRejectsInvalidRetryPolicy(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	badDuration := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/bad-duration", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"soon","maximumBackoff":"10s"}
	}`)
	if badDuration.Code != http.StatusBadRequest {
		t.Fatalf("bad duration status = %d, want %d: %s", badDuration.Code, http.StatusBadRequest, badDuration.Body.String())
	}

	inverted := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/inverted", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"20s","maximumBackoff":"10s"}
	}`)
	if inverted.Code != http.StatusBadRequest {
		t.Fatalf("inverted retryPolicy status = %d, want %d: %s", inverted.Code, http.StatusBadRequest, inverted.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=retryPolicy", `{
		"retryPolicy":{"minimumBackoff":"11s","maximumBackoff":"10s"}
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch invalid retryPolicy status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}
}

func TestRESTRejectsSubscriptionForMissingTopic(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	rec := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/missing"
	}`)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestRESTRejectsAckDeadlinesAboveConfiguredMaximum(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     5,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":6
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bWF4"}]}`)
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

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"],
		"ackDeadlineSeconds":6
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}
}

func TestRESTRejectsPublishWithInvalidMessageData(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	invalidBase64 := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"not base64"}]
	}`)
	if invalidBase64.Code != http.StatusBadRequest {
		t.Fatalf("invalid base64 status = %d, want %d: %s", invalidBase64.Code, http.StatusBadRequest, invalidBase64.Body.String())
	}

	emptyMessage := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{}]
	}`)
	if emptyMessage.Code != http.StatusBadRequest {
		t.Fatalf("empty message status = %d, want %d: %s", emptyMessage.Code, http.StatusBadRequest, emptyMessage.Body.String())
	}

	emptyAttributeKey := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"attributes":{"":"do-not-leak"}}]
	}`)
	if emptyAttributeKey.Code != http.StatusBadRequest {
		t.Fatalf("empty attribute key status = %d, want %d: %s", emptyAttributeKey.Code, http.StatusBadRequest, emptyAttributeKey.Body.String())
	}
	if strings.Contains(emptyAttributeKey.Body.String(), "do-not-leak") {
		t.Fatalf("publish validation leaked attribute value: %s", emptyAttributeKey.Body.String())
	}

	attributeOnly := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"attributes":{"kind":"signal"}}]
	}`)
	if attributeOnly.Code != http.StatusOK {
		t.Fatalf("attribute-only status = %d, want %d: %s", attributeOnly.Code, http.StatusOK, attributeOnly.Body.String())
	}
}

func TestRESTPublishDoesNotRetainMessagesWithoutMatchingSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"bm8tc3Vic2NyaWJlcnM="}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}
	var response struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publish.Body).Decode(&response); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if len(response.MessageIDs) != 1 {
		t.Fatalf("messageIds = %#v, want one id", response.MessageIDs)
	}
	if _, found := server.MessageSnapshot(response.MessageIDs[0]); found {
		t.Fatalf("unreferenced published message %q was retained", response.MessageIDs[0])
	}
}

func TestRESTPersistsResources(t *testing.T) {
	dir := t.TempDir()
	server := NewServer(Config{Project: "devcloud", StoragePath: filepath.Join(dir, "pubsub")})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: filepath.Join(dir, "pubsub")})
	getTopic := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if getTopic.Code != http.StatusOK {
		t.Fatalf("reloaded topic status = %d, want %d", getTopic.Code, http.StatusOK)
	}
	getSubscription := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if getSubscription.Code != http.StatusOK {
		t.Fatalf("reloaded subscription status = %d, want %d", getSubscription.Code, http.StatusOK)
	}
}

func TestRESTPublishAcceptsProtoJSONBase64Variants(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"aGk"},
			{"data":"c3RhbmRhcmQ"}
		]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":10}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), `"data":"aGk"`) || !strings.Contains(pull.Body.String(), `"data":"c3RhbmRhcmQ"`) {
		t.Fatalf("pull response did not preserve accepted base64 spellings: %s", pull.Body.String())
	}
}

func TestRESTPersistsMessagesDeliveriesAndAckState(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableExactlyOnceDelivery":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cGVyc2lzdGVk"}]}`)

	reloaded := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	reloaded.now = func() time.Time { return now }
	getSubscription := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if getSubscription.Code != http.StatusOK {
		t.Fatalf("get reloaded subscription status = %d, want %d: %s", getSubscription.Code, http.StatusOK, getSubscription.Body.String())
	}
	var reloadedSubscription subscriptionResource
	if err := json.NewDecoder(getSubscription.Body).Decode(&reloadedSubscription); err != nil {
		t.Fatalf("decode reloaded subscription: %v", err)
	}
	if !reloadedSubscription.EnableExactlyOnceDelivery {
		t.Fatalf("exactly-once metadata was not persisted: %#v", reloadedSubscription)
	}
	pull := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	var pulled struct {
		ReceivedMessages []struct {
			AckID   string `json:"ackId"`
			Message struct {
				Data      string `json:"data"`
				MessageID string `json:"messageId"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].Message.Data != "cGVyc2lzdGVk" {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}

	ack := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}

	ackedReload := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	ackedReload.now = func() time.Time { return now.Add(3 * time.Second) }
	emptyPull := performPubSubRequest(ackedReload, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(emptyPull.Body.String(), "receivedMessages") {
		t.Fatalf("acked message should stay acknowledged after reload, got %s", emptyPull.Body.String())
	}

	publishAgain := performPubSubRequest(ackedReload, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bmV4dA=="}]}`)
	if publishAgain.Code != http.StatusOK {
		t.Fatalf("second publish status = %d, want %d: %s", publishAgain.Code, http.StatusOK, publishAgain.Body.String())
	}
	var published struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publishAgain.Body).Decode(&published); err != nil {
		t.Fatalf("decode second publish: %v", err)
	}
	if len(published.MessageIDs) != 1 || published.MessageIDs[0] == pulled.ReceivedMessages[0].Message.MessageID {
		t.Fatalf("message IDs did not advance after reload: first=%q next=%#v", pulled.ReceivedMessages[0].Message.MessageID, published.MessageIDs)
	}
}

func TestRESTPersistsMessageStateInSeparateMessageStoragePath(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	messageStoragePath := filepath.Join(dir, "message")
	server := NewServer(Config{
		Project:            "devcloud",
		StoragePath:        storagePath,
		MessageStoragePath: messageStoragePath,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"c2VwYXJhdGUtc3RhdGU=","attributes":{"kind":"local"}}]
	}`)

	resourceData, err := os.ReadFile(filepath.Join(storagePath, "resources.json"))
	if err != nil {
		t.Fatalf("read resource state: %v", err)
	}
	if strings.Contains(string(resourceData), "c2VwYXJhdGUtc3RhdGU=") || strings.Contains(string(resourceData), `"deliveries"`) {
		t.Fatal("resource state should not contain message delivery data")
	}
	messageData, err := os.ReadFile(filepath.Join(messageStoragePath, "pubsub.json"))
	if err != nil {
		t.Fatalf("read message state: %v", err)
	}
	if !strings.Contains(string(messageData), "c2VwYXJhdGUtc3RhdGU=") || !strings.Contains(string(messageData), `"deliveries"`) {
		t.Fatal("message state missing persisted delivery data")
	}

	reloaded := NewServer(Config{
		Project:            "devcloud",
		StoragePath:        storagePath,
		MessageStoragePath: messageStoragePath,
	})
	pull := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK || !strings.Contains(pull.Body.String(), "c2VwYXJhdGUtc3RhdGU=") {
		t.Fatalf("pull after separate reload = status %d body %s", pull.Code, pull.Body.String())
	}
}

func TestRESTPublishPullAckAndModifyAckDeadline(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"aGVsbG8=","attributes":{"kind":"test"},"orderingKey":"group-1"}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}
	var publishBody struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publish.Body).Decode(&publishBody); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if len(publishBody.MessageIDs) != 1 || publishBody.MessageIDs[0] == "" {
		t.Fatalf("messageIds = %#v", publishBody.MessageIDs)
	}

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var firstPullBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
			Message         struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				MessageID   string            `json:"messageId"`
				PublishTime string            `json:"publishTime"`
				OrderingKey string            `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&firstPullBody); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(firstPullBody.ReceivedMessages) != 1 {
		t.Fatalf("receivedMessages = %#v", firstPullBody.ReceivedMessages)
	}
	message := firstPullBody.ReceivedMessages[0]
	if message.AckID == "" || message.DeliveryAttempt != 1 {
		t.Fatalf("received message lease = %#v", message)
	}
	if message.Message.Data != "aGVsbG8=" || message.Message.Attributes["kind"] != "test" || message.Message.OrderingKey != "group-1" {
		t.Fatalf("received message payload = %#v", message.Message)
	}

	invisible := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if invisible.Code != http.StatusOK {
		t.Fatalf("invisible pull status = %d, want %d", invisible.Code, http.StatusOK)
	}
	if strings.Contains(invisible.Body.String(), "receivedMessages") {
		t.Fatalf("leased message should be invisible, got %s", invisible.Body.String())
	}

	release := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+message.AckID+`"],
		"ackDeadlineSeconds":0
	}`)
	if release.Code != http.StatusOK {
		t.Fatalf("modifyAckDeadline status = %d, want %d: %s", release.Code, http.StatusOK, release.Body.String())
	}
	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var redeliveryBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&redeliveryBody); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(redeliveryBody.ReceivedMessages) != 1 || redeliveryBody.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery = %#v", redeliveryBody.ReceivedMessages)
	}

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{
		"ackIds":["`+redeliveryBody.ReceivedMessages[0].AckID+`"]
	}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	now = now.Add(3 * time.Second)
	afterAck := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(afterAck.Body.String(), "receivedMessages") {
		t.Fatalf("acked message should not be received, got %s", afterAck.Body.String())
	}
}

func TestRESTPullWaitsWhenReturnImmediatelyIsFalse(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		PullWaitTimeout:           500 * time.Millisecond,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	published := make(chan struct{})
	go func() {
		defer close(published)
		time.Sleep(20 * time.Millisecond)
		performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
			"messages":[{"data":"bG9uZy1wb2xs"}]
		}`)
	}()

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{
		"maxMessages":1,
		"returnImmediately":false
	}`)
	<-published
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "bG9uZy1wb2xs") {
		t.Fatalf("waited pull did not receive published message: %s", pull.Body.String())
	}
}

func TestRESTRejectsExpiredAckID(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZXhwaXJlZA=="}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var firstPullBody struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&firstPullBody); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(firstPullBody.ReceivedMessages) != 1 || firstPullBody.ReceivedMessages[0].AckID == "" {
		t.Fatalf("first pull = %#v", firstPullBody.ReceivedMessages)
	}
	expiredAckID := firstPullBody.ReceivedMessages[0].AckID

	now = now.Add(3 * time.Second)
	staleAck := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+expiredAckID+`"]}`)
	if staleAck.Code != http.StatusOK {
		t.Fatalf("stale ack status = %d, want %d: %s", staleAck.Code, http.StatusOK, staleAck.Body.String())
	}

	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var redeliveryBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&redeliveryBody); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(redeliveryBody.ReceivedMessages) != 1 || redeliveryBody.ReceivedMessages[0].AckID == expiredAckID || redeliveryBody.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery = %#v, expired ackID = %q", redeliveryBody.ReceivedMessages, expiredAckID)
	}
}

func TestRESTModifyAckDeadlineExtendsLease(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZXh0ZW5k"}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var first struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 1 || first.ReceivedMessages[0].AckID == "" {
		t.Fatalf("first pull receivedMessages = %#v", first.ReceivedMessages)
	}

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+first.ReceivedMessages[0].AckID+`"],
		"ackDeadlineSeconds":5
	}`)
	if modify.Code != http.StatusOK {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusOK, modify.Body.String())
	}

	now = now.Add(3 * time.Second)
	stillLeased := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if stillLeased.Code != http.StatusOK {
		t.Fatalf("still leased pull status = %d, want %d: %s", stillLeased.Code, http.StatusOK, stillLeased.Body.String())
	}
	if strings.Contains(stillLeased.Body.String(), "receivedMessages") {
		t.Fatalf("extended lease should hide message past original deadline, got %s", stillLeased.Body.String())
	}

	now = now.Add(3 * time.Second)
	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var second struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&second); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery after extended deadline = %#v", second.ReceivedMessages)
	}
}

func TestRESTRejectsEmptyAckIDsWithoutAckingBacklog(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"c3RpbGwtYmFja2xvZw=="}]
	}`)

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":[""]}`)
	if ack.Code != http.StatusBadRequest {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusBadRequest, ack.Body.String())
	}
	if !strings.Contains(ack.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("ack error body = %s", ack.Body.String())
	}

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":[" "],
		"ackDeadlineSeconds":0
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "c3RpbGwtYmFja2xvZw==") {
		t.Fatalf("empty ack ID should not remove backlog, got %s", pull.Body.String())
	}
}

func TestRESTRetryPolicyDelaysRedeliveryAfterAckDeadline(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"retryPolicy":{"minimumBackoff":"5s","maximumBackoff":"10s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0cnk="}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var first struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 1 || first.ReceivedMessages[0].DeliveryAttempt != 1 {
		t.Fatalf("first receivedMessages = %#v", first.ReceivedMessages)
	}

	now = now.Add(3 * time.Second)
	beforeBackoff := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeBackoff.Body.String(), "receivedMessages") {
		t.Fatalf("retryPolicy.minimumBackoff should delay redelivery, got %s", beforeBackoff.Body.String())
	}

	now = now.Add(5 * time.Second)
	afterBackoff := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var second struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(afterBackoff.Body).Decode(&second); err != nil {
		t.Fatalf("decode second pull: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("second receivedMessages = %#v", second.ReceivedMessages)
	}
}

func TestRESTPublishFansOutPerSubscription(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZmFub3V0"}]}`)

	for _, subscription := range []string{"orders-a", "orders-b"} {
		pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/"+subscription+":pull", `{"maxMessages":1}`)
		if pull.Code != http.StatusOK {
			t.Fatalf("%s pull status = %d, want %d", subscription, pull.Code, http.StatusOK)
		}
		if !strings.Contains(pull.Body.String(), "ZmFub3V0") {
			t.Fatalf("%s did not receive fan-out message: %s", subscription, pull.Body.String())
		}
	}
}

func TestRESTAckDoesNotRemoveMessageForOtherSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"c2hhcmVk"}]}`)

	pullA := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-a:pull", `{"maxMessages":1}`)
	var pulledA struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pullA.Body).Decode(&pulledA); err != nil {
		t.Fatalf("decode pull A: %v", err)
	}
	if len(pulledA.ReceivedMessages) != 1 {
		t.Fatalf("pull A receivedMessages = %#v", pulledA.ReceivedMessages)
	}
	ackA := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-a:acknowledge", `{"ackIds":["`+pulledA.ReceivedMessages[0].AckID+`"]}`)
	if ackA.Code != http.StatusOK {
		t.Fatalf("ack A status = %d, want %d: %s", ackA.Code, http.StatusOK, ackA.Body.String())
	}

	pullB := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-b:pull", `{"maxMessages":1}`)
	if pullB.Code != http.StatusOK {
		t.Fatalf("pull B status = %d, want %d: %s", pullB.Code, http.StatusOK, pullB.Body.String())
	}
	if !strings.Contains(pullB.Body.String(), "c2hhcmVk") {
		t.Fatalf("subscription B lost fan-out message after A ack: %s", pullB.Body.String())
	}
}

func TestRESTPullRespectsOrderingKeyGate(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 10,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableMessageOrdering":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"Zmlyc3Q=","orderingKey":"customer-1"},
			{"data":"c2Vjb25k","orderingKey":"customer-1"},
			{"data":"b3RoZXI=","orderingKey":"customer-2"}
		]
	}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":3}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var first struct {
		ReceivedMessages []struct {
			AckID   string `json:"ackId"`
			Message struct {
				Data        string `json:"data"`
				OrderingKey string `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 2 {
		t.Fatalf("first pull receivedMessages = %#v", first.ReceivedMessages)
	}
	if first.ReceivedMessages[0].Message.Data != "Zmlyc3Q=" || first.ReceivedMessages[1].Message.Data != "b3RoZXI=" {
		t.Fatalf("ordering gate delivered wrong messages: %#v", first.ReceivedMessages)
	}
	if first.ReceivedMessages[0].Message.OrderingKey != "customer-1" || first.ReceivedMessages[1].Message.OrderingKey != "customer-2" {
		t.Fatalf("ordering keys = %#v", first.ReceivedMessages)
	}

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+first.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	secondPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":3}`)
	var second struct {
		ReceivedMessages []struct {
			Message struct {
				Data        string `json:"data"`
				OrderingKey string `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(secondPull.Body).Decode(&second); err != nil {
		t.Fatalf("decode second pull: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].Message.Data != "c2Vjb25k" || second.ReceivedMessages[0].Message.OrderingKey != "customer-1" {
		t.Fatalf("second pull receivedMessages = %#v", second.ReceivedMessages)
	}
}

func TestRESTOrderingKeyGateBlocksBehindRetryDelay(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"enableMessageOrdering":true,
		"retryPolicy":{"minimumBackoff":"5s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"Zmlyc3Q=","orderingKey":"customer-1"},
			{"data":"c2Vjb25k","orderingKey":"customer-1"}
		]
	}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK || !strings.Contains(firstPull.Body.String(), "Zmlyc3Q=") {
		t.Fatalf("first pull = status %d body %s", firstPull.Code, firstPull.Body.String())
	}

	now = now.Add(2 * time.Second)
	blockedPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":2}`)
	if blockedPull.Code != http.StatusOK {
		t.Fatalf("blocked pull status = %d, want %d: %s", blockedPull.Code, http.StatusOK, blockedPull.Body.String())
	}
	if strings.Contains(blockedPull.Body.String(), "receivedMessages") || strings.Contains(blockedPull.Body.String(), "c2Vjb25k") {
		t.Fatalf("later ordering key message bypassed retry-delayed predecessor: %s", blockedPull.Body.String())
	}

	now = now.Add(4 * time.Second)
	redeliveryPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":2}`)
	if redeliveryPull.Code != http.StatusOK || !strings.Contains(redeliveryPull.Body.String(), "Zmlyc3Q=") || strings.Contains(redeliveryPull.Body.String(), "c2Vjb25k") {
		t.Fatalf("redelivery pull = status %d body %s", redeliveryPull.Code, redeliveryPull.Body.String())
	}
}

func TestRESTDeadLetterPolicyTransfersAfterMaxDeliveryAttempts(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-dlq-sub", `{
		"topic":"projects/devcloud/topics/orders-dlq"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"ZGxx","attributes":{"kind":"retry"},"orderingKey":"group-1"}]
	}`)

	for attempt := 1; attempt <= 5; attempt++ {
		pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
		if pull.Code != http.StatusOK {
			t.Fatalf("attempt %d pull status = %d, want %d: %s", attempt, pull.Code, http.StatusOK, pull.Body.String())
		}
		var pulled struct {
			ReceivedMessages []struct {
				DeliveryAttempt int `json:"deliveryAttempt"`
			} `json:"receivedMessages"`
		}
		if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
			t.Fatalf("decode attempt %d pull: %v", attempt, err)
		}
		if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].DeliveryAttempt != attempt {
			t.Fatalf("attempt %d receivedMessages = %#v", attempt, pulled.ReceivedMessages)
		}
		now = now.Add(3 * time.Second)
	}

	mainAfterLimit := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(mainAfterLimit.Body.String(), "receivedMessages") {
		t.Fatalf("main subscription should not receive message after dead-letter transfer: %s", mainAfterLimit.Body.String())
	}

	dlqPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-dlq-sub:pull", `{"maxMessages":1}`)
	if dlqPull.Code != http.StatusOK {
		t.Fatalf("dlq pull status = %d, want %d: %s", dlqPull.Code, http.StatusOK, dlqPull.Body.String())
	}
	var dlq struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
			Message         struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				OrderingKey string            `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(dlqPull.Body).Decode(&dlq); err != nil {
		t.Fatalf("decode dlq pull: %v", err)
	}
	if len(dlq.ReceivedMessages) != 1 || dlq.ReceivedMessages[0].DeliveryAttempt != 1 {
		t.Fatalf("dlq receivedMessages = %#v", dlq.ReceivedMessages)
	}
	message := dlq.ReceivedMessages[0].Message
	if message.Data != "ZGxx" || message.Attributes["kind"] != "retry" || message.OrderingKey != "group-1" {
		t.Fatalf("dlq message = %#v", message)
	}
}

func TestRESTRetryPolicyUsesCappedBackoffAfterLeaseExpiration(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"retryPolicy":{"minimumBackoff":"2s","maximumBackoff":"3s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnk="}]
	}`)

	first := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), "cmV0cnk=") {
		t.Fatalf("first pull = status %d body %s", first.Code, first.Body.String())
	}
	now = now.Add(2 * time.Second)
	beforeMinimum := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeMinimum.Body.String(), "receivedMessages") {
		t.Fatalf("message was redelivered before minimum backoff elapsed: %s", beforeMinimum.Body.String())
	}
	now = now.Add(1 * time.Second)
	second := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"deliveryAttempt":2`) {
		t.Fatalf("second pull = status %d body %s", second.Code, second.Body.String())
	}
	now = now.Add(3 * time.Second)
	beforeMaximum := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeMaximum.Body.String(), "receivedMessages") {
		t.Fatalf("message was redelivered before capped maximum backoff elapsed: %s", beforeMaximum.Body.String())
	}
	now = now.Add(1 * time.Second)
	third := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if third.Code != http.StatusOK || !strings.Contains(third.Body.String(), `"deliveryAttempt":3`) {
		t.Fatalf("third pull = status %d body %s", third.Code, third.Body.String())
	}
}

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

func TestRESTSchemaCRUDAndPagination(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var schema schemaResource
	if err := json.NewDecoder(create.Body).Decode(&schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	if schema.Name != "projects/devcloud/schemas/order-event" || schema.Type != "AVRO" || schema.RevisionID != "1" {
		t.Fatalf("schema = %#v", schema)
	}

	duplicate := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{"type":"AVRO"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/schemas/payment-event", `{"type":"PROTOCOL_BUFFER","definition":"message PaymentEvent {}"}`)
	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?pageSize=1", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d: %s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed struct {
		Schemas       []schemaResource `json:"schemas"`
		NextPageToken string           `json:"nextPageToken"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode schemas: %v", err)
	}
	if len(listed.Schemas) != 1 || listed.Schemas[0].Name != "projects/devcloud/schemas/order-event" || listed.NextPageToken == "" {
		t.Fatalf("schemas page = %#v", listed)
	}

	deleteSchema := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/schemas/order-event", "")
	if deleteSchema.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d: %s", deleteSchema.Code, http.StatusNoContent, deleteSchema.Body.String())
	}
	getDeleted := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if getDeleted.Code != http.StatusNotFound {
		t.Fatalf("get deleted status = %d, want %d", getDeleted.Code, http.StatusNotFound)
	}
}

func TestRESTSchemaViewBasicOmitsDefinition(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	basicGet := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event?view=BASIC", "")
	if basicGet.Code != http.StatusOK {
		t.Fatalf("basic get status = %d, want %d: %s", basicGet.Code, http.StatusOK, basicGet.Body.String())
	}
	var basic schemaResource
	if err := json.NewDecoder(basicGet.Body).Decode(&basic); err != nil {
		t.Fatalf("decode basic schema: %v", err)
	}
	if basic.Name != "projects/devcloud/schemas/order-event" || basic.Definition != "" {
		t.Fatalf("basic schema = %#v", basic)
	}

	fullList := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?view=FULL", "")
	if fullList.Code != http.StatusOK || !strings.Contains(fullList.Body.String(), "OrderEvent") {
		t.Fatalf("full list status = %d body %s", fullList.Code, fullList.Body.String())
	}
	basicList := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?view=BASIC", "")
	if basicList.Code != http.StatusOK || strings.Contains(basicList.Body.String(), "OrderEvent") {
		t.Fatalf("basic list status = %d body %s", basicList.Code, basicList.Body.String())
	}
	invalid := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event?view=SECRET", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid view status = %d, want %d: %s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestRESTSchemaValidationAndPersistence(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	missingID := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas", `{"type":"AVRO"}`)
	if missingID.Code != http.StatusBadRequest {
		t.Fatalf("missing schemaId status = %d, want %d: %s", missingID.Code, http.StatusBadRequest, missingID.Body.String())
	}
	nameMismatch := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"name":"projects/devcloud/schemas/other",
		"type":"AVRO"
	}`)
	if nameMismatch.Code != http.StatusBadRequest {
		t.Fatalf("name mismatch status = %d, want %d: %s", nameMismatch.Code, http.StatusBadRequest, nameMismatch.Body.String())
	}
	invalidType := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{"type":"JSON_SCHEMA"}`)
	if invalidType.Code != http.StatusBadRequest {
		t.Fatalf("invalid type status = %d, want %d: %s", invalidType.Code, http.StatusBadRequest, invalidType.Body.String())
	}

	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	persisted := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	create := performPubSubRequest(persisted, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}",
		"revisionId":"custom-revision"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create persisted status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var schema schemaResource
	if err := json.NewDecoder(get.Body).Decode(&schema); err != nil {
		t.Fatalf("decode reloaded schema: %v", err)
	}
	if schema.RevisionID != "custom-revision" || schema.Definition == "" {
		t.Fatalf("reloaded schema = %#v", schema)
	}
}

func TestRESTSchemaValidateMessageAcceptsExistingAndInlineSchemas(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create schema status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	existing := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"name":"projects/devcloud/schemas/order-event",
		"message":"e30=",
		"encoding":"JSON"
	}`)
	if existing.Code != http.StatusOK {
		t.Fatalf("validate existing status = %d, want %d: %s", existing.Code, http.StatusOK, existing.Body.String())
	}
	if strings.Contains(existing.Body.String(), "e30=") || strings.Contains(existing.Body.String(), "OrderEvent") {
		t.Fatalf("validate existing leaked request payload: %s", existing.Body.String())
	}

	inline := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"PROTOCOL_BUFFER","definition":"message OrderEvent {}"},
		"message":"CgVvcmRlckE=",
		"encoding":"BINARY"
	}`)
	if inline.Code != http.StatusOK {
		t.Fatalf("validate inline status = %d, want %d: %s", inline.Code, http.StatusOK, inline.Body.String())
	}

	unpaddedInline := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"PROTOCOL_BUFFER","definition":"message OrderEvent {}"},
		"message":"CgVvcmRlckE",
		"encoding":"BINARY"
	}`)
	if unpaddedInline.Code != http.StatusOK {
		t.Fatalf("validate unpadded inline status = %d, want %d: %s", unpaddedInline.Code, http.StatusOK, unpaddedInline.Body.String())
	}
}

func TestRESTSchemaValidateMessageRejectsInvalidRequests(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	invalidCreate := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=invalid-avro", `{
		"type":"AVRO",
		"definition":"not-json"
	}`)
	if invalidCreate.Code != http.StatusBadRequest {
		t.Fatalf("invalid create status = %d, want %d: %s", invalidCreate.Code, http.StatusBadRequest, invalidCreate.Body.String())
	}

	missingSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{"message":"e30="}`)
	if missingSchema.Code != http.StatusBadRequest {
		t.Fatalf("missing schema status = %d, want %d: %s", missingSchema.Code, http.StatusBadRequest, missingSchema.Body.String())
	}

	missingStoredSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"name":"projects/devcloud/schemas/missing",
		"message":"e30="
	}`)
	if missingStoredSchema.Code != http.StatusNotFound {
		t.Fatalf("missing stored schema status = %d, want %d: %s", missingStoredSchema.Code, http.StatusNotFound, missingStoredSchema.Body.String())
	}

	invalidMessage := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO"},
		"message":"not-base64"
	}`)
	if invalidMessage.Code != http.StatusBadRequest {
		t.Fatalf("invalid message status = %d, want %d: %s", invalidMessage.Code, http.StatusBadRequest, invalidMessage.Body.String())
	}

	invalidJSON := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO"},
		"message":"bm90LWpzb24=",
		"encoding":"JSON"
	}`)
	if invalidJSON.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want %d: %s", invalidJSON.Code, http.StatusBadRequest, invalidJSON.Body.String())
	}

	invalidInlineSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO","definition":"[]"},
		"message":"e30=",
		"encoding":"JSON"
	}`)
	if invalidInlineSchema.Code != http.StatusBadRequest {
		t.Fatalf("invalid inline schema status = %d, want %d: %s", invalidInlineSchema.Code, http.StatusBadRequest, invalidInlineSchema.Body.String())
	}

	wrongProject := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"name":"projects/other/schemas/order-event","type":"AVRO"},
		"message":"e30="
	}`)
	if wrongProject.Code != http.StatusBadRequest {
		t.Fatalf("wrong project status = %d, want %d: %s", wrongProject.Code, http.StatusBadRequest, wrongProject.Body.String())
	}
}

func TestRESTTopicAndSubscriptionIAMNoops(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	getTopicPolicy := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:getIamPolicy", `{}`)
	if getTopicPolicy.Code != http.StatusOK {
		t.Fatalf("topic getIamPolicy status = %d, want %d: %s", getTopicPolicy.Code, http.StatusOK, getTopicPolicy.Body.String())
	}
	if !strings.Contains(getTopicPolicy.Body.String(), `"bindings":[]`) {
		t.Fatalf("topic policy = %s", getTopicPolicy.Body.String())
	}

	testTopicPermissions := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:testIamPermissions", `{
		"permissions":["pubsub.topics.get","pubsub.topics.publish"]
	}`)
	if testTopicPermissions.Code != http.StatusOK {
		t.Fatalf("topic testIamPermissions status = %d, want %d: %s", testTopicPermissions.Code, http.StatusOK, testTopicPermissions.Body.String())
	}
	if !strings.Contains(testTopicPermissions.Body.String(), "pubsub.topics.publish") {
		t.Fatalf("topic permissions = %s", testTopicPermissions.Body.String())
	}

	setSubscriptionPolicy := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:setIamPolicy", `{
		"policy":{"version":1,"bindings":[{"role":"roles/pubsub.viewer","members":["allUsers"]}]}
	}`)
	if setSubscriptionPolicy.Code != http.StatusOK {
		t.Fatalf("subscription setIamPolicy status = %d, want %d: %s", setSubscriptionPolicy.Code, http.StatusOK, setSubscriptionPolicy.Body.String())
	}
	if !strings.Contains(setSubscriptionPolicy.Body.String(), "roles/pubsub.viewer") {
		t.Fatalf("subscription policy = %s", setSubscriptionPolicy.Body.String())
	}

	missingTopic := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/missing:getIamPolicy", `{}`)
	if missingTopic.Code != http.StatusNotFound {
		t.Fatalf("missing topic IAM status = %d, want %d: %s", missingTopic.Code, http.StatusNotFound, missingTopic.Body.String())
	}
	wrongMethod := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub:getIamPolicy", "")
	if wrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("subscription IAM wrong method status = %d, want %d: %s", wrongMethod.Code, http.StatusMethodNotAllowed, wrongMethod.Body.String())
	}
}

func TestRESTCleansUpUnreferencedMessages(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	publishWithoutSubscribers := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bm8tc3Vi"}]}`)
	if publishWithoutSubscribers.Code != http.StatusOK {
		t.Fatalf("publish without subscribers status = %d, want %d: %s", publishWithoutSubscribers.Code, http.StatusOK, publishWithoutSubscribers.Body.String())
	}
	if len(server.messages) != 0 {
		t.Fatalf("messages retained without subscriptions = %#v", server.messages)
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"YWNrZWQ="}]}`)
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
	if len(server.messages) != 0 {
		t.Fatalf("acked messages retained = %#v", server.messages)
	}
}

func TestRESTRetainAckedMessagesAllowsSeekReplay(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud"})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"retainAckedMessages":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0YWluLWFja2Vk"}]}`)

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
	if len(server.messages) != 1 {
		t.Fatalf("acked message was not retained: %#v", server.messages)
	}

	emptyPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(emptyPull.Body.String(), "receivedMessages") {
		t.Fatalf("acked retained message should stay hidden until seek, got %s", emptyPull.Body.String())
	}
	seek := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"time":"2026-05-02T11:59:59Z"
	}`)
	if seek.Code != http.StatusOK {
		t.Fatalf("seek status = %d, want %d: %s", seek.Code, http.StatusOK, seek.Body.String())
	}
	replayed := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replayed pull status = %d, want %d: %s", replayed.Code, http.StatusOK, replayed.Body.String())
	}
	if !strings.Contains(replayed.Body.String(), "cmV0YWluLWFja2Vk") {
		t.Fatalf("seek did not replay retained acked message: %s", replayed.Body.String())
	}
}

func TestRESTCleansUpMessagesAfterRetention(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 5,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0YWluZWQ="}]}`)

	now = now.Add(6 * time.Second)
	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if strings.Contains(pull.Body.String(), "receivedMessages") {
		t.Fatalf("expired message should not be received, got %s", pull.Body.String())
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("snapshot = %#v", snapshot.Subscriptions)
	}
}

func TestRESTSubscriptionRetentionIsPerSubscription(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 60,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/short-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"5s"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/long-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"30s"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cGVyLXN1Yi1yZXRlbnRpb24="}]}`)

	now = now.Add(6 * time.Second)
	shortPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/short-retention:pull", `{"maxMessages":1}`)
	if shortPull.Code != http.StatusOK {
		t.Fatalf("short pull status = %d, want %d: %s", shortPull.Code, http.StatusOK, shortPull.Body.String())
	}
	if strings.Contains(shortPull.Body.String(), "receivedMessages") {
		t.Fatalf("short-retention subscription should not receive expired message, got %s", shortPull.Body.String())
	}

	longPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/long-retention:pull", `{"maxMessages":1}`)
	if longPull.Code != http.StatusOK {
		t.Fatalf("long pull status = %d, want %d: %s", longPull.Code, http.StatusOK, longPull.Body.String())
	}
	if !strings.Contains(longPull.Body.String(), "cGVyLXN1Yi1yZXRlbnRpb24=") {
		t.Fatalf("long-retention subscription should still receive message, got %s", longPull.Body.String())
	}
}

func TestRESTTopicRetentionAppliesWhenSubscriptionDoesNotOverride(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 60,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"messageRetentionDuration":"5s"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/topic-retention", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/subscription-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"30s"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"dG9waWMtcmV0ZW50aW9u"}]}`)

	now = now.Add(6 * time.Second)
	topicRetentionPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/topic-retention:pull", `{"maxMessages":1}`)
	if topicRetentionPull.Code != http.StatusOK {
		t.Fatalf("topic retention pull status = %d, want %d: %s", topicRetentionPull.Code, http.StatusOK, topicRetentionPull.Body.String())
	}
	if strings.Contains(topicRetentionPull.Body.String(), "receivedMessages") {
		t.Fatalf("topic-retention subscription should not receive expired message, got %s", topicRetentionPull.Body.String())
	}

	subscriptionRetentionPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/subscription-retention:pull", `{"maxMessages":1}`)
	if subscriptionRetentionPull.Code != http.StatusOK {
		t.Fatalf("subscription retention pull status = %d, want %d: %s", subscriptionRetentionPull.Code, http.StatusOK, subscriptionRetentionPull.Body.String())
	}
	if !strings.Contains(subscriptionRetentionPull.Body.String(), "dG9waWMtcmV0ZW50aW9u") {
		t.Fatalf("subscription retention should override topic retention, got %s", subscriptionRetentionPull.Body.String())
	}
}

func TestSnapshotExposesSafeDeliverySummaries(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 10,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableExactlyOnceDelivery":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"YmFja2xvZw==","attributes":{"secret":"hidden"},"orderingKey":"backlog-key"},
			{"data":"aW4tZmxpZ2h0","attributes":{"secret":"hidden"},"orderingKey":"leased-key"}
		]
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)

	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %#v", snapshot.Subscriptions)
	}
	subscription := snapshot.Subscriptions[0]
	if !subscription.EnableExactlyOnceDelivery {
		t.Fatalf("subscription snapshot did not expose exactly-once metadata: %#v", subscription)
	}
	if subscription.BacklogMessages != 1 || subscription.InFlightMessages != 1 || len(subscription.RecentDeliveries) != 2 {
		t.Fatalf("subscription snapshot = %#v", subscription)
	}
	if subscription.RecentDeliveries[0].State != "in-flight" || subscription.RecentDeliveries[0].LeaseDeadline == "" {
		t.Fatalf("in-flight delivery = %#v", subscription.RecentDeliveries[0])
	}
	if subscription.RecentDeliveries[1].State != "backlog" || subscription.RecentDeliveries[1].LeaseDeadline != "" {
		t.Fatalf("backlog delivery = %#v", subscription.RecentDeliveries[1])
	}
	data, err := json.Marshal(subscription.RecentDeliveries)
	if err != nil {
		t.Fatalf("marshal delivery summaries: %v", err)
	}
	for _, forbidden := range []string{"ackId", "YmFja2xvZw==", "aW4tZmxpZ2h0", "secret"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("delivery summaries leaked %q: %s", forbidden, data)
		}
	}
}

func TestSnapshotExpiresLeasesAndShowsRetryDelay(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"retryPolicy":{"minimumBackoff":"5s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnktZGVsYXk=","attributes":{"secret":"hidden"}}]
	}`)

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK || !strings.Contains(pull.Body.String(), `"deliveryAttempt":1`) {
		t.Fatalf("pull = status %d body %s", pull.Code, pull.Body.String())
	}

	now = now.Add(2 * time.Second)
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || len(snapshot.Subscriptions[0].RecentDeliveries) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	subscription := snapshot.Subscriptions[0]
	if subscription.InFlightMessages != 0 || subscription.BacklogMessages != 1 {
		t.Fatalf("subscription counters = %#v", subscription)
	}
	delivery := subscription.RecentDeliveries[0]
	if delivery.State != "delayed" || delivery.LeaseDeadline != "" || delivery.NextDeliveryTime == "" {
		t.Fatalf("delivery summary = %#v", delivery)
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		t.Fatalf("marshal delivery summary: %v", err)
	}
	for _, forbidden := range []string{"ackId", "cmV0cnktZGVsYXk=", "secret"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("delivery summary leaked %q: %s", forbidden, data)
		}
	}
}

func performPubSubRequest(server *Server, method string, path string, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	server.ServeHTTP(rec, req)
	return rec
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
