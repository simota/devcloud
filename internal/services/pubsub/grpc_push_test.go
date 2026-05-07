package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
