package pubsub

import (
	"context"
	"net"
	"encoding/json"
	"io"
	"net/http"
	"os"
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
