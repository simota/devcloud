package pubsub

import (
	"context"
	"net"
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
)

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
