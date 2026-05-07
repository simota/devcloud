package pubsub

import (
	"context"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
