package pubsub

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

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
