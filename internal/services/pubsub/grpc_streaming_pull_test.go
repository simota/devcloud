package pubsub

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

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
