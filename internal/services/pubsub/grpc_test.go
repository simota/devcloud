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
