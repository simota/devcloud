package pubsub

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pubSubGRPCAdapter struct {
	pubsubpb.UnimplementedPublisherServer
	pubsubpb.UnimplementedSubscriberServer
	pubsubpb.UnimplementedSchemaServiceServer
	server *Server
}

func (s *Server) newGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer()
	adapter := &pubSubGRPCAdapter{server: s}
	pubsubpb.RegisterPublisherServer(grpcServer, adapter)
	pubsubpb.RegisterSubscriberServer(grpcServer, adapter)
	pubsubpb.RegisterSchemaServiceServer(grpcServer, adapter)
	return grpcServer
}

func (a *pubSubGRPCAdapter) CreateTopic(ctx context.Context, request *pubsubpb.Topic) (*pubsubpb.Topic, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	if err := validateTopicMetadata(topicResource{
		Name:                     request.GetName(),
		Labels:                   request.GetLabels(),
		MessageRetentionDuration: grpcDurationString(request.GetMessageRetentionDuration()),
		SchemaSettings:           grpcSchemaSettings(request.GetSchemaSettings()),
		KMSKeyName:               request.GetKmsKeyName(),
	}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	now := a.server.now().UTC().Format(time.RFC3339Nano)
	topic := topicResource{
		Name:                     request.GetName(),
		Labels:                   copyStringMap(request.GetLabels()),
		CreatedAt:                now,
		UpdatedAt:                now,
		MessageRetentionDuration: grpcDurationString(request.GetMessageRetentionDuration()),
		SchemaSettings:           grpcSchemaSettings(request.GetSchemaSettings()),
		KMSKeyName:               request.GetKmsKeyName(),
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, exists := a.server.topics[topic.Name]; exists {
		return nil, status.Error(codes.AlreadyExists, "topic already exists")
	}
	a.server.topics[topic.Name] = topic
	if err := a.server.saveResourcesLocked(); err != nil {
		delete(a.server.topics, topic.Name)
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return topic.toProto(), nil
}

func (a *pubSubGRPCAdapter) GetTopic(ctx context.Context, request *pubsubpb.GetTopicRequest) (*pubsubpb.Topic, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	topic, found := a.server.topics[request.GetTopic()]
	if !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	return topic.toProto(), nil
}

func (a *pubSubGRPCAdapter) UpdateTopic(ctx context.Context, request *pubsubpb.UpdateTopicRequest) (*pubsubpb.Topic, error) {
	_ = ctx
	topicUpdate := request.GetTopic()
	if topicUpdate == nil || !validFullTopicName(topicUpdate.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	paths, err := grpcUpdateMaskPaths(request.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, err
	}
	metadata := topicResource{
		Name:                     topicUpdate.GetName(),
		Labels:                   topicUpdate.GetLabels(),
		MessageRetentionDuration: grpcDurationString(topicUpdate.GetMessageRetentionDuration()),
		SchemaSettings:           grpcSchemaSettings(topicUpdate.GetSchemaSettings()),
		KMSKeyName:               topicUpdate.GetKmsKeyName(),
	}
	if err := validateTopicMetadata(metadata); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	topic, found := a.server.topics[topicUpdate.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	for _, path := range paths {
		switch grpcCanonicalMaskPath(path) {
		case "labels":
			topic.Labels = copyStringMap(topicUpdate.GetLabels())
		case "message_retention_duration":
			topic.MessageRetentionDuration = grpcDurationString(topicUpdate.GetMessageRetentionDuration())
		case "schema_settings":
			topic.SchemaSettings = grpcSchemaSettings(topicUpdate.GetSchemaSettings())
		case "kms_key_name":
			topic.KMSKeyName = topicUpdate.GetKmsKeyName()
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported topic update mask path %q", path)
		}
	}
	topic.UpdatedAt = a.server.now().UTC().Format(time.RFC3339Nano)
	a.server.topics[topic.Name] = topic
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return topic.toProto(), nil
}

func (a *pubSubGRPCAdapter) ListTopics(ctx context.Context, request *pubsubpb.ListTopicsRequest) (*pubsubpb.ListTopicsResponse, error) {
	_ = ctx
	project, ok := grpcProjectID(request.GetProject())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	topics := make([]topicResource, 0, len(a.server.topics))
	for _, topic := range a.server.topics {
		if resourceProject(topic.Name) == project {
			topics = append(topics, topic)
		}
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	start, end, nextToken, err := grpcPageBounds(len(topics), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &pubsubpb.ListTopicsResponse{NextPageToken: nextToken}
	for _, topic := range topics[start:end] {
		response.Topics = append(response.Topics, topic.toProto())
	}
	return response, nil
}

func (a *pubSubGRPCAdapter) ListTopicSubscriptions(ctx context.Context, request *pubsubpb.ListTopicSubscriptionsRequest) (*pubsubpb.ListTopicSubscriptionsResponse, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.topics[request.GetTopic()]; !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	subscriptions := make([]string, 0)
	for _, subscription := range a.server.subscriptions {
		if subscription.Topic == request.GetTopic() {
			subscriptions = append(subscriptions, subscription.Name)
		}
	}
	sort.Strings(subscriptions)
	start, end, nextToken, err := grpcPageBounds(len(subscriptions), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &pubsubpb.ListTopicSubscriptionsResponse{
		Subscriptions: subscriptions[start:end],
		NextPageToken: nextToken,
	}, nil
}

func (a *pubSubGRPCAdapter) ListTopicSnapshots(ctx context.Context, request *pubsubpb.ListTopicSnapshotsRequest) (*pubsubpb.ListTopicSnapshotsResponse, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.topics[request.GetTopic()]; !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	now := a.server.now().UTC()
	snapshots := make([]string, 0)
	for _, snapshot := range a.server.snapshots {
		if snapshot.Topic == request.GetTopic() && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot.Name)
		}
	}
	sort.Strings(snapshots)
	start, end, nextToken, err := grpcPageBounds(len(snapshots), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &pubsubpb.ListTopicSnapshotsResponse{
		Snapshots:     snapshots[start:end],
		NextPageToken: nextToken,
	}, nil
}

func (a *pubSubGRPCAdapter) Publish(ctx context.Context, request *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	if len(request.GetMessages()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "messages are required")
	}
	for _, message := range request.GetMessages() {
		if message == nil {
			return nil, status.Error(codes.InvalidArgument, "messages must not contain nil values")
		}
		encoded := base64.StdEncoding.EncodeToString(message.GetData())
		if err := validatePublishMessage(encoded, message.GetAttributes()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	topic, found := a.server.topics[request.GetTopic()]
	if !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	for _, message := range request.GetMessages() {
		encoded := base64.StdEncoding.EncodeToString(message.GetData())
		if err := validateMessageAgainstTopicSchemaSettings(encoded, topic.SchemaSettings); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	now := a.server.now().UTC().Format(time.RFC3339Nano)
	messageIDs := make([]string, 0, len(request.GetMessages()))
	for _, incoming := range request.GetMessages() {
		a.server.nextMessageID++
		messageID := fmt.Sprintf("%d", a.server.nextMessageID)
		message := pubsubMessage{
			Data:        base64.StdEncoding.EncodeToString(incoming.GetData()),
			Attributes:  copyStringMap(incoming.GetAttributes()),
			MessageID:   messageID,
			PublishTime: now,
			OrderingKey: incoming.GetOrderingKey(),
		}
		a.server.messages[messageID] = message
		for _, subscription := range a.server.subscriptions {
			if subscription.Topic == request.GetTopic() && !subscription.Detached && subscriptionMatchesMessage(subscription, message) {
				a.server.deliveries[subscription.Name] = append(a.server.deliveries[subscription.Name], deliveryRecord{MessageID: messageID})
			}
		}
		messageIDs = append(messageIDs, messageID)
	}
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &pubsubpb.PublishResponse{MessageIds: messageIDs}, nil
}

func (a *pubSubGRPCAdapter) DeleteTopic(ctx context.Context, request *pubsubpb.DeleteTopicRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.topics[request.GetTopic()]; !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	delete(a.server.topics, request.GetTopic())
	for name, subscription := range a.server.subscriptions {
		if subscription.Topic == request.GetTopic() {
			subscription.Topic = "_deleted-topic_"
			subscription.UpdatedAt = a.server.now().UTC().Format(time.RFC3339Nano)
			a.server.subscriptions[name] = subscription
		}
	}
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) CreateSubscription(ctx context.Context, request *pubsubpb.Subscription) (*pubsubpb.Subscription, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "subscription is required")
	}
	if !validFullTopicName(request.GetTopic()) {
		return nil, status.Error(codes.InvalidArgument, "invalid topic name")
	}
	name := request.GetName()
	if name != "" && !validFullSubscriptionName(name) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	subscription := subscriptionResource{
		Name:                      name,
		Topic:                     request.GetTopic(),
		Labels:                    copyStringMap(request.GetLabels()),
		AckDeadlineSeconds:        int(request.GetAckDeadlineSeconds()),
		EnableMessageOrdering:     request.GetEnableMessageOrdering(),
		EnableExactlyOnceDelivery: request.GetEnableExactlyOnceDelivery(),
		RetainAckedMessages:       request.GetRetainAckedMessages(),
		MessageRetentionDuration:  grpcDurationString(request.GetMessageRetentionDuration()),
		Filter:                    request.GetFilter(),
		DeadLetterPolicy:          grpcDeadLetterPolicy(request.GetDeadLetterPolicy()),
		RetryPolicy:               grpcRetryPolicy(request.GetRetryPolicy()),
		PushConfig:                grpcPushConfig(request.GetPushConfig()),
	}
	if subscription.AckDeadlineSeconds == 0 {
		subscription.AckDeadlineSeconds = a.server.config.DefaultAckDeadlineSeconds
	}
	if err := validateSubscriptionMetadata(subscription); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateSubscriptionFilter(subscription.Filter); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateDeadLetterPolicy(subscription.DeadLetterPolicy); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRetryPolicy(subscription.RetryPolicy); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validatePushConfig(subscription.PushConfig); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	now := a.server.now().UTC().Format(time.RFC3339Nano)
	subscription.CreatedAt = now
	subscription.UpdatedAt = now

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if subscription.Name == "" {
		subscription.Name = a.server.generatedSubscriptionNameLocked(resourceProject(subscription.Topic))
	}
	if _, exists := a.server.subscriptions[subscription.Name]; exists {
		return nil, status.Error(codes.AlreadyExists, "subscription already exists")
	}
	if _, found := a.server.topics[subscription.Topic]; !found {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	if !a.server.deadLetterTopicExistsLocked(subscription.DeadLetterPolicy) {
		return nil, status.Error(codes.NotFound, "dead-letter topic not found")
	}
	a.server.subscriptions[subscription.Name] = subscription
	if err := a.server.saveResourcesLocked(); err != nil {
		delete(a.server.subscriptions, subscription.Name)
		delete(a.server.deliveries, subscription.Name)
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return subscription.toProto(), nil
}

func (a *pubSubGRPCAdapter) GetSubscription(ctx context.Context, request *pubsubpb.GetSubscriptionRequest) (*pubsubpb.Subscription, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	return subscription.toProto(), nil
}

func (a *pubSubGRPCAdapter) UpdateSubscription(ctx context.Context, request *pubsubpb.UpdateSubscriptionRequest) (*pubsubpb.Subscription, error) {
	_ = ctx
	subscriptionUpdate := request.GetSubscription()
	if subscriptionUpdate == nil || !validFullSubscriptionName(subscriptionUpdate.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	paths, err := grpcUpdateMaskPaths(request.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, err
	}
	metadata := subscriptionResource{
		Name:                      subscriptionUpdate.GetName(),
		Topic:                     subscriptionUpdate.GetTopic(),
		Labels:                    copyStringMap(subscriptionUpdate.GetLabels()),
		AckDeadlineSeconds:        int(subscriptionUpdate.GetAckDeadlineSeconds()),
		EnableMessageOrdering:     subscriptionUpdate.GetEnableMessageOrdering(),
		EnableExactlyOnceDelivery: subscriptionUpdate.GetEnableExactlyOnceDelivery(),
		RetainAckedMessages:       subscriptionUpdate.GetRetainAckedMessages(),
		MessageRetentionDuration:  grpcDurationString(subscriptionUpdate.GetMessageRetentionDuration()),
		Filter:                    subscriptionUpdate.GetFilter(),
		DeadLetterPolicy:          grpcDeadLetterPolicy(subscriptionUpdate.GetDeadLetterPolicy()),
		RetryPolicy:               grpcRetryPolicy(subscriptionUpdate.GetRetryPolicy()),
		PushConfig:                grpcPushConfig(subscriptionUpdate.GetPushConfig()),
	}
	if metadata.AckDeadlineSeconds > a.server.config.MaxAckDeadlineSeconds {
		return nil, status.Error(codes.InvalidArgument, "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
	}
	if err := validateSubscriptionMetadata(metadata); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateSubscriptionFilter(metadata.Filter); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateDeadLetterPolicy(metadata.DeadLetterPolicy); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRetryPolicy(metadata.RetryPolicy); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validatePushConfig(metadata.PushConfig); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionUpdate.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if grpcContainsCanonicalMaskPath(paths, "dead_letter_policy") && !a.server.deadLetterTopicExistsLocked(metadata.DeadLetterPolicy) {
		return nil, status.Error(codes.NotFound, "dead-letter topic not found")
	}
	for _, path := range paths {
		switch grpcCanonicalMaskPath(path) {
		case "topic":
			if subscriptionUpdate.GetTopic() != "" && subscriptionUpdate.GetTopic() != subscription.Topic {
				return nil, status.Error(codes.FailedPrecondition, "subscription topic cannot be changed")
			}
		case "labels":
			subscription.Labels = copyStringMap(subscriptionUpdate.GetLabels())
		case "ack_deadline_seconds":
			if subscriptionUpdate.GetAckDeadlineSeconds() == 0 {
				subscription.AckDeadlineSeconds = a.server.config.DefaultAckDeadlineSeconds
			} else {
				subscription.AckDeadlineSeconds = int(subscriptionUpdate.GetAckDeadlineSeconds())
			}
		case "enable_message_ordering":
			subscription.EnableMessageOrdering = subscriptionUpdate.GetEnableMessageOrdering()
		case "enable_exactly_once_delivery":
			subscription.EnableExactlyOnceDelivery = subscriptionUpdate.GetEnableExactlyOnceDelivery()
		case "retain_acked_messages":
			subscription.RetainAckedMessages = subscriptionUpdate.GetRetainAckedMessages()
		case "message_retention_duration":
			subscription.MessageRetentionDuration = grpcDurationString(subscriptionUpdate.GetMessageRetentionDuration())
		case "filter":
			subscription.Filter = subscriptionUpdate.GetFilter()
		case "dead_letter_policy":
			subscription.DeadLetterPolicy = grpcDeadLetterPolicy(subscriptionUpdate.GetDeadLetterPolicy())
		case "retry_policy":
			subscription.RetryPolicy = grpcRetryPolicy(subscriptionUpdate.GetRetryPolicy())
		case "push_config":
			subscription.PushConfig = grpcPushConfig(subscriptionUpdate.GetPushConfig())
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported subscription update mask path %q", path)
		}
	}
	subscription.UpdatedAt = a.server.now().UTC().Format(time.RFC3339Nano)
	a.server.subscriptions[subscription.Name] = subscription
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return subscription.toProto(), nil
}

func (a *pubSubGRPCAdapter) ListSubscriptions(ctx context.Context, request *pubsubpb.ListSubscriptionsRequest) (*pubsubpb.ListSubscriptionsResponse, error) {
	_ = ctx
	project, ok := grpcProjectID(request.GetProject())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscriptions := make([]subscriptionResource, 0, len(a.server.subscriptions))
	for _, subscription := range a.server.subscriptions {
		if resourceProject(subscription.Name) == project {
			subscriptions = append(subscriptions, subscription)
		}
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Name < subscriptions[j].Name })
	start, end, nextToken, err := grpcPageBounds(len(subscriptions), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &pubsubpb.ListSubscriptionsResponse{NextPageToken: nextToken}
	for _, subscription := range subscriptions[start:end] {
		response.Subscriptions = append(response.Subscriptions, subscription.toProto())
	}
	return response, nil
}

func (a *pubSubGRPCAdapter) DeleteSubscription(ctx context.Context, request *pubsubpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.subscriptions[request.GetSubscription()]; !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	delete(a.server.subscriptions, request.GetSubscription())
	delete(a.server.deliveries, request.GetSubscription())
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) DetachSubscription(ctx context.Context, request *pubsubpb.DetachSubscriptionRequest) (*pubsubpb.DetachSubscriptionResponse, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	subscription.Detached = true
	subscription.UpdatedAt = a.server.now().UTC().Format(time.RFC3339Nano)
	a.server.subscriptions[subscription.Name] = subscription
	delete(a.server.deliveries, subscription.Name)
	for snapshotName, snapshot := range a.server.snapshots {
		if snapshot.Subscription == subscription.Name {
			delete(a.server.snapshots, snapshotName)
		}
	}
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &pubsubpb.DetachSubscriptionResponse{}, nil
}

func (a *pubSubGRPCAdapter) ModifyPushConfig(ctx context.Context, request *pubsubpb.ModifyPushConfigRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	pushConfig := grpcPushConfig(request.GetPushConfig())
	if err := validatePushConfig(pushConfig); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	subscription.PushConfig = copyAnyMap(pushConfig)
	subscription.UpdatedAt = a.server.now().UTC().Format(time.RFC3339Nano)
	a.server.subscriptions[subscription.Name] = subscription
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) GetSnapshot(ctx context.Context, request *pubsubpb.GetSnapshotRequest) (*pubsubpb.Snapshot, error) {
	_ = ctx
	if request == nil || !validFullSnapshotName(request.GetSnapshot()) {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	snapshot, found := a.server.snapshots[request.GetSnapshot()]
	if !found || snapshotExpired(snapshot, a.server.now().UTC()) {
		return nil, status.Error(codes.NotFound, "snapshot not found")
	}
	return snapshot.toProto(), nil
}

func (a *pubSubGRPCAdapter) ListSnapshots(ctx context.Context, request *pubsubpb.ListSnapshotsRequest) (*pubsubpb.ListSnapshotsResponse, error) {
	_ = ctx
	project, ok := grpcProjectID(request.GetProject())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	now := a.server.now().UTC()
	snapshots := make([]snapshotResource, 0, len(a.server.snapshots))
	for _, snapshot := range a.server.snapshots {
		if resourceProject(snapshot.Name) == project && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot)
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	start, end, nextToken, err := grpcPageBounds(len(snapshots), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &pubsubpb.ListSnapshotsResponse{NextPageToken: nextToken}
	for _, snapshot := range snapshots[start:end] {
		response.Snapshots = append(response.Snapshots, snapshot.toProto())
	}
	return response, nil
}

func (a *pubSubGRPCAdapter) CreateSnapshot(ctx context.Context, request *pubsubpb.CreateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot is required")
	}
	if request.GetName() != "" && !validFullSnapshotName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot name")
	}
	if !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, exists := a.server.snapshots[request.GetName()]; exists {
		return nil, status.Error(codes.AlreadyExists, "snapshot already exists")
	}
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	name := request.GetName()
	if name == "" {
		name = a.server.generatedSnapshotNameLocked(resourceProject(subscription.Name))
	}
	snapshot := snapshotResource{
		Name:         name,
		Topic:        subscription.Topic,
		Subscription: subscription.Name,
		ExpireTime:   a.server.now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339Nano),
		Labels:       copyStringMap(request.GetLabels()),
		Deliveries:   snapshotDeliveries(a.server.deliveries[subscription.Name]),
	}
	a.server.snapshots[snapshot.Name] = snapshot
	if err := a.server.saveResourcesLocked(); err != nil {
		delete(a.server.snapshots, snapshot.Name)
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return snapshot.toProto(), nil
}

func (a *pubSubGRPCAdapter) DeleteSnapshot(ctx context.Context, request *pubsubpb.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSnapshotName(request.GetSnapshot()) {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.snapshots[request.GetSnapshot()]; !found {
		return nil, status.Error(codes.NotFound, "snapshot not found")
	}
	delete(a.server.snapshots, request.GetSnapshot())
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) UpdateSnapshot(ctx context.Context, request *pubsubpb.UpdateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	_ = ctx
	snapshotUpdate := request.GetSnapshot()
	if snapshotUpdate == nil || !validFullSnapshotName(snapshotUpdate.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot name")
	}
	paths, err := grpcUpdateMaskPaths(request.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, err
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	snapshot, found := a.server.snapshots[snapshotUpdate.GetName()]
	if !found || snapshotExpired(snapshot, a.server.now().UTC()) {
		return nil, status.Error(codes.NotFound, "snapshot not found")
	}
	for _, path := range paths {
		switch grpcCanonicalMaskPath(path) {
		case "labels":
			snapshot.Labels = copyStringMap(snapshotUpdate.GetLabels())
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported snapshot update mask path %q", path)
		}
	}
	a.server.snapshots[snapshot.Name] = snapshot
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return snapshot.toProto(), nil
}

func (a *pubSubGRPCAdapter) Seek(ctx context.Context, request *pubsubpb.SeekRequest) (*pubsubpb.SeekResponse, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	if request.GetTarget() == nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot or time is required")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	switch target := request.GetTarget().(type) {
	case *pubsubpb.SeekRequest_Time:
		if target.Time == nil || !target.Time.IsValid() {
			return nil, status.Error(codes.InvalidArgument, "invalid seek time")
		}
		a.server.deliveries[subscription.Name] = a.server.seekDeliveriesByTimeLocked(subscription, target.Time.AsTime().UTC())
	case *pubsubpb.SeekRequest_Snapshot:
		if !validFullSnapshotName(target.Snapshot) {
			return nil, status.Error(codes.InvalidArgument, "invalid snapshot name")
		}
		snapshot, found := a.server.snapshots[target.Snapshot]
		if !found || snapshotExpired(snapshot, a.server.now().UTC()) {
			return nil, status.Error(codes.NotFound, "snapshot not found")
		}
		if snapshot.Subscription != subscription.Name {
			return nil, status.Error(codes.FailedPrecondition, "snapshot belongs to a different subscription")
		}
		a.server.deliveries[subscription.Name] = snapshotDeliveries(snapshot.Deliveries)
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported seek target")
	}
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &pubsubpb.SeekResponse{}, nil
}

func (a *pubSubGRPCAdapter) CreateSchema(ctx context.Context, request *pubsubpb.CreateSchemaRequest) (*pubsubpb.Schema, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	project, ok := grpcProjectID(request.GetParent())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	if !validResourceID(request.GetSchemaId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	schema, err := schemaResourceFromProto(request.GetSchema())
	if err != nil {
		return nil, err
	}
	schema.Name = schemaName(project, request.GetSchemaId())
	if schema.RevisionID == "" {
		schema.RevisionID = "1"
	}
	schema.RevisionCreateTime = a.server.now().UTC().Format(time.RFC3339Nano)
	schema.Revisions = []schemaRevisionResource{schema.currentRevision()}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, exists := a.server.schemas[schema.Name]; exists {
		return nil, status.Error(codes.AlreadyExists, "schema already exists")
	}
	a.server.schemas[schema.Name] = schema
	if err := a.server.saveResourcesLocked(); err != nil {
		delete(a.server.schemas, schema.Name)
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return schema.toProto(pubsubpb.SchemaView_FULL), nil
}

func (a *pubSubGRPCAdapter) ListSchemaRevisions(ctx context.Context, request *pubsubpb.ListSchemaRevisionsRequest) (*pubsubpb.ListSchemaRevisionsResponse, error) {
	_ = ctx
	if request == nil || !validFullSchemaName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schema, found := a.server.schemas[request.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	revisions := schema.revisions()
	start, end, nextToken, err := grpcPageBounds(len(revisions), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &pubsubpb.ListSchemaRevisionsResponse{NextPageToken: nextToken}
	for _, revision := range revisions[start:end] {
		response.Schemas = append(response.Schemas, revision.toSchema(schema.Name).toProto(listSchemaView(request.GetView())))
	}
	return response, nil
}

func (a *pubSubGRPCAdapter) CommitSchema(ctx context.Context, request *pubsubpb.CommitSchemaRequest) (*pubsubpb.Schema, error) {
	_ = ctx
	if request == nil || !validFullSchemaName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	revision, err := schemaResourceFromProto(request.GetSchema())
	if err != nil {
		return nil, err
	}
	if revision.Name != "" && revision.Name != request.GetName() {
		return nil, status.Error(codes.InvalidArgument, "schema name mismatch")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schema, found := a.server.schemas[request.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	if revision.Type != schema.Type {
		return nil, status.Error(codes.FailedPrecondition, "schema type cannot be changed")
	}
	revision.Name = schema.Name
	revision.RevisionID = nextSchemaRevisionID(schema.revisions())
	revision.RevisionCreateTime = a.server.now().UTC().Format(time.RFC3339Nano)
	schema.Type = revision.Type
	schema.Definition = revision.Definition
	schema.RevisionID = revision.RevisionID
	schema.RevisionCreateTime = revision.RevisionCreateTime
	schema.Revisions = append(schema.revisions(), revision.currentRevision())
	a.server.schemas[schema.Name] = schema
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return schema.toProto(pubsubpb.SchemaView_FULL), nil
}

func (a *pubSubGRPCAdapter) RollbackSchema(ctx context.Context, request *pubsubpb.RollbackSchemaRequest) (*pubsubpb.Schema, error) {
	_ = ctx
	if request == nil || !validFullSchemaName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	if strings.TrimSpace(request.GetRevisionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "revision id is required")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schema, found := a.server.schemas[request.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	revisions := schema.revisions()
	var target schemaRevisionResource
	for _, revision := range revisions {
		if revision.RevisionID == request.GetRevisionId() {
			target = revision
			break
		}
	}
	if target.RevisionID == "" {
		return nil, status.Error(codes.NotFound, "schema revision not found")
	}
	target.RevisionID = nextSchemaRevisionID(revisions)
	target.RevisionCreateTime = a.server.now().UTC().Format(time.RFC3339Nano)
	schema.Type = target.Type
	schema.Definition = target.Definition
	schema.RevisionID = target.RevisionID
	schema.RevisionCreateTime = target.RevisionCreateTime
	schema.Revisions = append(revisions, target)
	a.server.schemas[schema.Name] = schema
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return schema.toProto(pubsubpb.SchemaView_FULL), nil
}

func (a *pubSubGRPCAdapter) DeleteSchemaRevision(ctx context.Context, request *pubsubpb.DeleteSchemaRevisionRequest) (*pubsubpb.Schema, error) {
	_ = ctx
	schemaName, revisionID, ok := schemaRevisionRequestTarget(request)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid schema revision name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schema, found := a.server.schemas[schemaName]
	if !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	revisions := schema.revisions()
	if len(revisions) <= 1 {
		return nil, status.Error(codes.FailedPrecondition, "cannot delete the only schema revision")
	}
	kept := make([]schemaRevisionResource, 0, len(revisions)-1)
	deleted := false
	for _, revision := range revisions {
		if revision.RevisionID == revisionID {
			deleted = true
			continue
		}
		kept = append(kept, revision)
	}
	if !deleted {
		return nil, status.Error(codes.NotFound, "schema revision not found")
	}
	current := kept[len(kept)-1]
	schema.Type = current.Type
	schema.Definition = current.Definition
	schema.RevisionID = current.RevisionID
	schema.RevisionCreateTime = current.RevisionCreateTime
	schema.Revisions = kept
	a.server.schemas[schema.Name] = schema
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return schema.toProto(pubsubpb.SchemaView_FULL), nil
}

func (a *pubSubGRPCAdapter) GetSchema(ctx context.Context, request *pubsubpb.GetSchemaRequest) (*pubsubpb.Schema, error) {
	_ = ctx
	if request == nil || !validFullSchemaName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schema, found := a.server.schemas[request.GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	return schema.toProto(request.GetView()), nil
}

func (a *pubSubGRPCAdapter) ListSchemas(ctx context.Context, request *pubsubpb.ListSchemasRequest) (*pubsubpb.ListSchemasResponse, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	project, ok := grpcProjectID(request.GetParent())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	schemas := make([]schemaResource, 0, len(a.server.schemas))
	for _, schema := range a.server.schemas {
		if resourceProject(schema.Name) == project {
			schemas = append(schemas, schema)
		}
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Name < schemas[j].Name })
	start, end, nextToken, err := grpcPageBounds(len(schemas), request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &pubsubpb.ListSchemasResponse{NextPageToken: nextToken}
	for _, schema := range schemas[start:end] {
		response.Schemas = append(response.Schemas, schema.toProto(listSchemaView(request.GetView())))
	}
	return response, nil
}

func (a *pubSubGRPCAdapter) DeleteSchema(ctx context.Context, request *pubsubpb.DeleteSchemaRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSchemaName(request.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	if _, found := a.server.schemas[request.GetName()]; !found {
		return nil, status.Error(codes.NotFound, "schema not found")
	}
	delete(a.server.schemas, request.GetName())
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) ValidateSchema(ctx context.Context, request *pubsubpb.ValidateSchemaRequest) (*pubsubpb.ValidateSchemaResponse, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	if _, ok := grpcProjectID(request.GetParent()); !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	if _, err := schemaResourceFromProto(request.GetSchema()); err != nil {
		return nil, err
	}
	return &pubsubpb.ValidateSchemaResponse{}, nil
}

func (a *pubSubGRPCAdapter) ValidateMessage(ctx context.Context, request *pubsubpb.ValidateMessageRequest) (*pubsubpb.ValidateMessageResponse, error) {
	_ = ctx
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	project, ok := grpcProjectID(request.GetParent())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid project name")
	}
	if !validSchemaEncoding(schemaEncodingString(request.GetEncoding())) {
		return nil, status.Error(codes.InvalidArgument, "invalid schema encoding")
	}
	if request.GetName() == "" && request.GetSchema() == nil {
		return nil, status.Error(codes.InvalidArgument, "schema name or inline schema is required")
	}
	if request.GetName() != "" && request.GetSchema() != nil {
		return nil, status.Error(codes.InvalidArgument, "only one of schema name or inline schema may be set")
	}
	if request.GetName() != "" {
		if !validFullSchemaName(request.GetName()) {
			return nil, status.Error(codes.InvalidArgument, "invalid schema name")
		}
		if resourceProject(request.GetName()) != project {
			return nil, status.Error(codes.FailedPrecondition, "schema belongs to a different project")
		}
		a.server.mu.Lock()
		_, found := a.server.schemas[request.GetName()]
		a.server.mu.Unlock()
		if !found {
			return nil, status.Error(codes.NotFound, "schema not found")
		}
		if !validSchemaMessageData(request.GetMessage(), schemaEncodingString(request.GetEncoding())) {
			return nil, status.Error(codes.InvalidArgument, "message is invalid for schema encoding")
		}
		return &pubsubpb.ValidateMessageResponse{}, nil
	}
	schema, err := schemaResourceFromProto(request.GetSchema())
	if err != nil {
		return nil, err
	}
	if schema.Name != "" {
		if !validFullSchemaName(schema.Name) {
			return nil, status.Error(codes.InvalidArgument, "invalid schema name")
		}
		if resourceProject(schema.Name) != project {
			return nil, status.Error(codes.FailedPrecondition, "schema belongs to a different project")
		}
	}
	if !validSchemaMessageData(request.GetMessage(), schemaEncodingString(request.GetEncoding())) {
		return nil, status.Error(codes.InvalidArgument, "message is invalid for schema encoding")
	}
	return &pubsubpb.ValidateMessageResponse{}, nil
}

func (a *pubSubGRPCAdapter) Pull(ctx context.Context, request *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	maxMessages := int(request.GetMaxMessages())
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > a.server.config.MaxPullMessages {
		maxMessages = a.server.config.MaxPullMessages
	}
	if !request.GetReturnImmediately() {
		a.server.waitForPullAvailability(ctx, request.GetSubscription())
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if subscription.Detached {
		return nil, status.Error(codes.FailedPrecondition, "subscription is detached")
	}
	if subscriptionPushEndpoint(subscription) != "" {
		return nil, status.Error(codes.FailedPrecondition, "subscription is configured for push delivery")
	}
	now := a.server.now().UTC()
	a.server.cleanupRetainedMessagesLocked(now)
	a.server.expireLeasesLocked(now)
	received, deliveries := a.server.pullLocked(subscription, maxMessages, now)
	a.server.deliveries[subscription.Name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &pubsubpb.PullResponse{ReceivedMessages: received}, nil
}

func (a *pubSubGRPCAdapter) Acknowledge(ctx context.Context, request *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	if err := a.server.updateAckDeadlineLocked(request.GetSubscription(), request.GetAckIds(), 0, true); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) ModifyAckDeadline(ctx context.Context, request *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	if request.GetAckDeadlineSeconds() < 0 {
		return nil, status.Error(codes.InvalidArgument, "ackDeadlineSeconds must be non-negative")
	}
	if int(request.GetAckDeadlineSeconds()) > a.server.config.MaxAckDeadlineSeconds {
		return nil, status.Error(codes.InvalidArgument, "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
	}
	if err := a.server.updateAckDeadlineLocked(request.GetSubscription(), request.GetAckIds(), int(request.GetAckDeadlineSeconds()), false); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	if a.server.config.StreamingPullDisabled {
		return status.Error(codes.Unimplemented, "streaming pull is disabled")
	}
	initial, err := stream.Recv()
	if errorsIsEOF(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if initial == nil || !validFullSubscriptionName(initial.GetSubscription()) {
		return status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	subscriptionName := initial.GetSubscription()
	streamAckDeadline, err := a.streamingAckDeadline(initial.GetStreamAckDeadlineSeconds())
	if err != nil {
		return err
	}
	if err := a.validateStreamingPullSubscription(subscriptionName); err != nil {
		return err
	}
	maxOutstandingMessages := initial.GetMaxOutstandingMessages()
	maxOutstandingBytes := initial.GetMaxOutstandingBytes()
	outstandingAckIDs := map[string]int{}
	defer a.releaseStreamingOutstandingAckIDs(subscriptionName, outstandingAckIDs)
	if err := a.applyStreamingPullRequest(subscriptionName, initial, true, &streamAckDeadline, outstandingAckIDs); err != nil {
		return err
	}

	requests := make(chan *pubsubpb.StreamingPullRequest, 1)
	requestErrs := make(chan error, 1)
	go func() {
		for {
			request, recvErr := stream.Recv()
			if recvErr != nil {
				requestErrs <- recvErr
				return
			}
			select {
			case requests <- request:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case recvErr := <-requestErrs:
			if errorsIsEOF(recvErr) {
				return nil
			}
			return recvErr
		case request := <-requests:
			if err := a.applyStreamingPullRequest(subscriptionName, request, false, &streamAckDeadline, outstandingAckIDs); err != nil {
				return err
			}
		case <-ticker.C:
			a.pruneStreamingOutstandingAckIDs(subscriptionName, outstandingAckIDs)
			if !streamingPullHasCapacity(outstandingAckIDs, maxOutstandingMessages, maxOutstandingBytes) {
				continue
			}
			remaining := streamingPullRemainingCapacity(outstandingAckIDs, maxOutstandingMessages)
			if remaining <= 0 {
				continue
			}
			response, err := a.streamingPullResponse(subscriptionName, remaining, streamAckDeadline, maxOutstandingBytes, outstandingAckIDs)
			if err != nil {
				return err
			}
			if response == nil {
				continue
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func (a *pubSubGRPCAdapter) pruneStreamingOutstandingAckIDs(subscriptionName string, outstandingAckIDs map[string]int) {
	if len(outstandingAckIDs) == 0 {
		return
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	now := a.server.now().UTC()
	a.server.expireLeasesLocked(now)
	active := map[string]struct{}{}
	for _, delivery := range a.server.deliveries[subscriptionName] {
		if delivery.AckID == "" || delivery.Acked || !delivery.LeaseDeadline.After(now) {
			continue
		}
		active[delivery.AckID] = struct{}{}
	}
	for ackID := range outstandingAckIDs {
		if _, ok := active[ackID]; !ok {
			delete(outstandingAckIDs, ackID)
		}
	}
}

func (a *pubSubGRPCAdapter) releaseStreamingOutstandingAckIDs(subscriptionName string, outstandingAckIDs map[string]int) {
	if len(outstandingAckIDs) == 0 {
		return
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
	if !found {
		return
	}
	deliveries := a.server.deliveries[subscriptionName]
	changed := false
	for ackID := range outstandingAckIDs {
		for i := range deliveries {
			if deliveries[i].AckID != ackID || deliveries[i].Acked {
				continue
			}
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	a.server.deliveries[subscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	_ = a.server.saveResourcesLocked()
}

func (s *Server) pullLocked(subscription subscriptionResource, maxMessages int, now time.Time) ([]*pubsubpb.ReceivedMessage, []deliveryRecord) {
	ackDeadline := subscription.AckDeadlineSeconds
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	return s.pullWithAckDeadlineLocked(subscription, maxMessages, now, ackDeadline)
}

func (s *Server) pullWithAckDeadlineLocked(subscription subscriptionResource, maxMessages int, now time.Time, ackDeadline int) ([]*pubsubpb.ReceivedMessage, []deliveryRecord) {
	received := make([]*pubsubpb.ReceivedMessage, 0, maxMessages)
	deliveries := s.deliveries[subscription.Name]
	blockedOrderingKeys := map[string]struct{}{}
	if subscription.EnableMessageOrdering {
		for _, delivery := range deliveries {
			if delivery.Acked || !delivery.LeaseDeadline.After(now) {
				continue
			}
			message, found := s.messages[delivery.MessageID]
			if !found || message.OrderingKey == "" {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
	}
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	for i := range deliveries {
		if len(received) >= maxMessages {
			break
		}
		if deliveries[i].Acked || deliveries[i].LeaseDeadline.After(now) {
			continue
		}
		if deliveries[i].NextDeliveryTime.After(now) {
			if subscription.EnableMessageOrdering {
				if message, found := s.messages[deliveries[i].MessageID]; found && message.OrderingKey != "" {
					blockedOrderingKeys[message.OrderingKey] = struct{}{}
				}
			}
			continue
		}
		message, found := s.messages[deliveries[i].MessageID]
		if !found {
			continue
		}
		if s.deadLetterDeliveryLocked(subscription, &deliveries[i], message, now) {
			continue
		}
		if subscription.EnableMessageOrdering && message.OrderingKey != "" {
			if _, blocked := blockedOrderingKeys[message.OrderingKey]; blocked {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
		s.nextAckID++
		deliveries[i].AckID = fmt.Sprintf("%s-%d", deliveries[i].MessageID, s.nextAckID)
		deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadline) * time.Second)
		deliveries[i].NextDeliveryTime = time.Time{}
		deliveries[i].DeliveryAttempt++
		received = append(received, &pubsubpb.ReceivedMessage{
			AckId:           deliveries[i].AckID,
			Message:         message.toProto(),
			DeliveryAttempt: int32(deliveries[i].DeliveryAttempt),
		})
	}
	return received, deliveries
}

func (a *pubSubGRPCAdapter) validateStreamingPullSubscription(subscriptionName string) error {
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
	if !found {
		return status.Error(codes.NotFound, "subscription not found")
	}
	if subscription.Detached {
		return status.Error(codes.FailedPrecondition, "subscription is detached")
	}
	if subscriptionPushEndpoint(subscription) != "" {
		return status.Error(codes.FailedPrecondition, "subscription is configured for push delivery")
	}
	return nil
}

func (a *pubSubGRPCAdapter) streamingAckDeadline(seconds int32) (int, error) {
	if seconds < 0 {
		return 0, status.Error(codes.InvalidArgument, "streamAckDeadlineSeconds must be non-negative")
	}
	if int(seconds) > a.server.config.MaxAckDeadlineSeconds {
		return 0, status.Error(codes.InvalidArgument, "streamAckDeadlineSeconds exceeds maxAckDeadlineSeconds")
	}
	if seconds == 0 {
		return a.server.config.DefaultAckDeadlineSeconds, nil
	}
	return int(seconds), nil
}

func (a *pubSubGRPCAdapter) applyStreamingPullRequest(subscriptionName string, request *pubsubpb.StreamingPullRequest, initial bool, streamAckDeadline *int, outstandingAckIDs map[string]int) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "streaming pull request is required")
	}
	if !initial && request.GetSubscription() != "" && request.GetSubscription() != subscriptionName {
		return status.Error(codes.InvalidArgument, "subscription must not change on a streaming pull stream")
	}
	if !initial && request.GetMaxOutstandingMessages() != 0 {
		return status.Error(codes.InvalidArgument, "maxOutstandingMessages can only be set on the initial request")
	}
	if !initial && request.GetMaxOutstandingBytes() != 0 {
		return status.Error(codes.InvalidArgument, "maxOutstandingBytes can only be set on the initial request")
	}
	if request.GetStreamAckDeadlineSeconds() != 0 {
		deadline, err := a.streamingAckDeadline(request.GetStreamAckDeadlineSeconds())
		if err != nil {
			return err
		}
		*streamAckDeadline = deadline
	}
	if len(request.GetModifyDeadlineAckIds()) != len(request.GetModifyDeadlineSeconds()) {
		return status.Error(codes.InvalidArgument, "modifyDeadlineAckIds and modifyDeadlineSeconds must have the same length")
	}
	if len(request.GetAckIds()) > 0 {
		if err := a.server.updateAckDeadlineLocked(subscriptionName, request.GetAckIds(), 0, true); err != nil {
			return err
		}
		for _, ackID := range request.GetAckIds() {
			delete(outstandingAckIDs, ackID)
		}
	}
	for i, ackID := range request.GetModifyDeadlineAckIds() {
		deadline := request.GetModifyDeadlineSeconds()[i]
		if deadline < 0 {
			return status.Error(codes.InvalidArgument, "modifyDeadlineSeconds must be non-negative")
		}
		if int(deadline) > a.server.config.MaxAckDeadlineSeconds {
			return status.Error(codes.InvalidArgument, "modifyDeadlineSeconds exceeds maxAckDeadlineSeconds")
		}
		if err := a.server.updateAckDeadlineLocked(subscriptionName, []string{ackID}, int(deadline), false); err != nil {
			return err
		}
		if deadline == 0 {
			delete(outstandingAckIDs, ackID)
		}
	}
	return nil
}

func (a *pubSubGRPCAdapter) streamingPullResponse(subscriptionName string, maxMessages int, ackDeadline int, maxOutstandingBytes int64, outstandingAckIDs map[string]int) (*pubsubpb.StreamingPullResponse, error) {
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if subscription.Detached {
		return nil, status.Error(codes.FailedPrecondition, "subscription is detached")
	}
	if subscriptionPushEndpoint(subscription) != "" {
		return nil, status.Error(codes.FailedPrecondition, "subscription is configured for push delivery")
	}
	now := a.server.now().UTC()
	a.server.cleanupRetainedMessagesLocked(now)
	a.server.expireLeasesLocked(now)
	received, deliveries := a.server.pullWithAckDeadlineLocked(subscription, maxMessages, now, ackDeadline)
	if len(received) == 0 {
		return nil, nil
	}
	if maxOutstandingBytes > 0 {
		kept := received[:0]
		remainingBytes := maxOutstandingBytes - int64(streamingOutstandingBytes(outstandingAckIDs))
		for _, message := range received {
			size := streamingReceivedMessageSize(message)
			if int64(size) > remainingBytes && len(kept) > 0 {
				break
			}
			kept = append(kept, message)
			remainingBytes -= int64(size)
		}
		for _, message := range received[len(kept):] {
			releaseUnsentStreamingAckIDLocked(deliveries, message.GetAckId())
		}
		received = kept
	}
	a.server.deliveries[subscription.Name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	if len(received) == 0 {
		return nil, nil
	}
	for _, message := range received {
		outstandingAckIDs[message.GetAckId()] = streamingReceivedMessageSize(message)
	}
	return &pubsubpb.StreamingPullResponse{ReceivedMessages: received}, nil
}

func releaseAckIDLocked(deliveries []deliveryRecord, ackID string) {
	for i := range deliveries {
		if deliveries[i].AckID != ackID || deliveries[i].Acked {
			continue
		}
		deliveries[i].AckID = ""
		deliveries[i].LeaseDeadline = time.Time{}
		deliveries[i].NextDeliveryTime = time.Time{}
		return
	}
}

func releaseUnsentStreamingAckIDLocked(deliveries []deliveryRecord, ackID string) {
	for i := range deliveries {
		if deliveries[i].AckID != ackID || deliveries[i].Acked {
			continue
		}
		deliveries[i].AckID = ""
		deliveries[i].LeaseDeadline = time.Time{}
		deliveries[i].NextDeliveryTime = time.Time{}
		if deliveries[i].DeliveryAttempt > 0 {
			deliveries[i].DeliveryAttempt--
		}
		return
	}
}

func streamingPullHasCapacity(outstandingAckIDs map[string]int, maxOutstandingMessages int64, maxOutstandingBytes int64) bool {
	if maxOutstandingMessages > 0 && int64(len(outstandingAckIDs)) >= maxOutstandingMessages {
		return false
	}
	if maxOutstandingBytes > 0 && int64(streamingOutstandingBytes(outstandingAckIDs)) >= maxOutstandingBytes {
		return false
	}
	return true
}

func streamingPullRemainingCapacity(outstandingAckIDs map[string]int, maxOutstandingMessages int64) int {
	if maxOutstandingMessages <= 0 {
		return 1
	}
	remaining := int(maxOutstandingMessages) - len(outstandingAckIDs)
	if remaining < 1 {
		return 0
	}
	return remaining
}

func streamingOutstandingBytes(outstandingAckIDs map[string]int) int {
	total := 0
	for _, size := range outstandingAckIDs {
		total += size
	}
	return total
}

func streamingReceivedMessageSize(received *pubsubpb.ReceivedMessage) int {
	if received == nil || received.GetMessage() == nil {
		return 0
	}
	size := len(received.GetMessage().GetData()) + len(received.GetAckId()) + len(received.GetMessage().GetOrderingKey())
	for key, value := range received.GetMessage().GetAttributes() {
		size += len(key) + len(value)
	}
	return size
}

func errorsIsEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func (s *Server) updateAckDeadlineLocked(subscriptionName string, ackIDs []string, ackDeadlineSeconds int, acknowledge bool) error {
	if len(ackIDs) == 0 {
		return nil
	}
	for _, ackID := range ackIDs {
		if strings.TrimSpace(ackID) == "" {
			return status.Error(codes.InvalidArgument, "ackIds must not contain empty values")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[subscriptionName]
	if !found {
		return status.Error(codes.NotFound, "subscription not found")
	}
	ackIDSet := map[string]struct{}{}
	for _, ackID := range ackIDs {
		ackIDSet[ackID] = struct{}{}
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	deliveries := s.deliveries[subscriptionName]
	for i := range deliveries {
		if _, ok := ackIDSet[deliveries[i].AckID]; !ok || deliveries[i].Acked {
			continue
		}
		if acknowledge {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			continue
		}
		if ackDeadlineSeconds == 0 {
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadlineSeconds) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
		}
	}
	s.deliveries[subscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		return status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return nil
}

func (t topicResource) toProto() *pubsubpb.Topic {
	return &pubsubpb.Topic{
		Name:                     t.Name,
		Labels:                   copyStringMap(t.Labels),
		KmsKeyName:               t.KMSKeyName,
		SchemaSettings:           protoSchemaSettings(t.SchemaSettings),
		MessageRetentionDuration: protoDuration(t.MessageRetentionDuration),
	}
}

func (s subscriptionResource) toProto() *pubsubpb.Subscription {
	return &pubsubpb.Subscription{
		Name:                          s.Name,
		Topic:                         s.Topic,
		Labels:                        copyStringMap(s.Labels),
		AckDeadlineSeconds:            int32(s.AckDeadlineSeconds),
		RetainAckedMessages:           s.RetainAckedMessages,
		MessageRetentionDuration:      protoDuration(s.MessageRetentionDuration),
		EnableMessageOrdering:         s.EnableMessageOrdering,
		Filter:                        s.Filter,
		Detached:                      s.Detached,
		EnableExactlyOnceDelivery:     s.EnableExactlyOnceDelivery,
		DeadLetterPolicy:              protoDeadLetterPolicy(s.DeadLetterPolicy),
		RetryPolicy:                   protoRetryPolicy(s.RetryPolicy),
		PushConfig:                    protoPushConfig(s.PushConfig),
		TopicMessageRetentionDuration: nil,
	}
}

func grpcDeadLetterPolicy(policy *pubsubpb.DeadLetterPolicy) map[string]any {
	if policy == nil {
		return nil
	}
	return map[string]any{
		"deadLetterTopic":     policy.GetDeadLetterTopic(),
		"maxDeliveryAttempts": int(policy.GetMaxDeliveryAttempts()),
	}
}

func protoDeadLetterPolicy(policy map[string]any) *pubsubpb.DeadLetterPolicy {
	if len(policy) == 0 {
		return nil
	}
	maxAttempts, ok := deadLetterMaxDeliveryAttempts(policy)
	if !ok {
		return nil
	}
	return &pubsubpb.DeadLetterPolicy{
		DeadLetterTopic:     deadLetterTopic(policy),
		MaxDeliveryAttempts: int32(maxAttempts),
	}
}

func grpcRetryPolicy(policy *pubsubpb.RetryPolicy) map[string]any {
	if policy == nil {
		return nil
	}
	converted := map[string]any{}
	if policy.GetMinimumBackoff() != nil {
		converted["minimumBackoff"] = grpcDurationString(policy.GetMinimumBackoff())
	}
	if policy.GetMaximumBackoff() != nil {
		converted["maximumBackoff"] = grpcDurationString(policy.GetMaximumBackoff())
	}
	if len(converted) == 0 {
		return nil
	}
	return converted
}

func protoRetryPolicy(policy map[string]any) *pubsubpb.RetryPolicy {
	if len(policy) == 0 {
		return nil
	}
	retryPolicy := &pubsubpb.RetryPolicy{}
	if minimum, ok, err := retryPolicyDuration(policy, "minimumBackoff"); err == nil && ok {
		retryPolicy.MinimumBackoff = durationpb.New(minimum)
	}
	if maximum, ok, err := retryPolicyDuration(policy, "maximumBackoff"); err == nil && ok {
		retryPolicy.MaximumBackoff = durationpb.New(maximum)
	}
	if retryPolicy.MinimumBackoff == nil && retryPolicy.MaximumBackoff == nil {
		return nil
	}
	return retryPolicy
}

func grpcPushConfig(config *pubsubpb.PushConfig) map[string]any {
	if config == nil {
		return nil
	}
	converted := map[string]any{}
	if endpoint := strings.TrimSpace(config.GetPushEndpoint()); endpoint != "" {
		converted["pushEndpoint"] = endpoint
	}
	if attributes := config.GetAttributes(); len(attributes) > 0 {
		converted["attributes"] = copyStringMap(attributes)
	}
	if len(converted) == 0 {
		return nil
	}
	return converted
}

func protoPushConfig(config map[string]any) *pubsubpb.PushConfig {
	if len(config) == 0 {
		return nil
	}
	pushConfig := &pubsubpb.PushConfig{
		PushEndpoint: subscriptionPushEndpoint(subscriptionResource{PushConfig: config}),
		Attributes:   stringMapFromAny(config["attributes"]),
	}
	if pushConfig.PushEndpoint == "" && len(pushConfig.Attributes) == 0 {
		return nil
	}
	return pushConfig
}

func grpcSchemaSettings(settings *pubsubpb.SchemaSettings) map[string]any {
	if settings == nil {
		return nil
	}
	converted := map[string]any{}
	if schema := strings.TrimSpace(settings.GetSchema()); schema != "" {
		converted["schema"] = schema
	}
	if encoding := schemaEncodingString(settings.GetEncoding()); encoding != "" {
		converted["encoding"] = encoding
	}
	if firstRevisionID := strings.TrimSpace(settings.GetFirstRevisionId()); firstRevisionID != "" {
		converted["firstRevisionId"] = firstRevisionID
	}
	if lastRevisionID := strings.TrimSpace(settings.GetLastRevisionId()); lastRevisionID != "" {
		converted["lastRevisionId"] = lastRevisionID
	}
	if len(converted) == 0 {
		return nil
	}
	return converted
}

func protoSchemaSettings(settings map[string]any) *pubsubpb.SchemaSettings {
	if len(settings) == 0 {
		return nil
	}
	schemaSettings := &pubsubpb.SchemaSettings{
		Schema:          stringValue(settings["schema"]),
		Encoding:        protoSchemaEncoding(stringValue(settings["encoding"])),
		FirstRevisionId: stringValue(settings["firstRevisionId"]),
		LastRevisionId:  stringValue(settings["lastRevisionId"]),
	}
	if schemaSettings.Schema == "" && schemaSettings.Encoding == pubsubpb.Encoding_ENCODING_UNSPECIFIED && schemaSettings.FirstRevisionId == "" && schemaSettings.LastRevisionId == "" {
		return nil
	}
	return schemaSettings
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func protoSchemaEncoding(encoding string) pubsubpb.Encoding {
	switch encoding {
	case "JSON":
		return pubsubpb.Encoding_JSON
	case "BINARY":
		return pubsubpb.Encoding_BINARY
	default:
		return pubsubpb.Encoding_ENCODING_UNSPECIFIED
	}
}

func stringMapFromAny(value any) map[string]string {
	switch typed := value.(type) {
	case map[string]string:
		return copyStringMap(typed)
	case map[string]any:
		converted := map[string]string{}
		for key, raw := range typed {
			if text, ok := raw.(string); ok {
				converted[key] = text
			}
		}
		if len(converted) == 0 {
			return nil
		}
		return converted
	default:
		return nil
	}
}

func (s snapshotResource) toProto() *pubsubpb.Snapshot {
	return &pubsubpb.Snapshot{
		Name:       s.Name,
		Topic:      s.Topic,
		ExpireTime: snapshotProtoExpireTime(s.ExpireTime),
		Labels:     copyStringMap(s.Labels),
	}
}

func (s schemaResource) toProto(view pubsubpb.SchemaView) *pubsubpb.Schema {
	definition := s.Definition
	if view == pubsubpb.SchemaView_BASIC {
		definition = ""
	}
	return &pubsubpb.Schema{
		Name:               s.Name,
		Type:               protoSchemaType(s.Type),
		Definition:         definition,
		RevisionId:         s.RevisionID,
		RevisionCreateTime: schemaProtoRevisionCreateTime(s.RevisionCreateTime),
	}
}

func (s schemaResource) currentRevision() schemaRevisionResource {
	return schemaRevisionResource{
		Type:               s.Type,
		Definition:         s.Definition,
		RevisionID:         s.RevisionID,
		RevisionCreateTime: s.RevisionCreateTime,
	}
}

func (s schemaResource) revisions() []schemaRevisionResource {
	if len(s.Revisions) > 0 {
		revisions := make([]schemaRevisionResource, 0, len(s.Revisions))
		for _, revision := range s.Revisions {
			if revision.RevisionID != "" {
				revisions = append(revisions, revision)
			}
		}
		if len(revisions) > 0 {
			return revisions
		}
	}
	return []schemaRevisionResource{s.currentRevision()}
}

func (r schemaRevisionResource) toSchema(name string) schemaResource {
	return schemaResource{
		Name:               name,
		Type:               r.Type,
		Definition:         r.Definition,
		RevisionID:         r.RevisionID,
		RevisionCreateTime: r.RevisionCreateTime,
	}
}

func schemaResourceFromProto(schema *pubsubpb.Schema) (schemaResource, error) {
	if schema == nil {
		return schemaResource{}, status.Error(codes.InvalidArgument, "schema is required")
	}
	schemaType := schemaTypeString(schema.GetType())
	if !validSchemaType(schemaType) {
		return schemaResource{}, status.Error(codes.InvalidArgument, "invalid schema type")
	}
	if schema.GetName() != "" && !validFullSchemaName(schema.GetName()) {
		return schemaResource{}, status.Error(codes.InvalidArgument, "invalid schema name")
	}
	if err := validateSchemaDefinition(schemaType, schema.GetDefinition()); err != nil {
		return schemaResource{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return schemaResource{
		Name:       schema.GetName(),
		Type:       schemaType,
		Definition: schema.GetDefinition(),
		RevisionID: schema.GetRevisionId(),
	}, nil
}

func schemaRevisionRequestTarget(request *pubsubpb.DeleteSchemaRevisionRequest) (string, string, bool) {
	if request == nil {
		return "", "", false
	}
	name := strings.TrimSpace(request.GetName())
	revisionID := strings.TrimSpace(request.GetRevisionId())
	if strings.Contains(name, "@") {
		parts := strings.Split(name, "@")
		if len(parts) != 2 {
			return "", "", false
		}
		name = parts[0]
		revisionID = strings.TrimSpace(parts[1])
	}
	if !validFullSchemaName(name) || revisionID == "" {
		return "", "", false
	}
	return name, revisionID, true
}

func nextSchemaRevisionID(revisions []schemaRevisionResource) string {
	maxID := 0
	for _, revision := range revisions {
		if id, err := strconv.Atoi(revision.RevisionID); err == nil && id > maxID {
			maxID = id
		}
	}
	return strconv.Itoa(maxID + 1)
}

func schemaTypeString(schemaType pubsubpb.Schema_Type) string {
	switch schemaType {
	case pubsubpb.Schema_PROTOCOL_BUFFER:
		return "PROTOCOL_BUFFER"
	case pubsubpb.Schema_AVRO:
		return "AVRO"
	case pubsubpb.Schema_TYPE_UNSPECIFIED:
		return ""
	default:
		return "INVALID"
	}
}

func protoSchemaType(schemaType string) pubsubpb.Schema_Type {
	switch schemaType {
	case "PROTOCOL_BUFFER":
		return pubsubpb.Schema_PROTOCOL_BUFFER
	case "AVRO":
		return pubsubpb.Schema_AVRO
	default:
		return pubsubpb.Schema_TYPE_UNSPECIFIED
	}
}

func schemaEncodingString(encoding pubsubpb.Encoding) string {
	switch encoding {
	case pubsubpb.Encoding_JSON:
		return "JSON"
	case pubsubpb.Encoding_BINARY:
		return "BINARY"
	case pubsubpb.Encoding_ENCODING_UNSPECIFIED:
		return ""
	default:
		return "INVALID"
	}
}

func listSchemaView(view pubsubpb.SchemaView) pubsubpb.SchemaView {
	if view == pubsubpb.SchemaView_SCHEMA_VIEW_UNSPECIFIED {
		return pubsubpb.SchemaView_BASIC
	}
	return view
}

func snapshotProtoExpireTime(raw string) *timestamppb.Timestamp {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	expireTime, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		expireTime, err = time.Parse(time.RFC3339, raw)
	}
	if err != nil {
		return nil
	}
	return timestamppb.New(expireTime)
}

func schemaProtoRevisionCreateTime(raw string) *timestamppb.Timestamp {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	revisionCreateTime, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		revisionCreateTime, err = time.Parse(time.RFC3339, raw)
	}
	if err != nil {
		return nil
	}
	return timestamppb.New(revisionCreateTime)
}

func (m pubsubMessage) toProto() *pubsubpb.PubsubMessage {
	data, _ := decodeBase64Bytes(m.Data)
	publishedAt, _ := time.Parse(time.RFC3339Nano, m.PublishTime)
	return &pubsubpb.PubsubMessage{
		Data:        data,
		Attributes:  copyStringMap(m.Attributes),
		MessageId:   m.MessageID,
		PublishTime: timestamppb.New(publishedAt),
		OrderingKey: m.OrderingKey,
	}
}

func grpcProjectID(project string) (string, bool) {
	project = strings.TrimSpace(project)
	parts := strings.Split(project, "/")
	if len(parts) != 2 || parts[0] != "projects" || !validProjectID(parts[1]) {
		return "", false
	}
	return parts[1], true
}

func grpcPageBounds(total int, pageSize int32, pageToken string) (int, int, string, error) {
	start := 0
	if pageToken != "" {
		parsed, err := strconv.Atoi(pageToken)
		if err != nil || parsed < 0 || parsed > total {
			return 0, 0, "", status.Error(codes.InvalidArgument, "invalid page token")
		}
		start = parsed
	}
	limit := total
	if pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}
	end := start + limit
	if end > total {
		end = total
	}
	nextToken := ""
	if end < total {
		nextToken = strconv.Itoa(end)
	}
	return start, end, nextToken, nil
}

func grpcUpdateMaskPaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update mask paths are required")
	}
	normalized := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, status.Error(codes.InvalidArgument, "update mask paths must not contain empty values")
		}
		normalized = append(normalized, path)
	}
	return normalized, nil
}

func grpcCanonicalMaskPath(path string) string {
	var builder strings.Builder
	for i, r := range path {
		switch {
		case r == '.':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			if i > 0 {
				builder.WriteByte('_')
			}
			builder.WriteRune(r + ('a' - 'A'))
		default:
			builder.WriteRune(r)
		}
	}
	return strings.ReplaceAll(builder.String(), ".", "_")
}

func grpcContainsCanonicalMaskPath(paths []string, target string) bool {
	for _, path := range paths {
		if grpcCanonicalMaskPath(path) == target {
			return true
		}
	}
	return false
}

func protoDuration(raw string) *durationpb.Duration {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	duration, err := parseGoogleDuration(raw)
	if err != nil {
		return nil
	}
	return durationpb.New(duration)
}

func grpcDurationString(duration *durationpb.Duration) string {
	if duration == nil {
		return ""
	}
	return fmt.Sprintf("%ds", int64(duration.AsDuration().Seconds()))
}
