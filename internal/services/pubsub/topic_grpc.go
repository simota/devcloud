package pubsub

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

func (t topicResource) toProto() *pubsubpb.Topic {
	return &pubsubpb.Topic{
		Name:                     t.Name,
		Labels:                   copyStringMap(t.Labels),
		KmsKeyName:               t.KMSKeyName,
		SchemaSettings:           protoSchemaSettings(t.SchemaSettings),
		MessageRetentionDuration: protoDuration(t.MessageRetentionDuration),
	}
}
