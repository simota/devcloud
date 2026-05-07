package pubsub

import (
	"context"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

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
