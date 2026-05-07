package pubsub

import (
	"context"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func (s snapshotResource) toProto() *pubsubpb.Snapshot {
	return &pubsubpb.Snapshot{
		Name:       s.Name,
		Topic:      s.Topic,
		ExpireTime: snapshotProtoExpireTime(s.ExpireTime),
		Labels:     copyStringMap(s.Labels),
	}
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
