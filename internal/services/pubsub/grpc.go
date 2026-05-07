package pubsub

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

func stringValue(value any) string {
	text, _ := value.(string)
	return text
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
