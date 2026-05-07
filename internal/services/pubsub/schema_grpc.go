package pubsub

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
