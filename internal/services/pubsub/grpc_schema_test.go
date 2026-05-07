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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestGRPCSchemaServiceCRUDAndValidateMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
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

	client := pubsubpb.NewSchemaServiceClient(conn)
	created, err := client.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
		Parent:   "projects/devcloud",
		SchemaId: "order-event",
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"OrderEvent","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	if created.GetName() != "projects/devcloud/schemas/order-event" || created.GetType() != pubsubpb.Schema_AVRO || created.GetRevisionId() != "1" {
		t.Fatalf("created schema = %#v", created)
	}

	basic, err := client.GetSchema(ctx, &pubsubpb.GetSchemaRequest{
		Name: created.GetName(),
		View: pubsubpb.SchemaView_BASIC,
	})
	if err != nil {
		t.Fatalf("GetSchema BASIC: %v", err)
	}
	if basic.GetDefinition() != "" {
		t.Fatalf("BASIC schema leaked definition")
	}
	list, err := client.ListSchemas(ctx, &pubsubpb.ListSchemasRequest{
		Parent:   "projects/devcloud",
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("ListSchemas: %v", err)
	}
	if len(list.GetSchemas()) != 1 || list.GetSchemas()[0].GetName() != created.GetName() || list.GetSchemas()[0].GetDefinition() != "" {
		t.Fatalf("schema list = %#v", list.GetSchemas())
	}
	if _, err := client.ValidateSchema(ctx, &pubsubpb.ValidateSchemaRequest{
		Parent: "projects/devcloud",
		Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_PROTOCOL_BUFFER, Definition: "message OrderEvent {}"},
	}); err != nil {
		t.Fatalf("ValidateSchema: %v", err)
	}
	if _, err := client.ValidateSchema(ctx, &pubsubpb.ValidateSchemaRequest{
		Parent: "projects/devcloud",
		Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: `not-json`},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ValidateSchema invalid AVRO code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent:     "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Name{Name: created.GetName()},
		Message:    []byte(`{}`),
		Encoding:   pubsubpb.Encoding_JSON,
	}); err != nil {
		t.Fatalf("ValidateMessage existing schema: %v", err)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent:     "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Name{Name: created.GetName()},
		Message:    []byte(`not-json`),
		Encoding:   pubsubpb.Encoding_JSON,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ValidateMessage invalid JSON code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := client.ValidateMessage(ctx, &pubsubpb.ValidateMessageRequest{
		Parent: "projects/devcloud",
		SchemaSpec: &pubsubpb.ValidateMessageRequest_Schema{Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_PROTOCOL_BUFFER,
			Definition: "message OrderEvent {}",
		}},
		Message:  []byte("order"),
		Encoding: pubsubpb.Encoding_BINARY,
	}); err != nil {
		t.Fatalf("ValidateMessage inline schema: %v", err)
	}
	committed, err := client.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
		Name: created.GetName(),
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"OrderEventV2","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CommitSchema: %v", err)
	}
	if committed.GetRevisionId() != "2" || committed.GetDefinition() == created.GetDefinition() || committed.GetRevisionCreateTime() == nil {
		t.Fatalf("committed schema = %#v", committed)
	}
	if _, err := client.CommitSchema(ctx, &pubsubpb.CommitSchemaRequest{
		Name: created.GetName(),
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `["not","an","object"]`,
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CommitSchema invalid AVRO code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	revisions, err := client.ListSchemaRevisions(ctx, &pubsubpb.ListSchemaRevisionsRequest{
		Name: created.GetName(),
		View: pubsubpb.SchemaView_FULL,
	})
	if err != nil {
		t.Fatalf("ListSchemaRevisions: %v", err)
	}
	if len(revisions.GetSchemas()) != 2 || revisions.GetSchemas()[0].GetRevisionId() != "1" || revisions.GetSchemas()[1].GetRevisionId() != "2" {
		t.Fatalf("schema revisions = %#v", revisions.GetSchemas())
	}
	rolledBack, err := client.RollbackSchema(ctx, &pubsubpb.RollbackSchemaRequest{
		Name:       created.GetName(),
		RevisionId: "1",
	})
	if err != nil {
		t.Fatalf("RollbackSchema: %v", err)
	}
	if rolledBack.GetRevisionId() != "3" || rolledBack.GetDefinition() != created.GetDefinition() {
		t.Fatalf("rolled back schema = %#v", rolledBack)
	}
	afterDelete, err := client.DeleteSchemaRevision(ctx, &pubsubpb.DeleteSchemaRevisionRequest{
		Name: created.GetName() + "@2",
	})
	if err != nil {
		t.Fatalf("DeleteSchemaRevision: %v", err)
	}
	if afterDelete.GetRevisionId() != "3" {
		t.Fatalf("schema after deleting revision = %#v", afterDelete)
	}
	if _, err := client.DeleteSchema(ctx, &pubsubpb.DeleteSchemaRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteSchema: %v", err)
	}
	if _, err := client.GetSchema(ctx, &pubsubpb.GetSchemaRequest{Name: created.GetName()}); err == nil {
		t.Fatalf("GetSchema after delete error = nil")
	}
}

func TestGRPCTopicSchemaSettingsRoundTripAndPublishValidation(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
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

	schemaClient := pubsubpb.NewSchemaServiceClient(conn)
	schema, err := schemaClient.CreateSchema(ctx, &pubsubpb.CreateSchemaRequest{
		Parent:   "projects/devcloud",
		SchemaId: "topic-schema",
		Schema: &pubsubpb.Schema{
			Type:       pubsubpb.Schema_AVRO,
			Definition: `{"type":"record","name":"TopicSchema","fields":[]}`,
		},
	})
	if err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	publisher := pubsubpb.NewPublisherClient(conn)
	topic, err := publisher.CreateTopic(ctx, &pubsubpb.Topic{
		Name: "projects/devcloud/topics/schema-topic",
		SchemaSettings: &pubsubpb.SchemaSettings{
			Schema:          schema.GetName(),
			Encoding:        pubsubpb.Encoding_JSON,
			FirstRevisionId: "1",
			LastRevisionId:  "1",
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if topic.GetSchemaSettings().GetSchema() != schema.GetName() || topic.GetSchemaSettings().GetEncoding() != pubsubpb.Encoding_JSON || topic.GetSchemaSettings().GetFirstRevisionId() != "1" || topic.GetSchemaSettings().GetLastRevisionId() != "1" {
		t.Fatalf("created topic schema settings = %#v", topic.GetSchemaSettings())
	}

	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("not-json")}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Publish invalid JSON code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic.GetName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte(`{"ok":true}`)}},
	}); err != nil {
		t.Fatalf("Publish valid JSON: %v", err)
	}

	updated, err := publisher.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic: &pubsubpb.Topic{
			Name: topic.GetName(),
			SchemaSettings: &pubsubpb.SchemaSettings{
				Schema:   schema.GetName(),
				Encoding: pubsubpb.Encoding_BINARY,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schema_settings"}},
	})
	if err != nil {
		t.Fatalf("UpdateTopic schema settings: %v", err)
	}
	if updated.GetSchemaSettings().GetEncoding() != pubsubpb.Encoding_BINARY {
		t.Fatalf("updated topic schema settings = %#v", updated.GetSchemaSettings())
	}
}
