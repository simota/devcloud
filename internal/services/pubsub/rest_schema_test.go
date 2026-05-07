package pubsub

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestRESTSchemaCRUDAndPagination(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var schema schemaResource
	if err := json.NewDecoder(create.Body).Decode(&schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	if schema.Name != "projects/devcloud/schemas/order-event" || schema.Type != "AVRO" || schema.RevisionID != "1" {
		t.Fatalf("schema = %#v", schema)
	}

	duplicate := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{"type":"AVRO"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/schemas/payment-event", `{"type":"PROTOCOL_BUFFER","definition":"message PaymentEvent {}"}`)
	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?pageSize=1", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d: %s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed struct {
		Schemas       []schemaResource `json:"schemas"`
		NextPageToken string           `json:"nextPageToken"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode schemas: %v", err)
	}
	if len(listed.Schemas) != 1 || listed.Schemas[0].Name != "projects/devcloud/schemas/order-event" || listed.NextPageToken == "" {
		t.Fatalf("schemas page = %#v", listed)
	}

	deleteSchema := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/schemas/order-event", "")
	if deleteSchema.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d: %s", deleteSchema.Code, http.StatusNoContent, deleteSchema.Body.String())
	}
	getDeleted := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if getDeleted.Code != http.StatusNotFound {
		t.Fatalf("get deleted status = %d, want %d", getDeleted.Code, http.StatusNotFound)
	}
}

func TestRESTSchemaViewBasicOmitsDefinition(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	basicGet := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event?view=BASIC", "")
	if basicGet.Code != http.StatusOK {
		t.Fatalf("basic get status = %d, want %d: %s", basicGet.Code, http.StatusOK, basicGet.Body.String())
	}
	var basic schemaResource
	if err := json.NewDecoder(basicGet.Body).Decode(&basic); err != nil {
		t.Fatalf("decode basic schema: %v", err)
	}
	if basic.Name != "projects/devcloud/schemas/order-event" || basic.Definition != "" {
		t.Fatalf("basic schema = %#v", basic)
	}

	fullList := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?view=FULL", "")
	if fullList.Code != http.StatusOK || !strings.Contains(fullList.Body.String(), "OrderEvent") {
		t.Fatalf("full list status = %d body %s", fullList.Code, fullList.Body.String())
	}
	basicList := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas?view=BASIC", "")
	if basicList.Code != http.StatusOK || strings.Contains(basicList.Body.String(), "OrderEvent") {
		t.Fatalf("basic list status = %d body %s", basicList.Code, basicList.Body.String())
	}
	invalid := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/schemas/order-event?view=SECRET", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid view status = %d, want %d: %s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestRESTSchemaValidationAndPersistence(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	missingID := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas", `{"type":"AVRO"}`)
	if missingID.Code != http.StatusBadRequest {
		t.Fatalf("missing schemaId status = %d, want %d: %s", missingID.Code, http.StatusBadRequest, missingID.Body.String())
	}
	nameMismatch := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"name":"projects/devcloud/schemas/other",
		"type":"AVRO"
	}`)
	if nameMismatch.Code != http.StatusBadRequest {
		t.Fatalf("name mismatch status = %d, want %d: %s", nameMismatch.Code, http.StatusBadRequest, nameMismatch.Body.String())
	}
	invalidType := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{"type":"JSON_SCHEMA"}`)
	if invalidType.Code != http.StatusBadRequest {
		t.Fatalf("invalid type status = %d, want %d: %s", invalidType.Code, http.StatusBadRequest, invalidType.Body.String())
	}

	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	persisted := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	create := performPubSubRequest(persisted, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}",
		"revisionId":"custom-revision"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create persisted status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/schemas/order-event", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var schema schemaResource
	if err := json.NewDecoder(get.Body).Decode(&schema); err != nil {
		t.Fatalf("decode reloaded schema: %v", err)
	}
	if schema.RevisionID != "custom-revision" || schema.Definition == "" {
		t.Fatalf("reloaded schema = %#v", schema)
	}
}

func TestRESTSchemaValidateMessageAcceptsExistingAndInlineSchemas(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	create := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=order-event", `{
		"type":"AVRO",
		"definition":"{\"type\":\"record\",\"name\":\"OrderEvent\",\"fields\":[]}"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create schema status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	existing := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"name":"projects/devcloud/schemas/order-event",
		"message":"e30=",
		"encoding":"JSON"
	}`)
	if existing.Code != http.StatusOK {
		t.Fatalf("validate existing status = %d, want %d: %s", existing.Code, http.StatusOK, existing.Body.String())
	}
	if strings.Contains(existing.Body.String(), "e30=") || strings.Contains(existing.Body.String(), "OrderEvent") {
		t.Fatalf("validate existing leaked request payload: %s", existing.Body.String())
	}

	inline := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"PROTOCOL_BUFFER","definition":"message OrderEvent {}"},
		"message":"CgVvcmRlckE=",
		"encoding":"BINARY"
	}`)
	if inline.Code != http.StatusOK {
		t.Fatalf("validate inline status = %d, want %d: %s", inline.Code, http.StatusOK, inline.Body.String())
	}

	unpaddedInline := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"PROTOCOL_BUFFER","definition":"message OrderEvent {}"},
		"message":"CgVvcmRlckE",
		"encoding":"BINARY"
	}`)
	if unpaddedInline.Code != http.StatusOK {
		t.Fatalf("validate unpadded inline status = %d, want %d: %s", unpaddedInline.Code, http.StatusOK, unpaddedInline.Body.String())
	}
}

func TestRESTSchemaValidateMessageRejectsInvalidRequests(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	invalidCreate := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas?schemaId=invalid-avro", `{
		"type":"AVRO",
		"definition":"not-json"
	}`)
	if invalidCreate.Code != http.StatusBadRequest {
		t.Fatalf("invalid create status = %d, want %d: %s", invalidCreate.Code, http.StatusBadRequest, invalidCreate.Body.String())
	}

	missingSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{"message":"e30="}`)
	if missingSchema.Code != http.StatusBadRequest {
		t.Fatalf("missing schema status = %d, want %d: %s", missingSchema.Code, http.StatusBadRequest, missingSchema.Body.String())
	}

	missingStoredSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"name":"projects/devcloud/schemas/missing",
		"message":"e30="
	}`)
	if missingStoredSchema.Code != http.StatusNotFound {
		t.Fatalf("missing stored schema status = %d, want %d: %s", missingStoredSchema.Code, http.StatusNotFound, missingStoredSchema.Body.String())
	}

	invalidMessage := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO"},
		"message":"not-base64"
	}`)
	if invalidMessage.Code != http.StatusBadRequest {
		t.Fatalf("invalid message status = %d, want %d: %s", invalidMessage.Code, http.StatusBadRequest, invalidMessage.Body.String())
	}

	invalidJSON := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO"},
		"message":"bm90LWpzb24=",
		"encoding":"JSON"
	}`)
	if invalidJSON.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want %d: %s", invalidJSON.Code, http.StatusBadRequest, invalidJSON.Body.String())
	}

	invalidInlineSchema := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"type":"AVRO","definition":"[]"},
		"message":"e30=",
		"encoding":"JSON"
	}`)
	if invalidInlineSchema.Code != http.StatusBadRequest {
		t.Fatalf("invalid inline schema status = %d, want %d: %s", invalidInlineSchema.Code, http.StatusBadRequest, invalidInlineSchema.Body.String())
	}

	wrongProject := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/schemas:validateMessage", `{
		"schema":{"name":"projects/other/schemas/order-event","type":"AVRO"},
		"message":"e30="
	}`)
	if wrongProject.Code != http.StatusBadRequest {
		t.Fatalf("wrong project status = %d, want %d: %s", wrongProject.Code, http.StatusBadRequest, wrongProject.Body.String())
	}
}
