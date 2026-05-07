package redshift

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthReportsRunningWithoutSecrets(t *testing.T) {
	server := NewServer(Config{
		SQLAddr: "127.0.0.1:15439",
		APIAddr: "127.0.0.1:19099",
		User:    "dev",
	})
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("health response leaked sensitive fields: %s", rec.Body.String())
	}
	var response map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["service"] != "redshift" || response["status"] != "running" || response["running"] != true {
		t.Fatalf("response = %#v", response)
	}
}

func TestSnapshotUsesConfiguredClusterMetadata(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		Region:            "ap-northeast-1",
		ClusterIdentifier: "local-cluster",
		Database:          "warehouse",
		NodeType:          "ra3.xlplus",
		NumberOfNodes:     2,
		StoragePath:       ".devcloud/data/redshift",
		User:              "analyst",
	})

	snapshot := server.Snapshot()

	if snapshot.Status != "running" || !snapshot.Running || snapshot.Region != "ap-northeast-1" {
		t.Fatalf("snapshot metadata = %#v", snapshot)
	}
	if len(snapshot.Clusters) != 1 {
		t.Fatalf("clusters = %#v", snapshot.Clusters)
	}
	cluster := snapshot.Clusters[0]
	if cluster.ClusterIdentifier != "local-cluster" || cluster.DatabaseName != "warehouse" || cluster.NodeType != "ra3.xlplus" || cluster.NumberOfNodes != 2 {
		t.Fatalf("cluster = %#v", cluster)
	}
	if cluster.Endpoint.Address != "127.0.0.1" || cluster.Endpoint.Port != 15439 {
		t.Fatalf("endpoint = %#v", cluster.Endpoint)
	}
	if cluster.MasterUsername != "analyst" {
		t.Fatalf("master username = %q", cluster.MasterUsername)
	}
}

func redshiftDataAPIRequest(t *testing.T, server *Server, operation string, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftData."+operation)
	server.ServeHTTP(rec, req)
	return rec
}

func redshiftServerlessRequest(t *testing.T, server *Server, operation string, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftServerless."+operation)
	server.ServeHTTP(rec, req)
	return rec
}

func writeTestStartup(conn net.Conn, params map[string]string) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgProtocolVersion)
	for key, value := range params {
		body.WriteString(key)
		body.WriteByte(0)
		body.WriteString(value)
		body.WriteByte(0)
	}
	body.WriteByte(0)
	return writeMessage(conn, 0, body.Bytes())
}

func writeTestTypedMessage(conn net.Conn, messageType byte, body []byte) error {
	return writeMessage(conn, messageType, body)
}

func readTestMessage(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	messageType := []byte{0}
	if _, err := conn.Read(messageType); err != nil {
		t.Fatalf("read message type: %v", err)
	}
	payload, err := readMessagePayload(conn)
	if err != nil {
		t.Fatalf("read message payload: %v", err)
	}
	return messageType[0], payload
}

func readTestBufferMessageTypes(t *testing.T, buffer *bytes.Buffer) []byte {
	t.Helper()
	var messageTypes []byte
	for buffer.Len() > 0 {
		messageType, err := buffer.ReadByte()
		if err != nil {
			t.Fatalf("read buffer message type: %v", err)
		}
		if _, err := readMessagePayload(buffer); err != nil {
			t.Fatalf("read buffer message payload: %v", err)
		}
		messageTypes = append(messageTypes, messageType)
	}
	return messageTypes
}

func waitForReady(t *testing.T, conn net.Conn) {
	t.Helper()
	for {
		messageType, _ := readTestMessage(t, conn)
		if messageType == 'Z' {
			return
		}
	}
}

func resultContainsRow(result queryResult, values ...string) bool {
	for _, row := range result.rows {
		for start := 0; start+len(values) <= len(row); start++ {
			matches := true
			for i, value := range values {
				if row[start+i] != value {
					matches = false
					break
				}
			}
			if matches {
				return true
			}
		}
	}
	return false
}

func columnSnapshotHas(columns []TableColumnSnapshot, name string, encoding string, defaultValue string, identity bool) bool {
	for _, column := range columns {
		if column.Name == name && column.Encoding == encoding && column.DefaultValue == defaultValue && column.Identity == identity {
			return true
		}
	}
	return false
}

