package redshift

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	s3svc "devcloud/internal/services/s3"
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

func TestDescribeClustersUsesAWSQueryXMLShape(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	rec := httptest.NewRecorder()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusters&Version=2012-12-01"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"DescribeClustersResponse", "ClusterIdentifier>devcloud", "DBName>dev", "Port>15439"} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}
}

func TestCreateAndDeleteClusterUpdateManagementMetadata(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateCluster&ClusterIdentifier=analytics&DBName=warehouse&MasterUsername=analyst&NodeType=ra3.xlplus&NumberOfNodes=2"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateCluster status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, want := range []string{"CreateClusterResponse", "ClusterIdentifier>analytics", "DBName>warehouse", "NodeType>ra3.xlplus", "NumberOfNodes>2"} {
		if !strings.Contains(createRec.Body.String(), want) {
			t.Fatalf("CreateCluster response missing %q: %s", want, createRec.Body.String())
		}
	}

	snapshot := server.Snapshot()
	if len(snapshot.Clusters) != 2 {
		t.Fatalf("clusters after create = %#v", snapshot.Clusters)
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DeleteCluster&ClusterIdentifier=analytics"))
	deleteReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteCluster status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	if !strings.Contains(deleteRec.Body.String(), "DeleteClusterResponse") || !strings.Contains(deleteRec.Body.String(), "ClusterIdentifier>analytics") {
		t.Fatalf("DeleteCluster response = %s", deleteRec.Body.String())
	}

	snapshot = server.Snapshot()
	if len(snapshot.Clusters) != 1 || snapshot.Clusters[0].ClusterIdentifier != "devcloud" {
		t.Fatalf("clusters after delete = %#v", snapshot.Clusters)
	}
}

func TestManagementClusterSnapshotsAreCreatedListedDeletedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		NodeType:          "ra3.xlplus",
		NumberOfNodes:     2,
		User:              "dev",
	})

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateClusterSnapshot&ClusterIdentifier=devcloud&SnapshotIdentifier=snap-1"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateClusterSnapshot status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, want := range []string{"CreateClusterSnapshotResponse", "SnapshotIdentifier>snap-1", "ClusterIdentifier>devcloud", "Status>available", "NodeType>ra3.xlplus", "NumberOfNodes>2"} {
		if !strings.Contains(createRec.Body.String(), want) {
			t.Fatalf("CreateClusterSnapshot response missing %q: %s", want, createRec.Body.String())
		}
	}

	reloaded := NewServer(Config{
		SQLAddr:     "127.0.0.1:25439",
		StoragePath: storagePath,
		Database:    "dev",
		User:        "dev",
	})
	describeRec := httptest.NewRecorder()
	describeReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusterSnapshots&ClusterIdentifier=devcloud"))
	describeReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reloaded.ServeHTTP(describeRec, describeReq)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeClusterSnapshots status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	for _, want := range []string{"DescribeClusterSnapshotsResponse", "SnapshotIdentifier>snap-1", "ClusterIdentifier>devcloud", "Port>15439"} {
		if !strings.Contains(describeRec.Body.String(), want) {
			t.Fatalf("DescribeClusterSnapshots response missing %q: %s", want, describeRec.Body.String())
		}
	}

	deleteRec := httptest.NewRecorder()
	deleteReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DeleteClusterSnapshot&SnapshotIdentifier=snap-1"))
	deleteReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reloaded.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || !strings.Contains(deleteRec.Body.String(), "DeleteClusterSnapshotResponse") {
		t.Fatalf("DeleteClusterSnapshot status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	describeRec = httptest.NewRecorder()
	reloaded.ServeHTTP(describeRec, describeReq)
	if strings.Contains(describeRec.Body.String(), "SnapshotIdentifier>snap-1") {
		t.Fatalf("deleted snapshot still listed: %s", describeRec.Body.String())
	}
}

func TestManagementClusterSnapshotRejectsUnknownClusterWithoutSecrets(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Password:          "local-password",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateClusterSnapshot&ClusterIdentifier=missing&SnapshotIdentifier=snap-1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "local-password") {
		t.Fatalf("error response leaked credential material: %s", rec.Body.String())
	}
}

func TestManagementRestoreFromClusterSnapshotCreatesClusterMetadataOnly(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		NodeType:          "ra3.xlplus",
		NumberOfNodes:     2,
		User:              "dev",
		Password:          "local-password",
	})

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateClusterSnapshot&ClusterIdentifier=devcloud&SnapshotIdentifier=snap-restore"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateClusterSnapshot status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	restoreRec := httptest.NewRecorder()
	restoreReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=RestoreFromClusterSnapshot&ClusterIdentifier=restored&SnapshotIdentifier=snap-restore"))
	restoreReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(restoreRec, restoreReq)
	if restoreRec.Code != http.StatusOK {
		t.Fatalf("RestoreFromClusterSnapshot status = %d, body = %s", restoreRec.Code, restoreRec.Body.String())
	}
	for _, want := range []string{"RestoreFromClusterSnapshotResponse", "ClusterIdentifier>restored", "DBName>dev", "NodeType>ra3.xlplus", "NumberOfNodes>2"} {
		if !strings.Contains(restoreRec.Body.String(), want) {
			t.Fatalf("RestoreFromClusterSnapshot response missing %q: %s", want, restoreRec.Body.String())
		}
	}
	if strings.Contains(restoreRec.Body.String(), "local-password") {
		t.Fatalf("restore response leaked credential material: %s", restoreRec.Body.String())
	}

	reloaded := NewServer(Config{
		SQLAddr:     "127.0.0.1:25439",
		StoragePath: storagePath,
		Database:    "dev",
		User:        "dev",
	})
	describeRec := httptest.NewRecorder()
	describeReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusters"))
	describeReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reloaded.ServeHTTP(describeRec, describeReq)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeClusters status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	if !strings.Contains(describeRec.Body.String(), "ClusterIdentifier>restored") {
		t.Fatalf("restored cluster was not persisted: %s", describeRec.Body.String())
	}
}

func TestManagementTagsAreAttachedListedAndDeleted(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		Region:            "us-east-1",
		AccountID:         "000000000000",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	resourceName := "arn:aws:redshift:us-east-1:000000000000:cluster:devcloud"

	createRec := httptest.NewRecorder()
	createValues := url.Values{
		"Action":              {"CreateTags"},
		"ResourceName":        {resourceName},
		"Tags.member.1.Key":   {"env"},
		"Tags.member.1.Value": {"local"},
		"Tags.member.2.Key":   {"owner"},
		"Tags.member.2.Value": {"dev"},
	}
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(createValues.Encode()))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK || !strings.Contains(createRec.Body.String(), "CreateTagsResponse") {
		t.Fatalf("CreateTags status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	describeRec := httptest.NewRecorder()
	describeValues := url.Values{"Action": {"DescribeTags"}, "ResourceName": {resourceName}}
	describeReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(describeValues.Encode()))
	describeReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(describeRec, describeReq)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTags status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	for _, want := range []string{"DescribeTagsResponse", "ResourceName>" + resourceName, "Key>env", "Value>local", "Key>owner", "Value>dev"} {
		if !strings.Contains(describeRec.Body.String(), want) {
			t.Fatalf("DescribeTags response missing %q: %s", want, describeRec.Body.String())
		}
	}

	deleteRec := httptest.NewRecorder()
	deleteValues := url.Values{
		"Action":           {"DeleteTags"},
		"ResourceName":     {resourceName},
		"TagKeys.member.1": {"env"},
	}
	deleteReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(deleteValues.Encode()))
	deleteReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK || !strings.Contains(deleteRec.Body.String(), "DeleteTagsResponse") {
		t.Fatalf("DeleteTags status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	describeRec = httptest.NewRecorder()
	describeReq = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(describeValues.Encode()))
	describeReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(describeRec, describeReq)
	if strings.Contains(describeRec.Body.String(), "Key>env") || !strings.Contains(describeRec.Body.String(), "Key>owner") {
		t.Fatalf("DescribeTags after delete = %s", describeRec.Body.String())
	}
}

func TestDescribeClusterParameterGroupsAndParameters(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Password:          "local-password",
	})

	groupsRec := httptest.NewRecorder()
	groupsReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusterParameterGroups&ParameterGroupName=default.redshift-1.0"))
	groupsReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(groupsRec, groupsReq)
	if groupsRec.Code != http.StatusOK {
		t.Fatalf("DescribeClusterParameterGroups status = %d, body = %s", groupsRec.Code, groupsRec.Body.String())
	}
	for _, want := range []string{"DescribeClusterParameterGroupsResponse", "ParameterGroupName>default.redshift-1.0", "ParameterGroupFamily>redshift-1.0"} {
		if !strings.Contains(groupsRec.Body.String(), want) {
			t.Fatalf("DescribeClusterParameterGroups response missing %q: %s", want, groupsRec.Body.String())
		}
	}

	parametersRec := httptest.NewRecorder()
	parametersReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusterParameters&ParameterGroupName=default.redshift-1.0"))
	parametersReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(parametersRec, parametersReq)
	if parametersRec.Code != http.StatusOK {
		t.Fatalf("DescribeClusterParameters status = %d, body = %s", parametersRec.Code, parametersRec.Body.String())
	}
	for _, want := range []string{"DescribeClusterParametersResponse", "ParameterName>datestyle", "ParameterName>enable_user_activity_logging", "ParameterName>max_query_execution_time"} {
		if !strings.Contains(parametersRec.Body.String(), want) {
			t.Fatalf("DescribeClusterParameters response missing %q: %s", want, parametersRec.Body.String())
		}
	}
	if strings.Contains(parametersRec.Body.String(), "local-password") {
		t.Fatalf("parameter response leaked credential material: %s", parametersRec.Body.String())
	}
}

func TestDescribeClusterParametersRejectsUnknownGroupWithoutSecrets(t *testing.T) {
	server := NewServer(Config{Password: "local-password"})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=DescribeClusterParameters&ParameterGroupName=missing"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "local-password") {
		t.Fatalf("error response leaked credential material: %s", rec.Body.String())
	}
}

func TestGetClusterCredentialsReturnsLocalCredentials(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		User:              "dev",
		Password:          "local-password",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=GetClusterCredentials&ClusterIdentifier=devcloud&DbUser=analyst&DurationSeconds=60"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"GetClusterCredentialsResponse", "DbUser>analyst", "DbPassword>local-password", "Expiration>", "RequestId>devcloud-redshift"} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}
}

func TestGetClusterCredentialsRejectsUnknownCluster(t *testing.T) {
	server := NewServer(Config{ClusterIdentifier: "devcloud"})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=GetClusterCredentials&ClusterIdentifier=missing"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "local-password") {
		t.Fatalf("error response leaked credential material: %s", rec.Body.String())
	}
}

func TestServerlessMetadataTargetsUseConfiguredCluster(t *testing.T) {
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "analytics",
		Database:          "warehouse",
		User:              "dev",
	})

	workgroupsRec := redshiftServerlessRequest(t, server, "ListWorkgroups", `{}`)
	if workgroupsRec.Code != http.StatusOK {
		t.Fatalf("ListWorkgroups status = %d, body = %s", workgroupsRec.Code, workgroupsRec.Body.String())
	}
	for _, want := range []string{`"workgroupName":"analytics"`, `"namespaceName":"warehouse"`, `"port":15439`, `"status":"AVAILABLE"`} {
		if !strings.Contains(workgroupsRec.Body.String(), want) {
			t.Fatalf("ListWorkgroups response missing %q: %s", want, workgroupsRec.Body.String())
		}
	}

	namespaceRec := redshiftServerlessRequest(t, server, "GetNamespace", `{"namespaceName":"warehouse"}`)
	if namespaceRec.Code != http.StatusOK {
		t.Fatalf("GetNamespace status = %d, body = %s", namespaceRec.Code, namespaceRec.Body.String())
	}
	for _, want := range []string{`"namespaceName":"warehouse"`, `"dbName":"warehouse"`, `"status":"AVAILABLE"`} {
		if !strings.Contains(namespaceRec.Body.String(), want) {
			t.Fatalf("GetNamespace response missing %q: %s", want, namespaceRec.Body.String())
		}
	}
}

func TestServerlessMetadataRejectsUnknownWorkgroupWithoutSecrets(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "analytics",
		Database:          "warehouse",
		Password:          "local-password",
	})

	rec := redshiftServerlessRequest(t, server, "GetWorkgroup", `{"workgroupName":"missing"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "local-password") {
		t.Fatalf("error response leaked credential material: %s", rec.Body.String())
	}
}

func TestPgWireSelectOneWithPasswordAuth(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{
		"user":            "dev",
		"database":        "dev",
		"client_encoding": "UTF8",
	}); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	messageType, payload := readTestMessage(t, client)
	if messageType != 'R' || binary.BigEndian.Uint32(payload) != uint32(pgAuthCleartext) {
		t.Fatalf("auth request = %q %#v", messageType, payload)
	}
	if err := writeTestTypedMessage(client, 'p', []byte("dev\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}
	waitForReady(t, client)

	if err := writeTestTypedMessage(client, 'Q', []byte("select 1;\x00")); err != nil {
		t.Fatalf("write query: %v", err)
	}

	var sawRow bool
	for {
		messageType, payload = readTestMessage(t, client)
		switch messageType {
		case 'D':
			if !bytes.Contains(payload, []byte("1")) {
				t.Fatalf("data row payload = %#v", payload)
			}
			sawRow = true
		case 'Z':
			if !sawRow {
				t.Fatal("ReadyForQuery arrived before DataRow")
			}
			writeTestTypedMessage(client, 'X', nil)
			return
		}
	}
}

func TestPgWireMinimalExtendedQuerySelectOne(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	messageTypes := readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	var bind bytes.Buffer
	writeCString(&bind, "portal1")
	writeCString(&bind, "stmt1")
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	wire.Reset()
	session.handleBind(&wire, bind.Bytes())
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('P')
	writeCString(&describe, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'T'}) {
		t.Fatalf("describe responses = %q", messageTypes)
	}

	var execute bytes.Buffer
	writeCString(&execute, "portal1")
	binary.Write(&execute, binary.BigEndian, int32(0))
	wire.Reset()
	session.handleExecute(server, &wire, execute.Bytes())
	if !bytes.Contains(wire.Bytes(), []byte("SELECT 1")) {
		t.Fatalf("execute response missing command tag: %#v", wire.Bytes())
	}
	messageTypes = readTestBufferMessageTypes(t, &wire)
	if !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("execute responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolDescribePreparedStatementAndSyncRecovery(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('S')
	writeCString(&describe, "stmt1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'t', 'T'}) {
		t.Fatalf("describe prepared statement responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, []byte("broken"))
	if !session.failed {
		t.Fatal("protocol error did not mark extended session failed")
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("protocol error responses = %q", messageTypes)
	}

	session.handleSync(&wire)
	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind after sync recovery responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolBindTextParametersWithoutLoggingValues(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (777, 'alpha')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select payload from public.events where id = $1")
	binary.Write(&parse, binary.BigEndian, int16(1))
	binary.Write(&parse, binary.BigEndian, pgTypeInt4OID)
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithTextParams("portal1", "stmt1", "777"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var execute bytes.Buffer
	writeCString(&execute, "portal1")
	binary.Write(&execute, binary.BigEndian, int32(0))
	wire.Reset()
	session.handleExecute(server, &wire, execute.Bytes())
	if !bytes.Contains(wire.Bytes(), []byte("alpha")) {
		t.Fatalf("execute response missing selected row: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("execute responses = %q", messageTypes)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history count = %d", len(statements))
	}
	if statements[0].QueryPreview != "select payload from public.events where id = $1" {
		t.Fatalf("statement history logged executable SQL with bind values: %#v", statements[0])
	}
}

func TestPgWireExtendedProtocolDescribePortalWithTextParameters(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (42, 'portal-describe')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select payload from public.events where id = $1")
	binary.Write(&parse, binary.BigEndian, int16(1))
	binary.Write(&parse, binary.BigEndian, pgTypeInt4OID)
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithTextParams("portal1", "stmt1", "42"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var describe bytes.Buffer
	describe.WriteByte('P')
	writeCString(&describe, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describe.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'T'}) {
		t.Fatalf("describe portal responses = %q", messageTypes)
	}
}

func TestPgWireExtendedProtocolRejectsBinaryResultFormats(t *testing.T) {
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayloadWithResultFormats("portal1", "stmt1", 1))
	if !session.failed {
		t.Fatal("binary result format did not mark extended session failed")
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}
	if strings.Contains(wire.String(), "select 1") {
		t.Fatalf("binary result format error leaked SQL text: %#v", wire.String())
	}
}

func TestPgWireExtendedProtocolExecuteHonorsMaxRowsAndResumesPortal(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id int, payload varchar(64))",
		"insert into public.events(id, payload) values (1, 'one')",
		"insert into public.events(id, payload) values (2, 'two')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute setup %q: %v", statement, err)
		}
	}
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select id, payload from public.events")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleExecute(server, &wire, executePayload("portal1", 1))
	if bytes.Contains(wire.Bytes(), []byte("two")) {
		t.Fatalf("first execute returned more than maxRows: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 's'}) {
		t.Fatalf("first execute responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleExecute(server, &wire, executePayload("portal1", 0))
	if !bytes.Contains(wire.Bytes(), []byte("two")) {
		t.Fatalf("second execute did not resume portal: %#v", wire.Bytes())
	}
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'D', 'C'}) {
		t.Fatalf("second execute responses = %q", messageTypes)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history count = %d", len(statements))
	}
}

func TestPgWireExtendedProtocolCloseStatementAndPortal(t *testing.T) {
	server := NewServer(Config{})
	session := newExtendedQuerySession()

	var parse bytes.Buffer
	writeCString(&parse, "stmt1")
	writeCString(&parse, "select 1")
	binary.Write(&parse, binary.BigEndian, int16(0))
	var wire bytes.Buffer
	session.handleParse(&wire, parse.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'1'}) {
		t.Fatalf("parse responses = %q", messageTypes)
	}

	wire.Reset()
	session.handleBind(&wire, bindPayload("portal1", "stmt1"))
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'2'}) {
		t.Fatalf("bind responses = %q", messageTypes)
	}

	var closePortal bytes.Buffer
	closePortal.WriteByte('P')
	writeCString(&closePortal, "portal1")
	wire.Reset()
	session.handleClose(&wire, closePortal.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'3'}) {
		t.Fatalf("close portal responses = %q", messageTypes)
	}

	var describePortal bytes.Buffer
	describePortal.WriteByte('P')
	writeCString(&describePortal, "portal1")
	wire.Reset()
	session.handleDescribe(server, &wire, describePortal.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'E'}) {
		t.Fatalf("describe closed portal responses = %q", messageTypes)
	}

	session.handleSync(&wire)
	var closeStatement bytes.Buffer
	closeStatement.WriteByte('S')
	writeCString(&closeStatement, "stmt1")
	wire.Reset()
	session.handleClose(&wire, closeStatement.Bytes())
	if messageTypes := readTestBufferMessageTypes(t, &wire); !reflect.DeepEqual(messageTypes, []byte{'3'}) {
		t.Fatalf("close statement responses = %q", messageTypes)
	}
}

func bindPayload(portalName string, statementName string) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	return bind.Bytes()
}

func executePayload(portalName string, maxRows int32) []byte {
	var execute bytes.Buffer
	writeCString(&execute, portalName)
	binary.Write(&execute, binary.BigEndian, maxRows)
	return execute.Bytes()
}

func bindPayloadWithResultFormats(portalName string, statementName string, formats ...int16) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(len(formats)))
	for _, format := range formats {
		binary.Write(&bind, binary.BigEndian, format)
	}
	return bind.Bytes()
}

func bindPayloadWithTextParams(portalName string, statementName string, values ...string) []byte {
	var bind bytes.Buffer
	writeCString(&bind, portalName)
	writeCString(&bind, statementName)
	binary.Write(&bind, binary.BigEndian, int16(0))
	binary.Write(&bind, binary.BigEndian, int16(len(values)))
	for _, value := range values {
		binary.Write(&bind, binary.BigEndian, int32(len(value)))
		bind.WriteString(value)
	}
	binary.Write(&bind, binary.BigEndian, int16(0))
	return bind.Bytes()
}

func TestSQLCoreCreateInsertSelectWorkflow(t *testing.T) {
	server := NewServer(Config{})

	statements := []string{
		"create schema if not exists loop",
		"drop table if exists loop.events",
		`create table loop.events(
			id integer encode raw,
			payload varchar(64)
		)
		diststyle key
		distkey(id)
		sortkey(id)`,
		"insert into loop.events values (1, 'created')",
	}
	for _, statement := range statements {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload from loop.events where id = 1 limit 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 2 || result.fields[0].Name != "id" || result.fields[1].Name != "payload" {
		t.Fatalf("fields = %#v", result.fields)
	}
	if len(result.rows) != 1 || len(result.rows[0]) != 2 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows = %#v", result.rows)
	}
}

func TestSQLCoreSelectLiteralProjection(t *testing.T) {
	server := NewServer(Config{})

	result, err := server.executeSQL("select 1 as id, 'created' payload")
	if err != nil {
		t.Fatalf("select literals: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 2 {
		t.Fatalf("fields = %#v", result.fields)
	}
	if result.fields[0].Name != "id" || result.fields[0].TypeOID != pgTypeInt4OID {
		t.Fatalf("first field = %#v", result.fields[0])
	}
	if result.fields[1].Name != "payload" || result.fields[1].TypeOID != pgTypeVarcharOID {
		t.Fatalf("second field = %#v", result.fields[1])
	}
	if len(result.rows) != 1 || len(result.rows[0]) != 2 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows = %#v", result.rows)
	}
}

func TestSQLClientIntrospectionFunctionsAndShow(t *testing.T) {
	server := NewServer(Config{
		User:     "analyst",
		Password: "local-password",
	})

	for _, tc := range []struct {
		statement string
		field     string
		value     string
	}{
		{statement: "select current_user", field: "current_user", value: "analyst"},
		{statement: "select session_user()", field: "session_user", value: "analyst"},
		{statement: "select pg_backend_pid()", field: "pg_backend_pid", value: "1"},
		{statement: "show search_path", field: "search_path", value: "public"},
		{statement: "show transaction isolation level", field: "transaction isolation level", value: "read committed"},
		{statement: "show standard_conforming_strings", field: "standard_conforming_strings", value: "on"},
	} {
		result, err := server.executeSQL(tc.statement)
		if err != nil {
			t.Fatalf("execute %q: %v", tc.statement, err)
		}
		if len(result.fields) != 1 || result.fields[0].Name != tc.field {
			t.Fatalf("%q fields = %#v", tc.statement, result.fields)
		}
		if len(result.rows) != 1 || len(result.rows[0]) != 1 || result.rows[0][0] != tc.value {
			t.Fatalf("%q rows = %#v", tc.statement, result.rows)
		}
		if strings.Contains(result.rows[0][0], "local-password") {
			t.Fatalf("%q leaked password in result: %#v", tc.statement, result.rows)
		}
	}
}

func TestSQLCoreInsertColumnListDefaultsAndIdentity(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
		"insert into public.events(payload, status) values ('created', 'open')",
		"insert into public.events(status, payload, id) values ('closed', default, default)",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, status from public.events order by id")
	if err != nil {
		t.Fatalf("select inserted rows: %v", err)
	}
	if len(result.rows) != 2 {
		t.Fatalf("rows = %#v", result.rows)
	}
	if result.rows[0][0] != "1" || result.rows[0][1] != "created" || result.rows[0][2] != "open" {
		t.Fatalf("first row = %#v", result.rows[0])
	}
	if result.rows[1][0] != "2" || result.rows[1][1] != "new" || result.rows[1][2] != "closed" {
		t.Fatalf("second row = %#v", result.rows[1])
	}
}

func TestSQLCoreInsertMultipleValuesRows(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
		"insert into public.events(payload, status) values ('created', 'open'), ('queued', 'open'), (default, 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, status from public.events order by id")
	if err != nil {
		t.Fatalf("select inserted rows: %v", err)
	}
	want := [][]string{
		{"1", "created", "open"},
		{"2", "queued", "open"},
		{"3", "new", "closed"},
	}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestSQLCoreUpdateAndDeleteWorkflow(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'created', 'open')",
		"insert into public.events values (2, 'queued', 'open')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	updateResult, err := server.executeSQL("update public.events set payload = 'processed', status = 'closed' where id = 2")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updateResult.tag != "UPDATE 1" {
		t.Fatalf("update tag = %q", updateResult.tag)
	}
	updated, err := server.executeSQL("select id, payload, status from public.events where id = 2")
	if err != nil {
		t.Fatalf("select updated row: %v", err)
	}
	if len(updated.rows) != 1 || updated.rows[0][1] != "processed" || updated.rows[0][2] != "closed" {
		t.Fatalf("updated rows = %#v", updated.rows)
	}

	deleteResult, err := server.executeSQL("delete from public.events where status = 'open'")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleteResult.tag != "DELETE 1" {
		t.Fatalf("delete tag = %q", deleteResult.tag)
	}
	remaining, err := server.executeSQL("select id, payload from public.events order by id")
	if err != nil {
		t.Fatalf("select remaining rows: %v", err)
	}
	if len(remaining.rows) != 1 || remaining.rows[0][0] != "2" || remaining.rows[0][1] != "processed" {
		t.Fatalf("remaining rows = %#v", remaining.rows)
	}
}

func TestSQLCoreWhereComparisonOperators(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'alpha', 'open')",
		"insert into public.events values (2, 'bravo', 'open')",
		"insert into public.events values (10, 'charlie', 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from public.events where id >= 2 order by id")
	if err != nil {
		t.Fatalf("select comparison: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"2", "bravo"}, {"10", "charlie"}}) {
		t.Fatalf("selected rows = %#v", selected.rows)
	}

	updated, err := server.executeSQL("update public.events set status = 'archived' where payload <> 'alpha'")
	if err != nil {
		t.Fatalf("update comparison: %v", err)
	}
	if updated.tag != "UPDATE 2" {
		t.Fatalf("update tag = %q", updated.tag)
	}

	deleted, err := server.executeSQL("delete from public.events where id < 10")
	if err != nil {
		t.Fatalf("delete comparison: %v", err)
	}
	if deleted.tag != "DELETE 2" {
		t.Fatalf("delete tag = %q", deleted.tag)
	}

	remaining, err := server.executeSQL("select id, status from public.events")
	if err != nil {
		t.Fatalf("select remaining: %v", err)
	}
	if !reflect.DeepEqual(remaining.rows, [][]string{{"10", "archived"}}) {
		t.Fatalf("remaining rows = %#v", remaining.rows)
	}
}

func TestSQLCoreSelectCountFromTable(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'alpha', 'open')",
		"insert into public.events values (2, 'bravo', 'open')",
		"insert into public.events values (3, 'charlie', 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select count(*) as total from public.events where status = 'open'")
	if err != nil {
		t.Fatalf("select count: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 1 || result.fields[0].Name != "total" || result.fields[0].TypeOID != pgTypeInt4OID {
		t.Fatalf("fields = %#v", result.fields)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"2"}}) {
		t.Fatalf("rows = %#v", result.rows)
	}

	columnCount, err := server.executeSQL("select count(id) row_count from public.events")
	if err != nil {
		t.Fatalf("select count column: %v", err)
	}
	if len(columnCount.rows) != 1 || columnCount.rows[0][0] != "3" || columnCount.fields[0].Name != "row_count" {
		t.Fatalf("column count result = %#v", columnCount)
	}
}

func TestSQLCoreCreateSelectDropView(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create view analytics.open_events as select id, payload from analytics.events where status = 'open'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select payload from analytics.open_events where id = 1")
	if err != nil {
		t.Fatalf("select view: %v", err)
	}
	if len(selected.fields) != 1 || selected.fields[0].Name != "payload" {
		t.Fatalf("view fields = %#v", selected.fields)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"alpha"}}) {
		t.Fatalf("view rows = %#v", selected.rows)
	}

	tables, err := server.executeSQL("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_events'")
	if err != nil {
		t.Fatalf("information_schema view row: %v", err)
	}
	if !reflect.DeepEqual(tables.rows, [][]string{{"analytics", "open_events", "VIEW"}}) {
		t.Fatalf("view catalog rows = %#v", tables.rows)
	}

	pgClass, err := server.executeSQL("select relname, relkind from pg_catalog.pg_class where relname = 'open_events'")
	if err != nil {
		t.Fatalf("pg_class view row: %v", err)
	}
	if !reflect.DeepEqual(pgClass.rows, [][]string{{"open_events", "v"}}) {
		t.Fatalf("view pg_class rows = %#v", pgClass.rows)
	}

	if _, err := server.executeSQL("drop view if exists analytics.open_events"); err != nil {
		t.Fatalf("drop view: %v", err)
	}
	if _, err := server.executeSQL("select * from analytics.open_events"); err == nil {
		t.Fatal("select from dropped view succeeded")
	}
}

func TestSQLCoreCreateTableAsSelectWorkflow(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create table analytics.open_events diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from analytics.open_events")
	if err != nil {
		t.Fatalf("select CTAS table: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"1", "alpha"}}) {
		t.Fatalf("CTAS rows = %#v", selected.rows)
	}

	tableInfo, err := server.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info: %v", err)
	}
	if !resultContainsRow(tableInfo, "analytics", "open_events", "key", "id", "id", "1") {
		t.Fatalf("svv_table_info rows = %#v", tableInfo.rows)
	}
}

func TestSQLCoreCreateSelectDropMaterializedView(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create materialized view analytics.open_event_mv diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
		"insert into analytics.events values (3, 'charlie', 'open')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from analytics.open_event_mv order by id")
	if err != nil {
		t.Fatalf("select materialized view: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"1", "alpha"}}) {
		t.Fatalf("materialized view rows = %#v", selected.rows)
	}

	tables, err := server.executeSQL("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_event_mv'")
	if err != nil {
		t.Fatalf("information_schema materialized view row: %v", err)
	}
	if !reflect.DeepEqual(tables.rows, [][]string{{"analytics", "open_event_mv", "MATERIALIZED VIEW"}}) {
		t.Fatalf("materialized view catalog rows = %#v", tables.rows)
	}

	pgClass, err := server.executeSQL("select relname, relkind from pg_catalog.pg_class where relname = 'open_event_mv'")
	if err != nil {
		t.Fatalf("pg_class materialized view row: %v", err)
	}
	if !reflect.DeepEqual(pgClass.rows, [][]string{{"open_event_mv", "m"}}) {
		t.Fatalf("materialized view pg_class rows = %#v", pgClass.rows)
	}

	mvInfo, err := server.executeSQL("select schema, name, state, is_stale from svv_mv_info where name = 'open_event_mv'")
	if err != nil {
		t.Fatalf("svv_mv_info: %v", err)
	}
	if !reflect.DeepEqual(mvInfo.rows, [][]string{{"analytics", "open_event_mv", "1", "false"}}) {
		t.Fatalf("svv_mv_info rows = %#v", mvInfo.rows)
	}

	catalog := server.CatalogSnapshot()
	if len(catalog.Tables) != 2 {
		t.Fatalf("catalog tables = %#v", catalog.Tables)
	}
	var materializedView TableSnapshot
	for _, table := range catalog.Tables {
		if table.Name == "open_event_mv" {
			materializedView = table
			break
		}
	}
	if materializedView.Type != "MATERIALIZED_VIEW" || materializedView.RowCount != 1 || materializedView.DistKey != "id" {
		t.Fatalf("materialized view snapshot = %#v", materializedView)
	}

	if _, err := server.executeSQL("drop materialized view if exists analytics.open_event_mv"); err != nil {
		t.Fatalf("drop materialized view: %v", err)
	}
	if _, err := server.executeSQL("select * from analytics.open_event_mv"); err == nil {
		t.Fatal("select from dropped materialized view succeeded")
	}
}

func TestDataAPIExecuteStatementSupportsCreateTableAsSelect(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64))",
		"insert into public.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"create table public.created_events as select id, payload from public.events where id = 1"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}

	result, err := server.executeSQL("select id, payload from public.created_events")
	if err != nil {
		t.Fatalf("select Data API CTAS table: %v", err)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"1", "created"}}) {
		t.Fatalf("Data API CTAS rows = %#v", result.rows)
	}
}

func TestSQLCoreDropSchemaRemovesTablesAndPreservesPublic(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create schema if not exists scratch",
		"create table scratch.events(id integer, payload varchar(64))",
		"drop schema if exists scratch cascade",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	schemas, err := server.executeSQL("select * from information_schema.schemata")
	if err != nil {
		t.Fatalf("information_schema.schemata: %v", err)
	}
	if resultContainsRow(schemas, "scratch") {
		t.Fatalf("scratch schema should be removed: %#v", schemas.rows)
	}
	if !resultContainsRow(schemas, "public") {
		t.Fatalf("public schema should be preserved: %#v", schemas.rows)
	}

	if _, err := server.executeSQL("select * from scratch.events"); err == nil {
		t.Fatal("select from dropped schema table succeeded")
	}
}

func TestCatalogViewsExposeSchemasTablesColumnsAndRedshiftMetadata(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		`create table analytics.events(
			id integer encode raw,
			payload varchar(64) default 'unknown'
		)
		diststyle key
		distkey(id)
		sortkey(id)`,
		"insert into analytics.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	tables, err := server.executeSQL("select * from information_schema.tables")
	if err != nil {
		t.Fatalf("information_schema.tables: %v", err)
	}
	if !resultContainsRow(tables, "analytics", "events", "BASE TABLE") {
		t.Fatalf("tables rows = %#v", tables.rows)
	}

	columns, err := server.executeSQL("select * from information_schema.columns")
	if err != nil {
		t.Fatalf("information_schema.columns: %v", err)
	}
	if !resultContainsRow(columns, "events", "id", "1", "", "integer", "raw") {
		t.Fatalf("columns rows = %#v", columns.rows)
	}

	pgTables, err := server.executeSQL("select * from pg_catalog.pg_tables")
	if err != nil {
		t.Fatalf("pg_catalog.pg_tables: %v", err)
	}
	if !resultContainsRow(pgTables, "analytics", "events", "dev") {
		t.Fatalf("pg_tables rows = %#v", pgTables.rows)
	}

	tableInfo, err := server.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info: %v", err)
	}
	if !resultContainsRow(tableInfo, "analytics", "events", "key", "id", "id", "1") {
		t.Fatalf("svv_table_info rows = %#v", tableInfo.rows)
	}

	svvColumns, err := server.executeSQL("select * from svv_columns")
	if err != nil {
		t.Fatalf("svv_columns: %v", err)
	}
	if !resultContainsRow(svvColumns, "dev", "analytics", "events", "id", "1", "", "integer", "raw") {
		t.Fatalf("svv_columns rows = %#v", svvColumns.rows)
	}

	tableDef, err := server.executeSQL("select * from pg_table_def")
	if err != nil {
		t.Fatalf("pg_table_def: %v", err)
	}
	if !resultContainsRow(tableDef, "analytics", "events", "id", "integer", "raw", "true", "1", "false") {
		t.Fatalf("pg_table_def rows = %#v", tableDef.rows)
	}
}

func TestCatalogSelectSupportsProjectionFilterOrderLimitAndCount(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64))",
		"create table analytics.logs(id integer, message varchar(64))",
		"create table public.events(id integer)",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	tables, err := server.executeSQL("select t.table_name from information_schema.tables t where t.table_schema = 'analytics' order by t.table_name limit 1")
	if err != nil {
		t.Fatalf("filtered information_schema.tables: %v", err)
	}
	if len(tables.fields) != 1 || tables.fields[0].Name != "table_name" {
		t.Fatalf("projected table fields = %#v", tables.fields)
	}
	if len(tables.rows) != 1 || tables.rows[0][0] != "events" {
		t.Fatalf("projected table rows = %#v", tables.rows)
	}

	count, err := server.executeSQL("select count(t.table_name) as table_count from information_schema.tables t where t.table_schema = 'analytics'")
	if err != nil {
		t.Fatalf("catalog count: %v", err)
	}
	if len(count.fields) != 1 || count.fields[0].Name != "table_count" || len(count.rows) != 1 || count.rows[0][0] != "2" {
		t.Fatalf("catalog count result = fields %#v rows %#v", count.fields, count.rows)
	}
}

func TestCatalogViewsExposeDriverIntrospectionMetadataWithoutSecrets(t *testing.T) {
	server := NewServer(Config{
		Database: "warehouse",
		User:     "analyst",
		Password: "local-password",
	})

	databases, err := server.executeSQL("select * from pg_catalog.pg_database")
	if err != nil {
		t.Fatalf("pg_catalog.pg_database: %v", err)
	}
	if !resultContainsRow(databases, "warehouse", "10", "6", "false", "true") {
		t.Fatalf("pg_database rows = %#v", databases.rows)
	}

	users, err := server.executeSQL("select * from pg_catalog.pg_user")
	if err != nil {
		t.Fatalf("pg_catalog.pg_user: %v", err)
	}
	if !resultContainsRow(users, "analyst", "10", "true", "true", "********") {
		t.Fatalf("pg_user rows = %#v", users.rows)
	}
	for _, row := range users.rows {
		for _, value := range row {
			if strings.Contains(value, "local-password") {
				t.Fatalf("pg_user leaked password: %#v", users.rows)
			}
		}
	}

	types, err := server.executeSQL("select * from pg_catalog.pg_type")
	if err != nil {
		t.Fatalf("pg_catalog.pg_type: %v", err)
	}
	if !resultContainsRow(types, "23", "int4", "4", "N") || !resultContainsRow(types, "1043", "varchar", "-1", "S") {
		t.Fatalf("pg_type rows = %#v", types.rows)
	}
}

func TestCreateTableAcceptsColumnLevelRedshiftAttributes(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		`create table analytics.column_attrs(
			id integer identity(1,1) distkey sortkey encode raw,
			generated_id integer generated by default as identity,
			payload varchar(64) default 'unknown'
		)`,
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	catalog := server.CatalogSnapshot()
	if len(catalog.Tables) != 1 {
		t.Fatalf("tables = %#v", catalog.Tables)
	}
	if len(catalog.Schemas) != 2 {
		t.Fatalf("schemas = %#v", catalog.Schemas)
	}
	for _, schema := range catalog.Schemas {
		switch schema.Name {
		case "analytics":
			if schema.TableCount != 1 {
				t.Fatalf("analytics tableCount = %d, want 1", schema.TableCount)
			}
		case "public":
			if schema.TableCount != 0 {
				t.Fatalf("public tableCount = %d, want 0", schema.TableCount)
			}
		}
	}
	table := catalog.Tables[0]
	if table.ColumnCount != 3 {
		t.Fatalf("columnCount = %d, want 3", table.ColumnCount)
	}
	if table.DistStyle != "key" || table.DistKey != "id" || len(table.SortKeys) != 1 || table.SortKeys[0] != "id" {
		t.Fatalf("table attributes = %#v", table)
	}
	if !columnSnapshotHas(catalog.Columns, "id", "raw", "", true) {
		t.Fatalf("id column metadata = %#v", catalog.Columns)
	}
	if !columnSnapshotHas(catalog.Columns, "generated_id", "", "", true) {
		t.Fatalf("generated identity metadata = %#v", catalog.Columns)
	}
	if !columnSnapshotHas(catalog.Columns, "payload", "", "'unknown'", false) {
		t.Fatalf("default metadata = %#v", catalog.Columns)
	}
}

func TestCopyAndUnloadLocalCSVWorkflow(t *testing.T) {
	server := NewServer(Config{})
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "events.csv")
	if err := os.WriteFile(sourcePath, []byte("2,unload\n1,copy\n"), 0o600); err != nil {
		t.Fatalf("write source CSV: %v", err)
	}

	for _, statement := range []string{
		"drop table if exists public.copy_events",
		"create table public.copy_events(id integer, payload varchar(64))",
		"copy public.copy_events from '" + strings.ReplaceAll(sourcePath, "'", "''") + "' csv",
		"unload ('select * from public.copy_events order by id') to '" + strings.ReplaceAll(filepath.Join(tempDir, "exports", "events_"), "'", "''") + "' csv allowoverwrite",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	exportPath := filepath.Join(tempDir, "exports", "events_000")
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read export CSV: %v", err)
	}
	if string(data) != "1,copy\n2,unload\n" {
		t.Fatalf("export data = %q", string(data))
	}
}

func TestCopyLocalCSVOptions(t *testing.T) {
	server := NewServer(Config{})
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "events.psv")
	if err := os.WriteFile(sourcePath, []byte("id|payload|note\n1|created|NULL\n2|updated|kept\n"), 0o600); err != nil {
		t.Fatalf("write source CSV: %v", err)
	}

	for _, statement := range []string{
		"create table public.copy_options(id integer, payload varchar(64), note varchar(64))",
		"copy public.copy_options from '" + strings.ReplaceAll(sourcePath, "'", "''") + "' csv delimiter '|' ignoreheader 1 null as 'NULL'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, note from public.copy_options order by id")
	if err != nil {
		t.Fatalf("select copied rows: %v", err)
	}
	want := [][]string{{"1", "created", ""}, {"2", "updated", "kept"}}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestCopyAndUnloadLocalS3CSVWorkflow(t *testing.T) {
	ctx := context.Background()
	store := s3svc.NewFileBucketStore(t.TempDir())
	if _, _, err := store.CreateBucket(ctx, "demo-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := store.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "inputs/events.csv",
		Body:        strings.NewReader("2,unload\n1,copy\n"),
		ContentType: "text/csv",
	}); err != nil {
		t.Fatalf("put source object: %v", err)
	}
	server := NewServer(Config{ObjectStore: store})

	for _, statement := range []string{
		"create table public.copy_events(id integer, payload varchar(64))",
		"copy public.copy_events from 's3://demo-bucket/inputs/events.csv' iam_role default csv",
		"unload ('select * from public.copy_events order by id') to 's3://demo-bucket/exports/events_' iam_role default csv allowoverwrite",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	_, data, ok, err := store.GetObject(ctx, "demo-bucket", "exports/events_000")
	if err != nil {
		t.Fatalf("get export object: %v", err)
	}
	if !ok {
		t.Fatalf("export object was not written")
	}
	if string(data) != "1,copy\n2,unload\n" {
		t.Fatalf("export data = %q", string(data))
	}
}

func TestCopyFromLocalJSONAutoMapsObjectsByColumnName(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.json")
	if err := os.WriteFile(source, []byte("{\"payload\":\"created\",\"id\":1,\"active\":true}\n{\"id\":2,\"payload\":\"updated\",\"extra\":\"ignored\"}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.copy_events(id integer, payload varchar(64), active boolean)",
		"copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' json 'auto'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, active from public.copy_events order by id")
	if err != nil {
		t.Fatalf("select copied rows: %v", err)
	}
	want := [][]string{{"1", "created", "true"}, {"2", "updated", ""}}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestCopyFromJSONRejectsRowsExceedingConfiguredInputLimit(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.json")
	if err := os.WriteFile(source, []byte("{\"id\":1,\"payload\":\"this-row-is-too-long\"}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{MaxCopyInputBytes: 8})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err := server.executeSQL("copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' json 'auto'")
	if err == nil || !strings.Contains(err.Error(), "maxCopyInputBytes") {
		t.Fatalf("COPY error = %v", err)
	}
	result, err := server.executeSQL("select count(*) from public.copy_events")
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"0"}}) {
		t.Fatalf("rows after rejected COPY = %#v", result.rows)
	}
}

func TestCopyFromS3RequiresObjectStore(t *testing.T) {
	server := NewServer(Config{})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err := server.executeSQL("copy public.copy_events from 's3://demo-bucket/inputs/events.csv' csv")
	if err == nil || !strings.Contains(err.Error(), "local S3 service") {
		t.Fatalf("COPY error = %v", err)
	}
}

func TestCopyRejectsRowsExceedingConfiguredInputLimit(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(source, []byte("1,short\n2,this-row-is-too-long\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{MaxCopyInputBytes: 8})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err := server.executeSQL("copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' csv")
	if err == nil || !strings.Contains(err.Error(), "maxCopyInputBytes") {
		t.Fatalf("COPY error = %v", err)
	}
	result, err := server.executeSQL("select count(*) from public.copy_events")
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"0"}}) {
		t.Fatalf("rows after rejected COPY = %#v", result.rows)
	}
}

func TestStatePersistsCatalogRowsAndClusterMetadata(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateCluster&ClusterIdentifier=analytics&DBName=warehouse&MasterUsername=analyst"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateCluster status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	reloaded := NewServer(Config{
		SQLAddr:     "127.0.0.1:25439",
		StoragePath: storagePath,
		Database:    "dev",
		User:        "dev",
	})
	result, err := reloaded.executeSQL("select id, payload from loop.events where id = 1")
	if err != nil {
		t.Fatalf("select after reload: %v", err)
	}
	if len(result.rows) != 1 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows after reload = %#v", result.rows)
	}
	tableInfo, err := reloaded.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info after reload: %v", err)
	}
	if !resultContainsRow(tableInfo, "loop", "events", "key", "id", "id", "1") {
		t.Fatalf("table metadata after reload = %#v", tableInfo.rows)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Clusters) != 2 {
		t.Fatalf("clusters after reload = %#v", snapshot.Clusters)
	}
	for _, cluster := range snapshot.Clusters {
		if cluster.Endpoint.Port != 25439 {
			t.Fatalf("cluster endpoint was not normalized to current config: %#v", cluster)
		}
	}
}

func TestStatePersistsDataAPIStatementHistoryAndResults(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id",
		"ClientToken":"persist-token"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	reloaded := NewServer(Config{
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	retryRec := redshiftDataAPIRequest(t, reloaded, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id",
		"ClientToken":"persist-token"
	}`)
	if retryRec.Code != http.StatusOK {
		t.Fatalf("idempotent ExecuteStatement status = %d, body = %s", retryRec.Code, retryRec.Body.String())
	}
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("reloaded idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	listRec := redshiftDataAPIRequest(t, reloaded, "ListStatements", `{}`)
	if listRec.Code != http.StatusOK || !strings.Contains(listRec.Body.String(), `"Status":"FINISHED"`) || !strings.Contains(listRec.Body.String(), "select 1 as id") {
		t.Fatalf("ListStatements after reload = %d, body = %s", listRec.Code, listRec.Body.String())
	}

	resultRec := redshiftDataAPIRequest(t, reloaded, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult after reload status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	if !strings.Contains(resultRec.Body.String(), `"longValue":1`) {
		t.Fatalf("GetStatementResult after reload body = %s", resultRec.Body.String())
	}
}

func TestDataAPIExecuteDescribeGetResultAndIdempotency(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"ClientToken":"token-1"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if executeResponse.ID == "" {
		t.Fatal("ExecuteStatement returned empty Id")
	}

	retryRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"ClientToken":"token-1"
	}`)
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Status       string
		ResultRows   int64
		HasResultSet bool
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode describe response: %v", err)
	}
	if describeResponse.Status != "FINISHED" || describeResponse.ResultRows != 1 || !describeResponse.HasResultSet {
		t.Fatalf("describe response = %#v", describeResponse)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{"ColumnMetadata", "Records", "longValue"} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResult missing %q: %s", want, body)
		}
	}
}

func TestDataAPIResultFieldsPreserveZeroFalseAndDoubleTypes(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 0 as zero_value, false as active, 1.5 as score"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{`"longValue":0`, `"booleanValue":false`, `"doubleValue":1.5`, `"typeName":"int4"`, `"typeName":"bool"`, `"typeName":"float8"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResult missing %q: %s", want, body)
		}
	}
}

func TestDataAPIGetStatementResultV2ReturnsCSVRecords(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id, 'hello, csv' as payload",
		"ResultFormat":"CSV"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResultV2", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResultV2 status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{`"ResultFormat":"CSV"`, `"CSVRecords":"1,\"hello, csv\""`, `"TotalNumRows":1`} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResultV2 missing %q: %s", want, body)
		}
	}
}

func TestDataAPIGetStatementResultV2RequiresCSVResultFormat(t *testing.T) {
	server := NewServer(Config{})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{"Sql":"select 1"}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResultV2", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusBadRequest || !strings.Contains(resultRec.Body.String(), "ResultFormat CSV") {
		t.Fatalf("GetStatementResultV2 status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
}

func TestDataAPIExecuteStatementTracksSessionMetadata(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"SessionKeepAliveSeconds":60
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID        string `json:"Id"`
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if executeResponse.ID == "" || executeResponse.SessionID == "" {
		t.Fatalf("execute response = %#v", executeResponse)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode describe response: %v", err)
	}
	if describeResponse.SessionID != executeResponse.SessionID {
		t.Fatalf("describe SessionId = %q, want %q", describeResponse.SessionID, executeResponse.SessionID)
	}

	batchRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":["select 1"],
		"SessionId":"`+executeResponse.SessionID+`",
		"SessionKeepAliveSeconds":120
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var batchResponse struct {
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&batchResponse); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if batchResponse.SessionID != executeResponse.SessionID {
		t.Fatalf("batch SessionId = %q, want %q", batchResponse.SessionID, executeResponse.SessionID)
	}

	statements := server.StatementSnapshots()
	var found bool
	for _, statement := range statements {
		if statement.SessionID == executeResponse.SessionID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("statement snapshots missing SessionId %q: %#v", executeResponse.SessionID, statements)
	}
}

func TestDataAPIExecuteStatementRejectsInvalidSessionKeepAlive(t *testing.T) {
	server := NewServer(Config{})

	rec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Sql":"select 1",
		"SessionKeepAliveSeconds":-1
	}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "SessionKeepAliveSeconds") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestDataAPIRejectsOversizeStatementsWithoutPersistingSQL(t *testing.T) {
	server := NewServer(Config{MaxStatementBytes: 8})

	rec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Sql":"select 123456789"
	}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maxStatementBytes") {
		t.Fatalf("ExecuteStatement status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if statements := server.StatementSnapshots(); len(statements) != 0 {
		t.Fatalf("oversize ExecuteStatement persisted statement history: %#v", statements)
	}

	batchRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":["select 1","select 123456789"]
	}`)
	if batchRec.Code != http.StatusBadRequest || !strings.Contains(batchRec.Body.String(), "maxStatementBytes") {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	if statements := server.StatementSnapshots(); len(statements) != 0 {
		t.Fatalf("oversize BatchExecuteStatement persisted statement history: %#v", statements)
	}
}

func TestDataAPIGetStatementResultPaginatesRows(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create table public.page_events(id integer, payload varchar(64))",
		"insert into public.page_events values (1, 'one')",
		"insert into public.page_events values (2, 'two')",
		"insert into public.page_events values (3, 'three')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select id, payload from public.page_events order by id"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	firstRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","MaxResults":2}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first GetStatementResult status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstPage struct {
		NextToken    string
		Records      [][]dataAPIResultField
		TotalNumRows int
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.NextToken != "2" || firstPage.TotalNumRows != 3 || len(firstPage.Records) != 2 || firstPage.Records[0][1].StringValue == nil || *firstPage.Records[0][1].StringValue != "one" {
		t.Fatalf("first page = %#v", firstPage)
	}

	nextRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","MaxResults":2,"NextToken":"2"}`)
	if nextRec.Code != http.StatusOK {
		t.Fatalf("next GetStatementResult status = %d, body = %s", nextRec.Code, nextRec.Body.String())
	}
	var nextPage struct {
		NextToken string
		Records   [][]dataAPIResultField
	}
	if err := json.NewDecoder(nextRec.Body).Decode(&nextPage); err != nil {
		t.Fatalf("decode next page: %v", err)
	}
	if nextPage.NextToken != "" || len(nextPage.Records) != 1 || nextPage.Records[0][1].StringValue == nil || *nextPage.Records[0][1].StringValue != "three" {
		t.Fatalf("next page = %#v", nextPage)
	}

	invalidRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","NextToken":"not-a-token"}`)
	if invalidRec.Code != http.StatusBadRequest || !strings.Contains(invalidRec.Body.String(), "NextToken is invalid") {
		t.Fatalf("invalid NextToken status = %d, body = %s", invalidRec.Code, invalidRec.Body.String())
	}
}

func TestDataAPIBatchExecuteStatementRunsStatementsAndIsIdempotent(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sqls":[
			"create schema if not exists batch",
			"create table batch.events(id integer, payload varchar(64))",
			"insert into batch.events values (1, 'created')",
			"select id, payload from batch.events where id = 1"
		],
		"ClientToken":"batch-token-1"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if executeResponse.ID == "" {
		t.Fatal("BatchExecuteStatement returned empty Id")
	}

	retryRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sqls":["select 1"],
		"ClientToken":"batch-token-1"
	}`)
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"FINISHED"`) {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK || !strings.Contains(resultRec.Body.String(), "created") {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
}

func TestDataAPIBatchExecuteStatementRollsBackOnFailure(t *testing.T) {
	server := NewServer(Config{})

	executeRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":[
			"create schema if not exists batch_fail",
			"create table batch_fail.events(id integer)",
			"insert into batch_fail.events values (1, 'extra')"
		]
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"FAILED"`) {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	if _, err := server.executeSQL("select * from batch_fail.events"); err == nil {
		t.Fatal("batch failure left table behind")
	}
}

func TestDataAPICancelStatementAndListStatusFilter(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	createdAt := time.Now().UTC()
	server.statements["running"] = &statement{
		ID:                "running",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		DbUser:            "dev",
		QueryString:       "select 1",
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "STARTED",
	}
	server.statements["finished"] = &statement{
		ID:                "finished",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		DbUser:            "dev",
		QueryString:       "select 2",
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            queryResult{fields: []pgField{{Name: "?column?", TypeOID: pgTypeInt4OID, TypeSize: 4}}, rows: [][]string{{"2"}}, tag: "SELECT 1"},
		HasResultSet:      true,
	}

	cancelRec := redshiftDataAPIRequest(t, server, "CancelStatement", `{"Id":"running"}`)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("CancelStatement status = %d, body = %s", cancelRec.Code, cancelRec.Body.String())
	}
	if !strings.Contains(cancelRec.Body.String(), `"Status":true`) {
		t.Fatalf("CancelStatement body = %s", cancelRec.Body.String())
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"running"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"ABORTED"`) {
		t.Fatalf("DescribeStatement after cancel status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}

	finishedCancelRec := redshiftDataAPIRequest(t, server, "CancelStatement", `{"Id":"finished"}`)
	if finishedCancelRec.Code != http.StatusOK || !strings.Contains(finishedCancelRec.Body.String(), `"Status":false`) {
		t.Fatalf("CancelStatement finished body = %d %s", finishedCancelRec.Code, finishedCancelRec.Body.String())
	}

	listRec := redshiftDataAPIRequest(t, server, "ListStatements", `{"Status":"ABORTED"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListStatements status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	body := listRec.Body.String()
	if !strings.Contains(body, `"Id":"running"`) || strings.Contains(body, `"Id":"finished"`) {
		t.Fatalf("ListStatements filter body = %s", body)
	}
}

func TestDataAPIMetadataListsUseCatalog(t *testing.T) {
	server := NewServer(Config{Database: "dev"})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer, payload varchar(64))",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	databasesRec := redshiftDataAPIRequest(t, server, "ListDatabases", `{"Database":"dev"}`)
	if databasesRec.Code != http.StatusOK || !strings.Contains(databasesRec.Body.String(), `"dev"`) {
		t.Fatalf("ListDatabases status = %d, body = %s", databasesRec.Code, databasesRec.Body.String())
	}

	schemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev"}`)
	if schemasRec.Code != http.StatusOK || !strings.Contains(schemasRec.Body.String(), `"loop"`) {
		t.Fatalf("ListSchemas status = %d, body = %s", schemasRec.Code, schemasRec.Body.String())
	}

	tablesRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","Schema":"loop"}`)
	if tablesRec.Code != http.StatusOK || !strings.Contains(tablesRec.Body.String(), `"events"`) {
		t.Fatalf("ListTables status = %d, body = %s", tablesRec.Code, tablesRec.Body.String())
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeTable", `{"Database":"dev","Schema":"loop","Table":"events"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"ColumnList"`) {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
}

func TestDataAPIMetadataListsSupportPatternFiltersAndPagination(t *testing.T) {
	server := NewServer(Config{Database: "dev"})
	for _, statement := range []string{
		"create schema if not exists alpha",
		"create schema if not exists loop",
		"create table alpha.metrics(id integer)",
		"create table loop.events(id integer, payload varchar(64))",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	firstSchemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev","SchemaPattern":"%","MaxResults":1}`)
	if firstSchemasRec.Code != http.StatusOK || !strings.Contains(firstSchemasRec.Body.String(), `"NextToken":"1"`) {
		t.Fatalf("first ListSchemas status = %d, body = %s", firstSchemasRec.Code, firstSchemasRec.Body.String())
	}
	nextSchemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev","SchemaPattern":"%","MaxResults":1,"NextToken":"1"}`)
	if nextSchemasRec.Code != http.StatusOK || !strings.Contains(nextSchemasRec.Body.String(), `"loop"`) {
		t.Fatalf("next ListSchemas status = %d, body = %s", nextSchemasRec.Code, nextSchemasRec.Body.String())
	}

	tablesRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","SchemaPattern":"lo%","TablePattern":"ev%"}`)
	body := tablesRec.Body.String()
	if tablesRec.Code != http.StatusOK || !strings.Contains(body, `"events"`) || strings.Contains(body, `"metrics"`) {
		t.Fatalf("ListTables filtered status = %d, body = %s", tablesRec.Code, body)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeTable", `{"Database":"dev","Schema":"loop","Table":"events","MaxResults":1}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"NextToken":"1"`) || !strings.Contains(describeRec.Body.String(), `"TableName":"events"`) {
		t.Fatalf("DescribeTable paged status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}

	invalidRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","NextToken":"not-a-token"}`)
	if invalidRec.Code != http.StatusBadRequest || !strings.Contains(invalidRec.Body.String(), "NextToken is invalid") {
		t.Fatalf("invalid NextToken status = %d, body = %s", invalidRec.Code, invalidRec.Body.String())
	}
}

func TestCatalogAndStatementSnapshotsExposeDashboardMetadata(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
	redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"copy loop.events from 's3://bucket/events.csv' iam_role 'secret-role' csv"
	}`)

	catalog := server.CatalogSnapshot()
	if catalog.Database != "dev" || len(catalog.Schemas) < 2 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if len(catalog.Tables) != 1 || catalog.Tables[0].Schema != "loop" || catalog.Tables[0].Name != "events" || catalog.Tables[0].RowCount != 1 {
		t.Fatalf("tables = %#v", catalog.Tables)
	}
	if catalog.Tables[0].DistStyle != "key" || catalog.Tables[0].DistKey != "id" || len(catalog.Tables[0].SortKeys) != 1 || catalog.Tables[0].SortKeys[0] != "id" {
		t.Fatalf("table Redshift metadata = %#v", catalog.Tables[0])
	}
	if len(catalog.Columns) != 2 || catalog.Columns[0].Name != "id" || catalog.Columns[0].Encoding != "raw" {
		t.Fatalf("columns = %#v", catalog.Columns)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statements = %#v", statements)
	}
	if !statements[0].QueryRedacted || statements[0].QueryPreview != "[redacted]" {
		t.Fatalf("statement preview should be redacted: %#v", statements[0])
	}
	if statements[0].ResultRows != 0 || statements[0].RedshiftQueryID == 0 {
		t.Fatalf("statement metadata = %#v", statements[0])
	}
}

func TestSimpleQueryRecordsRedactedQueryHistory(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	var wire bytes.Buffer

	server.handleSimpleQuery(&wire, "copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv;")

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statements = %#v", statements)
	}
	if statements[0].Status != "FAILED" || !statements[0].QueryRedacted || statements[0].QueryPreview != "[redacted]" {
		t.Fatalf("statement history = %#v", statements[0])
	}

	stlQuery, err := server.executeSQL("select * from stl_query")
	if err != nil {
		t.Fatalf("stl_query: %v", err)
	}
	if !resultContainsRow(stlQuery, "[redacted]", "FAILED") {
		t.Fatalf("stl_query should expose redacted preview only: %#v", stlQuery.rows)
	}
	for _, row := range stlQuery.rows {
		for _, value := range row {
			if strings.Contains(value, "secret-role") || strings.Contains(value, "s3://bucket/events.csv") {
				t.Fatalf("stl_query leaked sensitive SQL text: %#v", stlQuery.rows)
			}
		}
	}
}

func TestDataAPIStatementMetadataRedactsSensitiveSQL(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	for operation, payload := range map[string]string{
		"DescribeStatement": `{"Id":"` + executeResponse.ID + `"}`,
		"ListStatements":    `{}`,
	} {
		rec := redshiftDataAPIRequest(t, server, operation, payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s", operation, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if !strings.Contains(body, `"QueryString":"[redacted]"`) {
			t.Fatalf("%s did not redact QueryString: %s", operation, body)
		}
		if strings.Contains(body, "secret-role") || strings.Contains(body, "s3://bucket/events.csv") {
			t.Fatalf("%s leaked sensitive SQL: %s", operation, body)
		}
	}
}

func TestPgWireRunsMultipleSQLCoreStatements(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{"user": "dev", "database": "dev"}); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	readTestMessage(t, client)
	if err := writeTestTypedMessage(client, 'p', []byte("dev\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}
	waitForReady(t, client)

	sql := strings.Join([]string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id)",
		"insert into loop.events values (1, 'created')",
		"select id, payload from loop.events where id = 1",
	}, ";\n") + ";\x00"
	if err := writeTestTypedMessage(client, 'Q', []byte(sql)); err != nil {
		t.Fatalf("write query: %v", err)
	}

	var sawCreatedPayload bool
	for {
		messageType, payload := readTestMessage(t, client)
		switch messageType {
		case 'D':
			if bytes.Contains(payload, []byte("created")) {
				sawCreatedPayload = true
			}
		case 'Z':
			if !sawCreatedPayload {
				t.Fatal("ReadyForQuery arrived before selected row")
			}
			writeTestTypedMessage(client, 'X', nil)
			return
		}
	}
}

func TestPgWireRejectsBadPasswordWithoutLeakingValue(t *testing.T) {
	server := NewServer(Config{
		AuthMode: "strict",
		Password: "dev",
	})
	client, serverConn := net.Pipe()
	defer client.Close()
	go server.handleSQLConn(serverConn)

	if err := writeTestStartup(client, map[string]string{"user": "dev"}); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	readTestMessage(t, client)
	if err := writeTestTypedMessage(client, 'p', []byte("wrong-secret\x00")); err != nil {
		t.Fatalf("write password: %v", err)
	}

	messageType, payload := readTestMessage(t, client)
	if messageType != 'E' {
		t.Fatalf("message type = %q, want ErrorResponse", messageType)
	}
	if strings.Contains(string(payload), "wrong-secret") {
		t.Fatalf("error leaked password: %q", string(payload))
	}
}

func TestDashboardSQLRejectsOversizeStatementWithoutStoringSQL(t *testing.T) {
	server := NewServer(Config{MaxStatementBytes: 8})

	result, err := server.ExecuteDashboardSQL("select 123456789", 10)
	if err == nil || !strings.Contains(err.Error(), "maxStatementBytes") {
		t.Fatalf("ExecuteDashboardSQL error = %v", err)
	}
	if result.Statement.Status != "FAILED" || result.Statement.QueryPreview != "[statement exceeds maxStatementBytes]" {
		t.Fatalf("dashboard result statement = %#v", result.Statement)
	}
	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history = %#v", statements)
	}
	if strings.Contains(statements[0].QueryPreview, "123456789") {
		t.Fatalf("oversize statement leaked into preview: %#v", statements[0])
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
