package redshift

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

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
