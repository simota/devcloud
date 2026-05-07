package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBucketReplicationConfigurationReplicatesMatchingObjectWrites(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/source-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create source status = %d; body=%s", create.Code, create.Body.String())
	}
	if create := performRequest(routes, http.MethodPut, "/replica-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create replica status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<ReplicationConfiguration>
<Role>arn:aws:iam::000000000000:role/devcloud</Role>
<Rule><ID>docs-only</ID><Status>Enabled</Status><Filter><Prefix>docs/</Prefix></Filter><Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket><StorageClass>STANDARD</StorageClass></Destination></Rule>
</ReplicationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/source-bucket?replication", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put replication status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/source-bucket?replication", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get replication status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed ReplicationConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode replication config: %v", err)
	}
	if len(parsed.Rules) != 1 || parsed.Rules[0].ID != "docs-only" || parsed.Rules[0].Destination.Bucket != "arn:aws:s3:::replica-bucket" {
		t.Fatalf("replication config = %#v", parsed)
	}

	putObject := performRequest(routes, http.MethodPut, "/source-bucket/docs/readme.txt", strings.NewReader("replicated body"))
	if putObject.Code != http.StatusOK {
		t.Fatalf("put matching object status = %d; body=%s", putObject.Code, putObject.Body.String())
	}
	replica := performRequest(routes, http.MethodGet, "/replica-bucket/docs/readme.txt", nil)
	if replica.Code != http.StatusOK || replica.Body.String() != "replicated body" {
		t.Fatalf("replica object status = %d body=%q", replica.Code, replica.Body.String())
	}

	ignored := performRequest(routes, http.MethodPut, "/source-bucket/logs/readme.txt", strings.NewReader("ignored body"))
	if ignored.Code != http.StatusOK {
		t.Fatalf("put ignored object status = %d; body=%s", ignored.Code, ignored.Body.String())
	}
	missingReplica := performRequest(routes, http.MethodGet, "/replica-bucket/logs/readme.txt", nil)
	if missingReplica.Code != http.StatusNotFound {
		t.Fatalf("ignored replica status = %d, want %d; body=%s", missingReplica.Code, http.StatusNotFound, missingReplica.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/source-bucket/docs/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/source-bucket/docs/readme.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d; body=%s", copyRec.Code, copyRec.Body.String())
	}
	replicaCopy := performRequest(routes, http.MethodGet, "/replica-bucket/docs/copy.txt", nil)
	if replicaCopy.Code != http.StatusOK || replicaCopy.Body.String() != "replicated body" {
		t.Fatalf("replica copy status = %d body=%q", replicaCopy.Code, replicaCopy.Body.String())
	}

	deleteConfig := performRequest(routes, http.MethodDelete, "/source-bucket?replication", nil)
	if deleteConfig.Code != http.StatusNoContent {
		t.Fatalf("delete replication status = %d, want %d; body=%s", deleteConfig.Code, http.StatusNoContent, deleteConfig.Body.String())
	}
	missingConfig := performRequest(routes, http.MethodGet, "/source-bucket?replication", nil)
	if missingConfig.Code != http.StatusNotFound {
		t.Fatalf("missing replication status = %d, want %d; body=%s", missingConfig.Code, http.StatusNotFound, missingConfig.Body.String())
	}
}

func TestBucketReplicationRejectsInvalidConfiguration(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/source-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	config := `<ReplicationConfiguration><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::Invalid_Bucket</Bucket></Destination></Rule></ReplicationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/source-bucket?replication", strings.NewReader(config))
	if put.Code != http.StatusBadRequest {
		t.Fatalf("invalid replication status = %d, want %d; body=%s", put.Code, http.StatusBadRequest, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode invalid replication error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("invalid replication code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestBucketReplicationReplicatesEnabledDeleteMarkers(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, bucket := range []string{"source-bucket", "replica-bucket"} {
		if create := performRequest(routes, http.MethodPut, "/"+bucket, nil); create.Code != http.StatusOK {
			t.Fatalf("create %s status = %d; body=%s", bucket, create.Code, create.Body.String())
		}
		if versioning := performRequest(routes, http.MethodPut, "/"+bucket+"?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
			t.Fatalf("enable versioning on %s status = %d; body=%s", bucket, versioning.Code, versioning.Body.String())
		}
	}

	config := `<ReplicationConfiguration>
<Role>arn:aws:iam::000000000000:role/devcloud</Role>
<Rule><ID>docs-delete-markers</ID><Status>Enabled</Status><Filter><Prefix>docs/</Prefix></Filter><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket></Destination></Rule>
</ReplicationConfiguration>`
	if putReplication := performRequest(routes, http.MethodPut, "/source-bucket?replication", strings.NewReader(config)); putReplication.Code != http.StatusOK {
		t.Fatalf("put replication status = %d; body=%s", putReplication.Code, putReplication.Body.String())
	}

	putObject := performRequest(routes, http.MethodPut, "/source-bucket/docs/readme.txt", strings.NewReader("replicated body"))
	if putObject.Code != http.StatusOK {
		t.Fatalf("put object status = %d; body=%s", putObject.Code, putObject.Body.String())
	}
	replicaVersionID := performRequest(routes, http.MethodGet, "/replica-bucket/docs/readme.txt", nil).Header().Get("x-amz-version-id")
	if replicaVersionID == "" {
		t.Fatal("replica object missing version id before delete marker replication")
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/source-bucket/docs/readme.txt", nil)
	if deleteObject.Code != http.StatusNoContent || deleteObject.Header().Get("x-amz-delete-marker") != "true" {
		t.Fatalf("delete object status=%d marker=%q body=%s", deleteObject.Code, deleteObject.Header().Get("x-amz-delete-marker"), deleteObject.Body.String())
	}
	replicaLatest := performRequest(routes, http.MethodGet, "/replica-bucket/docs/readme.txt", nil)
	if replicaLatest.Code != http.StatusNotFound {
		t.Fatalf("replica latest after delete marker status = %d, want %d; body=%s", replicaLatest.Code, http.StatusNotFound, replicaLatest.Body.String())
	}
	replicaOriginal := performRequest(routes, http.MethodGet, "/replica-bucket/docs/readme.txt?versionId="+replicaVersionID, nil)
	if replicaOriginal.Code != http.StatusOK || replicaOriginal.Body.String() != "replicated body" {
		t.Fatalf("replica original status=%d body=%q", replicaOriginal.Code, replicaOriginal.Body.String())
	}
}

