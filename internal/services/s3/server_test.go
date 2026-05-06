package s3

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBucketLifecycleAndListBuckets(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPut, "/demo-bucket", nil)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d", head.Code, http.StatusOK)
	}

	list := performRequest(routes, http.MethodGet, "/", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	if !strings.Contains(list.Body.String(), "<Name>demo-bucket</Name>") {
		t.Fatalf("list body missing bucket: %s", list.Body.String())
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/demo-bucket", nil)
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	missingHead := performRequest(routes, http.MethodHead, "/demo-bucket", nil)
	if missingHead.Code != http.StatusNotFound {
		t.Fatalf("missing head status = %d, want %d", missingHead.Code, http.StatusNotFound)
	}
}

func TestGetBucketLocationReturnsConfiguredRegion(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Region: "ap-northeast-1"}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	location := performRequest(routes, http.MethodGet, "/demo-bucket?location", nil)
	if location.Code != http.StatusOK {
		t.Fatalf("location status = %d, want %d; body=%s", location.Code, http.StatusOK, location.Body.String())
	}
	var parsed locationConstraint
	if err := xml.NewDecoder(location.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode location response: %v", err)
	}
	if parsed.Value != "ap-northeast-1" {
		t.Fatalf("location constraint = %q, want ap-northeast-1", parsed.Value)
	}

	missing := performRequest(routes, http.MethodGet, "/missing-bucket?location", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing bucket location status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestGetBucketLocationReturnsEmptyConstraintForUSEast1(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Region: "us-east-1"}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	location := performRequest(routes, http.MethodGet, "/demo-bucket?location", nil)
	if location.Code != http.StatusOK {
		t.Fatalf("location status = %d, want %d; body=%s", location.Code, http.StatusOK, location.Body.String())
	}
	var parsed locationConstraint
	if err := xml.NewDecoder(location.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode location response: %v", err)
	}
	if parsed.Value != "" {
		t.Fatalf("location constraint = %q, want empty us-east-1 constraint", parsed.Value)
	}
}

func TestObjectCRUDListRangeAndCopy(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPut, "/demo-bucket", nil)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("hello from devcloud s3\n"))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("x-amz-meta-source", "unit-test")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("ETag"); got == "" {
		t.Fatal("put response missing ETag")
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/readme.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("head Content-Type = %q, want text/plain", got)
	}
	if got := head.Header().Get("x-amz-meta-source"); got != "unit-test" {
		t.Fatalf("head metadata = %q, want unit-test", got)
	}
	if got := head.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("head Accept-Ranges = %q, want bytes", got)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != "hello from devcloud s3\n" {
		t.Fatalf("get body = %q", got)
	}

	rangeReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	rangeReq.Header.Set("Range", "bytes=0-4")
	rangeRec := httptest.NewRecorder()
	routes.ServeHTTP(rangeRec, rangeReq)
	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want %d; body=%s", rangeRec.Code, http.StatusPartialContent, rangeRec.Body.String())
	}
	if got := rangeRec.Body.String(); got != "hello" {
		t.Fatalf("range body = %q, want hello", got)
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list objects status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), "<Key>docs/readme.txt</Key>") {
		t.Fatalf("list objects body missing key: %s", list.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/readme.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	copyGet := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copy.txt", nil)
	if copyGet.Body.String() != "hello from devcloud s3\n" {
		t.Fatalf("copy body = %q", copyGet.Body.String())
	}

	deleteCopy := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/copy.txt", nil)
	if deleteCopy.Code != http.StatusNoContent {
		t.Fatalf("delete object status = %d, want %d", deleteCopy.Code, http.StatusNoContent)
	}
	missingCopy := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copy.txt", nil)
	if missingCopy.Code != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want %d", missingCopy.Code, http.StatusNotFound)
	}
}

func TestVirtualHostStyleRoutesUseHostBucket(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := httptest.NewRequest(http.MethodPut, "/", nil)
	create.Host = "demo-bucket.localhost"
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("virtual create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := httptest.NewRequest(http.MethodPut, "/docs/readme.txt", strings.NewReader("hello virtual host\n"))
	put.Host = "demo-bucket.localhost"
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("virtual put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/docs/readme.txt", nil)
	get.Host = "demo-bucket.localhost:4566"
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("virtual get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello virtual host\n" {
		t.Fatalf("virtual get body = %q", got)
	}

	list := httptest.NewRequest(http.MethodGet, "/?list-type=2&prefix=docs/", nil)
	list.Host = "demo-bucket.localhost"
	listRec := httptest.NewRecorder()
	routes.ServeHTTP(listRec, list)
	if listRec.Code != http.StatusOK {
		t.Fatalf("virtual list status = %d, want %d; body=%s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "<Key>docs/readme.txt</Key>") {
		t.Fatalf("virtual list body missing key: %s", listRec.Body.String())
	}
}

func TestBucketInventoryConfigurationMetadataEndpoints(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	body := `<InventoryConfiguration>
<Id>daily-current</Id>
<IsEnabled>true</IsEnabled>
<Destination><S3BucketDestination><Bucket>arn:aws:s3:::reports-bucket</Bucket><Format>CSV</Format><Prefix>inventory/</Prefix></S3BucketDestination></Destination>
<Schedule><Frequency>Daily</Frequency></Schedule>
<IncludedObjectVersions>Current</IncludedObjectVersions>
<OptionalFields><Field>Size</Field><Field>LastModifiedDate</Field></OptionalFields>
</InventoryConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?inventory&id=daily-current", strings.NewReader(body))
	if put.Code != http.StatusOK {
		t.Fatalf("put inventory status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?inventory&id=daily-current", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get inventory status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var config InventoryConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&config); err != nil {
		t.Fatalf("decode inventory config: %v", err)
	}
	if !config.IsEnabled || config.ID != "daily-current" || config.Schedule.Frequency != "Daily" || config.Destination.S3BucketDestination.Format != "CSV" {
		t.Fatalf("inventory config = %#v", config)
	}
	if len(config.OptionalFields) != 2 || config.OptionalFields[0] != "Size" {
		t.Fatalf("inventory optional fields = %#v", config.OptionalFields)
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?inventory", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list inventory status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed listInventoryConfigurationsResult
	if err := xml.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode inventory list: %v", err)
	}
	if listed.IsTruncated || len(listed.InventoryConfigurations) != 1 || listed.InventoryConfigurations[0].ID != "daily-current" {
		t.Fatalf("inventory list = %#v", listed)
	}

	deleteConfig := performRequest(routes, http.MethodDelete, "/demo-bucket?inventory&id=daily-current", nil)
	if deleteConfig.Code != http.StatusNoContent {
		t.Fatalf("delete inventory status = %d, want %d; body=%s", deleteConfig.Code, http.StatusNoContent, deleteConfig.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?inventory&id=daily-current", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing inventory status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}

func TestBucketInventoryConfigurationGeneratesLocalCSVReport(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("put versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}
	first := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("first"))
	if first.Code != http.StatusOK || first.Header().Get("x-amz-version-id") == "" {
		t.Fatalf("put first status = %d version=%q body=%s", first.Code, first.Header().Get("x-amz-version-id"), first.Body.String())
	}
	second := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("second"))
	if second.Code != http.StatusOK || second.Header().Get("x-amz-version-id") == "" {
		t.Fatalf("put second status = %d version=%q body=%s", second.Code, second.Header().Get("x-amz-version-id"), second.Body.String())
	}
	sseReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/logs/audit.txt", strings.NewReader("audit"))
	sseReq.Header.Set("x-amz-server-side-encryption", "AES256")
	sseRec := httptest.NewRecorder()
	routes.ServeHTTP(sseRec, sseReq)
	if sseRec.Code != http.StatusOK {
		t.Fatalf("put sse object status = %d; body=%s", sseRec.Code, sseRec.Body.String())
	}

	body := `<InventoryConfiguration>
<Id>all-versions</Id>
<IsEnabled>true</IsEnabled>
<IncludedObjectVersions>All</IncludedObjectVersions>
<OptionalFields><Field>EncryptionStatus</Field></OptionalFields>
</InventoryConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?inventory&id=all-versions", strings.NewReader(body))
	if put.Code != http.StatusOK {
		t.Fatalf("put inventory status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	manifestData, err := os.ReadFile(store.inventoryReportManifestPath("demo-bucket", "all-versions"))
	if err != nil {
		t.Fatalf("read inventory manifest: %v", err)
	}
	var manifest InventoryReportManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("decode inventory manifest: %v", err)
	}
	if manifest.ConfigurationID != "all-versions" || manifest.SourceBucket != "demo-bucket" || manifest.IncludedVersions != "All" || manifest.ObjectCount != 3 {
		t.Fatalf("inventory manifest = %#v", manifest)
	}
	if !containsString(manifest.Fields, "VersionId") || !containsString(manifest.Fields, "IsLatest") || !containsString(manifest.Fields, "EncryptionStatus") {
		t.Fatalf("inventory fields = %#v", manifest.Fields)
	}

	reportData, err := os.ReadFile(store.inventoryReportCSVPath("demo-bucket", "all-versions"))
	if err != nil {
		t.Fatalf("read inventory csv: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(string(reportData))).ReadAll()
	if err != nil {
		t.Fatalf("decode inventory csv: %v; raw=%s", err, string(reportData))
	}
	if len(records) != 4 {
		t.Fatalf("inventory csv records = %d, want 4: %#v", len(records), records)
	}
	header := records[0]
	keyIndex := indexOfString(header, "Key")
	latestIndex := indexOfString(header, "IsLatest")
	encryptionIndex := indexOfString(header, "EncryptionStatus")
	if keyIndex < 0 || latestIndex < 0 || encryptionIndex < 0 {
		t.Fatalf("inventory csv header = %#v", header)
	}
	readmeRows := 0
	latestRows := 0
	encryptedRows := 0
	for _, row := range records[1:] {
		if row[keyIndex] == "docs/readme.txt" {
			readmeRows++
			if row[latestIndex] == "true" {
				latestRows++
			}
		}
		if row[keyIndex] == "logs/audit.txt" && row[encryptionIndex] == "AES256" {
			encryptedRows++
		}
	}
	if readmeRows != 2 || latestRows != 1 || encryptedRows != 1 {
		t.Fatalf("inventory csv rows = %#v", records)
	}

	disable := performRequest(routes, http.MethodPut, "/demo-bucket?inventory&id=all-versions", strings.NewReader(`<InventoryConfiguration><Id>all-versions</Id><IsEnabled>false</IsEnabled></InventoryConfiguration>`))
	if disable.Code != http.StatusOK {
		t.Fatalf("disable inventory status = %d; body=%s", disable.Code, disable.Body.String())
	}
	if _, err := os.Stat(store.inventoryReportCSVPath("demo-bucket", "all-versions")); !os.IsNotExist(err) {
		t.Fatalf("disabled inventory report still exists or stat failed: %v", err)
	}
}

func TestBucketAnalyticsConfigurationMetadataEndpoints(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	body := `<AnalyticsConfiguration>
<Id>storage-class</Id>
<Filter><Prefix>logs/</Prefix></Filter>
<StorageClassAnalysis><DataExport><OutputSchemaVersion>V_1</OutputSchemaVersion><Destination><S3BucketDestination><Format>CSV</Format><Bucket>arn:aws:s3:::reports-bucket</Bucket><Prefix>analytics/</Prefix></S3BucketDestination></Destination></DataExport></StorageClassAnalysis>
</AnalyticsConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?analytics&id=storage-class", strings.NewReader(body))
	if put.Code != http.StatusOK {
		t.Fatalf("put analytics status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?analytics&id=storage-class", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get analytics status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var config AnalyticsConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&config); err != nil {
		t.Fatalf("decode analytics config: %v", err)
	}
	if config.ID != "storage-class" || config.Filter.Prefix != "logs/" || config.StorageClassAnalysis.DataExport.Destination.S3BucketDestination.Format != "CSV" {
		t.Fatalf("analytics config = %#v", config)
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?analytics", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list analytics status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed listAnalyticsConfigurationsResult
	if err := xml.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode analytics list: %v", err)
	}
	if listed.IsTruncated || len(listed.AnalyticsConfigurations) != 1 || listed.AnalyticsConfigurations[0].ID != "storage-class" {
		t.Fatalf("analytics list = %#v", listed)
	}

	deleteConfig := performRequest(routes, http.MethodDelete, "/demo-bucket?analytics&id=storage-class", nil)
	if deleteConfig.Code != http.StatusNoContent {
		t.Fatalf("delete analytics status = %d, want %d; body=%s", deleteConfig.Code, http.StatusNoContent, deleteConfig.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?analytics&id=storage-class", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing analytics status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}

func TestBucketInventoryAndAnalyticsRejectInvalidConfigurationIDs(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	inventory := performRequest(routes, http.MethodPut, "/demo-bucket?inventory", strings.NewReader(`<InventoryConfiguration><Id>daily</Id></InventoryConfiguration>`))
	if inventory.Code != http.StatusBadRequest {
		t.Fatalf("inventory without id status = %d, want %d; body=%s", inventory.Code, http.StatusBadRequest, inventory.Body.String())
	}
	analytics := performRequest(routes, http.MethodPut, "/demo-bucket?analytics&id=query-id", strings.NewReader(`<AnalyticsConfiguration><Id>body-id</Id></AnalyticsConfiguration>`))
	if analytics.Code != http.StatusBadRequest {
		t.Fatalf("analytics mismatched id status = %d, want %d; body=%s", analytics.Code, http.StatusBadRequest, analytics.Body.String())
	}
}

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

func TestSelectObjectContentSupportsNarrowCSVAndJSONQueries(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.csv", strings.NewReader("name,age\nalice,31\nbob,28\n")); put.Code != http.StatusOK {
		t.Fatalf("put csv status = %d; body=%s", put.Code, put.Body.String())
	}
	csvRequest := `<SelectObjectContentRequest>
<Expression>SELECT * FROM S3Object</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV /></OutputSerialization>
</SelectObjectContentRequest>`
	csvSelect := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.csv?select&select-type=2", strings.NewReader(csvRequest))
	if csvSelect.Code != http.StatusOK {
		t.Fatalf("csv select status = %d, want %d; body=%s", csvSelect.Code, http.StatusOK, csvSelect.Body.String())
	}
	if got := csvSelect.Header().Get("Content-Type"); got != "application/vnd.amazon.eventstream" {
		t.Fatalf("csv select content type = %q", got)
	}
	records := eventStreamRecords(t, csvSelect.Body.Bytes())
	if got := string(records); got != "alice,31\nbob,28\n" {
		t.Fatalf("csv select records = %q", got)
	}

	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.jsonl", strings.NewReader(`{"name":"alice","age":31}`+"\n"+`{"name":"bob","age":28}`+"\n")); put.Code != http.StatusOK {
		t.Fatalf("put json status = %d; body=%s", put.Code, put.Body.String())
	}
	jsonRequest := `<SelectObjectContentRequest>
<Expression>SELECT * FROM S3Object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
<OutputSerialization><JSON /></OutputSerialization>
</SelectObjectContentRequest>`
	jsonSelect := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.jsonl?select&select-type=2", strings.NewReader(jsonRequest))
	if jsonSelect.Code != http.StatusOK {
		t.Fatalf("json select status = %d, want %d; body=%s", jsonSelect.Code, http.StatusOK, jsonSelect.Body.String())
	}
	if got := string(eventStreamRecords(t, jsonSelect.Body.Bytes())); got != "{\"age\":31,\"name\":\"alice\"}\n{\"age\":28,\"name\":\"bob\"}\n" {
		t.Fatalf("json select records = %q", got)
	}
}

func TestSelectObjectContentRejectsUnsupportedSQL(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/reports/users.csv", strings.NewReader("name,age\nalice,31\n")); put.Code != http.StatusOK {
		t.Fatalf("put csv status = %d; body=%s", put.Code, put.Body.String())
	}
	request := `<SelectObjectContentRequest>
<Expression>SELECT name FROM S3Object WHERE age &gt; 30</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV /></InputSerialization>
<OutputSerialization><CSV /></OutputSerialization>
</SelectObjectContentRequest>`
	rec := performRequest(routes, http.MethodPost, "/demo-bucket/reports/users.csv?select&select-type=2", strings.NewReader(request))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported select status = %d, want %d; body=%s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported select error: %v", err)
	}
	if parsed.Code != "NotImplemented" {
		t.Fatalf("unsupported select error code = %q, want NotImplemented", parsed.Code)
	}
}

func TestBucketVersioningStoresAddressableVersionsAndDeleteMarkers(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	if versioning.Code != http.StatusOK {
		t.Fatalf("put versioning status = %d, want %d; body=%s", versioning.Code, http.StatusOK, versioning.Body.String())
	}
	getVersioning := performRequest(routes, http.MethodGet, "/demo-bucket?versioning", nil)
	if getVersioning.Code != http.StatusOK {
		t.Fatalf("get versioning status = %d, want %d; body=%s", getVersioning.Code, http.StatusOK, getVersioning.Body.String())
	}
	var config versioningConfiguration
	if err := xml.NewDecoder(getVersioning.Body).Decode(&config); err != nil {
		t.Fatalf("decode versioning config: %v", err)
	}
	if config.Status != "Enabled" {
		t.Fatalf("versioning status = %q, want Enabled", config.Status)
	}

	putOne := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("first"))
	if putOne.Code != http.StatusOK {
		t.Fatalf("put first status = %d; body=%s", putOne.Code, putOne.Body.String())
	}
	versionOne := putOne.Header().Get("x-amz-version-id")
	if versionOne == "" {
		t.Fatal("first put missing x-amz-version-id")
	}
	putTwo := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("second"))
	if putTwo.Code != http.StatusOK {
		t.Fatalf("put second status = %d; body=%s", putTwo.Code, putTwo.Body.String())
	}
	versionTwo := putTwo.Header().Get("x-amz-version-id")
	if versionTwo == "" || versionTwo == versionOne {
		t.Fatalf("second version id = %q, first = %q", versionTwo, versionOne)
	}

	latest := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if latest.Code != http.StatusOK || latest.Body.String() != "second" {
		t.Fatalf("latest get status=%d body=%q", latest.Code, latest.Body.String())
	}
	if got := latest.Header().Get("x-amz-version-id"); got != versionTwo {
		t.Fatalf("latest version header = %q, want %q", got, versionTwo)
	}
	first := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?versionId="+versionOne, nil)
	if first.Code != http.StatusOK || first.Body.String() != "first" {
		t.Fatalf("first version get status=%d body=%q", first.Code, first.Body.String())
	}

	listVersions := performRequest(routes, http.MethodGet, "/demo-bucket?versions&prefix=docs/", nil)
	if listVersions.Code != http.StatusOK {
		t.Fatalf("list versions status = %d; body=%s", listVersions.Code, listVersions.Body.String())
	}
	if body := listVersions.Body.String(); !strings.Contains(body, "<VersionId>"+versionOne+"</VersionId>") || !strings.Contains(body, "<VersionId>"+versionTwo+"</VersionId>") {
		t.Fatalf("list versions missing version ids: %s", body)
	}

	deleteLatest := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt", nil)
	if deleteLatest.Code != http.StatusNoContent {
		t.Fatalf("delete latest status = %d; body=%s", deleteLatest.Code, deleteLatest.Body.String())
	}
	deleteMarkerVersion := deleteLatest.Header().Get("x-amz-version-id")
	if deleteMarkerVersion == "" || deleteLatest.Header().Get("x-amz-delete-marker") != "true" {
		t.Fatalf("delete marker headers version=%q marker=%q", deleteMarkerVersion, deleteLatest.Header().Get("x-amz-delete-marker"))
	}
	missingLatest := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if missingLatest.Code != http.StatusNotFound {
		t.Fatalf("latest after delete marker status = %d, want %d", missingLatest.Code, http.StatusNotFound)
	}
	second := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?versionId="+versionTwo, nil)
	if second.Code != http.StatusOK || second.Body.String() != "second" {
		t.Fatalf("second version get status=%d body=%q", second.Code, second.Body.String())
	}

	removeMarker := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt?versionId="+deleteMarkerVersion, nil)
	if removeMarker.Code != http.StatusNoContent {
		t.Fatalf("remove delete marker status = %d; body=%s", removeMarker.Code, removeMarker.Body.String())
	}
	restored := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if restored.Code != http.StatusOK || restored.Body.String() != "second" {
		t.Fatalf("restored latest status=%d body=%q", restored.Code, restored.Body.String())
	}
}

func TestBucketVersioningListObjectVersionsPaginatesWithMarkers(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("put versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}

	putAOne := performRequest(routes, http.MethodPut, "/demo-bucket/docs/a.txt", strings.NewReader("a-one"))
	if putAOne.Code != http.StatusOK {
		t.Fatalf("put first a status = %d; body=%s", putAOne.Code, putAOne.Body.String())
	}
	versionAOne := putAOne.Header().Get("x-amz-version-id")
	putATwo := performRequest(routes, http.MethodPut, "/demo-bucket/docs/a.txt", strings.NewReader("a-two"))
	if putATwo.Code != http.StatusOK {
		t.Fatalf("put second a status = %d; body=%s", putATwo.Code, putATwo.Body.String())
	}
	versionATwo := putATwo.Header().Get("x-amz-version-id")
	putB := performRequest(routes, http.MethodPut, "/demo-bucket/docs/b.txt", strings.NewReader("b-one"))
	if putB.Code != http.StatusOK {
		t.Fatalf("put b status = %d; body=%s", putB.Code, putB.Body.String())
	}
	versionB := putB.Header().Get("x-amz-version-id")

	firstPage := performRequest(routes, http.MethodGet, "/demo-bucket?versions&prefix=docs/&max-keys=1", nil)
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d; body=%s", firstPage.Code, firstPage.Body.String())
	}
	var first listVersionsResult
	if err := xml.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if !first.IsTruncated || first.NextKeyMarker != "docs/a.txt" || first.NextVersionIDMarker != versionATwo {
		t.Fatalf("first page markers truncated=%v key=%q version=%q", first.IsTruncated, first.NextKeyMarker, first.NextVersionIDMarker)
	}
	if len(first.Versions) != 1 || first.Versions[0].VersionID != versionATwo || !first.Versions[0].IsLatest {
		t.Fatalf("first page versions = %#v, want latest a version %q", first.Versions, versionATwo)
	}

	secondPage := performRequest(routes, http.MethodGet, "/demo-bucket?versions&prefix=docs/&max-keys=1&key-marker=docs/a.txt&version-id-marker="+versionATwo, nil)
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d; body=%s", secondPage.Code, secondPage.Body.String())
	}
	var second listVersionsResult
	if err := xml.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if !second.IsTruncated || second.NextKeyMarker != "docs/a.txt" || second.NextVersionIDMarker != versionAOne {
		t.Fatalf("second page markers truncated=%v key=%q version=%q", second.IsTruncated, second.NextKeyMarker, second.NextVersionIDMarker)
	}
	if len(second.Versions) != 1 || second.Versions[0].VersionID != versionAOne || second.Versions[0].IsLatest {
		t.Fatalf("second page versions = %#v, want non-latest a version %q", second.Versions, versionAOne)
	}

	afterKey := performRequest(routes, http.MethodGet, "/demo-bucket?versions&prefix=docs/&key-marker=docs/a.txt", nil)
	if afterKey.Code != http.StatusOK {
		t.Fatalf("after key status = %d; body=%s", afterKey.Code, afterKey.Body.String())
	}
	var afterKeyResult listVersionsResult
	if err := xml.NewDecoder(afterKey.Body).Decode(&afterKeyResult); err != nil {
		t.Fatalf("decode after key: %v", err)
	}
	if len(afterKeyResult.Versions) != 1 || afterKeyResult.Versions[0].VersionID != versionB {
		t.Fatalf("after key versions = %#v, want b version %q", afterKeyResult.Versions, versionB)
	}
}

func TestBucketVersioningSuspendedUsesAddressableNullVersion(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	enable := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	if enable.Code != http.StatusOK {
		t.Fatalf("enable versioning status = %d; body=%s", enable.Code, enable.Body.String())
	}
	versioned := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("versioned"))
	if versioned.Code != http.StatusOK {
		t.Fatalf("put versioned status = %d; body=%s", versioned.Code, versioned.Body.String())
	}
	versionedID := versioned.Header().Get("x-amz-version-id")
	if versionedID == "" || versionedID == "null" {
		t.Fatalf("enabled put version id = %q, want generated id", versionedID)
	}

	suspend := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`))
	if suspend.Code != http.StatusOK {
		t.Fatalf("suspend versioning status = %d; body=%s", suspend.Code, suspend.Body.String())
	}
	nullPut := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("null-current"))
	if nullPut.Code != http.StatusOK {
		t.Fatalf("put null version status = %d; body=%s", nullPut.Code, nullPut.Body.String())
	}
	if got := nullPut.Header().Get("x-amz-version-id"); got != "null" {
		t.Fatalf("suspended put version id = %q, want null", got)
	}

	nullGet := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?versionId=null", nil)
	if nullGet.Code != http.StatusOK || nullGet.Body.String() != "null-current" {
		t.Fatalf("get null version status=%d body=%q", nullGet.Code, nullGet.Body.String())
	}
	if got := nullGet.Header().Get("x-amz-version-id"); got != "null" {
		t.Fatalf("get null version header = %q, want null", got)
	}

	listVersions := performRequest(routes, http.MethodGet, "/demo-bucket?versions&prefix=docs/", nil)
	if listVersions.Code != http.StatusOK {
		t.Fatalf("list versions status = %d; body=%s", listVersions.Code, listVersions.Body.String())
	}
	var listed listVersionsResult
	if err := xml.NewDecoder(listVersions.Body).Decode(&listed); err != nil {
		t.Fatalf("decode versions list: %v", err)
	}
	if len(listed.Versions) != 2 {
		t.Fatalf("listed versions = %#v, want generated and null versions", listed.Versions)
	}
	var nullListed, generatedListed versionElement
	for _, version := range listed.Versions {
		switch version.VersionID {
		case "null":
			nullListed = version
		case versionedID:
			generatedListed = version
		}
	}
	if nullListed.VersionID != "null" || !nullListed.IsLatest {
		t.Fatalf("null version listing = %#v, want latest null version", nullListed)
	}
	if generatedListed.VersionID != versionedID || generatedListed.IsLatest {
		t.Fatalf("generated version listing = %#v, want non-latest generated version", generatedListed)
	}

	deleteLatest := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt", nil)
	if deleteLatest.Code != http.StatusNoContent {
		t.Fatalf("delete latest status = %d; body=%s", deleteLatest.Code, deleteLatest.Body.String())
	}
	if got := deleteLatest.Header().Get("x-amz-version-id"); got != "null" {
		t.Fatalf("suspended delete marker version id = %q, want null", got)
	}
	if marker := deleteLatest.Header().Get("x-amz-delete-marker"); marker != "true" {
		t.Fatalf("suspended delete marker header = %q, want true", marker)
	}
	nullDeleteMarker := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?versionId=null", nil)
	if nullDeleteMarker.Code != http.StatusMethodNotAllowed || nullDeleteMarker.Header().Get("x-amz-delete-marker") != "true" {
		t.Fatalf("get null delete marker status=%d marker=%q body=%s", nullDeleteMarker.Code, nullDeleteMarker.Header().Get("x-amz-delete-marker"), nullDeleteMarker.Body.String())
	}

	removeNullMarker := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt?versionId=null", nil)
	if removeNullMarker.Code != http.StatusNoContent {
		t.Fatalf("remove null marker status = %d; body=%s", removeNullMarker.Code, removeNullMarker.Body.String())
	}
	restored := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if restored.Code != http.StatusOK || restored.Body.String() != "versioned" {
		t.Fatalf("restored version status=%d body=%q", restored.Code, restored.Body.String())
	}
	if got := restored.Header().Get("x-amz-version-id"); got != versionedID {
		t.Fatalf("restored version id = %q, want %q", got, versionedID)
	}
}

func TestBucketVersioningMultipartVersionUsesMultipartETag(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("put versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}

	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/docs/multipart.txt?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d; body=%s", initiate.Code, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	partOne := performRequest(routes, http.MethodPut, "/demo-bucket/docs/multipart.txt?partNumber=1&uploadId="+url.QueryEscape(initiated.UploadID), strings.NewReader("part-one-"))
	if partOne.Code != http.StatusOK {
		t.Fatalf("part one status = %d; body=%s", partOne.Code, partOne.Body.String())
	}
	partTwo := performRequest(routes, http.MethodPut, "/demo-bucket/docs/multipart.txt?partNumber=2&uploadId="+url.QueryEscape(initiated.UploadID), strings.NewReader("part-two"))
	if partTwo.Code != http.StatusOK {
		t.Fatalf("part two status = %d; body=%s", partTwo.Code, partTwo.Body.String())
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + partOne.Header().Get("ETag") + `</ETag></Part><Part><PartNumber>2</PartNumber><ETag>` + partTwo.Header().Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/docs/multipart.txt?uploadId="+url.QueryEscape(initiated.UploadID), strings.NewReader(completeBody))
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status = %d; body=%s", complete.Code, complete.Body.String())
	}
	versionID := complete.Header().Get("x-amz-version-id")
	multipartETag := complete.Header().Get("ETag")
	if versionID == "" || multipartETag == "" || !strings.HasSuffix(strings.Trim(multipartETag, `"`), "-2") {
		t.Fatalf("complete version=%q etag=%q, want multipart version and etag", versionID, multipartETag)
	}

	versionedHead := performRequest(routes, http.MethodHead, "/demo-bucket/docs/multipart.txt?versionId="+url.QueryEscape(versionID), nil)
	if versionedHead.Code != http.StatusOK {
		t.Fatalf("versioned head status = %d; body=%s", versionedHead.Code, versionedHead.Body.String())
	}
	if got := versionedHead.Header().Get("ETag"); got != multipartETag {
		t.Fatalf("versioned multipart ETag = %q, want %q", got, multipartETag)
	}
}

func TestBucketPolicyMetadataEndpointsPersistAndDelete(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo-bucket/*"}]}`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?policy", strings.NewReader(policy))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put policy status = %d, want %d; body=%s", put.Code, http.StatusNoContent, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?policy", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get policy status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != policy {
		t.Fatalf("policy body = %q, want %q", got, policy)
	}
	if got := get.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("policy content type = %q, want application/json", got)
	}

	deletePolicy := performRequest(routes, http.MethodDelete, "/demo-bucket?policy", nil)
	if deletePolicy.Code != http.StatusNoContent {
		t.Fatalf("delete policy status = %d, want %d; body=%s", deletePolicy.Code, http.StatusNoContent, deletePolicy.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?policy", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing policy status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(missing.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode missing policy error: %v", err)
	}
	if parsed.Code != "NoSuchBucketPolicy" {
		t.Fatalf("missing policy code = %q, want NoSuchBucketPolicy", parsed.Code)
	}
}

func TestBucketPolicyRejectsMalformedJSON(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	put := performRequest(routes, http.MethodPut, "/demo-bucket?policy", strings.NewReader(`{"Version":`))
	if put.Code != http.StatusBadRequest {
		t.Fatalf("malformed policy status = %d, want %d; body=%s", put.Code, http.StatusBadRequest, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode malformed policy error: %v", err)
	}
	if parsed.Code != "MalformedPolicy" {
		t.Fatalf("malformed policy code = %q, want MalformedPolicy", parsed.Code)
	}
}

func TestBucketAndObjectACLMetadataEndpoints(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if putObject := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("body")); putObject.Code != http.StatusOK {
		t.Fatalf("put object status = %d; body=%s", putObject.Code, putObject.Body.String())
	}

	bucketACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket?acl", nil)
	bucketACLReq.Header.Set("x-amz-acl", "public-read")
	bucketACL := httptest.NewRecorder()
	routes.ServeHTTP(bucketACL, bucketACLReq)
	if bucketACL.Code != http.StatusOK {
		t.Fatalf("put bucket acl status = %d, want %d; body=%s", bucketACL.Code, http.StatusOK, bucketACL.Body.String())
	}
	getBucketACL := performRequest(routes, http.MethodGet, "/demo-bucket?acl", nil)
	if getBucketACL.Code != http.StatusOK {
		t.Fatalf("get bucket acl status = %d, want %d; body=%s", getBucketACL.Code, http.StatusOK, getBucketACL.Body.String())
	}
	var bucketPolicy accessControlPolicy
	if err := xml.NewDecoder(getBucketACL.Body).Decode(&bucketPolicy); err != nil {
		t.Fatalf("decode bucket acl: %v", err)
	}
	if bucketPolicy.CannedACL != "public-read" || len(bucketPolicy.AccessControlList.Grants) != 1 || bucketPolicy.AccessControlList.Grants[0].Permission != "READ" {
		t.Fatalf("bucket acl response = %#v", bucketPolicy)
	}

	objectACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt?acl", nil)
	objectACLReq.Header.Set("x-amz-acl", "bucket-owner-full-control")
	objectACL := httptest.NewRecorder()
	routes.ServeHTTP(objectACL, objectACLReq)
	if objectACL.Code != http.StatusOK {
		t.Fatalf("put object acl status = %d, want %d; body=%s", objectACL.Code, http.StatusOK, objectACL.Body.String())
	}
	getObjectACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?acl", nil)
	if getObjectACL.Code != http.StatusOK {
		t.Fatalf("get object acl status = %d, want %d; body=%s", getObjectACL.Code, http.StatusOK, getObjectACL.Body.String())
	}
	var objectPolicy accessControlPolicy
	if err := xml.NewDecoder(getObjectACL.Body).Decode(&objectPolicy); err != nil {
		t.Fatalf("decode object acl: %v", err)
	}
	if objectPolicy.CannedACL != "bucket-owner-full-control" || objectPolicy.AccessControlList.Grants[0].Permission != "FULL_CONTROL" {
		t.Fatalf("object acl response = %#v", objectPolicy)
	}

	missingObjectACL := performRequest(routes, http.MethodGet, "/demo-bucket/missing.txt?acl", nil)
	if missingObjectACL.Code != http.StatusNotFound {
		t.Fatalf("missing object acl status = %d, want %d; body=%s", missingObjectACL.Code, http.StatusNotFound, missingObjectACL.Body.String())
	}
}

func TestObjectACLSupportsVersionID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("enable versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}
	first := performRequest(routes, http.MethodPut, "/demo-bucket/docs/versioned-acl.txt", strings.NewReader("first"))
	if first.Code != http.StatusOK {
		t.Fatalf("put first status = %d; body=%s", first.Code, first.Body.String())
	}
	firstVersionID := first.Header().Get("x-amz-version-id")
	if firstVersionID == "" {
		t.Fatal("first version id is empty")
	}
	second := performRequest(routes, http.MethodPut, "/demo-bucket/docs/versioned-acl.txt", strings.NewReader("second"))
	if second.Code != http.StatusOK {
		t.Fatalf("put second status = %d; body=%s", second.Code, second.Body.String())
	}
	secondVersionID := second.Header().Get("x-amz-version-id")
	if secondVersionID == "" || secondVersionID == firstVersionID {
		t.Fatalf("second version id = %q, first = %q", secondVersionID, firstVersionID)
	}

	putFirstACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/versioned-acl.txt?acl&versionId="+url.QueryEscape(firstVersionID), nil)
	putFirstACLReq.Header.Set("x-amz-acl", "public-read")
	putFirstACL := httptest.NewRecorder()
	routes.ServeHTTP(putFirstACL, putFirstACLReq)
	if putFirstACL.Code != http.StatusOK {
		t.Fatalf("put first version acl status = %d, want %d; body=%s", putFirstACL.Code, http.StatusOK, putFirstACL.Body.String())
	}

	getFirstACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/versioned-acl.txt?acl&versionId="+url.QueryEscape(firstVersionID), nil)
	if getFirstACL.Code != http.StatusOK {
		t.Fatalf("get first version acl status = %d, want %d; body=%s", getFirstACL.Code, http.StatusOK, getFirstACL.Body.String())
	}
	var firstPolicy accessControlPolicy
	if err := xml.NewDecoder(getFirstACL.Body).Decode(&firstPolicy); err != nil {
		t.Fatalf("decode first version acl: %v", err)
	}
	if firstPolicy.CannedACL != "public-read" {
		t.Fatalf("first version acl = %#v", firstPolicy)
	}

	getLatestACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/versioned-acl.txt?acl", nil)
	if getLatestACL.Code != http.StatusOK {
		t.Fatalf("get latest acl status = %d, want %d; body=%s", getLatestACL.Code, http.StatusOK, getLatestACL.Body.String())
	}
	var latestPolicy accessControlPolicy
	if err := xml.NewDecoder(getLatestACL.Body).Decode(&latestPolicy); err != nil {
		t.Fatalf("decode latest acl: %v", err)
	}
	if latestPolicy.CannedACL != "private" {
		t.Fatalf("latest acl = %#v, want private", latestPolicy)
	}
}

func TestBucketLifecycleMetadataEndpointsPersistAndDelete(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>expire-logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put lifecycle status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?lifecycle", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get lifecycle status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed LifecycleConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode lifecycle config: %v", err)
	}
	if len(parsed.Rules) != 1 || parsed.Rules[0].ID != "expire-logs" || parsed.Rules[0].Filter.Prefix != "logs/" || parsed.Rules[0].Expiration.Days == nil || *parsed.Rules[0].Expiration.Days != 30 {
		t.Fatalf("lifecycle config = %#v", parsed)
	}

	deleteLifecycle := performRequest(routes, http.MethodDelete, "/demo-bucket?lifecycle", nil)
	if deleteLifecycle.Code != http.StatusNoContent {
		t.Fatalf("delete lifecycle status = %d, want %d; body=%s", deleteLifecycle.Code, http.StatusNoContent, deleteLifecycle.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?lifecycle", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing lifecycle status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
	var parsedError errorResponse
	if err := xml.NewDecoder(missing.Body).Decode(&parsedError); err != nil {
		t.Fatalf("decode missing lifecycle error: %v", err)
	}
	if parsedError.Code != "NoSuchLifecycleConfiguration" {
		t.Fatalf("missing lifecycle code = %q, want NoSuchLifecycleConfiguration", parsedError.Code)
	}
}

func TestBucketLifecycleExpirationAppliesDeterministically(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if putLog := performRequest(routes, http.MethodPut, "/demo-bucket/logs/old.txt", strings.NewReader("old log")); putLog.Code != http.StatusOK {
		t.Fatalf("put log status = %d; body=%s", putLog.Code, putLog.Body.String())
	}
	if putDoc := performRequest(routes, http.MethodPut, "/demo-bucket/docs/keep.txt", strings.NewReader("keep doc")); putDoc.Code != http.StatusOK {
		t.Fatalf("put doc status = %d; body=%s", putDoc.Code, putDoc.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>expire-logs-now</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>`
	if putLifecycle := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config)); putLifecycle.Code != http.StatusOK {
		t.Fatalf("put lifecycle status = %d; body=%s", putLifecycle.Code, putLifecycle.Body.String())
	}

	expired := performRequest(routes, http.MethodGet, "/demo-bucket/logs/old.txt", nil)
	if expired.Code != http.StatusNotFound {
		t.Fatalf("expired object status = %d, want %d; body=%s", expired.Code, http.StatusNotFound, expired.Body.String())
	}
	kept := performRequest(routes, http.MethodGet, "/demo-bucket/docs/keep.txt", nil)
	if kept.Code != http.StatusOK || kept.Body.String() != "keep doc" {
		t.Fatalf("kept object status=%d body=%q", kept.Code, kept.Body.String())
	}
	list := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d; body=%s", list.Code, list.Body.String())
	}
	if body := list.Body.String(); strings.Contains(body, "logs/old.txt") || !strings.Contains(body, "docs/keep.txt") {
		t.Fatalf("list body after lifecycle expiration = %s", body)
	}
}

func TestBucketLifecycleRejectsUnsupportedTransitions(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config))
	if put.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported lifecycle status = %d, want %d; body=%s", put.Code, http.StatusNotImplemented, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported lifecycle error: %v", err)
	}
	if parsed.Code != "NotImplemented" {
		t.Fatalf("unsupported lifecycle code = %q, want NotImplemented", parsed.Code)
	}
}

func TestBucketNotificationMetadataEndpointsPersist(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><QueueConfiguration><Id>docs-created</Id><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put notification status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?notification", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get notification status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed NotificationConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode notification config: %v", err)
	}
	if len(parsed.QueueConfigurations) != 1 || parsed.QueueConfigurations[0].ID != "docs-created" || parsed.QueueConfigurations[0].Queue == "" || len(parsed.QueueConfigurations[0].Events) != 1 {
		t.Fatalf("notification config = %#v", parsed)
	}
	if rules := parsed.QueueConfigurations[0].Filter.S3Key.Rules; len(rules) != 2 || rules[0].Name != "prefix" || rules[0].Value != "docs/" || rules[1].Name != "suffix" || rules[1].Value != ".txt" {
		t.Fatalf("notification filter rules = %#v", parsed.QueueConfigurations[0].Filter.S3Key.Rules)
	}
}

func TestBucketNotificationEventBridgeMetadataPersists(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><EventBridgeConfiguration /></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put notification status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?notification", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get notification status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed NotificationConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode notification config: %v", err)
	}
	if parsed.EventBridgeConfiguration == nil {
		t.Fatalf("event bridge configuration was not persisted: %#v", parsed)
	}
}

func TestBucketNotificationRecordsMatchingObjectEvents(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	config := `<NotificationConfiguration><TopicConfiguration><Topic>arn:aws:sns:us-east-1:000000000000:local</Topic><Event>s3:ObjectCreated:Put</Event><Event>s3:ObjectRemoved:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></TopicConfiguration></NotificationConfiguration>`
	if putNotification := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config)); putNotification.Code != http.StatusOK {
		t.Fatalf("put notification status = %d; body=%s", putNotification.Code, putNotification.Body.String())
	}

	if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("body")); put.Code != http.StatusOK {
		t.Fatalf("put matching object status = %d; body=%s", put.Code, put.Body.String())
	}
	if putIgnored := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.bin", strings.NewReader("body")); putIgnored.Code != http.StatusOK {
		t.Fatalf("put ignored object status = %d; body=%s", putIgnored.Code, putIgnored.Body.String())
	}
	if deleteObject := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/readme.txt", nil); deleteObject.Code != http.StatusNoContent {
		t.Fatalf("delete matching object status = %d; body=%s", deleteObject.Code, deleteObject.Body.String())
	}

	events, ok, err := store.ListNotificationEvents(context.Background(), "demo-bucket")
	if err != nil {
		t.Fatalf("list notification events: %v", err)
	}
	if !ok {
		t.Fatal("bucket missing when listing notification events")
	}
	if len(events) != 2 {
		t.Fatalf("notification event count = %d, want 2: %#v", len(events), events)
	}
	if events[0].EventName != "s3:ObjectCreated:Put" || events[0].Key != "docs/readme.txt" || events[0].ETag == "" || events[0].EventID == "" {
		t.Fatalf("created event = %#v", events[0])
	}
	if events[1].EventName != "s3:ObjectRemoved:Delete" || events[1].Key != "docs/readme.txt" {
		t.Fatalf("removed event = %#v", events[1])
	}
}

func TestBucketNotificationRejectsUnsupportedEvent(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ReducedRedundancyLostObject</Event></QueueConfiguration></NotificationConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?notification", strings.NewReader(config))
	if put.Code != http.StatusBadRequest {
		t.Fatalf("unsupported notification status = %d, want %d; body=%s", put.Code, http.StatusBadRequest, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported notification error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("unsupported notification code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestServerSideEncryptionMetadataRoundTripsOnObjectRequests(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/kms.txt", strings.NewReader("kms metadata"))
	putReq.Header.Set("x-amz-server-side-encryption", "aws:kms")
	putReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:us-east-1:000000000000:key/local")
	putReq.Header.Set("x-amz-server-side-encryption-bucket-key-enabled", "true")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("put sse = %q, want aws:kms", got)
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got != "arn:aws:kms:us-east-1:000000000000:key/local" {
		t.Fatalf("put kms key id = %q", got)
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption-bucket-key-enabled"); got != "true" {
		t.Fatalf("put bucket key = %q, want true", got)
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/kms.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("head sse = %q, want aws:kms", got)
	}
	if got := head.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got == "" {
		t.Fatal("head missing kms key id")
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/kms.txt", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Header().Get("x-amz-server-side-encryption-bucket-key-enabled"); got != "true" {
		t.Fatalf("get bucket key = %q, want true", got)
	}

	reloaded := NewServer(Config{}, NewFileBucketStore(store.root)).routes()
	persistedHead := performRequest(reloaded, http.MethodHead, "/demo-bucket/docs/kms.txt", nil)
	if got := persistedHead.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("persisted head sse = %q, want aws:kms", got)
	}
}

func TestServerSideEncryptionMetadataOnCopyAndMultipart(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	sourceReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/source.txt", strings.NewReader("source body"))
	sourceReq.Header.Set("x-amz-server-side-encryption", "AES256")
	sourceRec := httptest.NewRecorder()
	routes.ServeHTTP(sourceRec, sourceReq)
	if sourceRec.Code != http.StatusOK {
		t.Fatalf("put source status = %d; body=%s", sourceRec.Code, sourceRec.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/source.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d; body=%s", copyRec.Code, copyRec.Body.String())
	}
	if got := copyRec.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("copy inherited sse = %q, want AES256", got)
	}

	replaceReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/copy-kms.txt", nil)
	replaceReq.Header.Set("x-amz-copy-source", "/demo-bucket/source.txt")
	replaceReq.Header.Set("x-amz-server-side-encryption", "aws:kms")
	replaceReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "local-key")
	replaceRec := httptest.NewRecorder()
	routes.ServeHTTP(replaceRec, replaceReq)
	if replaceRec.Code != http.StatusOK {
		t.Fatalf("copy replace status = %d; body=%s", replaceRec.Code, replaceRec.Body.String())
	}
	if got := replaceRec.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("copy replaced sse = %q, want aws:kms", got)
	}
	if got := replaceRec.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got != "local-key" {
		t.Fatalf("copy replaced kms key = %q, want local-key", got)
	}

	initReq := httptest.NewRequest(http.MethodPost, "/demo-bucket/multipart.txt?uploads", nil)
	initReq.Header.Set("x-amz-server-side-encryption", "AES256")
	initRec := httptest.NewRecorder()
	routes.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init multipart status = %d; body=%s", initRec.Code, initRec.Body.String())
	}
	if got := initRec.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("init multipart sse = %q, want AES256", got)
	}
	var initResult initiateMultipartUploadResult
	if err := xml.NewDecoder(initRec.Body).Decode(&initResult); err != nil {
		t.Fatalf("decode init multipart: %v", err)
	}
	uploadPart := performRequest(routes, http.MethodPut, "/demo-bucket/multipart.txt?partNumber=1&uploadId="+url.QueryEscape(initResult.UploadID), strings.NewReader("multipart body"))
	if uploadPart.Code != http.StatusOK {
		t.Fatalf("upload part status = %d; body=%s", uploadPart.Code, uploadPart.Body.String())
	}
	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + uploadPart.Header().Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/multipart.txt?uploadId="+url.QueryEscape(initResult.UploadID), strings.NewReader(completeBody))
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status = %d; body=%s", complete.Code, complete.Body.String())
	}
	if got := complete.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("complete multipart sse = %q, want AES256", got)
	}
}

func TestServerSideEncryptionRejectsUnsupportedOrInvalidHeaders(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	customerKeyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/ssec.txt", strings.NewReader("secret"))
	customerKeyReq.Header.Set("x-amz-server-side-encryption-customer-algorithm", "AES256")
	customerKeyRec := httptest.NewRecorder()
	routes.ServeHTTP(customerKeyRec, customerKeyReq)
	if customerKeyRec.Code != http.StatusNotImplemented {
		t.Fatalf("sse-c status = %d, want %d; body=%s", customerKeyRec.Code, http.StatusNotImplemented, customerKeyRec.Body.String())
	}

	invalidKMSReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/invalid.txt", strings.NewReader("invalid"))
	invalidKMSReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "local-key")
	invalidKMSRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidKMSRec, invalidKMSReq)
	if invalidKMSRec.Code != http.StatusBadRequest {
		t.Fatalf("kms without algorithm status = %d, want %d; body=%s", invalidKMSRec.Code, http.StatusBadRequest, invalidKMSRec.Body.String())
	}

	unsupportedReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/unsupported.txt", strings.NewReader("unsupported"))
	unsupportedReq.Header.Set("x-amz-server-side-encryption", "aws:kms:dsse")
	unsupportedRec := httptest.NewRecorder()
	routes.ServeHTTP(unsupportedRec, unsupportedReq)
	if unsupportedRec.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported sse status = %d, want %d; body=%s", unsupportedRec.Code, http.StatusNotImplemented, unsupportedRec.Body.String())
	}
}

func TestObjectLockConfigurationRetentionAndLegalHold(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	config := `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>`
	putConfig := performRequest(routes, http.MethodPut, "/demo-bucket?object-lock", strings.NewReader(config))
	if putConfig.Code != http.StatusOK {
		t.Fatalf("put object lock config status = %d, want %d; body=%s", putConfig.Code, http.StatusOK, putConfig.Body.String())
	}
	getConfig := performRequest(routes, http.MethodGet, "/demo-bucket?object-lock", nil)
	if getConfig.Code != http.StatusOK {
		t.Fatalf("get object lock config status = %d, want %d; body=%s", getConfig.Code, http.StatusOK, getConfig.Body.String())
	}
	var parsedConfig ObjectLockConfiguration
	if err := xml.NewDecoder(getConfig.Body).Decode(&parsedConfig); err != nil {
		t.Fatalf("decode object lock config: %v", err)
	}
	if parsedConfig.ObjectLockEnabled != "Enabled" || parsedConfig.Rule.DefaultRetention.Mode != "GOVERNANCE" || parsedConfig.Rule.DefaultRetention.Days != 7 {
		t.Fatalf("object lock config = %#v", parsedConfig)
	}
	if putDefault := performRequest(routes, http.MethodPut, "/demo-bucket/docs/default-retention.txt", strings.NewReader("default")); putDefault.Code != http.StatusOK {
		t.Fatalf("put default retention object status = %d; body=%s", putDefault.Code, putDefault.Body.String())
	}
	defaultHead := performRequest(routes, http.MethodHead, "/demo-bucket/docs/default-retention.txt", nil)
	if got := defaultHead.Header().Get("x-amz-object-lock-mode"); got != "GOVERNANCE" {
		t.Fatalf("default retention mode = %q, want GOVERNANCE", got)
	}
	if got := defaultHead.Header().Get("x-amz-object-lock-retain-until-date"); got == "" {
		t.Fatal("default retention missing retain until date")
	}

	retainUntil := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/locked.txt", strings.NewReader("locked body"))
	putReq.Header.Set("x-amz-object-lock-mode", "COMPLIANCE")
	putReq.Header.Set("x-amz-object-lock-retain-until-date", retainUntil)
	putReq.Header.Set("x-amz-object-lock-legal-hold", "ON")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put locked object status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("x-amz-object-lock-mode"); got != "COMPLIANCE" {
		t.Fatalf("put object lock mode = %q, want COMPLIANCE", got)
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/locked.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head locked object status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("x-amz-object-lock-retain-until-date"); got != retainUntil {
		t.Fatalf("head retain until = %q, want %q", got, retainUntil)
	}
	if got := head.Header().Get("x-amz-object-lock-legal-hold"); got != "ON" {
		t.Fatalf("head legal hold = %q, want ON", got)
	}

	getRetention := performRequest(routes, http.MethodGet, "/demo-bucket/docs/locked.txt?retention", nil)
	if getRetention.Code != http.StatusOK {
		t.Fatalf("get retention status = %d, want %d; body=%s", getRetention.Code, http.StatusOK, getRetention.Body.String())
	}
	var retention ObjectRetention
	if err := xml.NewDecoder(getRetention.Body).Decode(&retention); err != nil {
		t.Fatalf("decode retention: %v", err)
	}
	if retention.Mode != "COMPLIANCE" || retention.RetainUntilDate != retainUntil {
		t.Fatalf("retention = %#v", retention)
	}

	turnOffLegalHold := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?legal-hold", strings.NewReader(`<LegalHold><Status>OFF</Status></LegalHold>`))
	if turnOffLegalHold.Code != http.StatusOK {
		t.Fatalf("put legal hold off status = %d, want %d; body=%s", turnOffLegalHold.Code, http.StatusOK, turnOffLegalHold.Body.String())
	}
	getLegalHold := performRequest(routes, http.MethodGet, "/demo-bucket/docs/locked.txt?legal-hold", nil)
	if getLegalHold.Code != http.StatusOK {
		t.Fatalf("get legal hold status = %d, want %d; body=%s", getLegalHold.Code, http.StatusOK, getLegalHold.Body.String())
	}
	var legalHold ObjectLegalHold
	if err := xml.NewDecoder(getLegalHold.Body).Decode(&legalHold); err != nil {
		t.Fatalf("decode legal hold: %v", err)
	}
	if legalHold.Status != "OFF" {
		t.Fatalf("legal hold = %#v", legalHold)
	}
}

func TestObjectLockPreventsDeleteUntilRetentionExpires(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	retainUntil := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt", strings.NewReader("locked body")); put.Code != http.StatusOK {
		t.Fatalf("put object status = %d; body=%s", put.Code, put.Body.String())
	}
	body := `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?retention", strings.NewReader(body)); putRetention.Code != http.StatusOK {
		t.Fatalf("put retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}

	deleteLocked := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/locked.txt", nil)
	if deleteLocked.Code != http.StatusForbidden {
		t.Fatalf("delete locked status = %d, want %d; body=%s", deleteLocked.Code, http.StatusForbidden, deleteLocked.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(deleteLocked.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode locked delete error: %v", err)
	}
	if parsed.Code != "AccessDenied" {
		t.Fatalf("locked delete code = %q, want AccessDenied", parsed.Code)
	}

	if putExpiredRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?retention", strings.NewReader(`<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2000-01-01T00:00:00Z</RetainUntilDate></Retention>`)); putExpiredRetention.Code != http.StatusOK {
		t.Fatalf("put expired retention status = %d; body=%s", putExpiredRetention.Code, putExpiredRetention.Body.String())
	}
	deleteExpired := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/locked.txt", nil)
	if deleteExpired.Code != http.StatusNoContent {
		t.Fatalf("delete expired retention status = %d, want %d; body=%s", deleteExpired.Code, http.StatusNoContent, deleteExpired.Body.String())
	}
}

func TestObjectLockBypassGovernanceRetentionOnlyBypassesGovernance(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	retainUntil := "2099-01-01T00:00:00Z"
	for _, key := range []string{"governance.txt", "compliance.txt", "legal-hold.txt"} {
		if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/"+key, strings.NewReader(key)); put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}
	governance := `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/governance.txt?retention", strings.NewReader(governance)); putRetention.Code != http.StatusOK {
		t.Fatalf("put governance retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}
	compliance := `<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/compliance.txt?retention", strings.NewReader(compliance)); putRetention.Code != http.StatusOK {
		t.Fatalf("put compliance retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}
	if putLegalHold := performRequest(routes, http.MethodPut, "/demo-bucket/docs/legal-hold.txt?legal-hold", strings.NewReader(`<LegalHold><Status>ON</Status></LegalHold>`)); putLegalHold.Code != http.StatusOK {
		t.Fatalf("put legal hold status = %d; body=%s", putLegalHold.Code, putLegalHold.Body.String())
	}

	invalidReq := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/governance.txt", nil)
	invalidReq.Header.Set("x-amz-bypass-governance-retention", "not-bool")
	invalidRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid bypass header status = %d, want %d; body=%s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}

	bypassReq := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/governance.txt", nil)
	bypassReq.Header.Set("x-amz-bypass-governance-retention", "true")
	bypassRec := httptest.NewRecorder()
	routes.ServeHTTP(bypassRec, bypassReq)
	if bypassRec.Code != http.StatusNoContent {
		t.Fatalf("bypass governance delete status = %d, want %d; body=%s", bypassRec.Code, http.StatusNoContent, bypassRec.Body.String())
	}

	for _, key := range []string{"compliance.txt", "legal-hold.txt"} {
		req := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/"+key, nil)
		req.Header.Set("x-amz-bypass-governance-retention", "true")
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("bypass %s delete status = %d, want %d; body=%s", key, rec.Code, http.StatusForbidden, rec.Body.String())
		}
	}
}

func TestPutObjectValidatesContentMD5(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	body := "checksum body"
	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/checksum.txt", strings.NewReader(body))
	putReq.Header.Set("Content-MD5", contentMD5(body))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put with valid md5 status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	mismatchReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/bad-checksum.txt", strings.NewReader(body))
	mismatchReq.Header.Set("Content-MD5", contentMD5("different body"))
	mismatchRec := httptest.NewRecorder()
	routes.ServeHTTP(mismatchRec, mismatchReq)
	if mismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("put with bad md5 status = %d, want %d; body=%s", mismatchRec.Code, http.StatusBadRequest, mismatchRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(mismatchRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode md5 mismatch error: %v", err)
	}
	if parsed.Code != "BadDigest" {
		t.Fatalf("md5 mismatch code = %q, want BadDigest", parsed.Code)
	}

	invalidReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/invalid-checksum.txt", strings.NewReader(body))
	invalidReq.Header.Set("Content-MD5", "not-base64")
	invalidRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("put with invalid md5 status = %d, want %d; body=%s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}
	if err := xml.NewDecoder(invalidRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode invalid md5 error: %v", err)
	}
	if parsed.Code != "InvalidDigest" {
		t.Fatalf("invalid md5 code = %q, want InvalidDigest", parsed.Code)
	}
}

func TestCopyObjectAcceptsEscapedSourceWithQuery(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source%20file.txt", strings.NewReader("copy me"))
	if put.Code != http.StatusOK {
		t.Fatalf("put source status = %d; body=%s", put.Code, put.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copied.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/source%20file.txt?response-content-type=text/plain")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copied.txt", nil)
	if got := get.Body.String(); got != "copy me" {
		t.Fatalf("copied body = %q, want copy me", got)
	}
}

func TestCopyObjectUsesSourceVersionID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("enable versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}

	first := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source.txt", strings.NewReader("first"))
	if first.Code != http.StatusOK {
		t.Fatalf("put first status = %d; body=%s", first.Code, first.Body.String())
	}
	firstVersionID := first.Header().Get("x-amz-version-id")
	if firstVersionID == "" {
		t.Fatal("first put missing version id")
	}
	second := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source.txt", strings.NewReader("second"))
	if second.Code != http.StatusOK {
		t.Fatalf("put second status = %d; body=%s", second.Code, second.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copied.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/source.txt?versionId="+url.QueryEscape(firstVersionID))
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy version status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	if got := copyRec.Header().Get("x-amz-version-id"); got == "" {
		t.Fatal("copy response missing destination version id")
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copied.txt", nil)
	if got := get.Body.String(); got != "first" {
		t.Fatalf("copied version body = %q, want first", got)
	}
	latest := performRequest(routes, http.MethodGet, "/demo-bucket/docs/source.txt", nil)
	if got := latest.Body.String(); got != "second" {
		t.Fatalf("source latest body = %q, want second", got)
	}
}

func TestRangeOnEmptyObjectReturnsInvalidRange(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	put := performRequest(routes, http.MethodPut, "/demo-bucket/empty.txt", strings.NewReader(""))
	if put.Code != http.StatusOK {
		t.Fatalf("put empty object status = %d; body=%s", put.Code, put.Body.String())
	}

	rangeReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/empty.txt", nil)
	rangeReq.Header.Set("Range", "bytes=0-0")
	rangeRec := httptest.NewRecorder()
	routes.ServeHTTP(rangeRec, rangeReq)
	if rangeRec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("empty range status = %d, want %d; body=%s", rangeRec.Code, http.StatusRequestedRangeNotSatisfiable, rangeRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rangeRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode range error: %v", err)
	}
	if parsed.Code != "InvalidRange" {
		t.Fatalf("range error code = %q, want InvalidRange", parsed.Code)
	}
}

func TestListObjectsV2SupportsDelimiterAndContinuation(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	for _, key := range []string{
		"docs/a.txt",
		"docs/archive/2026.txt",
		"docs/b.txt",
		"logs/app.log",
	} {
		put := performRequest(routes, http.MethodPut, "/demo-bucket/"+key, strings.NewReader(key))
		if put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/&delimiter=/&max-keys=2", nil)
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first listBucketResult
	if err := xml.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if !first.IsTruncated {
		t.Fatal("first page IsTruncated = false, want true")
	}
	if first.KeyCount != 2 {
		t.Fatalf("first page KeyCount = %d, want 2", first.KeyCount)
	}
	if len(first.Contents) != 1 || first.Contents[0].Key != "docs/a.txt" {
		t.Fatalf("first page contents = %#v", first.Contents)
	}
	if len(first.CommonPrefixes) != 1 || first.CommonPrefixes[0].Prefix != "docs/archive/" {
		t.Fatalf("first page common prefixes = %#v", first.CommonPrefixes)
	}
	if first.NextContinuationToken == "" {
		t.Fatal("first page missing NextContinuationToken")
	}

	secondPage := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/&delimiter=/&continuation-token="+url.QueryEscape(first.NextContinuationToken), nil)
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second listBucketResult
	if err := xml.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.IsTruncated {
		t.Fatal("second page IsTruncated = true, want false")
	}
	if len(second.Contents) != 1 || second.Contents[0].Key != "docs/b.txt" {
		t.Fatalf("second page contents = %#v", second.Contents)
	}
	if len(second.CommonPrefixes) != 0 {
		t.Fatalf("second page common prefixes = %#v, want none", second.CommonPrefixes)
	}
}

func TestListObjectsSupportsV1MarkerAndURLEncoding(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	for _, key := range []string{"docs/a file.txt", "docs/b file.txt"} {
		put := performRequest(routes, http.MethodPut, "/demo-bucket/"+url.PathEscape(key), strings.NewReader(key))
		if put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?prefix=docs/&marker=docs/a%20file.txt&encoding-type=url", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var parsed listBucketResult
	if err := xml.NewDecoder(list.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(parsed.Contents) != 1 || parsed.Contents[0].Key != "docs%2Fb%20file.txt" {
		t.Fatalf("encoded contents = %#v", parsed.Contents)
	}
	if parsed.Marker != "docs%2Fa%20file.txt" {
		t.Fatalf("encoded marker = %q", parsed.Marker)
	}
}

func TestBucketRoutesReturnS3XMLErrors(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	invalid := performRequest(routes, http.MethodPut, "/../bad", nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, want %d", invalid.Code, http.StatusBadRequest)
	}
	var parsed errorResponse
	if err := xml.NewDecoder(invalid.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if parsed.Code != "InvalidBucketName" {
		t.Fatalf("error code = %q, want InvalidBucketName", parsed.Code)
	}

	objectRoute := performRequest(routes, http.MethodGet, "/demo-bucket/key.txt", nil)
	if objectRoute.Code != http.StatusNotFound {
		t.Fatalf("object route status = %d, want %d", objectRoute.Code, http.StatusNotFound)
	}
}

func TestCreateBucketRejectsS3InvalidBucketNames(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{
		".starts-with-dot",
		"ends-with-dot.",
		"-starts-with-hyphen",
		"ends-with-hyphen-",
		"has.-adjacent",
		"has-.adjacent",
		"192.168.0.1",
	} {
		t.Run(name, func(t *testing.T) {
			rec := performRequest(routes, http.MethodPut, "/"+name, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("create status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			var parsed errorResponse
			if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if parsed.Code != "InvalidBucketName" {
				t.Fatalf("error code = %q, want InvalidBucketName", parsed.Code)
			}
		})
	}
}

func TestPresignedURLValidatesSignature(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "relaxed",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	previousNow := nowUTC
	nowUTC = func() time.Time { return fixedNow }
	defer func() { nowUTC = previousNow }()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d", create.Code)
	}
	put := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("hello from devcloud s3\n"))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d", putRec.Code)
	}

	target := presignedTarget(t, "GET", "example.com", "/demo-bucket/docs/readme.txt", fixedNow)
	get := httptest.NewRequest(http.MethodGet, target, nil)
	get.Host = "example.com"
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("presigned get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello from devcloud s3\n" {
		t.Fatalf("presigned get body = %q", got)
	}

	bad := httptest.NewRequest(http.MethodGet, target[:len(target)-1]+"0", nil)
	bad.Host = "example.com"
	badRec := httptest.NewRecorder()
	routes.ServeHTTP(badRec, bad)
	if badRec.Code != http.StatusForbidden {
		t.Fatalf("bad signature status = %d, want %d", badRec.Code, http.StatusForbidden)
	}
	var parsed errorResponse
	if err := xml.NewDecoder(badRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode bad signature error: %v", err)
	}
	if parsed.Code != "SignatureDoesNotMatch" {
		t.Fatalf("bad signature code = %q", parsed.Code)
	}
}

func TestPresignedURLRejectsInvalidArguments(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "relaxed",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	previousNow := nowUTC
	nowUTC = func() time.Time { return fixedNow }
	defer func() { nowUTC = previousNow }()

	validTarget := presignedTarget(t, "GET", "example.com", "/demo-bucket/docs/readme.txt", fixedNow)
	tests := []struct {
		name       string
		mutate     func(url.Values)
		wantStatus int
		wantCode   string
	}{
		{
			name: "unsupported algorithm",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA1")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidArgument",
		},
		{
			name: "malformed credential",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Credential", "dev/us-east-1/s3")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "AuthorizationHeaderMalformed",
		},
		{
			name: "wrong access key",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Credential", strings.Replace(values.Get("X-Amz-Credential"), "dev/", "other/", 1))
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "InvalidAccessKeyId",
		},
		{
			name: "bad date",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Date", "not-a-date")
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
		{
			name: "expires too large",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Expires", "604801")
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
		{
			name: "missing signed headers",
			mutate: func(values url.Values) {
				values.Del("X-Amz-SignedHeaders")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "AuthorizationHeaderMalformed",
		},
	}

	parsedTarget, err := url.Parse(validTarget)
	if err != nil {
		t.Fatalf("parse presigned target: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := parsedTarget.Query()
			tt.mutate(values)
			req := httptest.NewRequest(http.MethodGet, parsedTarget.Path+"?"+values.Encode(), nil)
			req.Host = "example.com"
			rec := httptest.NewRecorder()
			routes.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			var parsed errorResponse
			if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if parsed.Code != tt.wantCode {
				t.Fatalf("error code = %q, want %q", parsed.Code, tt.wantCode)
			}
		})
	}
}

func TestAuthorizationHeaderValidatesPayloadHashAndPreservesBody(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	create := signedRequest(t, http.MethodPut, "/demo-bucket", "", fixedNow)
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("signed create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := signedRequest(t, http.MethodPut, "/demo-bucket/docs/readme.txt", "signed body\n", fixedNow)
	put.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("signed put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	get := signedRequest(t, http.MethodGet, "/demo-bucket/docs/readme.txt", "", fixedNow)
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("signed get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "signed body\n" {
		t.Fatalf("signed get body = %q", got)
	}
}

func TestAuthorizationHeaderRejectsPayloadHashMismatch(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	create := signedRequest(t, http.MethodPut, "/demo-bucket", "", fixedNow)
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("signed create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := signedRequest(t, http.MethodPut, "/demo-bucket/docs/readme.txt", "original body", fixedNow)
	put.Body = io.NopCloser(strings.NewReader("tampered body"))
	put.ContentLength = int64(len("tampered body"))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusBadRequest {
		t.Fatalf("tampered put status = %d, want %d; body=%s", putRec.Code, http.StatusBadRequest, putRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(putRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode tampered put error: %v", err)
	}
	if parsed.Code != "XAmzContentSHA256Mismatch" {
		t.Fatalf("tampered put code = %q, want XAmzContentSHA256Mismatch", parsed.Code)
	}
}

func TestFileBucketStoreUpdateObjectMetadataPreservesBody(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	ctx := context.Background()
	if _, created, err := store.CreateBucket(ctx, "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket:             "demo-bucket",
		Key:                "docs/readme.txt",
		Body:               strings.NewReader("metadata body"),
		ContentType:        "text/plain",
		ContentDisposition: `inline; filename="readme.txt"`,
		Metadata:           map[string]string{"source": "original"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	updated, found, err := store.UpdateObjectMetadata(ctx, UpdateObjectMetadataInput{
		Bucket:          "demo-bucket",
		Key:             "docs/readme.txt",
		ContentType:     "text/markdown",
		ContentEncoding: "gzip",
		CacheControl:    "max-age=60",
		Metadata:        map[string]string{"source": "updated", "empty": ""},
	})
	if err != nil || !found {
		t.Fatalf("update metadata found=%t err=%v", found, err)
	}
	if updated.ContentType != "text/markdown" || updated.ContentEncoding != "gzip" || updated.CacheControl != "max-age=60" {
		t.Fatalf("updated headers = contentType:%q contentEncoding:%q cacheControl:%q", updated.ContentType, updated.ContentEncoding, updated.CacheControl)
	}
	if got := updated.ContentDisposition; got != `inline; filename="readme.txt"` {
		t.Fatalf("content disposition = %q, want preserved inline disposition", got)
	}
	if got := updated.Metadata["source"]; got != "updated" {
		t.Fatalf("metadata source = %q, want updated", got)
	}
	if got := updated.Metadata["empty"]; got != "" {
		t.Fatalf("empty metadata = %q, want empty value preserved", got)
	}
	if updated.Metageneration != 2 {
		t.Fatalf("metageneration = %d, want 2", updated.Metageneration)
	}

	gotObject, body, found, err := store.GetObject(ctx, "demo-bucket", "docs/readme.txt")
	if err != nil || !found {
		t.Fatalf("get updated object found=%t err=%v", found, err)
	}
	if string(body) != "metadata body" {
		t.Fatalf("body changed after metadata update: %q", string(body))
	}
	if gotObject.ETag != updated.ETag {
		t.Fatalf("etag changed on metadata update: got %q want %q", gotObject.ETag, updated.ETag)
	}

	if _, found, err := store.UpdateObjectMetadata(ctx, UpdateObjectMetadataInput{
		Bucket:      "demo-bucket",
		Key:         "missing.txt",
		ContentType: "text/plain",
	}); err != nil || found {
		t.Fatalf("update missing object found=%t err=%v", found, err)
	}
}

func TestMultipartUploadFlow(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiateReq := httptest.NewRequest(http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	initiateReq.Header.Set("Content-Type", "application/octet-stream")
	initiateReq.Header.Set("Cache-Control", "max-age=60")
	initiateReq.Header.Set("x-amz-meta-source", "multipart-test")
	initiate := httptest.NewRecorder()
	routes.ServeHTTP(initiate, initiateReq)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}
	if initiated.UploadID == "" {
		t.Fatal("initiate response missing UploadId")
	}

	partOne := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId="+initiated.UploadID, strings.NewReader("part-one-"))
	if partOne.Code != http.StatusOK {
		t.Fatalf("part one status = %d, want %d; body=%s", partOne.Code, http.StatusOK, partOne.Body.String())
	}
	if got := partOne.Header().Get("ETag"); got == "" {
		t.Fatal("part one missing ETag")
	}
	partTwo := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=2&uploadId="+initiated.UploadID, strings.NewReader("part-two"))
	if partTwo.Code != http.StatusOK {
		t.Fatalf("part two status = %d, want %d; body=%s", partTwo.Code, http.StatusOK, partTwo.Body.String())
	}

	listUploads := performRequest(routes, http.MethodGet, "/demo-bucket?uploads", nil)
	if listUploads.Code != http.StatusOK {
		t.Fatalf("list uploads status = %d, want %d; body=%s", listUploads.Code, http.StatusOK, listUploads.Body.String())
	}
	if !strings.Contains(listUploads.Body.String(), "<Key>large.bin</Key>") || !strings.Contains(listUploads.Body.String(), "<UploadId>"+initiated.UploadID+"</UploadId>") {
		t.Fatalf("list uploads missing initiated upload: %s", listUploads.Body.String())
	}

	listParts := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, nil)
	if listParts.Code != http.StatusOK {
		t.Fatalf("list parts status = %d, want %d; body=%s", listParts.Code, http.StatusOK, listParts.Body.String())
	}
	if !strings.Contains(listParts.Body.String(), "<PartNumber>1</PartNumber>") {
		t.Fatalf("list parts missing first part: %s", listParts.Body.String())
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusOK, complete.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get completed object status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != "part-one-part-two" {
		t.Fatalf("completed object body = %q", got)
	}
	if got := get.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("completed object Content-Type = %q, want application/octet-stream", got)
	}
	if got := get.Header().Get("Cache-Control"); got != "max-age=60" {
		t.Fatalf("completed object Cache-Control = %q, want max-age=60", got)
	}
	if got := get.Header().Get("x-amz-meta-source"); got != "multipart-test" {
		t.Fatalf("completed object metadata = %q, want multipart-test", got)
	}
	if got := get.Header().Get("ETag"); !strings.HasSuffix(got, `-2"`) {
		t.Fatalf("completed object ETag = %q, want multipart ETag with part count", got)
	}

	abortInit := performRequest(routes, http.MethodPost, "/demo-bucket/aborted.bin?uploads", nil)
	var abortUpload initiateMultipartUploadResult
	if err := xml.NewDecoder(abortInit.Body).Decode(&abortUpload); err != nil {
		t.Fatalf("decode abort initiate response: %v", err)
	}
	abort := performRequest(routes, http.MethodDelete, "/demo-bucket/aborted.bin?uploadId="+abortUpload.UploadID, nil)
	if abort.Code != http.StatusNoContent {
		t.Fatalf("abort status = %d, want %d; body=%s", abort.Code, http.StatusNoContent, abort.Body.String())
	}
	listAborted := performRequest(routes, http.MethodGet, "/demo-bucket/aborted.bin?uploadId="+abortUpload.UploadID, nil)
	if listAborted.Code != http.StatusNotFound {
		t.Fatalf("list aborted status = %d, want %d", listAborted.Code, http.StatusNotFound)
	}
}

func TestUploadPartValidatesContentMD5(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	partReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId="+initiated.UploadID, strings.NewReader("part-body"))
	partReq.Header.Set("Content-MD5", contentMD5("different-body"))
	partRec := httptest.NewRecorder()
	routes.ServeHTTP(partRec, partReq)
	if partRec.Code != http.StatusBadRequest {
		t.Fatalf("part with bad md5 status = %d, want %d; body=%s", partRec.Code, http.StatusBadRequest, partRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(partRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode part md5 mismatch error: %v", err)
	}
	if parsed.Code != "BadDigest" {
		t.Fatalf("part md5 mismatch code = %q, want BadDigest", parsed.Code)
	}
}

func TestUploadPartRejectsOutOfRangePartNumber(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	part := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=10001&uploadId="+initiated.UploadID, strings.NewReader("part-body"))
	if part.Code != http.StatusBadRequest {
		t.Fatalf("part status = %d, want %d; body=%s", part.Code, http.StatusBadRequest, part.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(part.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode part number error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("part number error code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestCompleteMultipartUploadRejectsEmptyPartList(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	if complete.Code != http.StatusBadRequest {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusBadRequest, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode empty complete error: %v", err)
	}
	if parsed.Code != "MalformedXML" {
		t.Fatalf("empty complete error code = %q, want MalformedXML", parsed.Code)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusNotFound {
		t.Fatalf("empty complete created object; get status = %d, want %d", get.Code, http.StatusNotFound)
	}
}

func TestListPartsSupportsPagination(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}
	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "one"},
		{number: 2, body: "two"},
		{number: 3, body: "three"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID+"&max-parts=2", nil)
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first listPartsResult
	if err := xml.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if !first.IsTruncated {
		t.Fatal("first page IsTruncated = false, want true")
	}
	if first.MaxParts != 2 || first.NextPartNumberMarker != 2 {
		t.Fatalf("first page markers MaxParts=%d NextPartNumberMarker=%d", first.MaxParts, first.NextPartNumberMarker)
	}
	if len(first.Parts) != 2 || first.Parts[0].PartNumber != 1 || first.Parts[1].PartNumber != 2 {
		t.Fatalf("first page parts = %#v", first.Parts)
	}

	secondPage := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID+"&part-number-marker=2&max-parts=2", nil)
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second listPartsResult
	if err := xml.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.IsTruncated {
		t.Fatal("second page IsTruncated = true, want false")
	}
	if second.PartNumberMarker != 2 || second.NextPartNumberMarker != 0 {
		t.Fatalf("second page markers PartNumberMarker=%d NextPartNumberMarker=%d", second.PartNumberMarker, second.NextPartNumberMarker)
	}
	if len(second.Parts) != 1 || second.Parts[0].PartNumber != 3 {
		t.Fatalf("second page parts = %#v", second.Parts)
	}
}

func TestCompleteMultipartUploadRejectsOutOfOrderParts(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "part-one-"},
		{number: 2, body: "part-two"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusBadRequest {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusBadRequest, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode complete error: %v", err)
	}
	if parsed.Code != "InvalidPartOrder" {
		t.Fatalf("complete error code = %q, want InvalidPartOrder", parsed.Code)
	}
}

func TestCompleteMultipartUploadRejectsCombinedObjectOverMaxBytes(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{MaxObjectBytes: 10}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "123456"},
		{number: 2, body: "78901"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusRequestEntityTooLarge, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode complete error: %v", err)
	}
	if parsed.Code != "EntityTooLarge" {
		t.Fatalf("complete error code = %q, want EntityTooLarge", parsed.Code)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusNotFound {
		t.Fatalf("oversized multipart object should not be stored; get status = %d, want %d", get.Code, http.StatusNotFound)
	}
}

func TestMultipartUploadRejectsInvalidUploadID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId=../escape", strings.NewReader("part"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid upload id status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode invalid upload id error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("invalid upload id code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestStrictAuthRejectsUnsignedRequests(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()

	rec := performRequest(routes, http.MethodGet, "/", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("strict unsigned status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func performRequest(handler http.Handler, method string, target string, body *strings.Reader) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == nil {
		reader = strings.NewReader("")
	} else {
		reader = body
	}
	req := httptest.NewRequest(method, target, reader)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func eventStreamRecords(t *testing.T, data []byte) []byte {
	t.Helper()
	var records []byte
	for len(data) > 0 {
		if len(data) < 16 {
			t.Fatalf("eventstream message too short: %d bytes", len(data))
		}
		totalLength := int(binary.BigEndian.Uint32(data[0:4]))
		headersLength := int(binary.BigEndian.Uint32(data[4:8]))
		if totalLength < 16 || totalLength > len(data) {
			t.Fatalf("eventstream total length = %d for %d bytes", totalLength, len(data))
		}
		if got, want := crc32.ChecksumIEEE(data[0:8]), binary.BigEndian.Uint32(data[8:12]); got != want {
			t.Fatalf("eventstream prelude crc = %08x, want %08x", got, want)
		}
		if got, want := crc32.ChecksumIEEE(data[:totalLength-4]), binary.BigEndian.Uint32(data[totalLength-4:totalLength]); got != want {
			t.Fatalf("eventstream message crc = %08x, want %08x", got, want)
		}
		payloadStart := 12 + headersLength
		if payloadStart > totalLength-4 {
			t.Fatalf("eventstream headers length = %d exceeds message length %d", headersLength, totalLength)
		}
		records = append(records, data[payloadStart:totalLength-4]...)
		data = data[totalLength:]
	}
	return records
}

func containsString(values []string, target string) bool {
	return indexOfString(values, target) >= 0
}

func indexOfString(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

func presignedTarget(t *testing.T, method string, host string, path string, now time.Time) string {
	t.Helper()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	values := url.Values{}
	values.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	values.Set("X-Amz-Credential", "dev/"+scope)
	values.Set("X-Amz-Date", amzDate)
	values.Set("X-Amz-Expires", "300")
	values.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := testCanonicalQuery(values)
	canonicalRequest := strings.Join([]string{
		method,
		path,
		canonicalQuery,
		"host:" + host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		testSHA256Hex(canonicalRequest),
	}, "\n")
	signingKey := testSigningKey("dev", dateStamp, "us-east-1")
	signature := hmac.New(sha256.New, signingKey)
	signature.Write([]byte(stringToSign))
	values.Set("X-Amz-Signature", hex.EncodeToString(signature.Sum(nil)))
	return path + "?" + testCanonicalQuery(values)
}

func testCanonicalQuery(values url.Values) string {
	type pair struct {
		key   string
		value string
	}
	var pairs []pair
	for key, vals := range values {
		for _, val := range vals {
			pairs = append(pairs, pair{key: key, value: val})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	out := make([]string, 0, len(pairs))
	for _, item := range pairs {
		out = append(out, awsPercentEncode(item.key, "~-_")+"="+awsPercentEncode(item.value, "~-_"))
	}
	return strings.Join(out, "&")
}

func testSigningKey(secret string, dateStamp string, region string) []byte {
	sign := func(key []byte, value string) []byte {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(value))
		return mac.Sum(nil)
	}
	dateKey := sign([]byte("AWS4"+secret), dateStamp)
	regionKey := sign(dateKey, region)
	serviceKey := sign(regionKey, "s3")
	return sign(serviceKey, "aws4_request")
}

func testSHA256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func contentMD5(value string) string {
	sum := md5.Sum([]byte(value))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func signedRequest(t *testing.T, method string, path string, body string, now time.Time) *http.Request {
	t.Helper()
	bodyHash := testSHA256Hex(body)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "example.com"
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", bodyHash)
	req.Header.Set("Authorization", authorizationHeader(t, method, path, "example.com", bodyHash, amzDate))
	return req
}

func authorizationHeader(t *testing.T, method string, path string, host string, bodyHash string, amzDate string) string {
	t.Helper()
	dateStamp := amzDate[:8]
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		method,
		path,
		"",
		"host:" + host + "\n" +
			"x-amz-content-sha256:" + bodyHash + "\n" +
			"x-amz-date:" + amzDate + "\n",
		signedHeaders,
		bodyHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		testSHA256Hex(canonicalRequest),
	}, "\n")
	signature := hmac.New(sha256.New, testSigningKey("dev", dateStamp, "us-east-1"))
	signature.Write([]byte(stringToSign))
	return "AWS4-HMAC-SHA256 Credential=dev/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + hex.EncodeToString(signature.Sum(nil))
}
