package s3

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

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

