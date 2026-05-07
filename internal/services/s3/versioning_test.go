package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

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

