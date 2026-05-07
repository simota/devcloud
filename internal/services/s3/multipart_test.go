package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

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

