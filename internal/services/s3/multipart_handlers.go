package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	encryption, err := serverSideEncryptionFromHeaders(r.Header)
	if err != nil {
		writeServerSideEncryptionError(w, err)
		return
	}
	upload, err := s.store.CreateMultipartUpload(r.Context(), CreateMultipartUploadInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           userMetadataFromHeaders(r.Header),
		Encryption:         encryption,
	})
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeServerSideEncryptionHeaders(w, upload.Encryption)
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		XMLName:  xml.Name{Local: "InitiateMultipartUploadResult"},
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadID: upload.UploadID,
	})
}

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket string, key string, uploadID string) {
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber <= 0 || partNumber > 10000 {
		writeXMLError(w, "InvalidArgument", "invalid part number", http.StatusBadRequest)
		return
	}
	body := r.Body
	if s.config.MaxObjectBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, s.config.MaxObjectBytes)
	}
	defer body.Close()
	part, err := s.store.UploadPart(r.Context(), bucket, key, uploadID, partNumber, body, r.Header.Get("Content-MD5"))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeXMLError(w, "EntityTooLarge", "part is too large", http.StatusRequestEntityTooLarge)
			return
		}
		if errors.Is(err, errInvalidContentMD5) {
			writeXMLError(w, "InvalidDigest", "the Content-MD5 you specified was invalid", http.StatusBadRequest)
			return
		}
		if errors.Is(err, errContentMD5Mismatch) {
			writeXMLError(w, "BadDigest", "the Content-MD5 you specified did not match what was received", http.StatusBadRequest)
			return
		}
		writeXMLError(w, "NoSuchUpload", "multipart upload does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", part.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request, bucket string, key string, uploadID string) {
	upload, parts, ok, err := s.store.ListParts(r.Context(), bucket, key, uploadID)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchUpload", "multipart upload does not exist", http.StatusNotFound)
		return
	}
	maxParts, err := parseMaxParts(r.URL.Query().Get("max-parts"))
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid max-parts", http.StatusBadRequest)
		return
	}
	partNumberMarker, err := parsePartNumberMarker(r.URL.Query().Get("part-number-marker"))
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid part-number-marker", http.StatusBadRequest)
		return
	}
	page, truncated, nextPartNumberMarker := paginateParts(parts, partNumberMarker, maxParts)
	response := listPartsResult{
		XMLName:              xml.Name{Local: "ListPartsResult"},
		Xmlns:                "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:               upload.Bucket,
		Key:                  upload.Key,
		UploadID:             upload.UploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          truncated,
	}
	for _, part := range page {
		response.Parts = append(response.Parts, partElement{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified.Format(time.RFC3339),
			ETag:         part.ETag,
			Size:         part.Size,
		})
	}
	writeXML(w, http.StatusOK, response)
}

func (s *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string, uploadID string) {
	var request completeMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if len(request.Parts) == 0 {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	partNumbers := make([]int, 0, len(request.Parts))
	previousPartNumber := 0
	for _, part := range request.Parts {
		if part.PartNumber <= 0 {
			writeXMLError(w, "InvalidPart", "invalid multipart part", http.StatusBadRequest)
			return
		}
		if part.PartNumber <= previousPartNumber {
			writeXMLError(w, "InvalidPartOrder", "multipart parts must be in ascending order", http.StatusBadRequest)
			return
		}
		partNumbers = append(partNumbers, part.PartNumber)
		previousPartNumber = part.PartNumber
	}
	if s.config.MaxObjectBytes > 0 {
		exceeds, ok, err := s.multipartCompletionExceedsMaxBytes(r.Context(), bucket, key, uploadID, partNumbers)
		if err != nil {
			writeXMLError(w, "InvalidPart", "multipart part is missing", http.StatusBadRequest)
			return
		}
		if !ok {
			writeXMLError(w, "NoSuchUpload", "multipart upload does not exist", http.StatusNotFound)
			return
		}
		if exceeds {
			writeXMLError(w, "EntityTooLarge", "object is too large", http.StatusRequestEntityTooLarge)
			return
		}
	}
	object, ok, err := s.store.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, partNumbers)
	if err != nil {
		writeXMLError(w, "InvalidPart", "multipart part is missing", http.StatusBadRequest)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchUpload", "multipart upload does not exist", http.StatusNotFound)
		return
	}
	if err := s.recordObjectEvent(r.Context(), bucket, key, "s3:ObjectCreated:CompleteMultipartUpload", object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.replicateObjectWrite(r.Context(), bucket, key, object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", object.ETag)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
	writeServerSideEncryptionHeaders(w, object.Encryption)
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		XMLName:  xml.Name{Local: "CompleteMultipartUploadResult"},
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     object.ETag,
	})
}

func (s *Server) multipartCompletionExceedsMaxBytes(ctx context.Context, bucket string, key string, uploadID string, partNumbers []int) (bool, bool, error) {
	_, parts, ok, err := s.store.ListParts(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return false, ok, err
	}
	partsByNumber := make(map[int]MultipartPart, len(parts))
	for _, part := range parts {
		partsByNumber[part.PartNumber] = part
	}
	var total int64
	for _, partNumber := range partNumbers {
		part, ok := partsByNumber[partNumber]
		if !ok {
			return false, true, fmt.Errorf("multipart part %d does not exist", partNumber)
		}
		total += part.Size
		if total > s.config.MaxObjectBytes {
			return true, true, nil
		}
	}
	return false, true, nil
}

func (s *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string, uploadID string) {
	ok, err := s.store.AbortMultipartUpload(r.Context(), bucket, key, uploadID)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchUpload", "multipart upload does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	uploads, ok, err := s.store.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	response := listMultipartUploadsResult{
		XMLName:     xml.Name{Local: "ListMultipartUploadsResult"},
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:      bucket,
		IsTruncated: false,
	}
	for _, upload := range uploads {
		response.Uploads = append(response.Uploads, uploadElement{
			Key:          upload.Key,
			UploadID:     upload.UploadID,
			Initiated:    upload.CreatedAt.Format(time.RFC3339),
			StorageClass: "STANDARD",
		})
	}
	writeXML(w, http.StatusOK, response)
}
func parseMaxParts(value string) (int, error) {
	if value == "" {
		return 1000, nil
	}
	maxParts, err := strconv.Atoi(value)
	if err != nil || maxParts < 0 {
		return 0, fmt.Errorf("invalid max-parts")
	}
	if maxParts > 1000 {
		return 1000, nil
	}
	return maxParts, nil
}

func parsePartNumberMarker(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	marker, err := strconv.Atoi(value)
	if err != nil || marker < 0 {
		return 0, fmt.Errorf("invalid part-number-marker")
	}
	return marker, nil
}

func paginateParts(parts []MultipartPart, partNumberMarker int, maxParts int) ([]MultipartPart, bool, int) {
	if maxParts == 0 {
		for _, part := range parts {
			if part.PartNumber > partNumberMarker {
				return nil, true, partNumberMarker
			}
		}
		return nil, false, 0
	}

	pageCapacity := maxParts
	if len(parts) < pageCapacity {
		pageCapacity = len(parts)
	}
	page := make([]MultipartPart, 0, pageCapacity)
	nextPartNumberMarker := 0
	for _, part := range parts {
		if part.PartNumber <= partNumberMarker {
			continue
		}
		if len(page) >= maxParts {
			return page, true, nextPartNumberMarker
		}
		page = append(page, part)
		nextPartNumberMarker = part.PartNumber
	}
	return page, false, 0
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type listPartsResult struct {
	XMLName              xml.Name      `xml:"ListPartsResult"`
	Xmlns                string        `xml:"xmlns,attr"`
	Bucket               string        `xml:"Bucket"`
	Key                  string        `xml:"Key"`
	UploadID             string        `xml:"UploadId"`
	PartNumberMarker     int           `xml:"PartNumberMarker"`
	NextPartNumberMarker int           `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int           `xml:"MaxParts"`
	IsTruncated          bool          `xml:"IsTruncated"`
	Parts                []partElement `xml:"Part"`
}

type partElement struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type completeMultipartUpload struct {
	Parts []completeMultipartPart `xml:"Part"`
}

type completeMultipartPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName     xml.Name        `xml:"ListMultipartUploadsResult"`
	Xmlns       string          `xml:"xmlns,attr"`
	Bucket      string          `xml:"Bucket"`
	IsTruncated bool            `xml:"IsTruncated"`
	Uploads     []uploadElement `xml:"Upload"`
}

type uploadElement struct {
	Key          string `xml:"Key"`
	UploadID     string `xml:"UploadId"`
	Initiated    string `xml:"Initiated"`
	StorageClass string `xml:"StorageClass"`
}
