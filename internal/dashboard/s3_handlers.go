package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	s3svc "devcloud/internal/services/s3"
)

type s3BucketSummary struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creationDate"`
	ObjectCount  int       `json:"objectCount"`
}

type s3ObjectSummary struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"contentType"`
	LastModified time.Time         `json:"lastModified"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	S3URI        string            `json:"s3Uri"`
	DownloadURL  string            `json:"downloadUrl"`
}

type s3MultipartUploadSummary struct {
	Key         string            `json:"key"`
	UploadID    string            `json:"uploadId"`
	Initiated   time.Time         `json:"initiated"`
	ContentType string            `json:"contentType"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (s *Server) handleS3Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	if s.s3 != nil {
		status = "running"
		running = true
	}
	writeJSON(w, map[string]any{
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.S3Endpoint, "http://127.0.0.1:4566"),
		"region":      defaultString(s.config.S3Region, "us-east-1"),
		"authMode":    defaultString(s.config.S3AuthMode, "relaxed"),
		"storagePath": defaultString(s.config.S3StoragePath, ".devcloud/data/s3"),
	})
}

func (s *Server) handleS3Buckets(w http.ResponseWriter, r *http.Request) {
	if s.s3 == nil {
		http.Error(w, "s3 service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		buckets, err := s.s3.ListBuckets(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := struct {
			Buckets []s3BucketSummary `json:"buckets"`
		}{Buckets: make([]s3BucketSummary, 0, len(buckets))}
		for _, bucket := range buckets {
			objects, _, err := s.s3.ListObjects(r.Context(), bucket.Name, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response.Buckets = append(response.Buckets, s3BucketSummary{
				Name:         bucket.Name,
				CreationDate: bucket.CreatedAt,
				ObjectCount:  len(objects),
			})
		}
		writeJSON(w, response)
	case http.MethodPost:
		var request struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		bucket, created, err := s.s3.CreateBucket(r.Context(), request.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(s3BucketSummary{Name: bucket.Name, CreationDate: bucket.CreatedAt})
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleS3Bucket(w http.ResponseWriter, r *http.Request) {
	if s.s3 == nil {
		http.Error(w, "s3 service is disabled", http.StatusServiceUnavailable)
		return
	}
	bucketPath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/s3/buckets/")
	escapedBucket, suffix, ok := strings.Cut(bucketPath, "/")
	bucket, err := url.PathUnescape(escapedBucket)
	if err != nil {
		http.Error(w, "invalid bucket path", http.StatusBadRequest)
		return
	}
	if bucket == "" {
		http.NotFound(w, r)
		return
	}
	if !ok {
		s.handleS3BucketDetail(w, r, bucket)
		return
	}
	if suffix == "objects" {
		s.handleS3Objects(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "objects/") {
		s.handleS3Object(w, r, bucket, strings.TrimPrefix(suffix, "objects/"))
		return
	}
	if suffix == "multipart" {
		s.handleS3MultipartUploads(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "multipart/") {
		s.handleS3MultipartUpload(w, r, bucket, strings.TrimPrefix(suffix, "multipart/"))
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleS3BucketDetail(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		item, ok, err := s.s3.GetBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		objects, _, err := s.s3.ListObjects(r.Context(), bucket, "")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, s3BucketSummary{Name: item.Name, CreationDate: item.CreatedAt, ObjectCount: len(objects)})
	case http.MethodDelete:
		deleted, err := s.s3.DeleteBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if !deleted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleS3Objects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	objects, ok, err := s.s3.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	response := struct {
		Bucket  string            `json:"bucket"`
		Prefix  string            `json:"prefix"`
		Objects []s3ObjectSummary `json:"objects"`
	}{
		Bucket:  bucket,
		Prefix:  prefix,
		Objects: make([]s3ObjectSummary, 0, len(objects)),
	}
	for _, object := range objects {
		response.Objects = append(response.Objects, s3ObjectResponse(bucket, object))
	}
	writeJSON(w, response)
}

func (s *Server) handleS3MultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	uploads, ok, err := s.s3.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	response := struct {
		Bucket  string                     `json:"bucket"`
		Uploads []s3MultipartUploadSummary `json:"uploads"`
	}{
		Bucket:  bucket,
		Uploads: make([]s3MultipartUploadSummary, 0, len(uploads)),
	}
	for _, upload := range uploads {
		response.Uploads = append(response.Uploads, s3MultipartUploadSummary{
			Key:         upload.Key,
			UploadID:    upload.UploadID,
			Initiated:   upload.CreatedAt,
			ContentType: upload.ContentType,
			Metadata:    upload.Metadata,
		})
	}
	writeJSON(w, response)
}

func (s *Server) handleS3MultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, escapedUploadID string) {
	if r.Method != http.MethodDelete {
		methodNotAllowed(w, "DELETE")
		return
	}
	uploadID, err := url.PathUnescape(escapedUploadID)
	if err != nil || uploadID == "" {
		http.Error(w, "invalid upload id", http.StatusBadRequest)
		return
	}
	uploads, ok, err := s.s3.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	for _, upload := range uploads {
		if upload.UploadID != uploadID {
			continue
		}
		aborted, err := s.s3.AbortMultipartUpload(r.Context(), bucket, upload.Key, uploadID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !aborted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleS3Object(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	if path == "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleS3ObjectDownload(w, r, bucket, path)
	case http.MethodPut:
		s.handleS3ObjectPut(w, r, bucket, path)
	case http.MethodDelete:
		key, err := url.PathUnescape(path)
		if err != nil {
			http.Error(w, "invalid object path", http.StatusBadRequest)
			return
		}
		deleted, err := s.s3.DeleteObject(r.Context(), bucket, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !deleted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (s *Server) handleS3ObjectPut(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	key, err := url.PathUnescape(path)
	if err != nil {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	if key == "" {
		http.NotFound(w, r)
		return
	}

	copySource := r.Header.Get("x-amz-copy-source")
	var object s3svc.Object
	if copySource == "" {
		defer r.Body.Close()
		object, err = s.s3.PutObject(r.Context(), s3svc.PutObjectInput{
			Bucket:             bucket,
			Key:                key,
			Body:               r.Body,
			ContentType:        r.Header.Get("Content-Type"),
			ContentEncoding:    r.Header.Get("Content-Encoding"),
			CacheControl:       r.Header.Get("Cache-Control"),
			ContentDisposition: r.Header.Get("Content-Disposition"),
			Metadata:           s3UserMetadataFromHeaders(r.Header),
		})
	} else {
		sourceBucket, sourceKey, parseErr := parseDashboardS3CopySource(copySource)
		if parseErr != nil {
			http.Error(w, parseErr.Error(), http.StatusBadRequest)
			return
		}
		sourceObject, body, found, getErr := s.s3.GetObject(r.Context(), sourceBucket, sourceKey)
		if getErr != nil {
			http.Error(w, getErr.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		input := s3svc.PutObjectInput{
			Bucket:             bucket,
			Key:                key,
			Body:               bytes.NewReader(body),
			ContentType:        sourceObject.ContentType,
			ContentEncoding:    sourceObject.ContentEncoding,
			CacheControl:       sourceObject.CacheControl,
			ContentDisposition: sourceObject.ContentDisposition,
			Metadata:           sourceObject.Metadata,
		}
		if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
			input.ContentType = r.Header.Get("Content-Type")
			input.ContentEncoding = r.Header.Get("Content-Encoding")
			input.CacheControl = r.Header.Get("Cache-Control")
			input.ContentDisposition = r.Header.Get("Content-Disposition")
			input.Metadata = s3UserMetadataFromHeaders(r.Header)
		}
		object, err = s.s3.PutObject(r.Context(), input)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("ETag", object.ETag)
	writeJSON(w, s3ObjectResponse(bucket, object))
}

func (s *Server) handleS3ObjectDownload(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	escapedKey, ok := strings.CutSuffix(path, "/download")
	if !ok || escapedKey == "" {
		http.NotFound(w, r)
		return
	}
	key, err := url.PathUnescape(escapedKey)
	if err != nil {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	object, body, found, err := s.s3.GetObject(r.Context(), bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	contentType := object.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	} else {
		w.Header().Set("Content-Disposition", `attachment; filename="`+downloadFilename(key)+`"`)
	}
	for key, value := range object.Metadata {
		w.Header().Set("x-amz-meta-"+key, value)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func s3ObjectResponse(bucket string, object s3svc.Object) s3ObjectSummary {
	return s3ObjectSummary{
		Key:          object.Key,
		Size:         object.Size,
		ETag:         object.ETag,
		ContentType:  object.ContentType,
		LastModified: object.LastModified,
		Metadata:     object.Metadata,
		S3URI:        "s3://" + bucket + "/" + object.Key,
		DownloadURL:  "/api/s3/buckets/" + url.PathEscape(bucket) + "/objects/" + url.PathEscape(object.Key) + "/download",
	}
}

func s3UserMetadataFromHeaders(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-amz-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func parseDashboardS3CopySource(source string) (string, string, error) {
	trimmed := strings.TrimPrefix(source, "/")
	sourceBucket, sourceKey, ok := strings.Cut(trimmed, "/")
	if !ok || sourceBucket == "" || sourceKey == "" {
		return "", "", errors.New("invalid copy source")
	}
	bucket, err := url.PathUnescape(sourceBucket)
	if err != nil {
		return "", "", errors.New("invalid copy source bucket")
	}
	key, err := url.PathUnescape(sourceKey)
	if err != nil {
		return "", "", errors.New("invalid copy source key")
	}
	return bucket, key, nil
}
