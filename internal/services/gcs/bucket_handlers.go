package gcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	s3svc "devcloud/internal/services/s3"
)

func (s *Server) handleBuckets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		buckets, err := s.store.ListBuckets(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
		prefix := r.URL.Query().Get("prefix")
		items := make([]bucketResource, 0, len(buckets))
		for _, bucket := range buckets {
			if prefix != "" && !strings.HasPrefix(bucket.Name, prefix) {
				continue
			}
			items = append(items, s.bucketResource(bucket))
		}
		start, end, nextToken, err := paginationWindow(r.URL.Query(), len(items))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, bucketsListResponse{
			Kind:          "storage#buckets",
			Items:         items[start:end],
			NextPageToken: nextToken,
		})
	case http.MethodPost:
		var request struct {
			Name         string `json:"name"`
			Location     string `json:"location"`
			StorageClass string `json:"storageClass"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
			return
		}
		if request.Name == "" {
			writeError(w, http.StatusBadRequest, "required", "bucket name is required")
			return
		}
		bucket, created, err := s.store.CreateBucket(r.Context(), request.Name)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if !created {
			writeError(w, http.StatusConflict, "conflict", "bucket already exists")
			return
		}
		resource := s.bucketResource(bucket)
		if request.Location != "" {
			resource.Location = request.Location
		}
		if request.StorageClass != "" {
			resource.StorageClass = request.StorageClass
		}
		w.Header().Set("Location", "/storage/v1/b/"+url.PathEscape(bucket.Name))
		writeJSON(w, http.StatusOK, resource)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	name, ok := bucketNameFromPath(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		bucket, found, err := s.store.GetBucket(r.Context(), name)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		writeJSON(w, http.StatusOK, s.bucketResource(bucket))
	case http.MethodDelete:
		deleted, err := s.store.DeleteBucket(r.Context(), name)
		if err != nil {
			writeError(w, http.StatusConflict, "conflict", err.Error())
			return
		}
		if !deleted {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleBucketOrObject(w http.ResponseWriter, r *http.Request) {
	bucket, suffix, ok := bucketAndSuffixFromPath(r.URL.EscapedPath(), "/storage/v1/b/")
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "not found")
		return
	}
	if suffix == "" {
		s.handleBucket(w, r)
		return
	}
	if suffix == "o" {
		s.handleObjects(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "o/") {
		s.handleObjectOrCopy(w, r, bucket, strings.TrimPrefix(suffix, "o/"))
		return
	}
	writeError(w, http.StatusNotFound, "notFound", "not found")
}

func (s *Server) bucketResource(bucket s3svc.Bucket) bucketResource {
	location := s.config.Location
	if location == "" {
		location = "US"
	}
	return bucketResource{
		Kind:          "storage#bucket",
		ID:            bucket.Name,
		Name:          bucket.Name,
		ProjectNumber: "0",
		Location:      location,
		StorageClass:  "STANDARD",
		TimeCreated:   bucket.CreatedAt.Format(time.RFC3339Nano),
		Updated:       bucket.CreatedAt.Format(time.RFC3339Nano),
		SelfLink:      fmt.Sprintf("/storage/v1/b/%s", url.PathEscape(bucket.Name)),
	}
}
