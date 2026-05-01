package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"devcloud/internal/services/mail"
	s3svc "devcloud/internal/services/s3"
	"devcloud/internal/storage/mailstore"
)

type Config struct {
	Addr                 string
	MailDisabled         bool
	MailEndpoint         string
	MailStoragePath      string
	S3Endpoint           string
	S3Region             string
	S3AuthMode           string
	S3StoragePath        string
	GCSEndpoint          string
	GCSProject           string
	GCSStoragePath       string
	GCSUploadSessionPath string
}

type Server struct {
	config Config
	store  mailstore.Store
	s3     s3svc.BucketStore
	gcs    s3svc.BucketStore
}

func NewServer(cfg Config, store mailstore.Store, objectStores ...s3svc.BucketStore) *Server {
	server := &Server{config: cfg, store: store}
	if len(objectStores) > 0 {
		server.s3 = objectStores[0]
	}
	if len(objectStores) > 1 {
		server.gcs = objectStores[1]
	}
	return server
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleServiceIndex)
	mux.HandleFunc("/dashboard", s.handleReactDashboardAssets)
	mux.HandleFunc("/dashboard/", s.handleReactDashboardAssets)
	mux.HandleFunc("/mail", s.handleMailIndex)
	mux.HandleFunc("/s3", s.handleS3Index)
	mux.HandleFunc("/gcs", s.handleGCSIndex)
	mux.HandleFunc("/api/dashboard/services", s.handleDashboardServices)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/messages/", s.handleMessage)
	mux.HandleFunc("/api/s3/status", s.handleS3Status)
	mux.HandleFunc("/api/s3/buckets", s.handleS3Buckets)
	mux.HandleFunc("/api/s3/buckets/", s.handleS3Bucket)
	mux.HandleFunc("/api/gcs/status", s.handleGCSStatus)
	mux.HandleFunc("/api/gcs/buckets", s.handleGCSBuckets)
	mux.HandleFunc("/api/gcs/buckets/", s.handleGCSBucket)
	mux.HandleFunc("/api/gcs/uploads", s.handleGCSUploadSessions)
	mux.HandleFunc("/api/gcs/uploads/", s.handleGCSUploadSession)
	mux.HandleFunc("/api/gcs/upload-sessions", s.handleGCSUploadSessions)
	return mux
}

func (s *Server) handleServiceIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, serviceIndexHTML)
}

func (s *Server) handleMailIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, indexHTML)
}

func (s *Server) handleS3Index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, s3IndexHTML)
}

func (s *Server) handleGCSIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, gcsIndexHTML)
}

func (s *Server) handleDashboardServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	writeJSON(w, dashboardServicesResponse{Services: s.dashboardServices()})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		result, err := s.store.List(r.Context(), mail.ListMessagesInput{Limit: 100})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, result)
	case http.MethodDelete:
		if err := s.store.DeleteAll(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
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
		s.handleS3ObjectDownload(w, r, bucket, strings.TrimPrefix(suffix, "objects/"))
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
		response.Objects = append(response.Objects, s3ObjectSummary{
			Key:          object.Key,
			Size:         object.Size,
			ETag:         object.ETag,
			ContentType:  object.ContentType,
			LastModified: object.LastModified,
			Metadata:     object.Metadata,
			S3URI:        "s3://" + bucket + "/" + object.Key,
			DownloadURL:  "/api/s3/buckets/" + url.PathEscape(bucket) + "/objects/" + url.PathEscape(object.Key) + "/download",
		})
	}
	writeJSON(w, response)
}

func (s *Server) handleS3ObjectDownload(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
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

func (s *Server) handleGCSStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	if s.gcs != nil {
		status = "running"
		running = true
	}
	writeJSON(w, map[string]any{
		"status":            status,
		"running":           running,
		"endpoint":          defaultString(s.config.GCSEndpoint, "http://127.0.0.1:4443"),
		"project":           defaultString(s.config.GCSProject, "devcloud"),
		"storagePath":       defaultString(s.config.GCSStoragePath, ".devcloud/data/s3"),
		"uploadSessionPath": defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions"),
	})
}

func (s *Server) handleGCSUploadSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	sessions, err := listGCSUploadSessions(defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, struct {
		Sessions []gcsUploadSessionSummary `json:"sessions"`
	}{Sessions: sessions})
}

func (s *Server) handleGCSUploadSession(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	id, ok := gcsUploadSessionIDFromPath(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		root := defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions")
		if err := os.RemoveAll(filepath.Join(root, id)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "DELETE")
	}
}

func (s *Server) handleGCSBuckets(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		buckets, err := s.gcs.ListBuckets(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := struct {
			Buckets []gcsBucketSummary `json:"buckets"`
		}{Buckets: make([]gcsBucketSummary, 0, len(buckets))}
		for _, bucket := range buckets {
			objects, _, err := s.gcs.ListObjects(r.Context(), bucket.Name, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response.Buckets = append(response.Buckets, gcsBucketSummary{
				Name:        bucket.Name,
				TimeCreated: bucket.CreatedAt,
				ObjectCount: len(objects),
				GCSURI:      "gs://" + bucket.Name,
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
		bucket, created, err := s.gcs.CreateBucket(r.Context(), request.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !created {
			http.Error(w, "bucket already exists", http.StatusConflict)
			return
		}
		writeJSONStatus(w, http.StatusCreated, gcsBucketSummary{Name: bucket.Name, TimeCreated: bucket.CreatedAt, GCSURI: "gs://" + bucket.Name})
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleGCSBucket(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	bucketPath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/gcs/buckets/")
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
		s.handleGCSBucketDetail(w, r, bucket)
		return
	}
	if suffix == "objects" {
		s.handleGCSObjects(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "objects/") {
		s.handleGCSObjectDownload(w, r, bucket, strings.TrimPrefix(suffix, "objects/"))
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleGCSBucketDetail(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		item, ok, err := s.gcs.GetBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		objects, _, err := s.gcs.ListObjects(r.Context(), bucket, "")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, gcsBucketSummary{Name: item.Name, TimeCreated: item.CreatedAt, ObjectCount: len(objects), GCSURI: "gs://" + item.Name})
	case http.MethodDelete:
		deleted, err := s.gcs.DeleteBucket(r.Context(), bucket)
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

func (s *Server) handleGCSObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	objects, ok, err := s.gcs.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	response := struct {
		Bucket  string             `json:"bucket"`
		Prefix  string             `json:"prefix"`
		Objects []gcsObjectSummary `json:"objects"`
	}{
		Bucket:  bucket,
		Prefix:  prefix,
		Objects: make([]gcsObjectSummary, 0, len(objects)),
	}
	for _, object := range objects {
		response.Objects = append(response.Objects, gcsObjectSummaryFromObject(bucket, object))
	}
	writeJSON(w, response)
}

func (s *Server) handleGCSObjectDownload(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	escapedName, download := strings.CutSuffix(path, "/download")
	if download {
		s.handleGCSObjectMediaDownload(w, r, bucket, escapedName)
		return
	}

	name, err := url.PathUnescape(path)
	if err != nil || name == "" {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		object, _, found, err := s.gcs.GetObject(r.Context(), bucket, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, gcsObjectSummaryFromObject(bucket, object))
	case http.MethodDelete:
		deleted, err := s.gcs.DeleteObject(r.Context(), bucket, name)
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
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleGCSObjectMediaDownload(w http.ResponseWriter, r *http.Request, bucket string, escapedName string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if escapedName == "" {
		http.NotFound(w, r)
		return
	}
	name, err := url.PathUnescape(escapedName)
	if err != nil {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	object, body, found, err := s.gcs.GetObject(r.Context(), bucket, name)
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
	w.Header().Set("Content-Disposition", `attachment; filename="`+downloadFilename(name)+`"`)
	for key, value := range object.Metadata {
		w.Header().Set("x-goog-meta-"+key, value)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	id, raw, ok := parseMessagePath(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if id == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if raw {
			rc, ok, err := s.store.GetRaw(r.Context(), id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				http.NotFound(w, r)
				return
			}
			defer rc.Close()
			w.Header().Set("Content-Type", "message/rfc822")
			io.Copy(w, rc)
			return
		}
		message, ok, err := s.store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, message)
	case http.MethodDelete:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		if err := s.store.Delete(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		methodNotAllowed(w, "GET, DELETE")
	}
}

func parseMessagePath(path string) (id string, raw bool, ok bool) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", false, false
	}
	if strings.Contains(path, "/") {
		id, suffix, found := strings.Cut(path, "/")
		return id, suffix == "raw", found && suffix == "raw"
	}
	return path, false, true
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func downloadFilename(key string) string {
	name := key
	if index := strings.LastIndex(name, "/"); index >= 0 {
		name = name[index+1:]
	}
	if name == "" {
		return "object"
	}
	return strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			return '_'
		}
		return r
	}, name)
}

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

type gcsBucketSummary struct {
	Name        string    `json:"name"`
	TimeCreated time.Time `json:"timeCreated"`
	ObjectCount int       `json:"objectCount"`
	GCSURI      string    `json:"gcsUri"`
}

type gcsObjectSummary struct {
	Name           string            `json:"name"`
	Size           int64             `json:"size"`
	ETag           string            `json:"etag"`
	ContentType    string            `json:"contentType"`
	CRC32C         string            `json:"crc32c,omitempty"`
	StorageClass   string            `json:"storageClass"`
	Updated        time.Time         `json:"updated"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Generation     string            `json:"generation"`
	Metageneration string            `json:"metageneration"`
	GCSURI         string            `json:"gcsUri"`
	DownloadURL    string            `json:"downloadUrl"`
}

type gcsUploadSessionSummary struct {
	ID            string    `json:"id"`
	Bucket        string    `json:"bucket"`
	Name          string    `json:"name"`
	ContentType   string    `json:"contentType,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	ReceivedBytes int64     `json:"receivedBytes"`
}

func normalizedMetageneration(object s3svc.Object) int64 {
	if object.Metageneration < 1 {
		return 1
	}
	return object.Metageneration
}

func gcsObjectSummaryFromObject(bucket string, object s3svc.Object) gcsObjectSummary {
	return gcsObjectSummary{
		Name:           object.Key,
		Size:           object.Size,
		ETag:           object.ETag,
		ContentType:    object.ContentType,
		CRC32C:         object.CRC32C,
		StorageClass:   "STANDARD",
		Updated:        object.LastModified,
		Metadata:       object.Metadata,
		Generation:     strconv.FormatInt(object.LastModified.UTC().UnixNano(), 10),
		Metageneration: strconv.FormatInt(normalizedMetageneration(object), 10),
		GCSURI:         "gs://" + bucket + "/" + object.Key,
		DownloadURL:    "/api/gcs/buckets/" + url.PathEscape(bucket) + "/objects/" + url.PathEscape(object.Key) + "/download",
	}
}

func listGCSUploadSessions(root string) ([]gcsUploadSessionSummary, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []gcsUploadSessionSummary{}, nil
		}
		return nil, err
	}
	sessions := make([]gcsUploadSessionSummary, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name(), "session.json"))
		if err != nil {
			continue
		}
		var session struct {
			Bucket        string    `json:"Bucket"`
			Name          string    `json:"Name"`
			ContentType   string    `json:"ContentType"`
			CreatedAt     time.Time `json:"CreatedAt"`
			ReceivedBytes int64     `json:"ReceivedBytes"`
		}
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}
		sessions = append(sessions, gcsUploadSessionSummary{
			ID:            entry.Name(),
			Bucket:        session.Bucket,
			Name:          session.Name,
			ContentType:   session.ContentType,
			CreatedAt:     session.CreatedAt,
			ReceivedBytes: session.ReceivedBytes,
		})
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].CreatedAt.Equal(sessions[j].CreatedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].CreatedAt.Before(sessions[j].CreatedAt)
	})
	return sessions, nil
}

func gcsUploadSessionIDFromPath(escapedPath string) (string, bool) {
	escapedID := strings.TrimPrefix(escapedPath, "/api/gcs/uploads/")
	if escapedID == "" || escapedID == escapedPath || strings.Contains(escapedID, "/") {
		return "", false
	}
	id, err := url.PathUnescape(escapedID)
	if err != nil || id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return "", false
	}
	return id, true
}
