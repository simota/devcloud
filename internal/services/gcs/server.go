package gcs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	s3svc "devcloud/internal/services/s3"
)

type Config struct {
	Addr              string
	Project           string
	Location          string
	AuthMode          string
	BearerToken       string
	UploadSessionPath string
}

type Server struct {
	config   Config
	store    s3svc.BucketStore
	mu       sync.Mutex
	sessions map[string]resumableSession
}

func NewServer(cfg Config, store s3svc.BucketStore) *Server {
	server := &Server{config: cfg, store: store, sessions: map[string]resumableSession{}}
	server.loadResumableSessions()
	return server
}

type resumableSession struct {
	Bucket             string
	Name               string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
	Preconditions      objectPreconditions
	CreatedAt          time.Time
	ReceivedBytes      int64
}

type objectPreconditions struct {
	IfGenerationMatch        string
	IfGenerationNotMatch     string
	IfMetagenerationMatch    string
	IfMetagenerationNotMatch string
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
	return http.HandlerFunc(s.handle)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "backendError", "gcs service is disabled")
		return
	}
	w.Header().Set("Server", "devcloud-gcs")
	if !s.authorize(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="devcloud-gcs"`)
		writeError(w, http.StatusUnauthorized, "authError", "invalid authentication credentials")
		return
	}

	switch {
	case r.URL.Path == "/upload/storage/v1/b" || strings.HasPrefix(r.URL.EscapedPath(), "/upload/storage/v1/b/"):
		s.handleUpload(w, r)
	case strings.HasPrefix(r.URL.EscapedPath(), "/download/storage/v1/b/"):
		s.handleDownload(w, r)
	case r.URL.Path == "/storage/v1/b":
		s.handleBuckets(w, r)
	case strings.HasPrefix(r.URL.EscapedPath(), "/storage/v1/b/"):
		s.handleBucketOrObject(w, r)
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

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

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	bucket, suffix, ok := bucketAndSuffixFromPath(r.URL.EscapedPath(), "/upload/storage/v1/b/")
	if !ok || suffix != "o" {
		writeError(w, http.StatusNotFound, "notFound", "not found")
		return
	}
	switch r.URL.Query().Get("uploadType") {
	case "media":
		s.handleMediaUpload(w, r, bucket)
	case "multipart":
		s.handleMultipartUpload(w, r, bucket)
	case "resumable":
		s.handleResumableUpload(w, r, bucket)
	default:
		writeError(w, http.StatusBadRequest, "invalid", "unsupported uploadType")
	}
}

func (s *Server) handleMediaUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "required", "object name is required")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, name); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	defer r.Body.Close()
	object, err := s.store.PutObject(r.Context(), s3svc.PutObjectInput{
		Bucket:             bucket,
		Key:                name,
		Body:               r.Body,
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           gcsUserMetadataFromHeaders(r.Header),
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.objectResource(object))
}

func (s *Server) handleMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	metadata, body, bodyContentType, err := parseMultipartUpload(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	name := defaultString(metadata.Name, r.URL.Query().Get("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "required", "object name is required")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, name); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	contentType := defaultString(metadata.ContentType, bodyContentType)
	object, err := s.store.PutObject(r.Context(), s3svc.PutObjectInput{
		Bucket:             bucket,
		Key:                name,
		Body:               bytes.NewReader(body),
		ContentType:        contentType,
		ContentEncoding:    metadata.ContentEncoding,
		CacheControl:       metadata.CacheControl,
		ContentDisposition: metadata.ContentDisposition,
		Metadata:           metadata.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.objectResource(object))
}

func (s *Server) handleResumableUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPost:
		s.createResumableUpload(w, r, bucket)
	case http.MethodPut:
		s.putResumableUpload(w, r)
	default:
		methodNotAllowed(w, "POST, PUT")
	}
}

func (s *Server) createResumableUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	name := r.URL.Query().Get("name")
	contentType := r.Header.Get("X-Upload-Content-Type")
	var request multipartUploadMetadata
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	name = defaultString(name, request.Name)
	contentType = defaultString(contentType, request.ContentType)
	if name == "" {
		writeError(w, http.StatusBadRequest, "required", "object name is required")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, name); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	id, err := newUploadID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	s.mu.Lock()
	session := resumableSession{
		Bucket:             bucket,
		Name:               name,
		ContentType:        contentType,
		ContentEncoding:    defaultString(r.Header.Get("Content-Encoding"), request.ContentEncoding),
		CacheControl:       request.CacheControl,
		ContentDisposition: request.ContentDisposition,
		Metadata:           mergeMetadata(request.Metadata, gcsUserMetadataFromHeaders(r.Header)),
		Preconditions:      preconditionsFromRequest(r),
		CreatedAt:          time.Now().UTC(),
	}
	s.sessions[id] = session
	s.mu.Unlock()
	if err := s.saveResumableSession(id, session); err != nil {
		s.mu.Lock()
		delete(s.sessions, id)
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}

	location := "http://" + r.Host + "/upload/storage/v1/b/" + url.PathEscape(bucket) + "/o?uploadType=resumable&upload_id=" + url.QueryEscape(id)
	w.Header().Set("Location", location)
	w.Header().Set("X-GUploader-UploadID", id)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) putResumableUpload(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("upload_id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "required", "upload_id is required")
		return
	}
	s.mu.Lock()
	session, ok := s.sessions[id]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "notFound", "upload session not found")
		return
	}
	if isResumableStatusQuery(r.Header.Get("Content-Range")) {
		if session.ReceivedBytes > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", session.ReceivedBytes-1))
		}
		w.WriteHeader(308)
		return
	}
	if ok, err := s.checkStoredObjectPreconditions(r.Context(), session.Bucket, session.Name, session.Preconditions); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	defer r.Body.Close()
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "backendError", "internal error")
		return
	}
	if contentRange := strings.TrimSpace(r.Header.Get("Content-Range")); contentRange != "" {
		uploadRange, err := parseResumableContentRange(contentRange, int64(len(payload)))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		if uploadRange.start != session.ReceivedBytes {
			writeError(w, http.StatusBadRequest, "invalid", "upload chunk does not start at committed offset")
			return
		}
		session.ReceivedBytes = uploadRange.end + 1
		oneShotFinalChunk := uploadRange.start == 0 && session.ReceivedBytes == uploadRange.total
		if !oneShotFinalChunk {
			if err := s.appendResumableChunk(id, payload); err != nil {
				writeError(w, http.StatusInternalServerError, "backendError", "internal error")
				return
			}
			if err := s.saveResumableSession(id, session); err != nil {
				writeError(w, http.StatusInternalServerError, "backendError", "internal error")
				return
			}
		}
		if session.ReceivedBytes < uploadRange.total {
			s.mu.Lock()
			s.sessions[id] = session
			s.mu.Unlock()
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", session.ReceivedBytes-1))
			w.WriteHeader(308)
			return
		}
		if !oneShotFinalChunk {
			payload, err = s.readResumableBody(id)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "backendError", "internal error")
				return
			}
		}
	}
	object, err := s.store.PutObject(r.Context(), s3svc.PutObjectInput{
		Bucket:             session.Bucket,
		Key:                session.Name,
		Body:               bytes.NewReader(payload),
		ContentType:        defaultString(r.Header.Get("Content-Type"), session.ContentType),
		ContentEncoding:    session.ContentEncoding,
		CacheControl:       session.CacheControl,
		ContentDisposition: session.ContentDisposition,
		Metadata:           session.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	_ = s.deleteResumableSession(id)
	writeJSON(w, http.StatusOK, s.objectResource(object))
}

func (s *Server) handleObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	includeTrailingDelimiter := strings.EqualFold(r.URL.Query().Get("includeTrailingDelimiter"), "true")
	startOffset := r.URL.Query().Get("startOffset")
	endOffset := r.URL.Query().Get("endOffset")
	objects, bucketExists, err := s.store.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if !bucketExists {
		writeError(w, http.StatusNotFound, "notFound", "bucket not found")
		return
	}
	response := objectsListResponse{
		Kind:  "storage#objects",
		Items: make([]objectResource, 0, len(objects)),
	}
	prefixes := map[string]struct{}{}
	for _, object := range objects {
		if startOffset != "" && object.Key < startOffset {
			continue
		}
		if endOffset != "" && object.Key >= endOffset {
			continue
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				prefixes[prefix+remainder[:index+len(delimiter)]] = struct{}{}
				isTrailingDelimiterObject := includeTrailingDelimiter && index+len(delimiter) == len(remainder)
				if !isTrailingDelimiterObject {
					continue
				}
			}
		}
		response.Items = append(response.Items, s.objectResource(object))
	}
	if len(prefixes) > 0 {
		allPrefixes := make([]string, 0, len(prefixes))
		for prefix := range prefixes {
			allPrefixes = append(allPrefixes, prefix)
		}
		sort.Strings(allPrefixes)
		response.Prefixes = allPrefixes
	}
	response, err = paginateObjectsResponse(r.URL.Query(), response)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleObjectOrCopy(w http.ResponseWriter, r *http.Request, bucket string, escapedSuffix string) {
	destEscaped, isCompose := strings.CutSuffix(escapedSuffix, "/compose")
	if isCompose {
		s.handleComposeObject(w, r, bucket, destEscaped)
		return
	}
	srcEscaped, copySuffix, isCopy := strings.Cut(escapedSuffix, "/copyTo/b/")
	if isCopy {
		s.handleCopyObject(w, r, bucket, srcEscaped, copySuffix)
		return
	}
	srcEscaped, rewriteSuffix, isRewrite := strings.Cut(escapedSuffix, "/rewriteTo/b/")
	if isRewrite {
		s.handleRewriteObject(w, r, bucket, srcEscaped, rewriteSuffix)
		return
	}
	key, err := url.PathUnescape(escapedSuffix)
	if err != nil || key == "" {
		writeError(w, http.StatusBadRequest, "invalid", "invalid object name")
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if r.URL.Query().Get("alt") == "media" {
			s.serveObjectMedia(w, r, bucket, key)
			return
		}
		object, _, ok, err := s.store.GetObject(r.Context(), bucket, key)
		if err != nil {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, "notFound", "object not found")
			return
		}
		if ok, err := requestedGenerationMatches(r, object); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		} else if !ok {
			writeError(w, http.StatusNotFound, "notFound", "object not found")
			return
		}
		if ok, err := s.checkObjectPreconditions(r, bucket, key); err != nil {
			writePreconditionError(w, err)
			return
		} else if !ok {
			writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
			return
		}
		if r.Method == http.MethodHead {
			s.setObjectHeaders(w, object)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			return
		}
		writeJSON(w, http.StatusOK, s.objectResource(object))
	case http.MethodPatch:
		s.handlePatchObject(w, r, bucket, key)
	case http.MethodDelete:
		object, _, found, err := s.store.GetObject(r.Context(), bucket, key)
		if err != nil {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "notFound", "object not found")
			return
		}
		if ok, err := requestedGenerationMatches(r, object); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		} else if !ok {
			writeError(w, http.StatusNotFound, "notFound", "object not found")
			return
		}
		if ok, err := s.checkObjectPreconditions(r, bucket, key); err != nil {
			writePreconditionError(w, err)
			return
		} else if !ok {
			writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
			return
		}
		deleted, err := s.store.DeleteObject(r.Context(), bucket, key)
		if err != nil {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		if !deleted {
			writeError(w, http.StatusNotFound, "notFound", "object not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PATCH, DELETE")
	}
}

func (s *Server) handlePatchObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	object, _, found, err := s.store.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", "bucket not found")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := requestedGenerationMatches(r, object); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, key); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	var request multipartUploadMetadata
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	updated, found, err := s.store.UpdateObjectMetadata(r.Context(), s3svc.UpdateObjectMetadataInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        request.ContentType,
		ContentEncoding:    request.ContentEncoding,
		CacheControl:       request.CacheControl,
		ContentDisposition: request.ContentDisposition,
		Metadata:           request.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", "bucket not found")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	writeJSON(w, http.StatusOK, s.objectResource(updated))
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, "GET, HEAD")
		return
	}
	bucket, suffix, ok := bucketAndSuffixFromPath(r.URL.EscapedPath(), "/download/storage/v1/b/")
	if !ok || !strings.HasPrefix(suffix, "o/") {
		writeError(w, http.StatusNotFound, "notFound", "not found")
		return
	}
	key, err := url.PathUnescape(strings.TrimPrefix(suffix, "o/"))
	if err != nil || key == "" {
		writeError(w, http.StatusBadRequest, "invalid", "invalid object name")
		return
	}
	s.serveObjectMedia(w, r, bucket, key)
}

func (s *Server) serveObjectMedia(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	object, body, found, err := s.store.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", "bucket not found")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := requestedGenerationMatches(r, object); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	} else if !ok {
		writeError(w, http.StatusNotFound, "notFound", "object not found")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, key); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}
	start, end, partial, err := parseHTTPRange(r.Header.Get("Range"), int64(len(body)))
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "requestedRangeNotSatisfiable", "requested range is not satisfiable")
		return
	}
	payload := body[start : end+1]
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	s.setObjectHeaders(w, object)
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(payload)
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, srcBucket string, srcEscaped string, copySuffix string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	copied, err := s.copyObject(r, srcBucket, srcEscaped, copySuffix)
	if err != nil {
		s.writeCopyError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.objectResource(copied))
}

func (s *Server) handleRewriteObject(w http.ResponseWriter, r *http.Request, srcBucket string, srcEscaped string, rewriteSuffix string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	copied, err := s.copyObject(r, srcBucket, srcEscaped, rewriteSuffix)
	if err != nil {
		s.writeCopyError(w, err)
		return
	}
	size := strconv.FormatInt(copied.Size, 10)
	writeJSON(w, http.StatusOK, rewriteResponse{
		Kind:                "storage#rewriteResponse",
		TotalBytesRewritten: size,
		ObjectSize:          size,
		Done:                true,
		Resource:            s.objectResource(copied),
	})
}

func (s *Server) handleComposeObject(w http.ResponseWriter, r *http.Request, bucket string, destEscaped string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	destinationName, err := url.PathUnescape(destEscaped)
	if err != nil || destinationName == "" {
		writeError(w, http.StatusBadRequest, "invalid", "invalid destination object name")
		return
	}
	var request composeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json request")
		return
	}
	if len(request.SourceObjects) == 0 {
		writeError(w, http.StatusBadRequest, "required", "sourceObjects is required")
		return
	}
	if len(request.SourceObjects) > 32 {
		writeError(w, http.StatusBadRequest, "invalid", "sourceObjects must contain no more than 32 objects")
		return
	}
	if ok, err := s.checkObjectPreconditions(r, bucket, destinationName); err != nil {
		writePreconditionError(w, err)
		return
	} else if !ok {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "precondition failed")
		return
	}

	var body bytes.Buffer
	var firstSource s3svc.Object
	for index, source := range request.SourceObjects {
		if source.Name == "" {
			writeError(w, http.StatusBadRequest, "required", "source object name is required")
			return
		}
		object, payload, found, err := s.store.GetObject(r.Context(), bucket, source.Name)
		if err != nil {
			writeError(w, http.StatusNotFound, "notFound", "bucket not found")
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "notFound", "source object not found")
			return
		}
		generation := strconv.FormatInt(object.LastModified.UTC().UnixNano(), 10)
		if source.Generation != "" && source.Generation != generation {
			writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "source generation precondition failed")
			return
		}
		if match := source.ObjectPreconditions.IfGenerationMatch; match != "" && match != generation {
			writeError(w, http.StatusPreconditionFailed, "conditionNotMet", "source generation precondition failed")
			return
		}
		if index == 0 {
			firstSource = object
		}
		if _, err := body.Write(payload); err != nil {
			writeError(w, http.StatusInternalServerError, "backendError", "internal error")
			return
		}
	}

	contentType := defaultString(request.Destination.ContentType, firstSource.ContentType)
	composed, err := s.store.PutObject(r.Context(), s3svc.PutObjectInput{
		Bucket:             bucket,
		Key:                destinationName,
		Body:               bytes.NewReader(body.Bytes()),
		ContentType:        contentType,
		ContentEncoding:    request.Destination.ContentEncoding,
		CacheControl:       request.Destination.CacheControl,
		ContentDisposition: request.Destination.ContentDisposition,
		Metadata:           request.Destination.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "notFound", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.objectResource(composed))
}

func (s *Server) copyObject(r *http.Request, srcBucket string, srcEscaped string, dstSuffix string) (s3svc.Object, error) {
	destination, err := copyDestinationFromRequest(r)
	if err != nil {
		return s3svc.Object{}, copyObjectError{status: http.StatusBadRequest, reason: "invalid", message: err.Error()}
	}
	srcKey, err := url.PathUnescape(srcEscaped)
	if err != nil || srcKey == "" {
		return s3svc.Object{}, copyObjectError{status: http.StatusBadRequest, reason: "invalid", message: "invalid source object name"}
	}
	dstBucketEscaped, dstObjectEscaped, ok := strings.Cut(dstSuffix, "/o/")
	if !ok {
		return s3svc.Object{}, copyObjectError{status: http.StatusNotFound, reason: "notFound", message: "not found"}
	}
	dstBucket, err := url.PathUnescape(dstBucketEscaped)
	if err != nil || dstBucket == "" {
		return s3svc.Object{}, copyObjectError{status: http.StatusBadRequest, reason: "invalid", message: "invalid destination bucket"}
	}
	dstKey, err := url.PathUnescape(dstObjectEscaped)
	if err != nil || dstKey == "" {
		return s3svc.Object{}, copyObjectError{status: http.StatusBadRequest, reason: "invalid", message: "invalid destination object name"}
	}
	source, body, found, err := s.store.GetObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		return s3svc.Object{}, copyObjectError{status: http.StatusNotFound, reason: "notFound", message: "source bucket not found"}
	}
	if !found {
		return s3svc.Object{}, copyObjectError{status: http.StatusNotFound, reason: "notFound", message: "source object not found"}
	}
	if ok, err := s.checkStoredObjectPreconditions(r.Context(), srcBucket, srcKey, sourcePreconditionsFromRequest(r)); err != nil {
		return s3svc.Object{}, err
	} else if !ok {
		return s3svc.Object{}, copyObjectError{status: http.StatusPreconditionFailed, reason: "conditionNotMet", message: "precondition failed"}
	}
	if ok, err := s.checkObjectPreconditions(r, dstBucket, dstKey); err != nil {
		return s3svc.Object{}, err
	} else if !ok {
		return s3svc.Object{}, copyObjectError{status: http.StatusPreconditionFailed, reason: "conditionNotMet", message: "precondition failed"}
	}
	copied, err := s.store.PutObject(r.Context(), s3svc.PutObjectInput{
		Bucket:             dstBucket,
		Key:                dstKey,
		Body:               bytes.NewReader(body),
		ContentType:        defaultString(destination.ContentType, source.ContentType),
		ContentEncoding:    defaultString(destination.ContentEncoding, source.ContentEncoding),
		CacheControl:       defaultString(destination.CacheControl, source.CacheControl),
		ContentDisposition: defaultString(destination.ContentDisposition, source.ContentDisposition),
		Metadata:           copyDestinationMetadata(destination.Metadata, source.Metadata),
	})
	if err != nil {
		return s3svc.Object{}, copyObjectError{status: http.StatusNotFound, reason: "notFound", message: "destination bucket not found"}
	}
	return copied, nil
}

type copyObjectError struct {
	status  int
	reason  string
	message string
}

func copyDestinationFromRequest(r *http.Request) (multipartUploadMetadata, error) {
	defer r.Body.Close()
	if r.Body == nil {
		return multipartUploadMetadata{}, nil
	}
	var destination multipartUploadMetadata
	if err := json.NewDecoder(r.Body).Decode(&destination); err != nil && !errors.Is(err, io.EOF) {
		return multipartUploadMetadata{}, fmt.Errorf("invalid json request")
	}
	return destination, nil
}

func copyDestinationMetadata(destination map[string]string, source map[string]string) map[string]string {
	if destination != nil {
		return destination
	}
	return source
}

func (e copyObjectError) Error() string {
	return e.message
}

func (s *Server) writeCopyError(w http.ResponseWriter, err error) {
	var copyErr copyObjectError
	if errors.As(err, &copyErr) {
		writeError(w, copyErr.status, copyErr.reason, copyErr.message)
		return
	}
	writePreconditionError(w, err)
}

func (s *Server) bucketResource(bucket s3svc.Bucket) bucketResource {
	project := s.config.Project
	if project == "" {
		project = "devcloud"
	}
	location := s.config.Location
	if location == "" {
		location = "US"
	}
	return bucketResource{
		Kind:          "storage#bucket",
		ID:            bucket.Name,
		Name:          bucket.Name,
		ProjectNumber: project,
		Location:      location,
		StorageClass:  "STANDARD",
		TimeCreated:   bucket.CreatedAt.Format(time.RFC3339Nano),
		Updated:       bucket.CreatedAt.Format(time.RFC3339Nano),
		SelfLink:      fmt.Sprintf("/storage/v1/b/%s", url.PathEscape(bucket.Name)),
	}
}

func (s *Server) objectResource(object s3svc.Object) objectResource {
	created := object.CreatedAt.UTC()
	if created.IsZero() {
		created = object.LastModified.UTC()
	}
	updated := object.UpdatedAt.UTC()
	if updated.IsZero() {
		updated = object.LastModified.UTC()
	}
	generation := strconv.FormatInt(object.LastModified.UTC().UnixNano(), 10)
	metageneration := object.Metageneration
	if metageneration < 1 {
		metageneration = 1
	}
	return objectResource{
		Kind:               "storage#object",
		ID:                 fmt.Sprintf("%s/%s/%s", object.Bucket, object.Key, generation),
		SelfLink:           fmt.Sprintf("/storage/v1/b/%s/o/%s", url.PathEscape(object.Bucket), url.PathEscape(object.Key)),
		Name:               object.Key,
		Bucket:             object.Bucket,
		Generation:         generation,
		Metageneration:     strconv.FormatInt(metageneration, 10),
		ContentType:        object.ContentType,
		Size:               strconv.FormatInt(object.Size, 10),
		MD5Hash:            md5HashFromETag(object.ETag),
		CRC32C:             object.CRC32C,
		ETag:               s.gcsETag(object),
		TimeCreated:        created.Format(time.RFC3339Nano),
		Updated:            updated.Format(time.RFC3339Nano),
		StorageClass:       "STANDARD",
		Metadata:           object.Metadata,
		CacheControl:       object.CacheControl,
		ContentEncoding:    object.ContentEncoding,
		ContentDisposition: object.ContentDisposition,
	}
}

func (s *Server) gcsETag(object s3svc.Object) string {
	return fmt.Sprintf("CN%s=", strconv.FormatInt(object.LastModified.UTC().UnixNano(), 10))
}

func (s *Server) authorize(r *http.Request) bool {
	mode := strings.ToLower(strings.TrimSpace(s.config.AuthMode))
	switch mode {
	case "", "off", "relaxed":
		return true
	case "oauth-relaxed":
		token := bearerTokenFromRequest(r)
		return token != ""
	case "bearer-dev":
		token := bearerTokenFromRequest(r)
		expected := strings.TrimSpace(s.config.BearerToken)
		if token == "" || expected == "" {
			return false
		}
		return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
	default:
		return false
	}
}

func bearerTokenFromRequest(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	scheme, token, ok := strings.Cut(auth, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func requestedGenerationMatches(r *http.Request, object s3svc.Object) (bool, error) {
	value := strings.TrimSpace(r.URL.Query().Get("generation"))
	if value == "" {
		return true, nil
	}
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil || generation <= 0 {
		return false, fmt.Errorf("invalid generation")
	}
	return generation == object.LastModified.UTC().UnixNano(), nil
}

func (s *Server) setObjectHeaders(w http.ResponseWriter, object s3svc.Object) {
	resource := s.objectResource(object)
	w.Header().Set("ETag", resource.ETag)
	w.Header().Set("X-Goog-Generation", resource.Generation)
	w.Header().Set("X-Goog-Metageneration", resource.Metageneration)
	w.Header().Set("X-Goog-Stored-Content-Length", resource.Size)
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", object.ContentEncoding)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	hashes := make([]string, 0, 2)
	if object.CRC32C != "" {
		hashes = append(hashes, "crc32c="+object.CRC32C)
	}
	if resource.MD5Hash != "" {
		hashes = append(hashes, "md5="+resource.MD5Hash)
	}
	if len(hashes) > 0 {
		w.Header().Set("X-Goog-Hash", strings.Join(hashes, ","))
	}
	for key, value := range object.Metadata {
		w.Header().Set("X-Goog-Meta-"+key, value)
	}
}

func paginationWindow(query url.Values, total int) (int, int, string, error) {
	start := 0
	if token := strings.TrimSpace(query.Get("pageToken")); token != "" {
		offset, err := strconv.Atoi(token)
		if err != nil || offset < 0 {
			return 0, 0, "", fmt.Errorf("invalid pageToken")
		}
		if offset > total {
			offset = total
		}
		start = offset
	}

	limit := total - start
	if value := strings.TrimSpace(query.Get("maxResults")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, 0, "", fmt.Errorf("invalid maxResults")
		}
		if parsed < limit {
			limit = parsed
		}
	}

	end := start + limit
	nextToken := ""
	if end < total {
		nextToken = strconv.Itoa(end)
	}
	return start, end, nextToken, nil
}

type objectListEntry struct {
	name   string
	object *objectResource
	prefix string
}

func paginateObjectsResponse(query url.Values, response objectsListResponse) (objectsListResponse, error) {
	entries := make([]objectListEntry, 0, len(response.Items)+len(response.Prefixes))
	for i := range response.Items {
		entries = append(entries, objectListEntry{name: response.Items[i].Name, object: &response.Items[i]})
	}
	for _, prefix := range response.Prefixes {
		entries = append(entries, objectListEntry{name: prefix, prefix: prefix})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	start, end, nextToken, err := paginationWindow(query, len(entries))
	if err != nil {
		return objectsListResponse{}, err
	}
	paginated := objectsListResponse{
		Kind:          response.Kind,
		Items:         []objectResource{},
		Prefixes:      []string{},
		NextPageToken: nextToken,
	}
	for _, entry := range entries[start:end] {
		if entry.object != nil {
			paginated.Items = append(paginated.Items, *entry.object)
			continue
		}
		paginated.Prefixes = append(paginated.Prefixes, entry.prefix)
	}
	if len(paginated.Items) == 0 {
		paginated.Items = nil
	}
	if len(paginated.Prefixes) == 0 {
		paginated.Prefixes = nil
	}
	return paginated, nil
}

func (s *Server) checkObjectPreconditions(r *http.Request, bucket string, key string) (bool, error) {
	return s.checkStoredObjectPreconditions(r.Context(), bucket, key, preconditionsFromRequest(r))
}

func preconditionsFromRequest(r *http.Request) objectPreconditions {
	query := r.URL.Query()
	return objectPreconditions{
		IfGenerationMatch:        query.Get("ifGenerationMatch"),
		IfGenerationNotMatch:     query.Get("ifGenerationNotMatch"),
		IfMetagenerationMatch:    query.Get("ifMetagenerationMatch"),
		IfMetagenerationNotMatch: query.Get("ifMetagenerationNotMatch"),
	}
}

func sourcePreconditionsFromRequest(r *http.Request) objectPreconditions {
	query := r.URL.Query()
	return objectPreconditions{
		IfGenerationMatch:        query.Get("ifSourceGenerationMatch"),
		IfGenerationNotMatch:     query.Get("ifSourceGenerationNotMatch"),
		IfMetagenerationMatch:    query.Get("ifSourceMetagenerationMatch"),
		IfMetagenerationNotMatch: query.Get("ifSourceMetagenerationNotMatch"),
	}
}

func (s *Server) checkStoredObjectPreconditions(ctx context.Context, bucket string, key string, preconditions objectPreconditions) (bool, error) {
	object, _, found, err := s.store.GetObject(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	generation := int64(0)
	metageneration := int64(0)
	if found {
		generation = object.LastModified.UTC().UnixNano()
		metageneration = object.Metageneration
		if metageneration < 1 {
			metageneration = 1
		}
	}
	if match := preconditions.IfGenerationMatch; match != "" {
		value, err := strconv.ParseInt(match, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifGenerationMatch")
		}
		if value != generation {
			return false, nil
		}
	}
	if notMatch := preconditions.IfGenerationNotMatch; notMatch != "" {
		value, err := strconv.ParseInt(notMatch, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifGenerationNotMatch")
		}
		if value == generation {
			return false, nil
		}
	}
	if match := preconditions.IfMetagenerationMatch; match != "" {
		value, err := strconv.ParseInt(match, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifMetagenerationMatch")
		}
		if value != metageneration {
			return false, nil
		}
	}
	if notMatch := preconditions.IfMetagenerationNotMatch; notMatch != "" {
		value, err := strconv.ParseInt(notMatch, 10, 64)
		if err != nil {
			return false, fmt.Errorf("invalid ifMetagenerationNotMatch")
		}
		if value == metageneration {
			return false, nil
		}
	}
	return true, nil
}

func bucketNameFromPath(escapedPath string) (string, bool) {
	escapedName := strings.TrimPrefix(escapedPath, "/storage/v1/b/")
	if escapedName == "" || strings.Contains(escapedName, "/") {
		return "", false
	}
	name, err := url.PathUnescape(escapedName)
	if err != nil || name == "" {
		return "", false
	}
	return name, true
}

func bucketAndSuffixFromPath(escapedPath string, prefix string) (bucket string, suffix string, ok bool) {
	trimmed := strings.TrimPrefix(escapedPath, prefix)
	if trimmed == "" || trimmed == escapedPath {
		return "", "", false
	}
	escapedBucket, suffix, hasSuffix := strings.Cut(trimmed, "/")
	bucket, err := url.PathUnescape(escapedBucket)
	if err != nil || bucket == "" {
		return "", "", false
	}
	if !hasSuffix {
		return bucket, "", true
	}
	return bucket, suffix, true
}

func gcsUserMetadataFromHeaders(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-goog-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-goog-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func mergeMetadata(base map[string]string, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	metadata := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		metadata[key] = value
	}
	for key, value := range override {
		metadata[key] = value
	}
	return metadata
}

type multipartUploadMetadata struct {
	Name               string            `json:"name"`
	ContentType        string            `json:"contentType"`
	ContentEncoding    string            `json:"contentEncoding"`
	CacheControl       string            `json:"cacheControl"`
	ContentDisposition string            `json:"contentDisposition"`
	Metadata           map[string]string `json:"metadata"`
}

func parseMultipartUpload(r *http.Request) (multipartUploadMetadata, []byte, string, error) {
	defer r.Body.Close()
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("invalid multipart Content-Type")
	}
	if mediaType != "multipart/related" && mediaType != "multipart/form-data" {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("Content-Type must be multipart/related")
	}
	boundary := params["boundary"]
	if boundary == "" {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("multipart boundary is required")
	}
	reader := multipart.NewReader(r.Body, boundary)

	metadataPart, err := reader.NextPart()
	if err != nil {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("metadata part is required")
	}
	defer metadataPart.Close()
	var metadata multipartUploadMetadata
	if err := json.NewDecoder(metadataPart).Decode(&metadata); err != nil {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("invalid metadata json part")
	}

	bodyPart, err := reader.NextPart()
	if err != nil {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("media part is required")
	}
	defer bodyPart.Close()
	body, err := io.ReadAll(bodyPart)
	if err != nil {
		return multipartUploadMetadata{}, nil, "", fmt.Errorf("read media part: %w", err)
	}
	return metadata, body, bodyPart.Header.Get("Content-Type"), nil
}

func md5HashFromETag(etag string) string {
	hexDigest := strings.Trim(etag, `"`)
	if strings.Contains(hexDigest, "-") {
		return ""
	}
	bytes, err := hex.DecodeString(hexDigest)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

func newUploadID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

type resumableContentRange struct {
	start int64
	end   int64
	total int64
}

func parseResumableContentRange(header string, payloadSize int64) (resumableContentRange, error) {
	if !strings.HasPrefix(header, "bytes ") {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	span, totalValue, ok := strings.Cut(strings.TrimPrefix(header, "bytes "), "/")
	if !ok || totalValue == "*" {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	left, right, ok := strings.Cut(span, "-")
	if !ok {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	end, err := strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	total, err := strconv.ParseInt(totalValue, 10, 64)
	if err != nil || total <= 0 || end >= total {
		return resumableContentRange{}, fmt.Errorf("invalid Content-Range")
	}
	if got, want := payloadSize, end-start+1; got != want {
		return resumableContentRange{}, fmt.Errorf("Content-Range does not match payload size")
	}
	return resumableContentRange{start: start, end: end, total: total}, nil
}

func isResumableStatusQuery(contentRange string) bool {
	contentRange = strings.TrimSpace(contentRange)
	return strings.HasPrefix(contentRange, "bytes */")
}

func (s *Server) loadResumableSessions() {
	root := s.config.UploadSessionPath
	if root == "" {
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		data, err := os.ReadFile(filepath.Join(root, id, "session.json"))
		if err != nil {
			continue
		}
		var session resumableSession
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}
		s.sessions[id] = session
	}
}

func (s *Server) saveResumableSession(id string, session resumableSession) error {
	root := s.config.UploadSessionPath
	if root == "" {
		return nil
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create upload session: %w", err)
	}
	data, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return fmt.Errorf("encode upload session: %w", err)
	}
	return os.WriteFile(filepath.Join(dir, "session.json"), append(data, '\n'), 0o644)
}

func (s *Server) appendResumableChunk(id string, payload []byte) error {
	root := s.config.UploadSessionPath
	if root == "" {
		return fmt.Errorf("upload session storage is not configured")
	}
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create upload session: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "body.part"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open upload body: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(payload); err != nil {
		return fmt.Errorf("write upload body: %w", err)
	}
	return nil
}

func (s *Server) readResumableBody(id string) ([]byte, error) {
	if s.config.UploadSessionPath == "" {
		return nil, fmt.Errorf("upload session storage is not configured")
	}
	return os.ReadFile(filepath.Join(s.config.UploadSessionPath, id, "body.part"))
}

func (s *Server) deleteResumableSession(id string) error {
	if s.config.UploadSessionPath == "" {
		return nil
	}
	return os.RemoveAll(filepath.Join(s.config.UploadSessionPath, id))
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func parseHTTPRange(header string, size int64) (start int64, end int64, partial bool, err error) {
	if header == "" {
		if size == 0 {
			return 0, -1, false, nil
		}
		return 0, size - 1, false, nil
	}
	if size == 0 {
		return 0, 0, false, fmt.Errorf("empty object has no satisfiable range")
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	left, right, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, fmt.Errorf("invalid range")
	}
	if left == "" {
		suffix, err := strconv.ParseInt(right, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, nil
	}
	start, err = strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, fmt.Errorf("invalid range start")
	}
	if right == "" {
		return start, size - 1, true, nil
	}
	end, err = strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range end")
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true, nil
}

func writePreconditionError(w http.ResponseWriter, err error) {
	if strings.HasPrefix(err.Error(), "invalid if") {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeError(w, http.StatusNotFound, "notFound", err.Error())
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, reason string, message string) {
	writeJSON(w, status, errorResponse{
		Error: errorBody{
			Code:    status,
			Message: message,
			Errors: []errorItem{{
				Domain:  "global",
				Reason:  reason,
				Message: message,
			}},
		},
	})
}
