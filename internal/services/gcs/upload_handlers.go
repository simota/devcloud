package gcs

import (
	"bytes"
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
	"strings"
	"time"

	"devcloud/internal/events"
	s3svc "devcloud/internal/services/s3"
)

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
	events.Emit(s.eventPublisher, events.Event{
		Type:    "gcs.object.put",
		Service: "gcs",
		Payload: map[string]any{"bucket": bucket, "key": name, "etag": object.ETag, "contentLength": object.Size},
	})
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
	events.Emit(s.eventPublisher, events.Event{
		Type:    "gcs.object.put",
		Service: "gcs",
		Payload: map[string]any{"bucket": bucket, "key": name, "etag": object.ETag, "contentLength": object.Size},
	})
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
	events.Emit(s.eventPublisher, events.Event{
		Type:    "gcs.object.put",
		Service: "gcs",
		Payload: map[string]any{"bucket": session.Bucket, "key": session.Name, "etag": object.ETag, "contentLength": object.Size},
	})
	writeJSON(w, http.StatusOK, s.objectResource(object))
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
