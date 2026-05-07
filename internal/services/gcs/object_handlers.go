package gcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	s3svc "devcloud/internal/services/s3"
)

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
