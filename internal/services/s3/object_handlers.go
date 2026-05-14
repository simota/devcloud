package s3

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"devcloud/internal/events"
)

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	if err := validateObjectKey(key); err != nil {
		writeXMLError(w, "InvalidArgument", "invalid object key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if r.URL.Query().Has("select") {
			s.handleSelectObjectContent(w, r, bucket, key)
			return
		}
		if r.URL.Query().Has("uploads") {
			s.handleCreateMultipartUpload(w, r, bucket, key)
			return
		}
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := validateUploadID(uploadID); err != nil {
				writeXMLError(w, "InvalidArgument", "invalid upload id", http.StatusBadRequest)
				return
			}
			s.handleCompleteMultipartUpload(w, r, bucket, key, uploadID)
			return
		}
		methodNotAllowed(w, "PUT, HEAD, GET, DELETE, POST")
	case http.MethodPut:
		if r.URL.Query().Has("retention") {
			s.handlePutObjectRetention(w, r, bucket, key)
			return
		}
		if r.URL.Query().Has("legal-hold") {
			s.handlePutObjectLegalHold(w, r, bucket, key)
			return
		}
		if r.URL.Query().Has("acl") {
			s.handlePutObjectACL(w, r, bucket, key)
			return
		}
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := validateUploadID(uploadID); err != nil {
				writeXMLError(w, "InvalidArgument", "invalid upload id", http.StatusBadRequest)
				return
			}
			s.handleUploadPart(w, r, bucket, key, uploadID)
			return
		}
		if source := r.Header.Get("x-amz-copy-source"); source != "" {
			s.handleCopyObject(w, r, bucket, key, source)
			return
		}
		s.handlePutObject(w, r, bucket, key)
	case http.MethodHead:
		s.handleGetObject(w, r, bucket, key, true)
	case http.MethodGet:
		if r.URL.Query().Has("retention") {
			s.handleGetObjectRetention(w, r, bucket, key)
			return
		}
		if r.URL.Query().Has("legal-hold") {
			s.handleGetObjectLegalHold(w, r, bucket, key)
			return
		}
		if r.URL.Query().Has("acl") {
			s.handleGetObjectACL(w, r, bucket, key)
			return
		}
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := validateUploadID(uploadID); err != nil {
				writeXMLError(w, "InvalidArgument", "invalid upload id", http.StatusBadRequest)
				return
			}
			s.handleListParts(w, r, bucket, key, uploadID)
			return
		}
		s.handleGetObject(w, r, bucket, key, false)
	case http.MethodDelete:
		bypassGovernance, err := bypassGovernanceRetentionFromHeaders(r.Header)
		if err != nil {
			writeXMLError(w, "InvalidArgument", "bypass governance retention header is invalid", http.StatusBadRequest)
			return
		}
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := validateUploadID(uploadID); err != nil {
				writeXMLError(w, "InvalidArgument", "invalid upload id", http.StatusBadRequest)
				return
			}
			s.handleAbortMultipartUpload(w, r, bucket, key, uploadID)
			return
		}
		if versionID := r.URL.Query().Get("versionId"); versionID != "" {
			object, ok, err := s.store.DeleteObjectVersion(r.Context(), bucket, key, versionID, bypassGovernance)
			if err != nil {
				if errors.Is(err, errObjectLocked) {
					writeXMLError(w, "AccessDenied", "object is protected by Object Lock", http.StatusForbidden)
					return
				}
				writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
				return
			}
			if ok {
				w.Header().Set("x-amz-version-id", object.VersionID)
				if object.DeleteMarker {
					w.Header().Set("x-amz-delete-marker", "true")
				}
				if err := s.recordObjectEvent(r.Context(), bucket, key, "s3:ObjectRemoved:Delete", object); err != nil {
					writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		object, deleted, err := s.store.DeleteObjectWithResult(r.Context(), bucket, key, bypassGovernance)
		if err != nil {
			if errors.Is(err, errObjectLocked) {
				writeXMLError(w, "AccessDenied", "object is protected by Object Lock", http.StatusForbidden)
				return
			}
			writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
			return
		}
		if object.VersionID != "" {
			w.Header().Set("x-amz-version-id", object.VersionID)
		}
		if object.DeleteMarker {
			w.Header().Set("x-amz-delete-marker", "true")
		}
		if !deleted {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		eventName := "s3:ObjectRemoved:Delete"
		if object.DeleteMarker {
			eventName = "s3:ObjectRemoved:DeleteMarkerCreated"
		}
		if err := s.recordObjectEvent(r.Context(), bucket, key, eventName, object); err != nil {
			writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
			return
		}
		if object.DeleteMarker {
			if err := s.replicateObjectDeleteMarker(r.Context(), bucket, key); err != nil {
				writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
				return
			}
		}
		events.Emit(s.eventPublisher, events.Event{
			Type:    "s3.object.deleted",
			Service: "s3",
			Payload: map[string]any{"bucket": bucket, "key": key},
		})
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "PUT, HEAD, GET, DELETE, POST")
	}
}
func (s *Server) handlePutObjectACL(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	acl, err := aclFromRequest(r)
	if err != nil {
		writeXMLError(w, "MalformedACLError", "object ACL is malformed", http.StatusBadRequest)
		return
	}
	ok, err := s.store.PutObjectACL(r.Context(), bucket, key, r.URL.Query().Get("versionId"), acl)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObjectACL(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	acl, ok, err := s.store.GetObjectACL(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	writeACL(w, acl)
}

func (s *Server) handlePutObjectRetention(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	var retention ObjectRetention
	if err := xml.NewDecoder(r.Body).Decode(&retention); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateObjectRetention(retention); err != nil {
		writeXMLError(w, "InvalidArgument", "object retention is invalid", http.StatusBadRequest)
		return
	}
	_, ok, err := s.store.PutObjectRetention(r.Context(), bucket, key, r.URL.Query().Get("versionId"), retention)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObjectRetention(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	retention, ok, err := s.store.GetObjectRetention(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok || retention.Mode == "" {
		writeXMLError(w, "NoSuchObjectLockConfiguration", "object retention does not exist", http.StatusNotFound)
		return
	}
	retention.XMLName = xml.Name{Local: "Retention"}
	writeXML(w, http.StatusOK, retention)
}

func (s *Server) handlePutObjectLegalHold(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	var legalHold ObjectLegalHold
	if err := xml.NewDecoder(r.Body).Decode(&legalHold); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateObjectLegalHold(legalHold); err != nil {
		writeXMLError(w, "InvalidArgument", "object legal hold is invalid", http.StatusBadRequest)
		return
	}
	_, ok, err := s.store.PutObjectLegalHold(r.Context(), bucket, key, r.URL.Query().Get("versionId"), legalHold)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObjectLegalHold(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	legalHold, ok, err := s.store.GetObjectLegalHold(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok || legalHold.Status == "" {
		writeXMLError(w, "NoSuchObjectLockConfiguration", "object legal hold does not exist", http.StatusNotFound)
		return
	}
	legalHold.XMLName = xml.Name{Local: "LegalHold"}
	writeXML(w, http.StatusOK, legalHold)
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	body := r.Body
	if s.config.MaxObjectBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, s.config.MaxObjectBytes)
	}
	defer body.Close()

	encryption, err := serverSideEncryptionFromHeaders(r.Header)
	if err != nil {
		writeServerSideEncryptionError(w, err)
		return
	}
	retention, legalHold, err := objectLockFromHeaders(r.Header)
	if err != nil {
		writeXMLError(w, "InvalidArgument", "object lock headers are invalid", http.StatusBadRequest)
		return
	}
	object, err := s.store.PutObject(r.Context(), PutObjectInput{
		Bucket:             bucket,
		Key:                key,
		Body:               body,
		ContentMD5:         r.Header.Get("Content-MD5"),
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           userMetadataFromHeaders(r.Header),
		Encryption:         encryption,
		Retention:          retention,
		LegalHold:          legalHold,
	})
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) || errors.Is(err, http.ErrBodyReadAfterClose) {
			writeXMLError(w, "EntityTooLarge", "object is too large", http.StatusRequestEntityTooLarge)
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
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", object.ETag)
	writeServerSideEncryptionHeaders(w, object.Encryption)
	writeObjectLockHeaders(w, object)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
	if err := s.recordObjectEvent(r.Context(), bucket, key, "s3:ObjectCreated:Put", object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.replicateObjectWrite(r.Context(), bucket, key, object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	events.Emit(s.eventPublisher, events.Event{
		Type:    "s3.object.put",
		Service: "s3",
		Payload: map[string]any{
			"bucket":        bucket,
			"key":           key,
			"etag":          object.ETag,
			"contentLength": object.Size,
		},
	})
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket string, key string, source string) {
	sourceBucket, sourceKey, sourceVersionID, err := parseCopySource(source)
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid copy source", http.StatusBadRequest)
		return
	}
	sourceObject, body, ok, err := s.store.GetObjectVersion(r.Context(), sourceBucket, sourceKey, sourceVersionID)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "source bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "source object does not exist", http.StatusNotFound)
		return
	}

	input := PutObjectInput{
		Bucket:             bucket,
		Key:                key,
		Body:               bytes.NewReader(body),
		ContentType:        sourceObject.ContentType,
		ContentEncoding:    sourceObject.ContentEncoding,
		CacheControl:       sourceObject.CacheControl,
		ContentDisposition: sourceObject.ContentDisposition,
		Metadata:           sourceObject.Metadata,
		Encryption:         sourceObject.Encryption,
		Retention:          sourceObject.Retention,
		LegalHold:          sourceObject.LegalHold,
	}
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		input.ContentType = r.Header.Get("Content-Type")
		input.ContentEncoding = r.Header.Get("Content-Encoding")
		input.CacheControl = r.Header.Get("Cache-Control")
		input.ContentDisposition = r.Header.Get("Content-Disposition")
		input.Metadata = userMetadataFromHeaders(r.Header)
	}
	if hasServerSideEncryptionHeaders(r.Header) {
		encryption, err := serverSideEncryptionFromHeaders(r.Header)
		if err != nil {
			writeServerSideEncryptionError(w, err)
			return
		}
		input.Encryption = encryption
	}
	if hasObjectLockHeaders(r.Header) {
		retention, legalHold, err := objectLockFromHeaders(r.Header)
		if err != nil {
			writeXMLError(w, "InvalidArgument", "object lock headers are invalid", http.StatusBadRequest)
			return
		}
		if retention.Mode != "" {
			input.Retention = retention
		}
		if legalHold.Status != "" {
			input.LegalHold = legalHold
		}
	}
	object, err := s.store.PutObject(r.Context(), input)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", object.ETag)
	writeServerSideEncryptionHeaders(w, object.Encryption)
	writeObjectLockHeaders(w, object)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
	if err := s.recordObjectEvent(r.Context(), bucket, key, "s3:ObjectCreated:Copy", object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.replicateObjectWrite(r.Context(), bucket, key, object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		XMLName:      xml.Name{Local: "CopyObjectResult"},
		LastModified: object.LastModified.Format(time.RFC3339),
		ETag:         object.ETag,
	})
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket string, key string, headOnly bool) {
	if err := s.applyBucketLifecycle(r.Context(), bucket); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	versionID := r.URL.Query().Get("versionId")
	object, body, ok, err := s.store.GetObjectVersion(r.Context(), bucket, key, versionID)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		w.Header().Set("x-amz-version-id", object.VersionID)
		writeXMLError(w, "MethodNotAllowed", "the specified version is a delete marker", http.StatusMethodNotAllowed)
		return
	}

	start, end, partial, err := parseRange(r.Header.Get("Range"), int64(len(body)))
	if err != nil {
		writeXMLError(w, "InvalidRange", "requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	payload := body[start : end+1]
	writeObjectHeaders(w, object)
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
	}
	w.WriteHeader(status)
	if !headOnly {
		_, _ = w.Write(payload)
	}
}

func (s *Server) handleListObjectVersions(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	keyMarker := query.Get("key-marker")
	versionIDMarker := query.Get("version-id-marker")
	if err := s.applyBucketLifecycle(r.Context(), bucket); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	versions, bucketExists, err := s.store.ListObjectVersions(r.Context(), bucket, prefix)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	maxKeys, err := parseMaxKeys(query.Get("max-keys"))
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid max-keys", http.StatusBadRequest)
		return
	}
	latestByKey := latestObjectVersionIDs(versions)
	listing := buildVersionListing(versions, keyMarker, versionIDMarker, maxKeys)
	response := listVersionsResult{
		XMLName:             xml.Name{Local: "ListVersionsResult"},
		Xmlns:               "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                bucket,
		Prefix:              prefix,
		KeyMarker:           keyMarker,
		VersionIDMarker:     versionIDMarker,
		NextKeyMarker:       listing.nextKeyMarker,
		NextVersionIDMarker: listing.nextVersionIDMarker,
		MaxKeys:             maxKeys,
		IsTruncated:         listing.truncated,
	}
	for _, object := range listing.versions {
		element := versionElement{
			Key:          object.Key,
			VersionID:    object.VersionID,
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: "STANDARD",
		}
		if object.VersionID == "" {
			element.VersionID = "null"
		}
		element.IsLatest = latestByKey[object.Key] == element.VersionID
		if object.DeleteMarker {
			response.DeleteMarkers = append(response.DeleteMarkers, deleteMarkerElement{
				Key:          object.Key,
				VersionID:    element.VersionID,
				IsLatest:     element.IsLatest,
				LastModified: object.LastModified.Format(time.RFC3339),
			})
			continue
		}
		response.Versions = append(response.Versions, element)
	}
	writeXML(w, http.StatusOK, response)
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	if err := s.applyBucketLifecycle(r.Context(), bucket); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	objects, bucketExists, err := s.store.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}

	listTypeV2 := query.Get("list-type") == "2"
	maxKeys, err := parseMaxKeys(query.Get("max-keys"))
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid max-keys", http.StatusBadRequest)
		return
	}
	marker := query.Get("marker")
	if listTypeV2 {
		if token := query.Get("continuation-token"); token != "" {
			marker, err = decodeContinuationToken(token)
			if err != nil {
				writeXMLError(w, "InvalidArgument", "invalid continuation-token", http.StatusBadRequest)
				return
			}
		} else {
			marker = query.Get("start-after")
		}
	}
	delimiter := query.Get("delimiter")
	encodingType := query.Get("encoding-type")
	listing := buildObjectListing(objects, prefix, delimiter, marker, maxKeys)

	response := listBucketResult{
		XMLName:               xml.Name{Local: "ListBucketResult"},
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                encodeListValue(prefix, encodingType),
		Delimiter:             encodeListValue(delimiter, encodingType),
		KeyCount:              len(listing.contents) + len(listing.commonPrefixes),
		MaxKeys:               maxKeys,
		IsTruncated:           listing.truncated,
		Marker:                encodeListValue(query.Get("marker"), encodingType),
		ContinuationToken:     query.Get("continuation-token"),
		StartAfter:            encodeListValue(query.Get("start-after"), encodingType),
		NextContinuationToken: listing.nextContinuationToken,
	}
	if !listTypeV2 && listing.nextMarker != "" {
		response.NextMarker = encodeListValue(listing.nextMarker, encodingType)
	}
	if listTypeV2 {
		response.ListType = 2
	}
	for _, object := range listing.contents {
		response.Contents = append(response.Contents, objectElement{
			Key:          encodeListValue(object.Key, encodingType),
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, prefix := range listing.commonPrefixes {
		response.CommonPrefixes = append(response.CommonPrefixes, commonPrefixElement{
			Prefix: encodeListValue(prefix, encodingType),
		})
	}
	writeXML(w, http.StatusOK, response)
}
