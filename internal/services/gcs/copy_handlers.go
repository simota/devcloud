package gcs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	s3svc "devcloud/internal/services/s3"
)

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
