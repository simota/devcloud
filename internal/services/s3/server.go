package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr            string
	Region          string
	MaxObjectBytes  int64
	AuthMode        string
	AccessKeyID     string
	SecretAccessKey string
}

type Server struct {
	config Config
	store  BucketStore
}

func NewServer(cfg Config, store BucketStore) *Server {
	return &Server{config: cfg, store: store}
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
	w.Header().Set("Server", "AmazonS3")
	if err := s.verifySignature(r); err != nil {
		writeSignatureError(w, err)
		return
	}
	if r.URL.Path == "/" {
		s.handleService(w, r)
		return
	}

	bucket, key, ok := parsePathStyle(r.URL.Path)
	if !ok {
		writeXMLError(w, "NotImplemented", "operation is not implemented", http.StatusNotImplemented)
		return
	}
	if err := validateBucketName(bucket); err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if key != "" {
		s.handleObject(w, r, bucket, key)
		return
	}
	s.handleBucket(w, r, bucket)
}

func (s *Server) handleService(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	buckets, err := s.store.ListBuckets(r.Context())
	if err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	response := listAllMyBucketsResult{
		XMLName: xml.Name{Local: "ListAllMyBucketsResult"},
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: owner{
			ID:          "devcloud",
			DisplayName: "devcloud",
		},
	}
	for _, bucket := range buckets {
		response.Buckets.Bucket = append(response.Buckets.Bucket, bucketElement{
			Name:         bucket.Name,
			CreationDate: bucket.CreatedAt.Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, response)
}

func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodPut:
		_, created, err := s.store.CreateBucket(r.Context(), name)
		if err != nil {
			writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
			return
		}
		if !created {
			writeXMLError(w, "BucketAlreadyOwnedByYou", "bucket already exists", http.StatusConflict)
			return
		}
		w.Header().Set("Location", "/"+name)
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		_, ok, err := s.store.GetBucket(r.Context(), name)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if r.URL.Query().Has("location") {
			s.handleGetBucketLocation(w, r, name)
			return
		}
		if r.URL.Query().Has("uploads") {
			s.handleListMultipartUploads(w, r, name)
			return
		}
		s.handleListObjects(w, r, name)
	case http.MethodDelete:
		deleted, err := s.store.DeleteBucket(r.Context(), name)
		if err != nil {
			writeXMLError(w, "BucketNotEmpty", "bucket is not empty", http.StatusConflict)
			return
		}
		if !deleted {
			writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "PUT, HEAD, GET, DELETE")
	}
}

func (s *Server) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	_, ok, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	constraint := s.config.Region
	if constraint == "" || constraint == "us-east-1" {
		constraint = ""
	}
	writeXML(w, http.StatusOK, locationConstraint{
		XMLName: xml.Name{Local: "LocationConstraint"},
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Value:   constraint,
	})
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	if err := validateObjectKey(key); err != nil {
		writeXMLError(w, "InvalidArgument", "invalid object key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
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
		if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
			if err := validateUploadID(uploadID); err != nil {
				writeXMLError(w, "InvalidArgument", "invalid upload id", http.StatusBadRequest)
				return
			}
			s.handleAbortMultipartUpload(w, r, bucket, key, uploadID)
			return
		}
		deleted, err := s.store.DeleteObject(r.Context(), bucket, key)
		if err != nil {
			writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
			return
		}
		if !deleted {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "PUT, HEAD, GET, DELETE, POST")
	}
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	body := r.Body
	if s.config.MaxObjectBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, s.config.MaxObjectBytes)
	}
	defer body.Close()

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
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket string, key string, source string) {
	sourceBucket, sourceKey, err := parseCopySource(source)
	if err != nil {
		writeXMLError(w, "InvalidArgument", "invalid copy source", http.StatusBadRequest)
		return
	}
	sourceObject, body, ok, err := s.store.GetObject(r.Context(), sourceBucket, sourceKey)
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
	}
	if strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE") {
		input.ContentType = r.Header.Get("Content-Type")
		input.ContentEncoding = r.Header.Get("Content-Encoding")
		input.CacheControl = r.Header.Get("Cache-Control")
		input.ContentDisposition = r.Header.Get("Content-Disposition")
		input.Metadata = userMetadataFromHeaders(r.Header)
	}
	object, err := s.store.PutObject(r.Context(), input)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", object.ETag)
	writeXML(w, http.StatusOK, copyObjectResult{
		XMLName:      xml.Name{Local: "CopyObjectResult"},
		LastModified: object.LastModified.Format(time.RFC3339),
		ETag:         object.ETag,
	})
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket string, key string, headOnly bool) {
	object, body, ok, err := s.store.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
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

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
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

func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	upload, err := s.store.CreateMultipartUpload(r.Context(), CreateMultipartUploadInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           userMetadataFromHeaders(r.Header),
	})
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
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

func parsePathStyle(path string) (bucket string, key string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "", "", false
	}
	bucket, key, _ = strings.Cut(trimmed, "/")
	if bucket == "" {
		return "", "", false
	}
	return bucket, key, true
}

func parseCopySource(source string) (bucket string, key string, err error) {
	source = strings.TrimPrefix(source, "/")
	source, _, _ = strings.Cut(source, "?")
	if source == "" {
		return "", "", fmt.Errorf("copy source is empty")
	}
	bucket, key, ok := strings.Cut(source, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fmt.Errorf("copy source must include bucket and key")
	}
	decodedBucket, err := url.PathUnescape(bucket)
	if err != nil {
		return "", "", err
	}
	decodedKey, err := url.PathUnescape(key)
	if err != nil {
		return "", "", err
	}
	return decodedBucket, decodedKey, nil
}

func userMetadataFromHeaders(header http.Header) map[string]string {
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

func writeObjectHeaders(w http.ResponseWriter, object Object) {
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	if object.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", object.ContentEncoding)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	for key, value := range object.Metadata {
		w.Header().Set("x-amz-meta-"+key, value)
	}
}

func parseRange(header string, size int64) (start int64, end int64, partial bool, err error) {
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

type objectListing struct {
	contents              []Object
	commonPrefixes        []string
	truncated             bool
	nextMarker            string
	nextContinuationToken string
}

func buildObjectListing(objects []Object, prefix string, delimiter string, marker string, maxKeys int) objectListing {
	listing := objectListing{}
	if maxKeys == 0 {
		return listing
	}
	commonPrefixes := map[string]bool{}
	count := 0
	for i := 0; i < len(objects); i++ {
		object := objects[i]
		if marker != "" && object.Key <= marker {
			continue
		}

		itemKey := object.Key
		itemIsObject := true
		lastKeyForItem := object.Key
		if delimiter != "" {
			remainder := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				itemKey = prefix + remainder[:index+len(delimiter)]
				itemIsObject = false
				for i+1 < len(objects) && strings.HasPrefix(objects[i+1].Key, itemKey) {
					i++
					lastKeyForItem = objects[i].Key
				}
				if commonPrefixes[itemKey] {
					continue
				}
			}
		}

		if count >= maxKeys {
			listing.truncated = true
			listing.nextMarker = marker
			listing.nextContinuationToken = encodeContinuationToken(marker)
			if listing.nextMarker == "" {
				listing.nextMarker = object.Key
				listing.nextContinuationToken = encodeContinuationToken(object.Key)
			}
			return listing
		}

		if itemIsObject {
			listing.contents = append(listing.contents, object)
		} else {
			commonPrefixes[itemKey] = true
			listing.commonPrefixes = append(listing.commonPrefixes, itemKey)
		}
		count++
		marker = lastKeyForItem
	}
	return listing
}

func parseMaxKeys(value string) (int, error) {
	if value == "" {
		return 1000, nil
	}
	maxKeys, err := strconv.Atoi(value)
	if err != nil || maxKeys < 0 {
		return 0, fmt.Errorf("invalid max-keys")
	}
	if maxKeys > 1000 {
		return 1000, nil
	}
	return maxKeys, nil
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

type continuationToken struct {
	LastKey string `json:"lastKey"`
}

func encodeContinuationToken(lastKey string) string {
	data, err := json.Marshal(continuationToken{LastKey: lastKey})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeContinuationToken(value string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	var token continuationToken
	if err := json.Unmarshal(data, &token); err != nil {
		return "", err
	}
	return token.LastKey, nil
}

func encodeListValue(value string, encodingType string) string {
	if encodingType != "url" || value == "" {
		return value
	}
	return awsPercentEncode(value, "~-_.")
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeXMLError(w, "MethodNotAllowed", "method not allowed", http.StatusMethodNotAllowed)
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	xml.NewEncoder(w).Encode(value)
}

func writeXMLError(w http.ResponseWriter, code string, message string, status int) {
	writeXML(w, status, errorResponse{
		XMLName: xml.Name{Local: "Error"},
		Code:    code,
		Message: message,
	})
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucketElement `xml:"Bucket"`
}

type bucketElement struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type listBucketResult struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	Xmlns                 string                `xml:"xmlns,attr"`
	Name                  string                `xml:"Name"`
	Prefix                string                `xml:"Prefix"`
	Delimiter             string                `xml:"Delimiter,omitempty"`
	Marker                string                `xml:"Marker,omitempty"`
	NextMarker            string                `xml:"NextMarker,omitempty"`
	ContinuationToken     string                `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string                `xml:"NextContinuationToken,omitempty"`
	StartAfter            string                `xml:"StartAfter,omitempty"`
	KeyCount              int                   `xml:"KeyCount"`
	MaxKeys               int                   `xml:"MaxKeys"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	ListType              int                   `xml:"ListType,omitempty"`
	Contents              []objectElement       `xml:"Contents"`
	CommonPrefixes        []commonPrefixElement `xml:"CommonPrefixes"`
}

type objectElement struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixElement struct {
	Prefix string `xml:"Prefix"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
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
