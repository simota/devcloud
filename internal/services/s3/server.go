package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
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
	if bucket, key, ok := parseVirtualHostStyle(r.Host, r.URL.Path); ok {
		if err := validateBucketName(bucket); err != nil {
			writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
			return
		}
		if key != "" {
			s.handleObject(w, r, bucket, key)
			return
		}
		s.handleBucket(w, r, bucket)
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
		if r.URL.Query().Has("versioning") {
			s.handlePutBucketVersioning(w, r, name)
			return
		}
		if r.URL.Query().Has("object-lock") {
			s.handlePutBucketObjectLockConfiguration(w, r, name)
			return
		}
		if r.URL.Query().Has("lifecycle") {
			s.handlePutBucketLifecycle(w, r, name)
			return
		}
		if r.URL.Query().Has("notification") {
			s.handlePutBucketNotification(w, r, name)
			return
		}
		if r.URL.Query().Has("inventory") {
			s.handlePutBucketInventory(w, r, name)
			return
		}
		if r.URL.Query().Has("analytics") {
			s.handlePutBucketAnalytics(w, r, name)
			return
		}
		if r.URL.Query().Has("replication") {
			s.handlePutBucketReplication(w, r, name)
			return
		}
		if r.URL.Query().Has("policy") {
			s.handlePutBucketPolicy(w, r, name)
			return
		}
		if r.URL.Query().Has("acl") {
			s.handlePutBucketACL(w, r, name)
			return
		}
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
		if r.URL.Query().Has("versioning") {
			s.handleGetBucketVersioning(w, r, name)
			return
		}
		if r.URL.Query().Has("object-lock") {
			s.handleGetBucketObjectLockConfiguration(w, r, name)
			return
		}
		if r.URL.Query().Has("versions") {
			s.handleListObjectVersions(w, r, name)
			return
		}
		if r.URL.Query().Has("lifecycle") {
			s.handleGetBucketLifecycle(w, r, name)
			return
		}
		if r.URL.Query().Has("notification") {
			s.handleGetBucketNotification(w, r, name)
			return
		}
		if r.URL.Query().Has("inventory") {
			s.handleGetBucketInventory(w, r, name)
			return
		}
		if r.URL.Query().Has("analytics") {
			s.handleGetBucketAnalytics(w, r, name)
			return
		}
		if r.URL.Query().Has("replication") {
			s.handleGetBucketReplication(w, r, name)
			return
		}
		if r.URL.Query().Has("policy") {
			s.handleGetBucketPolicy(w, r, name)
			return
		}
		if r.URL.Query().Has("acl") {
			s.handleGetBucketACL(w, r, name)
			return
		}
		if r.URL.Query().Has("uploads") {
			s.handleListMultipartUploads(w, r, name)
			return
		}
		s.handleListObjects(w, r, name)
	case http.MethodDelete:
		if r.URL.Query().Has("object-lock") {
			s.handleDeleteBucketObjectLockConfiguration(w, r, name)
			return
		}
		if r.URL.Query().Has("lifecycle") {
			s.handleDeleteBucketLifecycle(w, r, name)
			return
		}
		if r.URL.Query().Has("policy") {
			s.handleDeleteBucketPolicy(w, r, name)
			return
		}
		if r.URL.Query().Has("inventory") {
			s.handleDeleteBucketInventory(w, r, name)
			return
		}
		if r.URL.Query().Has("analytics") {
			s.handleDeleteBucketAnalytics(w, r, name)
			return
		}
		if r.URL.Query().Has("replication") {
			s.handleDeleteBucketReplication(w, r, name)
			return
		}
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
