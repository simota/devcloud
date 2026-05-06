package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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

func (s *Server) handlePutBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := readRequestBody(r, 1<<20)
	if err != nil {
		writeXMLError(w, "MalformedPolicy", "bucket policy is too large", http.StatusBadRequest)
		return
	}
	if !json.Valid(body) {
		writeXMLError(w, "MalformedPolicy", "bucket policy must be valid JSON", http.StatusBadRequest)
		return
	}
	if err := s.store.PutBucketPolicy(r.Context(), bucket, body); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	policy, bucketExists, policyExists, err := s.store.GetBucketPolicy(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !policyExists {
		writeXMLError(w, "NoSuchBucketPolicy", "bucket policy does not exist", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(policy)
}

func (s *Server) handleDeleteBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.DeleteBucketPolicy(r.Context(), bucket); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePutBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	var config LifecycleConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateLifecycleConfiguration(config); err != nil {
		if errors.Is(err, errUnsupportedLifecycleRule) {
			writeXMLError(w, "NotImplemented", "unsupported lifecycle rule action", http.StatusNotImplemented)
			return
		}
		writeXMLError(w, "MalformedXML", "lifecycle configuration is malformed", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "LifecycleConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketLifecycle(r.Context(), bucket, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	config, bucketExists, lifecycleExists, err := s.store.GetBucketLifecycle(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !lifecycleExists {
		writeXMLError(w, "NoSuchLifecycleConfiguration", "bucket lifecycle does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "LifecycleConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handleDeleteBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.DeleteBucketLifecycle(r.Context(), bucket); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePutBucketNotification(w http.ResponseWriter, r *http.Request, bucket string) {
	var config NotificationConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateNotificationConfiguration(config); err != nil {
		writeXMLError(w, "InvalidArgument", "notification configuration is invalid", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "NotificationConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketNotification(r.Context(), bucket, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketNotification(w http.ResponseWriter, r *http.Request, bucket string) {
	config, bucketExists, err := s.store.GetBucketNotification(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "NotificationConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handlePutBucketInventory(w http.ResponseWriter, r *http.Request, bucket string) {
	id, err := configurationIDFromQuery(r.URL.Query())
	if err != nil {
		writeXMLError(w, "InvalidArgument", "inventory configuration id is invalid", http.StatusBadRequest)
		return
	}
	var config InventoryConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateInventoryConfiguration(id, config); err != nil {
		writeXMLError(w, "InvalidArgument", "inventory configuration is invalid", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "InventoryConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketInventory(r.Context(), bucket, id, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketInventory(w http.ResponseWriter, r *http.Request, bucket string) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		s.handleListBucketInventories(w, r, bucket)
		return
	}
	if err := validateConfigurationID(id); err != nil {
		writeXMLError(w, "InvalidArgument", "inventory configuration id is invalid", http.StatusBadRequest)
		return
	}
	config, bucketExists, configExists, err := s.store.GetBucketInventory(r.Context(), bucket, id)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !configExists {
		writeXMLError(w, "NoSuchConfiguration", "inventory configuration does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "InventoryConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handleListBucketInventories(w http.ResponseWriter, r *http.Request, bucket string) {
	configs, bucketExists, err := s.store.ListBucketInventories(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeXML(w, http.StatusOK, listInventoryConfigurationsResult{
		XMLName:                 xml.Name{Local: "ListInventoryConfigurationsResult"},
		Xmlns:                   "http://s3.amazonaws.com/doc/2006-03-01/",
		IsTruncated:             false,
		InventoryConfigurations: configs,
	})
}

func (s *Server) handleDeleteBucketInventory(w http.ResponseWriter, r *http.Request, bucket string) {
	id, err := configurationIDFromQuery(r.URL.Query())
	if err != nil {
		writeXMLError(w, "InvalidArgument", "inventory configuration id is invalid", http.StatusBadRequest)
		return
	}
	if _, err := s.store.DeleteBucketInventory(r.Context(), bucket, id); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePutBucketAnalytics(w http.ResponseWriter, r *http.Request, bucket string) {
	id, err := configurationIDFromQuery(r.URL.Query())
	if err != nil {
		writeXMLError(w, "InvalidArgument", "analytics configuration id is invalid", http.StatusBadRequest)
		return
	}
	var config AnalyticsConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateAnalyticsConfiguration(id, config); err != nil {
		writeXMLError(w, "InvalidArgument", "analytics configuration is invalid", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "AnalyticsConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketAnalytics(r.Context(), bucket, id, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketAnalytics(w http.ResponseWriter, r *http.Request, bucket string) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		s.handleListBucketAnalytics(w, r, bucket)
		return
	}
	if err := validateConfigurationID(id); err != nil {
		writeXMLError(w, "InvalidArgument", "analytics configuration id is invalid", http.StatusBadRequest)
		return
	}
	config, bucketExists, configExists, err := s.store.GetBucketAnalytics(r.Context(), bucket, id)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !configExists {
		writeXMLError(w, "NoSuchConfiguration", "analytics configuration does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "AnalyticsConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handleListBucketAnalytics(w http.ResponseWriter, r *http.Request, bucket string) {
	configs, bucketExists, err := s.store.ListBucketAnalytics(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeXML(w, http.StatusOK, listAnalyticsConfigurationsResult{
		XMLName:                 xml.Name{Local: "ListAnalyticsConfigurationsResult"},
		Xmlns:                   "http://s3.amazonaws.com/doc/2006-03-01/",
		IsTruncated:             false,
		AnalyticsConfigurations: configs,
	})
}

func (s *Server) handleDeleteBucketAnalytics(w http.ResponseWriter, r *http.Request, bucket string) {
	id, err := configurationIDFromQuery(r.URL.Query())
	if err != nil {
		writeXMLError(w, "InvalidArgument", "analytics configuration id is invalid", http.StatusBadRequest)
		return
	}
	if _, err := s.store.DeleteBucketAnalytics(r.Context(), bucket, id); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePutBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	var config ReplicationConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateReplicationConfiguration(config); err != nil {
		writeXMLError(w, "InvalidArgument", "replication configuration is invalid", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "ReplicationConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketReplication(r.Context(), bucket, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	config, bucketExists, replicationExists, err := s.store.GetBucketReplication(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !replicationExists {
		writeXMLError(w, "ReplicationConfigurationNotFoundError", "replication configuration does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "ReplicationConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handleDeleteBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.DeleteBucketReplication(r.Context(), bucket); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePutBucketACL(w http.ResponseWriter, r *http.Request, bucket string) {
	acl, err := aclFromRequest(r)
	if err != nil {
		writeXMLError(w, "MalformedACLError", "bucket ACL is malformed", http.StatusBadRequest)
		return
	}
	if err := s.store.PutBucketACL(r.Context(), bucket, acl); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketACL(w http.ResponseWriter, r *http.Request, bucket string) {
	acl, ok, err := s.store.GetBucketACL(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeACL(w, acl)
}

func (s *Server) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	var request versioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if request.Status != "Enabled" && request.Status != "Suspended" {
		writeXMLError(w, "MalformedXML", "versioning status must be Enabled or Suspended", http.StatusBadRequest)
		return
	}
	if err := s.store.PutBucketVersioning(r.Context(), bucket, request.Status); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePutBucketObjectLockConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	var config ObjectLockConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if err := validateObjectLockConfiguration(config); err != nil {
		writeXMLError(w, "InvalidArgument", "object lock configuration is invalid", http.StatusBadRequest)
		return
	}
	config.XMLName = xml.Name{Local: "ObjectLockConfiguration"}
	config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	if err := s.store.PutBucketObjectLockConfiguration(r.Context(), bucket, config); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetBucketObjectLockConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	config, bucketExists, configExists, err := s.store.GetBucketObjectLockConfiguration(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !bucketExists {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !configExists {
		writeXMLError(w, "ObjectLockConfigurationNotFoundError", "object lock configuration does not exist", http.StatusNotFound)
		return
	}
	config.XMLName = xml.Name{Local: "ObjectLockConfiguration"}
	if config.Xmlns == "" {
		config.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	}
	writeXML(w, http.StatusOK, config)
}

func (s *Server) handleDeleteBucketObjectLockConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.DeleteBucketObjectLockConfiguration(r.Context(), bucket); err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	status, ok, err := s.store.GetBucketVersioning(r.Context(), bucket)
	if err != nil {
		writeXMLError(w, "InvalidBucketName", "invalid bucket name", http.StatusBadRequest)
		return
	}
	if !ok {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeXML(w, http.StatusOK, versioningConfiguration{
		XMLName: xml.Name{Local: "VersioningConfiguration"},
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Status:  status,
	})
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
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "PUT, HEAD, GET, DELETE, POST")
	}
}

func (s *Server) handleSelectObjectContent(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	var request selectObjectContentRequest
	if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
		writeXMLError(w, "MalformedXML", "request body is malformed", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.ExpressionType) != "SQL" {
		writeXMLError(w, "InvalidExpressionType", "only SQL expressions are supported", http.StatusBadRequest)
		return
	}
	if !isSupportedSelectExpression(request.Expression) {
		writeXMLError(w, "NotImplemented", "only SELECT * FROM S3Object is supported", http.StatusNotImplemented)
		return
	}
	object, body, ok, err := s.store.GetObjectVersion(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	if !ok || object.DeleteMarker {
		writeXMLError(w, "NoSuchKey", "object does not exist", http.StatusNotFound)
		return
	}
	output, err := evaluateSelectObjectContent(request, body)
	if err != nil {
		writeXMLError(w, "NotImplemented", err.Error(), http.StatusNotImplemented)
		return
	}
	payload := append(encodeEventStreamMessage(map[string]string{
		":message-type": "event",
		":event-type":   "Records",
		":content-type": "application/octet-stream",
	}, output), encodeEventStreamMessage(map[string]string{
		":message-type": "event",
		":event-type":   "End",
	}, nil)...)
	w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
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

func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	encryption, err := serverSideEncryptionFromHeaders(r.Header)
	if err != nil {
		writeServerSideEncryptionError(w, err)
		return
	}
	upload, err := s.store.CreateMultipartUpload(r.Context(), CreateMultipartUploadInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        r.Header.Get("Content-Type"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		Metadata:           userMetadataFromHeaders(r.Header),
		Encryption:         encryption,
	})
	if err != nil {
		writeXMLError(w, "NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		return
	}
	writeServerSideEncryptionHeaders(w, upload.Encryption)
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
	if err := s.recordObjectEvent(r.Context(), bucket, key, "s3:ObjectCreated:CompleteMultipartUpload", object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.replicateObjectWrite(r.Context(), bucket, key, object); err != nil {
		writeXMLError(w, "InternalError", "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", object.ETag)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
	writeServerSideEncryptionHeaders(w, object.Encryption)
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

func parseVirtualHostStyle(host string, path string) (bucket string, key string, ok bool) {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return "", "", false
	}
	if withoutPort, _, found := strings.Cut(host, ":"); found {
		host = withoutPort
	}
	if !strings.HasSuffix(host, ".localhost") {
		return "", "", false
	}
	bucket = strings.TrimSuffix(host, ".localhost")
	if bucket == "" || strings.Contains(bucket, ".") {
		return "", "", false
	}
	key = strings.TrimPrefix(path, "/")
	return bucket, key, true
}

func parseCopySource(source string) (bucket string, key string, versionID string, err error) {
	source = strings.TrimPrefix(source, "/")
	sourcePath, rawQuery, _ := strings.Cut(source, "?")
	source = sourcePath
	if source == "" {
		return "", "", "", fmt.Errorf("copy source is empty")
	}
	bucket, key, ok := strings.Cut(source, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", "", fmt.Errorf("copy source must include bucket and key")
	}
	decodedBucket, err := url.PathUnescape(bucket)
	if err != nil {
		return "", "", "", err
	}
	decodedKey, err := url.PathUnescape(key)
	if err != nil {
		return "", "", "", err
	}
	if rawQuery != "" {
		values, err := url.ParseQuery(rawQuery)
		if err != nil {
			return "", "", "", err
		}
		versionID = values.Get("versionId")
	}
	return decodedBucket, decodedKey, versionID, nil
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

func serverSideEncryptionFromHeaders(header http.Header) (ServerSideEncryption, error) {
	if header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		header.Get("x-amz-server-side-encryption-customer-key-MD5") != "" {
		return ServerSideEncryption{}, errUnsupportedSSECustomerKey
	}
	algorithm := strings.TrimSpace(header.Get("x-amz-server-side-encryption"))
	kmsKeyID := strings.TrimSpace(header.Get("x-amz-server-side-encryption-aws-kms-key-id"))
	bucketKeyValue := strings.TrimSpace(header.Get("x-amz-server-side-encryption-bucket-key-enabled"))
	if algorithm == "" {
		if kmsKeyID != "" || bucketKeyValue != "" {
			return ServerSideEncryption{}, errInvalidServerSideEncryption
		}
		return ServerSideEncryption{}, nil
	}
	encryption := ServerSideEncryption{Algorithm: algorithm}
	switch algorithm {
	case "AES256":
		if kmsKeyID != "" || bucketKeyValue != "" {
			return ServerSideEncryption{}, errInvalidServerSideEncryption
		}
	case "aws:kms":
		encryption.KMSKeyID = kmsKeyID
		if bucketKeyValue != "" {
			enabled, err := strconv.ParseBool(bucketKeyValue)
			if err != nil {
				return ServerSideEncryption{}, errInvalidServerSideEncryption
			}
			encryption.BucketKeyEnabled = &enabled
		}
	default:
		return ServerSideEncryption{}, errUnsupportedServerSideEncryption
	}
	return encryption, nil
}

func hasServerSideEncryptionHeaders(header http.Header) bool {
	for _, key := range []string{
		"x-amz-server-side-encryption",
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-bucket-key-enabled",
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-customer-key",
		"x-amz-server-side-encryption-customer-key-MD5",
	} {
		if header.Get(key) != "" {
			return true
		}
	}
	return false
}

func objectLockFromHeaders(header http.Header) (ObjectRetention, ObjectLegalHold, error) {
	retention := ObjectRetention{
		Mode:            strings.TrimSpace(header.Get("x-amz-object-lock-mode")),
		RetainUntilDate: strings.TrimSpace(header.Get("x-amz-object-lock-retain-until-date")),
	}
	legalHold := ObjectLegalHold{Status: strings.TrimSpace(header.Get("x-amz-object-lock-legal-hold"))}
	if retention.Mode != "" || retention.RetainUntilDate != "" {
		if err := validateObjectRetention(retention); err != nil {
			return ObjectRetention{}, ObjectLegalHold{}, err
		}
	}
	if legalHold.Status != "" {
		if err := validateObjectLegalHold(legalHold); err != nil {
			return ObjectRetention{}, ObjectLegalHold{}, err
		}
	}
	return retention, legalHold, nil
}

func hasObjectLockHeaders(header http.Header) bool {
	for _, key := range []string{
		"x-amz-object-lock-mode",
		"x-amz-object-lock-retain-until-date",
		"x-amz-object-lock-legal-hold",
	} {
		if header.Get(key) != "" {
			return true
		}
	}
	return false
}

func bypassGovernanceRetentionFromHeaders(header http.Header) (bool, error) {
	value := strings.TrimSpace(header.Get("x-amz-bypass-governance-retention"))
	if value == "" {
		return false, nil
	}
	return strconv.ParseBool(value)
}

func writeServerSideEncryptionHeaders(w http.ResponseWriter, encryption ServerSideEncryption) {
	if encryption.Algorithm == "" {
		return
	}
	w.Header().Set("x-amz-server-side-encryption", encryption.Algorithm)
	if encryption.KMSKeyID != "" {
		w.Header().Set("x-amz-server-side-encryption-aws-kms-key-id", encryption.KMSKeyID)
	}
	if encryption.BucketKeyEnabled != nil {
		w.Header().Set("x-amz-server-side-encryption-bucket-key-enabled", strconv.FormatBool(*encryption.BucketKeyEnabled))
	}
}

func writeObjectLockHeaders(w http.ResponseWriter, object Object) {
	if object.Retention.Mode != "" {
		w.Header().Set("x-amz-object-lock-mode", object.Retention.Mode)
	}
	if object.Retention.RetainUntilDate != "" {
		w.Header().Set("x-amz-object-lock-retain-until-date", object.Retention.RetainUntilDate)
	}
	if object.LegalHold.Status != "" {
		w.Header().Set("x-amz-object-lock-legal-hold", object.LegalHold.Status)
	}
}

func writeServerSideEncryptionError(w http.ResponseWriter, err error) {
	if errors.Is(err, errUnsupportedSSECustomerKey) || errors.Is(err, errUnsupportedServerSideEncryption) {
		writeXMLError(w, "NotImplemented", "server-side encryption mode is not supported", http.StatusNotImplemented)
		return
	}
	writeXMLError(w, "InvalidArgument", "server-side encryption headers are invalid", http.StatusBadRequest)
}

func readRequestBody(r *http.Request, limit int64) ([]byte, error) {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("request body exceeds limit")
	}
	return data, nil
}

func isSupportedSelectExpression(expression string) bool {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(expression)), " ")
	return strings.EqualFold(normalized, "SELECT * FROM S3Object") ||
		strings.EqualFold(normalized, "SELECT * FROM S3Object s")
}

func evaluateSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	switch {
	case request.InputSerialization.CSV != nil:
		return evaluateCSVSelectObjectContent(request, body)
	case request.InputSerialization.JSON != nil:
		return evaluateJSONSelectObjectContent(request, body)
	default:
		return nil, fmt.Errorf("input serialization is not supported")
	}
}

func evaluateCSVSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	if request.OutputSerialization.CSV == nil {
		return nil, fmt.Errorf("only CSV output is supported for CSV input")
	}
	input := request.InputSerialization.CSV.withDefaults()
	output := request.OutputSerialization.CSV.withDefaults()
	if len(input.fieldDelimiter()) != 1 || len(output.fieldDelimiter()) != 1 {
		return nil, fmt.Errorf("only single-byte CSV field delimiters are supported")
	}
	reader := csv.NewReader(bytes.NewReader(body))
	reader.Comma = rune(input.fieldDelimiter()[0])
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSV input is malformed")
	}
	if strings.EqualFold(input.FileHeaderInfo, "USE") && len(records) > 0 {
		records = records[1:]
	}
	var out bytes.Buffer
	writer := csv.NewWriter(&out)
	writer.Comma = rune(output.fieldDelimiter()[0])
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	if delimiter := output.recordDelimiter(); delimiter != "\n" {
		return []byte(strings.ReplaceAll(out.String(), "\n", delimiter)), nil
	}
	return out.Bytes(), nil
}

func evaluateJSONSelectObjectContent(request selectObjectContentRequest, body []byte) ([]byte, error) {
	input := request.InputSerialization.JSON
	if request.OutputSerialization.JSON == nil {
		return nil, fmt.Errorf("only JSON output is supported for JSON input")
	}
	if input == nil || (input.Type != "" && input.Type != "LINES") {
		return nil, fmt.Errorf("only JSON LINES input is supported")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var out bytes.Buffer
	for {
		var value any
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("JSON input is malformed")
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		out.Write(encoded)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func encodeEventStreamMessage(headers map[string]string, payload []byte) []byte {
	var headerBytes bytes.Buffer
	for name, value := range headers {
		headerBytes.WriteByte(byte(len(name)))
		headerBytes.WriteString(name)
		headerBytes.WriteByte(7)
		_ = binary.Write(&headerBytes, binary.BigEndian, uint16(len(value)))
		headerBytes.WriteString(value)
	}
	totalLength := uint32(16 + headerBytes.Len() + len(payload))
	headersLength := uint32(headerBytes.Len())
	message := make([]byte, 0, totalLength)
	prelude := make([]byte, 8)
	binary.BigEndian.PutUint32(prelude[0:4], totalLength)
	binary.BigEndian.PutUint32(prelude[4:8], headersLength)
	preludeCRC := make([]byte, 4)
	binary.BigEndian.PutUint32(preludeCRC, crc32.ChecksumIEEE(prelude))
	message = append(message, prelude...)
	message = append(message, preludeCRC...)
	message = append(message, headerBytes.Bytes()...)
	message = append(message, payload...)
	messageCRC := make([]byte, 4)
	binary.BigEndian.PutUint32(messageCRC, crc32.ChecksumIEEE(message))
	message = append(message, messageCRC...)
	return message
}

func aclFromRequest(r *http.Request) (string, error) {
	if canned := strings.TrimSpace(r.Header.Get("x-amz-acl")); canned != "" {
		if !isSupportedCannedACL(canned) {
			return "", fmt.Errorf("unsupported canned acl")
		}
		return canned, nil
	}
	body, err := readRequestBody(r, 64<<10)
	if err != nil {
		return "", err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return "private", nil
	}
	var parsed accessControlPolicy
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	if parsed.CannedACL != "" {
		if !isSupportedCannedACL(parsed.CannedACL) {
			return "", fmt.Errorf("unsupported canned acl")
		}
		return parsed.CannedACL, nil
	}
	return "custom", nil
}

func (s *Server) applyBucketLifecycle(ctx context.Context, bucket string) error {
	if err := validateBucketName(bucket); err != nil {
		return nil
	}
	_, _, err := s.store.ApplyBucketLifecycle(ctx, bucket, time.Now().UTC())
	return err
}

func validateLifecycleConfiguration(config LifecycleConfiguration) error {
	if len(config.Rules) == 0 {
		return fmt.Errorf("lifecycle configuration requires at least one rule")
	}
	for _, rule := range config.Rules {
		if len(rule.Transitions) > 0 || len(rule.NoncurrentVersionTransitions) > 0 || len(rule.NoncurrentVersionExpirations) > 0 || len(rule.AbortIncompleteMultipartUpload) > 0 {
			return errUnsupportedLifecycleRule
		}
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return fmt.Errorf("invalid lifecycle rule status")
		}
		if rule.Expiration.Days == nil && strings.TrimSpace(rule.Expiration.Date) == "" {
			return fmt.Errorf("lifecycle rule requires expiration")
		}
		if rule.Expiration.Days != nil && *rule.Expiration.Days < 0 {
			return fmt.Errorf("lifecycle expiration days must be non-negative")
		}
		if rule.Expiration.Date != "" {
			if _, err := parseLifecycleExpirationDate(rule.Expiration.Date); err != nil {
				return fmt.Errorf("invalid lifecycle expiration date")
			}
		}
	}
	return nil
}

func validateObjectLockConfiguration(config ObjectLockConfiguration) error {
	if config.ObjectLockEnabled != "" && config.ObjectLockEnabled != "Enabled" {
		return fmt.Errorf("invalid object lock enabled value")
	}
	if config.Rule.DefaultRetention.Mode == "" && config.Rule.DefaultRetention.Days == 0 && config.Rule.DefaultRetention.Years == 0 {
		return nil
	}
	if config.Rule.DefaultRetention.Mode != "GOVERNANCE" && config.Rule.DefaultRetention.Mode != "COMPLIANCE" {
		return fmt.Errorf("invalid default retention mode")
	}
	if config.Rule.DefaultRetention.Days > 0 && config.Rule.DefaultRetention.Years > 0 {
		return fmt.Errorf("default retention must use days or years")
	}
	if config.Rule.DefaultRetention.Days < 0 || config.Rule.DefaultRetention.Years < 0 {
		return fmt.Errorf("default retention must be positive")
	}
	if config.Rule.DefaultRetention.Days == 0 && config.Rule.DefaultRetention.Years == 0 {
		return fmt.Errorf("default retention requires days or years")
	}
	return nil
}

func validateObjectRetention(retention ObjectRetention) error {
	retention = cleanObjectRetention(retention)
	if retention.Mode != "GOVERNANCE" && retention.Mode != "COMPLIANCE" {
		return fmt.Errorf("invalid retention mode")
	}
	if retention.RetainUntilDate == "" {
		return fmt.Errorf("retention requires retain until date")
	}
	if _, err := time.Parse(time.RFC3339, retention.RetainUntilDate); err != nil {
		return fmt.Errorf("invalid retain until date")
	}
	return nil
}

func validateObjectLegalHold(legalHold ObjectLegalHold) error {
	switch cleanObjectLegalHold(legalHold).Status {
	case "ON", "OFF":
		return nil
	default:
		return fmt.Errorf("invalid legal hold status")
	}
}

func validateNotificationConfiguration(config NotificationConfiguration) error {
	for _, topic := range config.TopicConfigurations {
		if strings.TrimSpace(topic.Topic) == "" || len(topic.Events) == 0 {
			return fmt.Errorf("topic notification requires destination and events")
		}
		if err := validateNotificationEventsAndFilter(topic.Events, topic.Filter); err != nil {
			return err
		}
	}
	for _, queue := range config.QueueConfigurations {
		if strings.TrimSpace(queue.Queue) == "" || len(queue.Events) == 0 {
			return fmt.Errorf("queue notification requires destination and events")
		}
		if err := validateNotificationEventsAndFilter(queue.Events, queue.Filter); err != nil {
			return err
		}
	}
	for _, lambda := range config.LambdaFunctionConfigurations {
		if strings.TrimSpace(lambda.LambdaFunction) == "" || len(lambda.Events) == 0 {
			return fmt.Errorf("lambda notification requires destination and events")
		}
		if err := validateNotificationEventsAndFilter(lambda.Events, lambda.Filter); err != nil {
			return err
		}
	}
	return nil
}

func validateInventoryConfiguration(id string, config InventoryConfiguration) error {
	if err := validateConfigurationID(id); err != nil {
		return err
	}
	if config.ID != "" && config.ID != id {
		return fmt.Errorf("inventory id must match query id")
	}
	if strings.TrimSpace(config.IncludedObjectVersions) != "" && config.IncludedObjectVersions != "All" && config.IncludedObjectVersions != "Current" {
		return fmt.Errorf("invalid included object versions")
	}
	if strings.TrimSpace(config.Schedule.Frequency) != "" && config.Schedule.Frequency != "Daily" && config.Schedule.Frequency != "Weekly" {
		return fmt.Errorf("invalid inventory frequency")
	}
	if strings.TrimSpace(config.Destination.S3BucketDestination.Format) != "" {
		switch config.Destination.S3BucketDestination.Format {
		case "CSV", "ORC", "Parquet":
		default:
			return fmt.Errorf("invalid inventory format")
		}
	}
	return nil
}

func validateAnalyticsConfiguration(id string, config AnalyticsConfiguration) error {
	if err := validateConfigurationID(id); err != nil {
		return err
	}
	if config.ID != "" && config.ID != id {
		return fmt.Errorf("analytics id must match query id")
	}
	if strings.TrimSpace(config.StorageClassAnalysis.DataExport.OutputSchemaVersion) != "" && config.StorageClassAnalysis.DataExport.OutputSchemaVersion != "V_1" {
		return fmt.Errorf("invalid analytics output schema version")
	}
	if strings.TrimSpace(config.StorageClassAnalysis.DataExport.Destination.S3BucketDestination.Format) != "" && config.StorageClassAnalysis.DataExport.Destination.S3BucketDestination.Format != "CSV" {
		return fmt.Errorf("invalid analytics destination format")
	}
	return nil
}

func validateReplicationConfiguration(config ReplicationConfiguration) error {
	if len(config.Rules) == 0 {
		return fmt.Errorf("replication configuration requires at least one rule")
	}
	for _, rule := range config.Rules {
		switch rule.Status {
		case "Enabled", "Disabled":
		default:
			return fmt.Errorf("invalid replication rule status")
		}
		destinationBucket, err := replicationDestinationBucket(rule.Destination.Bucket)
		if err != nil {
			return err
		}
		if err := validateBucketName(destinationBucket); err != nil {
			return fmt.Errorf("invalid replication destination bucket")
		}
		if rule.DeleteMarkerReplication.Status != "" && rule.DeleteMarkerReplication.Status != "Enabled" && rule.DeleteMarkerReplication.Status != "Disabled" {
			return fmt.Errorf("invalid delete marker replication status")
		}
		if rule.Destination.StorageClass != "" && !isSupportedReplicationStorageClass(rule.Destination.StorageClass) {
			return fmt.Errorf("invalid replication storage class")
		}
	}
	return nil
}

func configurationIDFromQuery(query url.Values) (string, error) {
	id := strings.TrimSpace(query.Get("id"))
	if err := validateConfigurationID(id); err != nil {
		return "", err
	}
	return id, nil
}

func validateConfigurationID(id string) error {
	if id == "" || len(id) > 64 {
		return fmt.Errorf("invalid configuration id")
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("invalid configuration id")
		}
	}
	return nil
}

func validateNotificationEventsAndFilter(events []string, filter NotificationFilter) error {
	for _, event := range events {
		if !isSupportedNotificationEvent(event) {
			return fmt.Errorf("unsupported notification event")
		}
	}
	for _, rule := range filter.S3Key.Rules {
		switch rule.Name {
		case "prefix", "suffix":
		default:
			return fmt.Errorf("unsupported notification filter rule")
		}
	}
	return nil
}

func isSupportedNotificationEvent(event string) bool {
	switch event {
	case "s3:ObjectCreated:*",
		"s3:ObjectCreated:Put",
		"s3:ObjectCreated:Post",
		"s3:ObjectCreated:Copy",
		"s3:ObjectCreated:CompleteMultipartUpload",
		"s3:ObjectRemoved:*",
		"s3:ObjectRemoved:Delete",
		"s3:ObjectRemoved:DeleteMarkerCreated":
		return true
	default:
		return false
	}
}

func (s *Server) replicateObjectWrite(ctx context.Context, bucket string, key string, object Object) error {
	config, bucketExists, replicationExists, err := s.store.GetBucketReplication(ctx, bucket)
	if err != nil || !bucketExists || !replicationExists {
		return err
	}
	if object.DeleteMarker {
		return nil
	}
	_, body, ok, err := s.store.GetObjectVersion(ctx, bucket, key, object.VersionID)
	if err != nil || !ok {
		return err
	}
	for _, rule := range config.Rules {
		if rule.Status != "Enabled" || !replicationRuleMatches(rule, key) {
			continue
		}
		destinationBucket, err := replicationDestinationBucket(rule.Destination.Bucket)
		if err != nil || destinationBucket == bucket {
			continue
		}
		if _, ok, err := s.store.GetBucket(ctx, destinationBucket); err != nil {
			return err
		} else if !ok {
			continue
		}
		_, err = s.store.PutObject(ctx, PutObjectInput{
			Bucket:             destinationBucket,
			Key:                key,
			Body:               bytes.NewReader(body),
			ContentType:        object.ContentType,
			ContentEncoding:    object.ContentEncoding,
			CacheControl:       object.CacheControl,
			ContentDisposition: object.ContentDisposition,
			Metadata:           object.Metadata,
			Encryption:         object.Encryption,
			Retention:          object.Retention,
			LegalHold:          object.LegalHold,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func replicationRuleMatches(rule ReplicationRule, key string) bool {
	prefix := rule.Filter.Prefix
	if prefix == "" {
		prefix = rule.Prefix
	}
	return prefix == "" || strings.HasPrefix(key, prefix)
}

func (s *Server) replicateObjectDeleteMarker(ctx context.Context, bucket string, key string) error {
	config, bucketExists, replicationExists, err := s.store.GetBucketReplication(ctx, bucket)
	if err != nil || !bucketExists || !replicationExists {
		return err
	}
	for _, rule := range config.Rules {
		if rule.Status != "Enabled" || rule.DeleteMarkerReplication.Status != "Enabled" || !replicationRuleMatches(rule, key) {
			continue
		}
		destinationBucket, err := replicationDestinationBucket(rule.Destination.Bucket)
		if err != nil || destinationBucket == bucket {
			continue
		}
		if _, ok, err := s.store.GetBucket(ctx, destinationBucket); err != nil {
			return err
		} else if !ok {
			continue
		}
		if _, _, err := s.store.DeleteObjectWithResult(ctx, destinationBucket, key, false); err != nil {
			return err
		}
	}
	return nil
}

func replicationDestinationBucket(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("replication destination bucket is required")
	}
	if strings.HasPrefix(value, "arn:aws:s3:::") {
		value = strings.TrimPrefix(value, "arn:aws:s3:::")
		if strings.Contains(value, "/") {
			value = strings.SplitN(value, "/", 2)[0]
		}
	}
	if value == "" {
		return "", fmt.Errorf("replication destination bucket is required")
	}
	return value, nil
}

func isSupportedReplicationStorageClass(value string) bool {
	switch value {
	case "STANDARD", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR":
		return true
	default:
		return false
	}
}

func (s *Server) recordObjectEvent(ctx context.Context, bucket string, key string, eventName string, object Object) error {
	config, bucketExists, err := s.store.GetBucketNotification(ctx, bucket)
	if err != nil || !bucketExists {
		return err
	}
	if !notificationMatches(config, eventName, key) {
		return nil
	}
	_, err = s.store.AppendNotificationEvent(ctx, bucket, NotificationEventRecord{
		EventID:      newUploadID(),
		EventName:    eventName,
		EventTime:    time.Now().UTC(),
		Bucket:       bucket,
		Key:          key,
		ETag:         object.ETag,
		Size:         object.Size,
		VersionID:    object.VersionID,
		DeleteMarker: object.DeleteMarker,
	})
	return err
}

func notificationMatches(config NotificationConfiguration, eventName string, key string) bool {
	for _, topic := range config.TopicConfigurations {
		if notificationRuleMatches(topic.Events, topic.Filter, eventName, key) {
			return true
		}
	}
	for _, queue := range config.QueueConfigurations {
		if notificationRuleMatches(queue.Events, queue.Filter, eventName, key) {
			return true
		}
	}
	for _, lambda := range config.LambdaFunctionConfigurations {
		if notificationRuleMatches(lambda.Events, lambda.Filter, eventName, key) {
			return true
		}
	}
	return false
}

func notificationRuleMatches(events []string, filter NotificationFilter, eventName string, key string) bool {
	eventMatches := false
	for _, event := range events {
		if event == eventName || strings.HasSuffix(event, ":*") && strings.HasPrefix(eventName, strings.TrimSuffix(event, "*")) {
			eventMatches = true
			break
		}
	}
	if !eventMatches {
		return false
	}
	for _, rule := range filter.S3Key.Rules {
		switch rule.Name {
		case "prefix":
			if !strings.HasPrefix(key, rule.Value) {
				return false
			}
		case "suffix":
			if !strings.HasSuffix(key, rule.Value) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func isSupportedCannedACL(acl string) bool {
	switch acl {
	case "private", "public-read", "public-read-write", "authenticated-read", "bucket-owner-read", "bucket-owner-full-control":
		return true
	default:
		return false
	}
}

func writeACL(w http.ResponseWriter, acl string) {
	if acl == "" {
		acl = "private"
	}
	writeXML(w, http.StatusOK, accessControlPolicy{
		XMLName: xml.Name{Local: "AccessControlPolicy"},
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: owner{
			ID:          "devcloud",
			DisplayName: "devcloud",
		},
		AccessControlList: accessControlList{
			Grants: []grant{grantForACL(acl)},
		},
		CannedACL: acl,
	})
}

func grantForACL(acl string) grant {
	permission := "FULL_CONTROL"
	if acl == "public-read" || acl == "authenticated-read" || acl == "bucket-owner-read" {
		permission = "READ"
	}
	return grant{
		Grantee: grantee{
			XmlnsXSI:    "http://www.w3.org/2001/XMLSchema-instance",
			Type:        "CanonicalUser",
			ID:          "devcloud",
			DisplayName: "devcloud",
		},
		Permission: permission,
	}
}

func writeObjectHeaders(w http.ResponseWriter, object Object) {
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	writeServerSideEncryptionHeaders(w, object.Encryption)
	writeObjectLockHeaders(w, object)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
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

type versionListing struct {
	versions            []Object
	truncated           bool
	nextKeyMarker       string
	nextVersionIDMarker string
}

func latestObjectVersionIDs(versions []Object) map[string]string {
	latestByKey := make(map[string]string)
	for _, object := range versions {
		if _, ok := latestByKey[object.Key]; ok {
			continue
		}
		latestByKey[object.Key] = objectVersionID(object)
	}
	return latestByKey
}

func objectVersionID(object Object) string {
	if object.VersionID == "" {
		return nullVersionID
	}
	return object.VersionID
}

func buildVersionListing(versions []Object, keyMarker string, versionIDMarker string, maxKeys int) versionListing {
	listing := versionListing{}
	if maxKeys == 0 {
		return listing
	}
	started := keyMarker == ""
	for _, object := range versions {
		versionID := objectVersionID(object)
		if !started {
			switch {
			case object.Key < keyMarker:
				continue
			case object.Key > keyMarker:
				started = true
			case versionIDMarker == "":
				continue
			case versionID == versionIDMarker:
				started = true
				continue
			default:
				continue
			}
		}
		if len(listing.versions) >= maxKeys {
			listing.truncated = true
			last := listing.versions[len(listing.versions)-1]
			listing.nextKeyMarker = last.Key
			listing.nextVersionIDMarker = objectVersionID(last)
			return listing
		}
		listing.versions = append(listing.versions, object)
	}
	return listing
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

var errUnsupportedLifecycleRule = fmt.Errorf("unsupported lifecycle rule")
var errUnsupportedServerSideEncryption = fmt.Errorf("unsupported server-side encryption")
var errUnsupportedSSECustomerKey = fmt.Errorf("unsupported sse-c")
var errInvalidServerSideEncryption = fmt.Errorf("invalid server-side encryption")

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

type accessControlPolicy struct {
	XMLName           xml.Name          `xml:"AccessControlPolicy"`
	Xmlns             string            `xml:"xmlns,attr,omitempty"`
	Owner             owner             `xml:"Owner"`
	AccessControlList accessControlList `xml:"AccessControlList"`
	CannedACL         string            `xml:"CannedACL,omitempty"`
}

type accessControlList struct {
	Grants []grant `xml:"Grant"`
}

type grant struct {
	Grantee    grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type grantee struct {
	XmlnsXSI    string `xml:"xmlns:xsi,attr,omitempty"`
	Type        string `xml:"xsi:type,attr,omitempty"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
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

type listInventoryConfigurationsResult struct {
	XMLName                 xml.Name                 `xml:"ListInventoryConfigurationsResult"`
	Xmlns                   string                   `xml:"xmlns,attr"`
	ContinuationToken       string                   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken   string                   `xml:"NextContinuationToken,omitempty"`
	IsTruncated             bool                     `xml:"IsTruncated"`
	InventoryConfigurations []InventoryConfiguration `xml:"InventoryConfiguration"`
}

type listAnalyticsConfigurationsResult struct {
	XMLName                 xml.Name                 `xml:"ListAnalyticsConfigurationsResult"`
	Xmlns                   string                   `xml:"xmlns,attr"`
	ContinuationToken       string                   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken   string                   `xml:"NextContinuationToken,omitempty"`
	IsTruncated             bool                     `xml:"IsTruncated"`
	AnalyticsConfigurations []AnalyticsConfiguration `xml:"AnalyticsConfiguration"`
}

type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status,omitempty"`
}

type selectObjectContentRequest struct {
	XMLName             xml.Name                  `xml:"SelectObjectContentRequest"`
	Expression          string                    `xml:"Expression"`
	ExpressionType      string                    `xml:"ExpressionType"`
	InputSerialization  selectInputSerialization  `xml:"InputSerialization"`
	OutputSerialization selectOutputSerialization `xml:"OutputSerialization"`
	RequestProgress     struct{}                  `xml:"RequestProgress"`
	ScanRange           struct{}                  `xml:"ScanRange"`
}

type selectInputSerialization struct {
	CSV  *selectCSVSerialization  `xml:"CSV"`
	JSON *selectJSONSerialization `xml:"JSON"`
}

type selectOutputSerialization struct {
	CSV  *selectCSVSerialization  `xml:"CSV"`
	JSON *selectJSONSerialization `xml:"JSON"`
}

type selectCSVSerialization struct {
	FileHeaderInfo  string `xml:"FileHeaderInfo"`
	RecordDelimiter string `xml:"RecordDelimiter"`
	FieldDelimiter  string `xml:"FieldDelimiter"`
}

type selectJSONSerialization struct {
	Type string `xml:"Type"`
}

func (s *selectCSVSerialization) withDefaults() selectCSVSerialization {
	if s == nil {
		return selectCSVSerialization{}
	}
	return *s
}

func (s selectCSVSerialization) fieldDelimiter() string {
	if s.FieldDelimiter == "" {
		return ","
	}
	return s.FieldDelimiter
}

func (s selectCSVSerialization) recordDelimiter() string {
	if s.RecordDelimiter == "" {
		return "\n"
	}
	return s.RecordDelimiter
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

type listVersionsResult struct {
	XMLName             xml.Name              `xml:"ListVersionsResult"`
	Xmlns               string                `xml:"xmlns,attr"`
	Name                string                `xml:"Name"`
	Prefix              string                `xml:"Prefix"`
	KeyMarker           string                `xml:"KeyMarker,omitempty"`
	VersionIDMarker     string                `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string                `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string                `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int                   `xml:"MaxKeys"`
	IsTruncated         bool                  `xml:"IsTruncated"`
	Versions            []versionElement      `xml:"Version"`
	DeleteMarkers       []deleteMarkerElement `xml:"DeleteMarker"`
}

type versionElement struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type deleteMarkerElement struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
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
