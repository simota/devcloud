package s3

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
)

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
