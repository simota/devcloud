package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Bucket struct {
	Name             string                  `json:"name"`
	CreatedAt        time.Time               `json:"createdAt"`
	Versioning       string                  `json:"versioning,omitempty"`
	ACL              string                  `json:"acl,omitempty"`
	ObjectLockConfig ObjectLockConfiguration `json:"objectLockConfig,omitempty"`
}

type Object struct {
	Bucket             string               `json:"bucket"`
	Key                string               `json:"key"`
	ETag               string               `json:"etag"`
	Size               int64                `json:"size"`
	CreatedAt          time.Time            `json:"createdAt,omitempty"`
	LastModified       time.Time            `json:"lastModified"`
	UpdatedAt          time.Time            `json:"updatedAt,omitempty"`
	Metageneration     int64                `json:"metageneration,omitempty"`
	ContentType        string               `json:"contentType,omitempty"`
	ContentEncoding    string               `json:"contentEncoding,omitempty"`
	CRC32C             string               `json:"crc32c,omitempty"`
	CacheControl       string               `json:"cacheControl,omitempty"`
	ContentDisposition string               `json:"contentDisposition,omitempty"`
	Metadata           map[string]string    `json:"metadata,omitempty"`
	VersionID          string               `json:"versionId,omitempty"`
	DeleteMarker       bool                 `json:"deleteMarker,omitempty"`
	ACL                string               `json:"acl,omitempty"`
	Encryption         ServerSideEncryption `json:"encryption,omitempty"`
	Retention          ObjectRetention      `json:"retention,omitempty"`
	LegalHold          ObjectLegalHold      `json:"legalHold,omitempty"`
}

const nullVersionID = "null"

type MultipartUpload struct {
	Bucket             string               `json:"bucket"`
	Key                string               `json:"key"`
	UploadID           string               `json:"uploadId"`
	CreatedAt          time.Time            `json:"createdAt"`
	ContentType        string               `json:"contentType,omitempty"`
	ContentEncoding    string               `json:"contentEncoding,omitempty"`
	CacheControl       string               `json:"cacheControl,omitempty"`
	ContentDisposition string               `json:"contentDisposition,omitempty"`
	Metadata           map[string]string    `json:"metadata,omitempty"`
	Encryption         ServerSideEncryption `json:"encryption,omitempty"`
}

type ServerSideEncryption struct {
	Algorithm        string `json:"algorithm,omitempty"`
	KMSKeyID         string `json:"kmsKeyId,omitempty"`
	BucketKeyEnabled *bool  `json:"bucketKeyEnabled,omitempty"`
}

type ObjectLockConfiguration struct {
	XMLName           xml.Name       `json:"-" xml:"ObjectLockConfiguration"`
	Xmlns             string         `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	ObjectLockEnabled string         `json:"objectLockEnabled,omitempty" xml:"ObjectLockEnabled,omitempty"`
	Rule              ObjectLockRule `json:"rule,omitempty" xml:"Rule,omitempty"`
}

type ObjectLockRule struct {
	DefaultRetention DefaultRetention `json:"defaultRetention,omitempty" xml:"DefaultRetention,omitempty"`
}

type DefaultRetention struct {
	Mode  string `json:"mode,omitempty" xml:"Mode,omitempty"`
	Days  int    `json:"days,omitempty" xml:"Days,omitempty"`
	Years int    `json:"years,omitempty" xml:"Years,omitempty"`
}

type ObjectRetention struct {
	XMLName         xml.Name `json:"-" xml:"Retention"`
	Mode            string   `json:"mode,omitempty" xml:"Mode,omitempty"`
	RetainUntilDate string   `json:"retainUntilDate,omitempty" xml:"RetainUntilDate,omitempty"`
}

type ObjectLegalHold struct {
	XMLName xml.Name `json:"-" xml:"LegalHold"`
	Status  string   `json:"status,omitempty" xml:"Status,omitempty"`
}

type MultipartPart struct {
	PartNumber   int       `json:"partNumber"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
}

type LifecycleConfiguration struct {
	XMLName xml.Name        `json:"-" xml:"LifecycleConfiguration"`
	Xmlns   string          `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	Rules   []LifecycleRule `json:"rules" xml:"Rule"`
}

type LifecycleRule struct {
	ID                             string              `json:"id,omitempty" xml:"ID,omitempty"`
	Prefix                         string              `json:"prefix,omitempty" xml:"Prefix,omitempty"`
	Filter                         LifecycleFilter     `json:"filter,omitempty" xml:"Filter"`
	Status                         string              `json:"status" xml:"Status"`
	Expiration                     LifecycleExpiration `json:"expiration" xml:"Expiration"`
	Transitions                    []struct{}          `json:"-" xml:"Transition"`
	NoncurrentVersionTransitions   []struct{}          `json:"-" xml:"NoncurrentVersionTransition"`
	NoncurrentVersionExpirations   []struct{}          `json:"-" xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload []struct{}          `json:"-" xml:"AbortIncompleteMultipartUpload"`
}

type LifecycleFilter struct {
	Prefix string `json:"prefix,omitempty" xml:"Prefix,omitempty"`
}

type LifecycleExpiration struct {
	Days *int   `json:"days,omitempty" xml:"Days,omitempty"`
	Date string `json:"date,omitempty" xml:"Date,omitempty"`
}

type NotificationConfiguration struct {
	XMLName                      xml.Name                   `json:"-" xml:"NotificationConfiguration"`
	Xmlns                        string                     `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	TopicConfigurations          []NotificationTopicConfig  `json:"topicConfigurations,omitempty" xml:"TopicConfiguration"`
	QueueConfigurations          []NotificationQueueConfig  `json:"queueConfigurations,omitempty" xml:"QueueConfiguration"`
	LambdaFunctionConfigurations []NotificationLambdaConfig `json:"lambdaFunctionConfigurations,omitempty" xml:"CloudFunctionConfiguration"`
	EventBridgeConfiguration     *EventBridgeConfiguration  `json:"eventBridgeConfiguration,omitempty" xml:"EventBridgeConfiguration,omitempty"`
}

type NotificationTopicConfig struct {
	ID     string             `json:"id,omitempty" xml:"Id,omitempty"`
	Topic  string             `json:"topic" xml:"Topic"`
	Events []string           `json:"events" xml:"Event"`
	Filter NotificationFilter `json:"filter,omitempty" xml:"Filter"`
}

type NotificationQueueConfig struct {
	ID     string             `json:"id,omitempty" xml:"Id,omitempty"`
	Queue  string             `json:"queue" xml:"Queue"`
	Events []string           `json:"events" xml:"Event"`
	Filter NotificationFilter `json:"filter,omitempty" xml:"Filter"`
}

type NotificationLambdaConfig struct {
	ID             string             `json:"id,omitempty" xml:"Id,omitempty"`
	LambdaFunction string             `json:"lambdaFunction" xml:"CloudFunction"`
	Events         []string           `json:"events" xml:"Event"`
	Filter         NotificationFilter `json:"filter,omitempty" xml:"Filter"`
}

type NotificationFilter struct {
	S3Key NotificationS3KeyFilter `json:"s3Key,omitempty" xml:"S3Key"`
}

type NotificationS3KeyFilter struct {
	Rules []NotificationFilterRule `json:"rules,omitempty" xml:"FilterRule"`
}

type NotificationFilterRule struct {
	Name  string `json:"name" xml:"Name"`
	Value string `json:"value" xml:"Value"`
}

type EventBridgeConfiguration struct{}

type NotificationEventRecord struct {
	EventID      string    `json:"eventId"`
	EventName    string    `json:"eventName"`
	EventTime    time.Time `json:"eventTime"`
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	ETag         string    `json:"etag,omitempty"`
	Size         int64     `json:"size,omitempty"`
	VersionID    string    `json:"versionId,omitempty"`
	DeleteMarker bool      `json:"deleteMarker,omitempty"`
}

type InventoryConfiguration struct {
	XMLName                xml.Name             `json:"-" xml:"InventoryConfiguration"`
	Xmlns                  string               `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	ID                     string               `json:"id" xml:"Id"`
	IsEnabled              bool                 `json:"isEnabled" xml:"IsEnabled"`
	IncludedObjectVersions string               `json:"includedObjectVersions,omitempty" xml:"IncludedObjectVersions,omitempty"`
	Schedule               InventorySchedule    `json:"schedule,omitempty" xml:"Schedule,omitempty"`
	Destination            InventoryDestination `json:"destination,omitempty" xml:"Destination,omitempty"`
	OptionalFields         []string             `json:"optionalFields,omitempty" xml:"OptionalFields>Field,omitempty"`
}

type InventoryReportManifest struct {
	ConfigurationID  string   `json:"configurationId"`
	SourceBucket     string   `json:"sourceBucket"`
	Format           string   `json:"format"`
	IncludedVersions string   `json:"includedObjectVersions"`
	Fields           []string `json:"fields"`
	ObjectCount      int      `json:"objectCount"`
	ReportKey        string   `json:"reportKey"`
}

type InventorySchedule struct {
	Frequency string `json:"frequency,omitempty" xml:"Frequency,omitempty"`
}

type InventoryDestination struct {
	S3BucketDestination InventoryS3BucketDestination `json:"s3BucketDestination,omitempty" xml:"S3BucketDestination,omitempty"`
}

type InventoryS3BucketDestination struct {
	AccountID string `json:"accountId,omitempty" xml:"AccountId,omitempty"`
	Bucket    string `json:"bucket,omitempty" xml:"Bucket,omitempty"`
	Format    string `json:"format,omitempty" xml:"Format,omitempty"`
	Prefix    string `json:"prefix,omitempty" xml:"Prefix,omitempty"`
}

type AnalyticsConfiguration struct {
	XMLName              xml.Name             `json:"-" xml:"AnalyticsConfiguration"`
	Xmlns                string               `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	ID                   string               `json:"id" xml:"Id"`
	Filter               AnalyticsFilter      `json:"filter,omitempty" xml:"Filter,omitempty"`
	StorageClassAnalysis StorageClassAnalysis `json:"storageClassAnalysis,omitempty" xml:"StorageClassAnalysis,omitempty"`
}

type AnalyticsFilter struct {
	Prefix string `json:"prefix,omitempty" xml:"Prefix,omitempty"`
}

type StorageClassAnalysis struct {
	DataExport AnalyticsDataExport `json:"dataExport,omitempty" xml:"DataExport,omitempty"`
}

type AnalyticsDataExport struct {
	OutputSchemaVersion string               `json:"outputSchemaVersion,omitempty" xml:"OutputSchemaVersion,omitempty"`
	Destination         AnalyticsDestination `json:"destination,omitempty" xml:"Destination,omitempty"`
}

type AnalyticsDestination struct {
	S3BucketDestination AnalyticsS3BucketDestination `json:"s3BucketDestination,omitempty" xml:"S3BucketDestination,omitempty"`
}

type AnalyticsS3BucketDestination struct {
	Format string `json:"format,omitempty" xml:"Format,omitempty"`
	Bucket string `json:"bucket,omitempty" xml:"Bucket,omitempty"`
	Prefix string `json:"prefix,omitempty" xml:"Prefix,omitempty"`
}

type ReplicationConfiguration struct {
	XMLName xml.Name          `json:"-" xml:"ReplicationConfiguration"`
	Xmlns   string            `json:"xmlns,omitempty" xml:"xmlns,attr,omitempty"`
	Role    string            `json:"role,omitempty" xml:"Role,omitempty"`
	Rules   []ReplicationRule `json:"rules" xml:"Rule"`
}

type ReplicationRule struct {
	ID                      string                         `json:"id,omitempty" xml:"ID,omitempty"`
	Priority                int                            `json:"priority,omitempty" xml:"Priority,omitempty"`
	Prefix                  string                         `json:"prefix,omitempty" xml:"Prefix,omitempty"`
	Filter                  ReplicationFilter              `json:"filter,omitempty" xml:"Filter"`
	Status                  string                         `json:"status" xml:"Status"`
	Destination             ReplicationDestination         `json:"destination" xml:"Destination"`
	DeleteMarkerReplication ReplicationDeleteMarkerSetting `json:"deleteMarkerReplication,omitempty" xml:"DeleteMarkerReplication,omitempty"`
}

type ReplicationFilter struct {
	Prefix string `json:"prefix,omitempty" xml:"Prefix,omitempty"`
}

type ReplicationDestination struct {
	Bucket       string `json:"bucket" xml:"Bucket"`
	StorageClass string `json:"storageClass,omitempty" xml:"StorageClass,omitempty"`
}

type ReplicationDeleteMarkerSetting struct {
	Status string `json:"status,omitempty" xml:"Status,omitempty"`
}

type PutObjectInput struct {
	Bucket             string
	Key                string
	Body               io.Reader
	ContentMD5         string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
	Encryption         ServerSideEncryption
	Retention          ObjectRetention
	LegalHold          ObjectLegalHold
}

type UpdateObjectMetadataInput struct {
	Bucket             string
	Key                string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
}

type CreateMultipartUploadInput struct {
	Bucket             string
	Key                string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
	Encryption         ServerSideEncryption
}

type BucketStore interface {
	CreateBucket(ctx context.Context, name string) (Bucket, bool, error)
	GetBucket(ctx context.Context, name string) (Bucket, bool, error)
	ListBuckets(ctx context.Context) ([]Bucket, error)
	DeleteBucket(ctx context.Context, name string) (bool, error)
	PutBucketVersioning(ctx context.Context, bucket string, status string) error
	GetBucketVersioning(ctx context.Context, bucket string) (string, bool, error)
	PutBucketObjectLockConfiguration(ctx context.Context, bucket string, config ObjectLockConfiguration) error
	GetBucketObjectLockConfiguration(ctx context.Context, bucket string) (ObjectLockConfiguration, bool, bool, error)
	DeleteBucketObjectLockConfiguration(ctx context.Context, bucket string) (bool, error)
	PutBucketLifecycle(ctx context.Context, bucket string, config LifecycleConfiguration) error
	GetBucketLifecycle(ctx context.Context, bucket string) (LifecycleConfiguration, bool, bool, error)
	DeleteBucketLifecycle(ctx context.Context, bucket string) (bool, error)
	ApplyBucketLifecycle(ctx context.Context, bucket string, now time.Time) (int, bool, error)
	PutBucketNotification(ctx context.Context, bucket string, config NotificationConfiguration) error
	GetBucketNotification(ctx context.Context, bucket string) (NotificationConfiguration, bool, error)
	AppendNotificationEvent(ctx context.Context, bucket string, event NotificationEventRecord) (bool, error)
	ListNotificationEvents(ctx context.Context, bucket string) ([]NotificationEventRecord, bool, error)
	PutBucketInventory(ctx context.Context, bucket string, id string, config InventoryConfiguration) error
	GetBucketInventory(ctx context.Context, bucket string, id string) (InventoryConfiguration, bool, bool, error)
	ListBucketInventories(ctx context.Context, bucket string) ([]InventoryConfiguration, bool, error)
	DeleteBucketInventory(ctx context.Context, bucket string, id string) (bool, error)
	PutBucketAnalytics(ctx context.Context, bucket string, id string, config AnalyticsConfiguration) error
	GetBucketAnalytics(ctx context.Context, bucket string, id string) (AnalyticsConfiguration, bool, bool, error)
	ListBucketAnalytics(ctx context.Context, bucket string) ([]AnalyticsConfiguration, bool, error)
	DeleteBucketAnalytics(ctx context.Context, bucket string, id string) (bool, error)
	PutBucketReplication(ctx context.Context, bucket string, config ReplicationConfiguration) error
	GetBucketReplication(ctx context.Context, bucket string) (ReplicationConfiguration, bool, bool, error)
	DeleteBucketReplication(ctx context.Context, bucket string) (bool, error)
	PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error
	GetBucketPolicy(ctx context.Context, bucket string) ([]byte, bool, bool, error)
	DeleteBucketPolicy(ctx context.Context, bucket string) (bool, error)
	PutBucketACL(ctx context.Context, bucket string, acl string) error
	GetBucketACL(ctx context.Context, bucket string) (string, bool, error)
	PutObject(ctx context.Context, input PutObjectInput) (Object, error)
	UpdateObjectMetadata(ctx context.Context, input UpdateObjectMetadataInput) (Object, bool, error)
	GetObject(ctx context.Context, bucket string, key string) (Object, []byte, bool, error)
	GetObjectVersion(ctx context.Context, bucket string, key string, versionID string) (Object, []byte, bool, error)
	PutObjectACL(ctx context.Context, bucket string, key string, versionID string, acl string) (bool, error)
	GetObjectACL(ctx context.Context, bucket string, key string, versionID string) (string, bool, error)
	PutObjectRetention(ctx context.Context, bucket string, key string, versionID string, retention ObjectRetention) (Object, bool, error)
	GetObjectRetention(ctx context.Context, bucket string, key string, versionID string) (ObjectRetention, bool, error)
	PutObjectLegalHold(ctx context.Context, bucket string, key string, versionID string, legalHold ObjectLegalHold) (Object, bool, error)
	GetObjectLegalHold(ctx context.Context, bucket string, key string, versionID string) (ObjectLegalHold, bool, error)
	DeleteObject(ctx context.Context, bucket string, key string) (bool, error)
	DeleteObjectWithResult(ctx context.Context, bucket string, key string, bypassGovernance bool) (Object, bool, error)
	DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string, bypassGovernance bool) (Object, bool, error)
	ListObjects(ctx context.Context, bucket string, prefix string) ([]Object, bool, error)
	ListObjectVersions(ctx context.Context, bucket string, prefix string) ([]Object, bool, error)
	CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (MultipartUpload, error)
	UploadPart(ctx context.Context, bucket string, key string, uploadID string, partNumber int, body io.Reader, contentMD5 string) (MultipartPart, error)
	ListParts(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, []MultipartPart, bool, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, partNumbers []int) (Object, bool, error)
	AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (bool, error)
	ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, bool, error)
}

type FileBucketStore struct {
	root string
}

func NewFileBucketStore(root string) *FileBucketStore {
	return &FileBucketStore{root: root}
}

func (s *FileBucketStore) CreateBucket(ctx context.Context, name string) (Bucket, bool, error) {
	if err := ctx.Err(); err != nil {
		return Bucket{}, false, err
	}
	if err := validateBucketName(name); err != nil {
		return Bucket{}, false, err
	}

	path := s.bucketPath(name)
	if _, err := os.Stat(path); err == nil {
		bucket, ok, err := s.GetBucket(ctx, name)
		return bucket, !ok, err
	} else if !os.IsNotExist(err) {
		return Bucket{}, false, fmt.Errorf("stat bucket: %w", err)
	}

	bucket := Bucket{Name: name, CreatedAt: time.Now().UTC()}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return Bucket{}, false, fmt.Errorf("create bucket directory: %w", err)
	}
	if err := writeBucketMetadata(filepath.Join(path, "bucket.json"), bucket); err != nil {
		return Bucket{}, false, err
	}
	return bucket, true, nil
}

func (s *FileBucketStore) GetBucket(ctx context.Context, name string) (Bucket, bool, error) {
	if err := ctx.Err(); err != nil {
		return Bucket{}, false, err
	}
	if err := validateBucketName(name); err != nil {
		return Bucket{}, false, err
	}

	data, err := os.ReadFile(filepath.Join(s.bucketPath(name), "bucket.json"))
	if err == nil {
		var bucket Bucket
		if err := json.Unmarshal(data, &bucket); err != nil {
			return Bucket{}, false, fmt.Errorf("decode bucket metadata: %w", err)
		}
		return bucket, true, nil
	}
	if os.IsNotExist(err) {
		return Bucket{}, false, nil
	}
	return Bucket{}, false, fmt.Errorf("read bucket metadata: %w", err)
}

func (s *FileBucketStore) ListBuckets(ctx context.Context) ([]Bucket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read buckets: %w", err)
	}

	buckets := make([]Bucket, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		bucket, ok, err := s.GetBucket(ctx, entry.Name())
		if err != nil {
			return nil, err
		}
		if ok {
			buckets = append(buckets, bucket)
		}
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (s *FileBucketStore) DeleteBucket(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateBucketName(name); err != nil {
		return false, err
	}

	path := s.bucketPath(name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat bucket: %w", err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("read bucket directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == "inventory" || entry.Name() == "analytics" {
			continue
		}
		if entry.Name() != "bucket.json" && entry.Name() != "versioning.json" && entry.Name() != "policy.json" && entry.Name() != "acl.json" && entry.Name() != "lifecycle.json" && entry.Name() != "notification.json" && entry.Name() != "notification-events.json" && entry.Name() != "object-lock.json" && entry.Name() != "replication.json" {
			return false, fmt.Errorf("bucket is not empty")
		}
	}
	if err := os.Remove(filepath.Join(path, "bucket.json")); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("delete bucket metadata: %w", err)
	}
	for _, name := range []string{"versioning.json", "policy.json", "acl.json", "lifecycle.json", "notification.json", "notification-events.json", "object-lock.json", "replication.json"} {
		if err := os.Remove(filepath.Join(path, name)); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("delete bucket metadata: %w", err)
		}
	}
	for _, name := range []string{"inventory", "analytics"} {
		if err := os.RemoveAll(filepath.Join(path, name)); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("delete bucket metadata: %w", err)
		}
	}
	if err := os.Remove(path); err != nil {
		return false, fmt.Errorf("delete bucket directory: %w", err)
	}
	return true, nil
}

func (s *FileBucketStore) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return os.WriteFile(filepath.Join(s.bucketPath(bucket), "policy.json"), append([]byte(nil), policy...), 0o644)
}

func (s *FileBucketStore) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, false, err
	} else if !ok {
		return nil, false, false, nil
	}
	data, err := os.ReadFile(filepath.Join(s.bucketPath(bucket), "policy.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, false, nil
		}
		return nil, true, false, fmt.Errorf("read bucket policy: %w", err)
	}
	return data, true, true, nil
}

func (s *FileBucketStore) DeleteBucketPolicy(ctx context.Context, bucket string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	if err := os.Remove(filepath.Join(s.bucketPath(bucket), "policy.json")); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete bucket policy: %w", err)
	}
	return true, nil
}

func (s *FileBucketStore) PutBucketACL(ctx context.Context, bucket string, acl string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	existing.ACL = acl
	if err := writeBucketMetadata(filepath.Join(s.bucketPath(bucket), "bucket.json"), existing); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "acl.json"), struct {
		ACL string `json:"acl"`
	}{ACL: acl})
}

func (s *FileBucketStore) GetBucketACL(ctx context.Context, bucket string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil || !ok {
		return "", ok, err
	}
	if existing.ACL != "" {
		return existing.ACL, true, nil
	}
	var persisted struct {
		ACL string `json:"acl"`
	}
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "acl.json"), &persisted); err != nil {
		if os.IsNotExist(err) {
			return "private", true, nil
		}
		return "", false, err
	}
	if persisted.ACL == "" {
		return "private", true, nil
	}
	return persisted.ACL, true, nil
}

func (s *FileBucketStore) PutBucketVersioning(ctx context.Context, bucket string, status string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if status != "Enabled" && status != "Suspended" {
		return fmt.Errorf("invalid versioning status")
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	existing.Versioning = status
	if err := writeBucketMetadata(filepath.Join(s.bucketPath(bucket), "bucket.json"), existing); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "versioning.json"), struct {
		Status string `json:"status"`
	}{Status: status})
}

func (s *FileBucketStore) GetBucketVersioning(ctx context.Context, bucket string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil || !ok {
		return "", ok, err
	}
	if existing.Versioning != "" {
		return existing.Versioning, true, nil
	}
	var persisted struct {
		Status string `json:"status"`
	}
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "versioning.json"), &persisted); err != nil {
		if os.IsNotExist(err) {
			return "", true, nil
		}
		return "", false, err
	}
	return persisted.Status, true, nil
}

func (s *FileBucketStore) PutBucketObjectLockConfiguration(ctx context.Context, bucket string, config ObjectLockConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	existing.ObjectLockConfig = config
	if err := writeBucketMetadata(filepath.Join(s.bucketPath(bucket), "bucket.json"), existing); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "object-lock.json"), config)
}

func (s *FileBucketStore) GetBucketObjectLockConfiguration(ctx context.Context, bucket string) (ObjectLockConfiguration, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return ObjectLockConfiguration{}, false, false, err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil || !ok {
		return ObjectLockConfiguration{}, ok, false, err
	}
	if existing.ObjectLockConfig.ObjectLockEnabled != "" {
		return existing.ObjectLockConfig, true, true, nil
	}
	var config ObjectLockConfiguration
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "object-lock.json"), &config); err != nil {
		if os.IsNotExist(err) {
			return ObjectLockConfiguration{}, true, false, nil
		}
		return ObjectLockConfiguration{}, true, false, err
	}
	return config, true, true, nil
}

func (s *FileBucketStore) DeleteBucketObjectLockConfiguration(ctx context.Context, bucket string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	existing, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	existing.ObjectLockConfig = ObjectLockConfiguration{}
	if err := writeBucketMetadata(filepath.Join(s.bucketPath(bucket), "bucket.json"), existing); err != nil {
		return true, err
	}
	if err := os.Remove(filepath.Join(s.bucketPath(bucket), "object-lock.json")); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete bucket object lock: %w", err)
	}
	return true, nil
}

func (s *FileBucketStore) PutBucketLifecycle(ctx context.Context, bucket string, config LifecycleConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "lifecycle.json"), config)
}

func (s *FileBucketStore) GetBucketLifecycle(ctx context.Context, bucket string) (LifecycleConfiguration, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleConfiguration{}, false, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return LifecycleConfiguration{}, false, false, err
	} else if !ok {
		return LifecycleConfiguration{}, false, false, nil
	}
	var config LifecycleConfiguration
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "lifecycle.json"), &config); err != nil {
		if os.IsNotExist(err) {
			return LifecycleConfiguration{}, true, false, nil
		}
		return LifecycleConfiguration{}, true, false, err
	}
	return config, true, true, nil
}

func (s *FileBucketStore) DeleteBucketLifecycle(ctx context.Context, bucket string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	if err := os.Remove(filepath.Join(s.bucketPath(bucket), "lifecycle.json")); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete bucket lifecycle: %w", err)
	}
	return true, nil
}

func (s *FileBucketStore) ApplyBucketLifecycle(ctx context.Context, bucket string, now time.Time) (int, bool, error) {
	config, bucketExists, lifecycleExists, err := s.GetBucketLifecycle(ctx, bucket)
	if err != nil || !bucketExists || !lifecycleExists {
		return 0, bucketExists, err
	}
	objects, bucketExists, err := s.ListObjects(ctx, bucket, "")
	if err != nil || !bucketExists {
		return 0, bucketExists, err
	}
	expired := 0
	for _, object := range objects {
		if lifecycleExpiresObject(config, object, now) {
			if _, deleted, err := s.DeleteObjectWithResult(ctx, object.Bucket, object.Key, false); err != nil {
				if errors.Is(err, errObjectLocked) {
					continue
				}
				return expired, true, err
			} else if deleted {
				expired++
			}
		}
	}
	return expired, true, nil
}

func (s *FileBucketStore) PutBucketNotification(ctx context.Context, bucket string, config NotificationConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "notification.json"), config)
}

func (s *FileBucketStore) GetBucketNotification(ctx context.Context, bucket string) (NotificationConfiguration, bool, error) {
	if err := ctx.Err(); err != nil {
		return NotificationConfiguration{}, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return NotificationConfiguration{}, false, err
	} else if !ok {
		return NotificationConfiguration{}, false, nil
	}
	var config NotificationConfiguration
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "notification.json"), &config); err != nil {
		if os.IsNotExist(err) {
			return NotificationConfiguration{}, true, nil
		}
		return NotificationConfiguration{}, true, err
	}
	return config, true, nil
}

func (s *FileBucketStore) AppendNotificationEvent(ctx context.Context, bucket string, event NotificationEventRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	path := filepath.Join(s.bucketPath(bucket), "notification-events.json")
	var events []NotificationEventRecord
	if err := readJSONFile(path, &events); err != nil && !os.IsNotExist(err) {
		return true, err
	}
	events = append(events, event)
	return true, writeJSONFile(path, events)
}

func (s *FileBucketStore) ListNotificationEvents(ctx context.Context, bucket string) ([]NotificationEventRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}
	var events []NotificationEventRecord
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "notification-events.json"), &events); err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, true, err
	}
	return events, true, nil
}

func (s *FileBucketStore) PutBucketInventory(ctx context.Context, bucket string, id string, config InventoryConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	config.ID = id
	if err := os.MkdirAll(s.inventoryPath(bucket), 0o755); err != nil {
		return fmt.Errorf("create inventory metadata directory: %w", err)
	}
	if err := writeJSONFile(s.inventoryConfigPath(bucket, id), config); err != nil {
		return err
	}
	if !config.IsEnabled {
		if err := os.RemoveAll(s.inventoryReportPath(bucket, id)); err != nil {
			return fmt.Errorf("delete disabled inventory report: %w", err)
		}
		return nil
	}
	if inventoryReportFormat(config) != "CSV" {
		if err := os.RemoveAll(s.inventoryReportPath(bucket, id)); err != nil {
			return fmt.Errorf("delete unsupported inventory report: %w", err)
		}
		return nil
	}
	if err := s.writeInventoryReport(ctx, bucket, id, config); err != nil {
		return fmt.Errorf("write inventory report: %w", err)
	}
	return nil
}

func (s *FileBucketStore) GetBucketInventory(ctx context.Context, bucket string, id string) (InventoryConfiguration, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return InventoryConfiguration{}, false, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return InventoryConfiguration{}, false, false, err
	} else if !ok {
		return InventoryConfiguration{}, false, false, nil
	}
	var config InventoryConfiguration
	if err := readJSONFile(s.inventoryConfigPath(bucket, id), &config); err != nil {
		if os.IsNotExist(err) {
			return InventoryConfiguration{}, true, false, nil
		}
		return InventoryConfiguration{}, true, false, err
	}
	return config, true, true, nil
}

func (s *FileBucketStore) ListBucketInventories(ctx context.Context, bucket string) ([]InventoryConfiguration, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}
	entries, err := os.ReadDir(s.inventoryPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("read inventory metadata: %w", err)
	}
	configs := make([]InventoryConfiguration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var config InventoryConfiguration
		if err := readJSONFile(filepath.Join(s.inventoryPath(bucket), entry.Name()), &config); err != nil {
			return nil, true, err
		}
		configs = append(configs, config)
	}
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].ID < configs[j].ID
	})
	return configs, true, nil
}

func (s *FileBucketStore) DeleteBucketInventory(ctx context.Context, bucket string, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	if err := os.Remove(s.inventoryConfigPath(bucket, id)); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete inventory metadata: %w", err)
	}
	if err := os.RemoveAll(s.inventoryReportPath(bucket, id)); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete inventory report: %w", err)
	}
	_ = os.Remove(s.inventoryPath(bucket))
	return true, nil
}

func (s *FileBucketStore) PutBucketAnalytics(ctx context.Context, bucket string, id string, config AnalyticsConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	config.ID = id
	if err := os.MkdirAll(s.analyticsPath(bucket), 0o755); err != nil {
		return fmt.Errorf("create analytics metadata directory: %w", err)
	}
	return writeJSONFile(s.analyticsConfigPath(bucket, id), config)
}

func (s *FileBucketStore) GetBucketAnalytics(ctx context.Context, bucket string, id string) (AnalyticsConfiguration, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return AnalyticsConfiguration{}, false, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return AnalyticsConfiguration{}, false, false, err
	} else if !ok {
		return AnalyticsConfiguration{}, false, false, nil
	}
	var config AnalyticsConfiguration
	if err := readJSONFile(s.analyticsConfigPath(bucket, id), &config); err != nil {
		if os.IsNotExist(err) {
			return AnalyticsConfiguration{}, true, false, nil
		}
		return AnalyticsConfiguration{}, true, false, err
	}
	return config, true, true, nil
}

func (s *FileBucketStore) ListBucketAnalytics(ctx context.Context, bucket string) ([]AnalyticsConfiguration, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}
	entries, err := os.ReadDir(s.analyticsPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("read analytics metadata: %w", err)
	}
	configs := make([]AnalyticsConfiguration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var config AnalyticsConfiguration
		if err := readJSONFile(filepath.Join(s.analyticsPath(bucket), entry.Name()), &config); err != nil {
			return nil, true, err
		}
		configs = append(configs, config)
	}
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].ID < configs[j].ID
	})
	return configs, true, nil
}

func (s *FileBucketStore) DeleteBucketAnalytics(ctx context.Context, bucket string, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	if err := os.Remove(s.analyticsConfigPath(bucket, id)); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete analytics metadata: %w", err)
	}
	_ = os.Remove(s.analyticsPath(bucket))
	return true, nil
}

func (s *FileBucketStore) PutBucketReplication(ctx context.Context, bucket string, config ReplicationConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return writeJSONFile(filepath.Join(s.bucketPath(bucket), "replication.json"), config)
}

func (s *FileBucketStore) GetBucketReplication(ctx context.Context, bucket string) (ReplicationConfiguration, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return ReplicationConfiguration{}, false, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return ReplicationConfiguration{}, false, false, err
	} else if !ok {
		return ReplicationConfiguration{}, false, false, nil
	}
	var config ReplicationConfiguration
	if err := readJSONFile(filepath.Join(s.bucketPath(bucket), "replication.json"), &config); err != nil {
		if os.IsNotExist(err) {
			return ReplicationConfiguration{}, true, false, nil
		}
		return ReplicationConfiguration{}, true, false, err
	}
	return config, true, true, nil
}

func (s *FileBucketStore) DeleteBucketReplication(ctx context.Context, bucket string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}
	if err := os.Remove(filepath.Join(s.bucketPath(bucket), "replication.json")); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("delete bucket replication: %w", err)
	}
	return true, nil
}

func (s *FileBucketStore) PutObject(ctx context.Context, input PutObjectInput) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if err := validateBucketName(input.Bucket); err != nil {
		return Object{}, err
	}
	if err := validateObjectKey(input.Key); err != nil {
		return Object{}, err
	}
	bucket, ok, err := s.GetBucket(ctx, input.Bucket)
	if err != nil {
		return Object{}, err
	} else if !ok {
		return Object{}, fmt.Errorf("bucket does not exist")
	}

	body, err := io.ReadAll(input.Body)
	if err != nil {
		return Object{}, fmt.Errorf("read object body: %w", err)
	}
	if err := validateContentMD5(input.ContentMD5, body); err != nil {
		return Object{}, err
	}
	sum := md5.Sum(body)
	now := time.Now().UTC()
	object := Object{
		Bucket:             input.Bucket,
		Key:                input.Key,
		ETag:               `"` + hex.EncodeToString(sum[:]) + `"`,
		Size:               int64(len(body)),
		CreatedAt:          now,
		LastModified:       now,
		UpdatedAt:          now,
		Metageneration:     1,
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		CRC32C:             crc32cBase64(body),
		CacheControl:       input.CacheControl,
		ContentDisposition: input.ContentDisposition,
		Metadata:           cleanMetadata(input.Metadata),
		Encryption:         cleanServerSideEncryption(input.Encryption),
		Retention:          cleanObjectRetention(input.Retention),
		LegalHold:          cleanObjectLegalHold(input.LegalHold),
	}
	if object.Retention.Mode == "" {
		object.Retention = defaultObjectRetention(bucket.ObjectLockConfig, now)
	}
	switch bucket.Versioning {
	case "Enabled":
		object.VersionID = newVersionID()
	case "Suspended":
		object.VersionID = nullVersionID
	}
	if object.ContentType == "" {
		object.ContentType = "application/octet-stream"
	}

	path := s.objectPath(input.Bucket, input.Key)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return Object{}, fmt.Errorf("create object directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(path, "body"), body, 0o644); err != nil {
		return Object{}, fmt.Errorf("write object body: %w", err)
	}
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return Object{}, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(path, object, body); err != nil {
			return Object{}, err
		}
	}
	return object, nil
}

func (s *FileBucketStore) UpdateObjectMetadata(ctx context.Context, input UpdateObjectMetadataInput) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	if err := validateBucketName(input.Bucket); err != nil {
		return Object{}, false, err
	}
	if err := validateObjectKey(input.Key); err != nil {
		return Object{}, false, err
	}
	if _, ok, err := s.GetBucket(ctx, input.Bucket); err != nil {
		return Object{}, false, err
	} else if !ok {
		return Object{}, false, fmt.Errorf("bucket does not exist")
	}

	path := s.objectPath(input.Bucket, input.Key)
	var object Object
	if err := readJSONFile(filepath.Join(path, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	if input.ContentType != "" {
		object.ContentType = input.ContentType
	}
	if input.ContentEncoding != "" {
		object.ContentEncoding = input.ContentEncoding
	}
	if input.CacheControl != "" {
		object.CacheControl = input.CacheControl
	}
	if input.ContentDisposition != "" {
		object.ContentDisposition = input.ContentDisposition
	}
	if input.Metadata != nil {
		object.Metadata = cleanMetadata(input.Metadata)
	}
	if object.CreatedAt.IsZero() {
		object.CreatedAt = object.LastModified
	}
	if object.Metageneration < 1 {
		object.Metageneration = 1
	}
	object.Metageneration++
	object.UpdatedAt = time.Now().UTC()
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return Object{}, true, err
	}
	return object, true, nil
}

func (s *FileBucketStore) GetObject(ctx context.Context, bucket string, key string) (Object, []byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return Object{}, nil, false, err
	}
	if err := validateObjectKey(key); err != nil {
		return Object{}, nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return Object{}, nil, false, err
	} else if !ok {
		return Object{}, nil, false, fmt.Errorf("bucket does not exist")
	}

	path := s.objectPath(bucket, key)
	data, err := os.ReadFile(filepath.Join(path, "object.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read object metadata: %w", err)
	}
	var object Object
	if err := json.Unmarshal(data, &object); err != nil {
		return Object{}, nil, false, fmt.Errorf("decode object metadata: %w", err)
	}
	if object.DeleteMarker {
		return Object{}, nil, false, nil
	}
	body, err := os.ReadFile(filepath.Join(path, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read object body: %w", err)
	}
	return object, body, true, nil
}

func (s *FileBucketStore) GetObjectVersion(ctx context.Context, bucket string, key string, versionID string) (Object, []byte, bool, error) {
	if versionID == "" {
		return s.GetObject(ctx, bucket, key)
	}
	if err := ctx.Err(); err != nil {
		return Object{}, nil, false, err
	}
	if err := s.requireBucketAndKey(ctx, bucket, key); err != nil {
		return Object{}, nil, false, err
	}
	if versionID == nullVersionID {
		if object, body, ok, err := s.getNullObjectVersion(bucket, key); err != nil || ok {
			return object, body, ok, err
		}
	}
	path := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
	var object Object
	if err := readJSONFile(filepath.Join(path, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read object version metadata: %w", err)
	}
	if object.DeleteMarker {
		return object, nil, true, nil
	}
	body, err := os.ReadFile(filepath.Join(path, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read object version body: %w", err)
	}
	return object, body, true, nil
}

func (s *FileBucketStore) PutObjectACL(ctx context.Context, bucket string, key string, versionID string, acl string) (bool, error) {
	object, body, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ok, err
	}
	object.ACL = acl
	if object.DeleteMarker {
		body = nil
	}
	if versionID != "" {
		versionPath := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
		if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
			return true, err
		}
		return true, nil
	}
	path := s.objectPath(bucket, key)
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(path, object, body); err != nil {
			return true, err
		}
	}
	return true, writeJSONFile(filepath.Join(path, "acl.json"), struct {
		ACL string `json:"acl"`
	}{ACL: acl})
}

func (s *FileBucketStore) GetObjectACL(ctx context.Context, bucket string, key string, versionID string) (string, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return "", ok, err
	}
	if object.ACL != "" {
		return object.ACL, true, nil
	}
	var persisted struct {
		ACL string `json:"acl"`
	}
	if err := readJSONFile(filepath.Join(s.objectPath(bucket, key), "acl.json"), &persisted); err != nil {
		if os.IsNotExist(err) {
			return "private", true, nil
		}
		return "", false, err
	}
	if persisted.ACL == "" {
		return "private", true, nil
	}
	return persisted.ACL, true, nil
}

func (s *FileBucketStore) PutObjectRetention(ctx context.Context, bucket string, key string, versionID string, retention ObjectRetention) (Object, bool, error) {
	return s.updateObjectLockMetadata(ctx, bucket, key, versionID, func(object *Object) {
		object.Retention = cleanObjectRetention(retention)
	})
}

func (s *FileBucketStore) GetObjectRetention(ctx context.Context, bucket string, key string, versionID string) (ObjectRetention, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ObjectRetention{}, ok, err
	}
	return cleanObjectRetention(object.Retention), true, nil
}

func (s *FileBucketStore) PutObjectLegalHold(ctx context.Context, bucket string, key string, versionID string, legalHold ObjectLegalHold) (Object, bool, error) {
	return s.updateObjectLockMetadata(ctx, bucket, key, versionID, func(object *Object) {
		object.LegalHold = cleanObjectLegalHold(legalHold)
	})
}

func (s *FileBucketStore) GetObjectLegalHold(ctx context.Context, bucket string, key string, versionID string) (ObjectLegalHold, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ObjectLegalHold{}, ok, err
	}
	return cleanObjectLegalHold(object.LegalHold), true, nil
}

func (s *FileBucketStore) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	_, deleted, err := s.DeleteObjectWithResult(ctx, bucket, key, false)
	return deleted, err
}

func (s *FileBucketStore) DeleteObjectWithResult(ctx context.Context, bucket string, key string, bypassGovernance bool) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return Object{}, false, err
	}
	if err := validateObjectKey(key); err != nil {
		return Object{}, false, err
	}
	existingBucket, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return Object{}, false, err
	} else if !ok {
		return Object{}, false, fmt.Errorf("bucket does not exist")
	}

	objectsPath := s.objectsPath(bucket)
	path := s.objectPath(bucket, key)
	if existingBucket.Versioning == "Enabled" || existingBucket.Versioning == "Suspended" {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return Object{}, false, nil
			}
			return Object{}, false, fmt.Errorf("stat object: %w", err)
		}
		current, ok, err := s.readCurrentObjectMetadata(bucket, key)
		if err != nil {
			return Object{}, false, err
		}
		if ok && !current.DeleteMarker && objectLockPreventsDelete(current, time.Now().UTC(), bypassGovernance) {
			return Object{}, false, errObjectLocked
		}
		now := time.Now().UTC()
		versionID := newVersionID()
		if existingBucket.Versioning == "Suspended" {
			versionID = nullVersionID
		}
		marker := Object{
			Bucket:       bucket,
			Key:          key,
			LastModified: now,
			UpdatedAt:    now,
			VersionID:    versionID,
			DeleteMarker: true,
		}
		if err := writeObjectMetadata(filepath.Join(path, "object.json"), marker); err != nil {
			return Object{}, false, err
		}
		if err := s.writeObjectVersion(path, marker, nil); err != nil {
			return Object{}, false, err
		}
		_ = os.Remove(filepath.Join(path, "body"))
		return marker, true, nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("stat object: %w", err)
	}
	current, ok, err := s.readCurrentObjectMetadata(bucket, key)
	if err != nil {
		return Object{}, false, err
	}
	if ok && objectLockPreventsDelete(current, time.Now().UTC(), bypassGovernance) {
		return Object{}, false, errObjectLocked
	}
	if err := os.RemoveAll(path); err != nil {
		return Object{}, false, fmt.Errorf("delete object: %w", err)
	}
	_ = os.Remove(objectsPath)
	return Object{}, true, nil
}

func (s *FileBucketStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string, bypassGovernance bool) (Object, bool, error) {
	if versionID == "" {
		return Object{}, false, fmt.Errorf("version id is required")
	}
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	if !object.DeleteMarker && objectLockPreventsDelete(object, time.Now().UTC(), bypassGovernance) {
		return Object{}, false, errObjectLocked
	}
	if versionID == nullVersionID {
		if err := os.RemoveAll(filepath.Join(s.objectVersionsPath(bucket, key), nullVersionID)); err != nil {
			return Object{}, false, fmt.Errorf("delete object version: %w", err)
		}
		if err := s.rebuildCurrentObject(bucket, key); err != nil {
			return Object{}, false, err
		}
		return object, true, nil
	}
	if err := os.RemoveAll(filepath.Join(s.objectVersionsPath(bucket, key), versionID)); err != nil {
		return Object{}, false, fmt.Errorf("delete object version: %w", err)
	}
	if err := s.rebuildCurrentObject(bucket, key); err != nil {
		return Object{}, false, err
	}
	return object, true, nil
}

func (s *FileBucketStore) ListObjects(ctx context.Context, bucket string, prefix string) ([]Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	entries, err := os.ReadDir(s.objectsPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read objects: %w", err)
	}
	objects := make([]Object, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.objectsPath(bucket), entry.Name(), "object.json"))
		if err != nil {
			return nil, false, fmt.Errorf("read object metadata: %w", err)
		}
		var object Object
		if err := json.Unmarshal(data, &object); err != nil {
			return nil, false, fmt.Errorf("decode object metadata: %w", err)
		}
		if !object.DeleteMarker && strings.HasPrefix(object.Key, prefix) {
			objects = append(objects, object)
		}
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	return objects, true, nil
}

func (s *FileBucketStore) ListObjectVersions(ctx context.Context, bucket string, prefix string) ([]Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	entries, err := os.ReadDir(s.objectsPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read objects: %w", err)
	}
	versions := []Object{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		objectPath := filepath.Join(s.objectsPath(bucket), entry.Name())
		keyVersionsPath := filepath.Join(objectPath, "versions")
		versionEntries, err := os.ReadDir(keyVersionsPath)
		if err != nil {
			if os.IsNotExist(err) {
				object, ok, err := readObjectMetadataForVersionList(filepath.Join(objectPath, "object.json"), nullVersionID)
				if err != nil {
					return nil, false, err
				}
				if ok && strings.HasPrefix(object.Key, prefix) {
					versions = append(versions, object)
				}
				continue
			}
			return nil, false, fmt.Errorf("read object versions: %w", err)
		}
		for _, versionEntry := range versionEntries {
			if !versionEntry.IsDir() {
				continue
			}
			var object Object
			if err := readJSONFile(filepath.Join(keyVersionsPath, versionEntry.Name(), "object.json"), &object); err != nil {
				return nil, false, fmt.Errorf("read object version metadata: %w", err)
			}
			if strings.HasPrefix(object.Key, prefix) {
				versions = append(versions, object)
			}
		}
		if _, err := os.Stat(filepath.Join(keyVersionsPath, nullVersionID)); os.IsNotExist(err) {
			object, ok, err := readObjectMetadataForVersionList(filepath.Join(objectPath, "object.json"), nullVersionID)
			if err != nil {
				return nil, false, err
			}
			if ok && objectVersionID(object) == nullVersionID && strings.HasPrefix(object.Key, prefix) {
				versions = append(versions, object)
			}
		}
	}
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key == versions[j].Key {
			return versions[i].LastModified.After(versions[j].LastModified)
		}
		return versions[i].Key < versions[j].Key
	})
	return versions, true, nil
}

func (s *FileBucketStore) CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (MultipartUpload, error) {
	if err := ctx.Err(); err != nil {
		return MultipartUpload{}, err
	}
	if err := s.requireBucketAndKey(ctx, input.Bucket, input.Key); err != nil {
		return MultipartUpload{}, err
	}
	upload := MultipartUpload{
		Bucket:             input.Bucket,
		Key:                input.Key,
		UploadID:           newUploadID(),
		CreatedAt:          time.Now().UTC(),
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		CacheControl:       input.CacheControl,
		ContentDisposition: input.ContentDisposition,
		Metadata:           cleanMetadata(input.Metadata),
		Encryption:         cleanServerSideEncryption(input.Encryption),
	}
	if upload.ContentType == "" {
		upload.ContentType = "application/octet-stream"
	}
	path := s.multipartUploadPath(input.Bucket, upload.UploadID)
	if err := os.MkdirAll(filepath.Join(path, "parts"), 0o755); err != nil {
		return MultipartUpload{}, fmt.Errorf("create multipart upload: %w", err)
	}
	if err := writeJSONFile(filepath.Join(path, "upload.json"), upload); err != nil {
		return MultipartUpload{}, err
	}
	return upload, nil
}

func (s *FileBucketStore) UploadPart(ctx context.Context, bucket string, key string, uploadID string, partNumber int, body io.Reader, contentMD5 string) (MultipartPart, error) {
	if err := ctx.Err(); err != nil {
		return MultipartPart{}, err
	}
	if partNumber < 1 || partNumber > 10000 {
		return MultipartPart{}, fmt.Errorf("invalid part number")
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		return MultipartPart{}, err
	}
	if !ok || upload.Key != key {
		return MultipartPart{}, fmt.Errorf("multipart upload does not exist")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return MultipartPart{}, fmt.Errorf("read multipart part: %w", err)
	}
	if err := validateContentMD5(contentMD5, data); err != nil {
		return MultipartPart{}, err
	}
	sum := md5.Sum(data)
	part := MultipartPart{
		PartNumber:   partNumber,
		ETag:         `"` + hex.EncodeToString(sum[:]) + `"`,
		Size:         int64(len(data)),
		LastModified: time.Now().UTC(),
	}
	path := s.multipartPartPath(bucket, uploadID, partNumber)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return MultipartPart{}, fmt.Errorf("create multipart part: %w", err)
	}
	if err := os.WriteFile(filepath.Join(path, "body"), data, 0o644); err != nil {
		return MultipartPart{}, fmt.Errorf("write multipart part body: %w", err)
	}
	if err := writeJSONFile(filepath.Join(path, "part.json"), part); err != nil {
		return MultipartPart{}, err
	}
	return part, nil
}

func (s *FileBucketStore) ListParts(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, []MultipartPart, bool, error) {
	if err := ctx.Err(); err != nil {
		return MultipartUpload{}, nil, false, err
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return upload, nil, ok, err
	}
	parts, err := s.readMultipartParts(bucket, uploadID)
	if err != nil {
		return MultipartUpload{}, nil, false, err
	}
	return upload, parts, true, nil
}

func (s *FileBucketStore) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, partNumbers []int) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	var combined bytes.Buffer
	partETags := make([]string, 0, len(partNumbers))
	for _, partNumber := range partNumbers {
		var part MultipartPart
		if err := readJSONFile(filepath.Join(s.multipartPartPath(bucket, uploadID, partNumber), "part.json"), &part); err != nil {
			if os.IsNotExist(err) {
				return Object{}, true, fmt.Errorf("multipart part %d does not exist", partNumber)
			}
			return Object{}, true, fmt.Errorf("read multipart part metadata: %w", err)
		}
		body, err := os.ReadFile(filepath.Join(s.multipartPartPath(bucket, uploadID, partNumber), "body"))
		if err != nil {
			if os.IsNotExist(err) {
				return Object{}, true, fmt.Errorf("multipart part %d does not exist", partNumber)
			}
			return Object{}, true, fmt.Errorf("read multipart part: %w", err)
		}
		combined.Write(body)
		partETags = append(partETags, part.ETag)
	}
	object, err := s.PutObject(ctx, PutObjectInput{
		Bucket:             upload.Bucket,
		Key:                upload.Key,
		Body:               bytes.NewReader(combined.Bytes()),
		ContentType:        upload.ContentType,
		ContentEncoding:    upload.ContentEncoding,
		CacheControl:       upload.CacheControl,
		ContentDisposition: upload.ContentDisposition,
		Metadata:           upload.Metadata,
		Encryption:         upload.Encryption,
	})
	if err != nil {
		return Object{}, true, err
	}
	object.ETag = multipartETag(partETags)
	if err := writeObjectMetadata(filepath.Join(s.objectPath(upload.Bucket, upload.Key), "object.json"), object); err != nil {
		return Object{}, true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(s.objectPath(upload.Bucket, upload.Key), object, combined.Bytes()); err != nil {
			return Object{}, true, err
		}
	}
	if err := os.RemoveAll(s.multipartUploadPath(bucket, uploadID)); err != nil {
		return Object{}, true, fmt.Errorf("delete completed multipart upload: %w", err)
	}
	_ = os.Remove(s.multipartPath(bucket))
	return object, true, nil
}

func (s *FileBucketStore) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return ok, err
	}
	if err := os.RemoveAll(s.multipartUploadPath(bucket, uploadID)); err != nil {
		return true, fmt.Errorf("abort multipart upload: %w", err)
	}
	_ = os.Remove(s.multipartPath(bucket))
	return true, nil
}

func (s *FileBucketStore) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}
	entries, err := os.ReadDir(s.multipartPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read multipart uploads: %w", err)
	}
	uploads := make([]MultipartUpload, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var upload MultipartUpload
		if err := readJSONFile(filepath.Join(s.multipartPath(bucket), entry.Name(), "upload.json"), &upload); err != nil {
			return nil, false, err
		}
		uploads = append(uploads, upload)
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key == uploads[j].Key {
			return uploads[i].UploadID < uploads[j].UploadID
		}
		return uploads[i].Key < uploads[j].Key
	})
	return uploads, true, nil
}

func (s *FileBucketStore) requireBucketAndKey(ctx context.Context, bucket string, key string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return nil
}

func (s *FileBucketStore) getMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, bool, error) {
	if err := validateUploadID(uploadID); err != nil {
		return MultipartUpload{}, false, err
	}
	if err := s.requireBucketAndKey(ctx, bucket, key); err != nil {
		return MultipartUpload{}, false, err
	}
	var upload MultipartUpload
	err := readJSONFile(filepath.Join(s.multipartUploadPath(bucket, uploadID), "upload.json"), &upload)
	if err != nil {
		if os.IsNotExist(err) {
			return MultipartUpload{}, false, nil
		}
		return MultipartUpload{}, false, err
	}
	if upload.Key != key {
		return MultipartUpload{}, false, nil
	}
	return upload, true, nil
}

func (s *FileBucketStore) readMultipartParts(bucket string, uploadID string) ([]MultipartPart, error) {
	entries, err := os.ReadDir(filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read multipart parts: %w", err)
	}
	parts := make([]MultipartPart, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var part MultipartPart
		if err := readJSONFile(filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts", entry.Name(), "part.json"), &part); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
}

func (s *FileBucketStore) updateObjectLockMetadata(ctx context.Context, bucket string, key string, versionID string, update func(*Object)) (Object, bool, error) {
	object, body, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	update(&object)
	if object.DeleteMarker {
		body = nil
	}
	if versionID != "" {
		versionPath := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
		if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
			return Object{}, true, err
		}
	} else if err := writeObjectMetadata(filepath.Join(s.objectPath(bucket, key), "object.json"), object); err != nil {
		return Object{}, true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(s.objectPath(bucket, key), object, body); err != nil {
			return Object{}, true, err
		}
	}
	return object, true, nil
}

func (s *FileBucketStore) readCurrentObjectMetadata(bucket string, key string) (Object, bool, error) {
	var object Object
	if err := readJSONFile(filepath.Join(s.objectPath(bucket, key), "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	return object, true, nil
}

func (s *FileBucketStore) bucketPath(name string) string {
	return filepath.Join(s.root, name)
}

func (s *FileBucketStore) objectsPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "objects")
}

func (s *FileBucketStore) objectPath(bucket string, key string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(key))
	return filepath.Join(s.objectsPath(bucket), encoded)
}

func (s *FileBucketStore) objectVersionsPath(bucket string, key string) string {
	return filepath.Join(s.objectPath(bucket, key), "versions")
}

func (s *FileBucketStore) multipartPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "multipart")
}

func (s *FileBucketStore) multipartUploadPath(bucket string, uploadID string) string {
	return filepath.Join(s.multipartPath(bucket), uploadID)
}

func (s *FileBucketStore) multipartPartPath(bucket string, uploadID string, partNumber int) string {
	return filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts", fmt.Sprintf("%05d", partNumber))
}

func (s *FileBucketStore) inventoryPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "inventory")
}

func (s *FileBucketStore) inventoryConfigPath(bucket string, id string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(id))
	return filepath.Join(s.inventoryPath(bucket), encoded+".json")
}

func (s *FileBucketStore) inventoryReportPath(bucket string, id string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(id))
	return filepath.Join(s.inventoryPath(bucket), "reports", encoded)
}

func (s *FileBucketStore) inventoryReportCSVPath(bucket string, id string) string {
	return filepath.Join(s.inventoryReportPath(bucket, id), "inventory.csv")
}

func (s *FileBucketStore) inventoryReportManifestPath(bucket string, id string) string {
	return filepath.Join(s.inventoryReportPath(bucket, id), "manifest.json")
}

func (s *FileBucketStore) analyticsPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "analytics")
}

func (s *FileBucketStore) analyticsConfigPath(bucket string, id string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(id))
	return filepath.Join(s.analyticsPath(bucket), encoded+".json")
}

func writeBucketMetadata(path string, bucket Bucket) error {
	return writeJSONFile(path, bucket)
}

func writeObjectMetadata(path string, object Object) error {
	return writeJSONFile(path, object)
}

func (s *FileBucketStore) writeObjectVersion(objectPath string, object Object, body []byte) error {
	if object.VersionID == "" {
		return nil
	}
	versionPath := filepath.Join(objectPath, "versions", object.VersionID)
	if err := os.MkdirAll(versionPath, 0o755); err != nil {
		return fmt.Errorf("create object version directory: %w", err)
	}
	if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
		return err
	}
	if object.DeleteMarker {
		_ = os.Remove(filepath.Join(versionPath, "body"))
		return nil
	}
	if err := os.WriteFile(filepath.Join(versionPath, "body"), body, 0o644); err != nil {
		return fmt.Errorf("write object version body: %w", err)
	}
	return nil
}

func (s *FileBucketStore) getNullObjectVersion(bucket string, key string) (Object, []byte, bool, error) {
	versionPath := filepath.Join(s.objectVersionsPath(bucket, key), nullVersionID)
	var object Object
	if err := readJSONFile(filepath.Join(versionPath, "object.json"), &object); err == nil {
		if object.DeleteMarker {
			return object, nil, true, nil
		}
		body, err := os.ReadFile(filepath.Join(versionPath, "body"))
		if err != nil {
			return Object{}, nil, false, fmt.Errorf("read null object version body: %w", err)
		}
		return object, body, true, nil
	} else if !os.IsNotExist(err) {
		return Object{}, nil, false, fmt.Errorf("read null object version metadata: %w", err)
	}

	currentPath := s.objectPath(bucket, key)
	if err := readJSONFile(filepath.Join(currentPath, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read current object metadata: %w", err)
	}
	if object.VersionID != "" && object.VersionID != nullVersionID {
		return Object{}, nil, false, nil
	}
	object.VersionID = nullVersionID
	if object.DeleteMarker {
		return object, nil, true, nil
	}
	body, err := os.ReadFile(filepath.Join(currentPath, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read current object body: %w", err)
	}
	return object, body, true, nil
}

func readObjectMetadataForVersionList(path string, defaultVersionID string) (Object, bool, error) {
	var object Object
	if err := readJSONFile(path, &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	if object.VersionID == "" {
		object.VersionID = defaultVersionID
	}
	return object, true, nil
}

func (s *FileBucketStore) rebuildCurrentObject(bucket string, key string) error {
	path := s.objectPath(bucket, key)
	entries, err := os.ReadDir(filepath.Join(path, "versions"))
	if err != nil {
		if os.IsNotExist(err) {
			return os.RemoveAll(path)
		}
		return fmt.Errorf("read object versions: %w", err)
	}
	var latest Object
	var latestBody []byte
	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		versionPath := filepath.Join(path, "versions", entry.Name())
		var object Object
		if err := readJSONFile(filepath.Join(versionPath, "object.json"), &object); err != nil {
			return err
		}
		if found && !object.LastModified.After(latest.LastModified) {
			continue
		}
		latest = object
		found = true
		if object.DeleteMarker {
			latestBody = nil
			continue
		}
		body, err := os.ReadFile(filepath.Join(versionPath, "body"))
		if err != nil {
			return fmt.Errorf("read object version body: %w", err)
		}
		latestBody = body
	}
	if !found {
		return os.RemoveAll(path)
	}
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), latest); err != nil {
		return err
	}
	bodyPath := filepath.Join(path, "body")
	if latest.DeleteMarker {
		_ = os.Remove(bodyPath)
		return nil
	}
	return os.WriteFile(bodyPath, latestBody, 0o644)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode json metadata: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write json metadata: %w", err)
	}
	return nil
}

func (s *FileBucketStore) writeInventoryReport(ctx context.Context, bucket string, id string, config InventoryConfiguration) error {
	objects, err := s.inventoryReportObjects(ctx, bucket, config)
	if err != nil {
		return err
	}
	fields := inventoryReportFields(config)
	reportPath := s.inventoryReportPath(bucket, id)
	if err := os.MkdirAll(reportPath, 0o755); err != nil {
		return fmt.Errorf("create inventory report directory: %w", err)
	}
	var csvBody bytes.Buffer
	writer := csv.NewWriter(&csvBody)
	if err := writer.Write(fields); err != nil {
		return err
	}
	latest := latestVersionIDs(objects)
	for _, object := range objects {
		if object.DeleteMarker {
			continue
		}
		if err := writer.Write(inventoryReportRow(object, latest, fields)); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	if err := os.WriteFile(s.inventoryReportCSVPath(bucket, id), csvBody.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write inventory csv: %w", err)
	}
	encodedID := base64.RawURLEncoding.EncodeToString([]byte(id))
	manifest := InventoryReportManifest{
		ConfigurationID:  id,
		SourceBucket:     bucket,
		Format:           inventoryReportFormat(config),
		IncludedVersions: inventoryIncludedVersions(config),
		Fields:           fields,
		ObjectCount:      inventoryReportObjectCount(objects),
		ReportKey:        filepath.ToSlash(filepath.Join("inventory", "reports", encodedID, "inventory.csv")),
	}
	return writeJSONFile(s.inventoryReportManifestPath(bucket, id), manifest)
}

func (s *FileBucketStore) inventoryReportObjects(ctx context.Context, bucket string, config InventoryConfiguration) ([]Object, error) {
	if inventoryIncludedVersions(config) == "All" {
		objects, bucketExists, err := s.ListObjectVersions(ctx, bucket, "")
		if err != nil {
			return nil, err
		}
		if !bucketExists {
			return nil, fmt.Errorf("bucket does not exist")
		}
		return deduplicateInventoryObjects(objects), nil
	}
	objects, bucketExists, err := s.ListObjects(ctx, bucket, "")
	if err != nil {
		return nil, err
	}
	if !bucketExists {
		return nil, fmt.Errorf("bucket does not exist")
	}
	return objects, nil
}

func deduplicateInventoryObjects(objects []Object) []Object {
	seen := map[string]struct{}{}
	deduplicated := make([]Object, 0, len(objects))
	for _, object := range objects {
		key := object.Bucket + "\x00" + object.Key + "\x00" + object.VersionID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduplicated = append(deduplicated, object)
	}
	return deduplicated
}

func inventoryReportFields(config InventoryConfiguration) []string {
	fields := []string{"Bucket", "Key", "Size", "LastModifiedDate", "ETag", "StorageClass"}
	if inventoryIncludedVersions(config) == "All" {
		fields = append(fields, "VersionId", "IsLatest")
	}
	for _, field := range config.OptionalFields {
		field = strings.TrimSpace(field)
		if field == "" || inventoryReportFieldExists(fields, field) {
			continue
		}
		fields = append(fields, field)
	}
	return fields
}

func inventoryReportFieldExists(fields []string, field string) bool {
	for _, existing := range fields {
		if existing == field {
			return true
		}
	}
	return false
}

func inventoryReportRow(object Object, latest map[string]string, fields []string) []string {
	row := make([]string, 0, len(fields))
	for _, field := range fields {
		switch field {
		case "Bucket":
			row = append(row, object.Bucket)
		case "Key":
			row = append(row, object.Key)
		case "Size":
			row = append(row, strconv.FormatInt(object.Size, 10))
		case "LastModifiedDate":
			row = append(row, object.LastModified.Format(time.RFC3339))
		case "ETag":
			row = append(row, object.ETag)
		case "StorageClass":
			row = append(row, "STANDARD")
		case "VersionId":
			row = append(row, object.VersionID)
		case "IsLatest":
			row = append(row, strconv.FormatBool(latest[object.Key] == object.VersionID))
		case "EncryptionStatus":
			row = append(row, object.Encryption.Algorithm)
		case "ObjectLockRetainUntilDate":
			row = append(row, object.Retention.RetainUntilDate)
		case "ObjectLockRetentionMode":
			row = append(row, object.Retention.Mode)
		case "ObjectLockLegalHoldStatus":
			row = append(row, object.LegalHold.Status)
		default:
			row = append(row, "")
		}
	}
	return row
}

func latestVersionIDs(objects []Object) map[string]string {
	latest := map[string]string{}
	latestModified := map[string]time.Time{}
	for _, object := range objects {
		if current, ok := latestModified[object.Key]; ok && !object.LastModified.After(current) {
			continue
		}
		latest[object.Key] = object.VersionID
		latestModified[object.Key] = object.LastModified
	}
	return latest
}

func inventoryReportObjectCount(objects []Object) int {
	count := 0
	for _, object := range objects {
		if !object.DeleteMarker {
			count++
		}
	}
	return count
}

func inventoryIncludedVersions(config InventoryConfiguration) string {
	if config.IncludedObjectVersions == "All" {
		return "All"
	}
	return "Current"
}

func inventoryReportFormat(config InventoryConfiguration) string {
	format := strings.TrimSpace(config.Destination.S3BucketDestination.Format)
	if format == "" {
		return "CSV"
	}
	return format
}

func readJSONFile(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode json metadata: %w", err)
	}
	return nil
}

func newUploadID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func newVersionID() string {
	return newUploadID()
}

func multipartETag(partETags []string) string {
	hashes := make([]byte, 0, len(partETags)*md5.Size)
	for _, etag := range partETags {
		raw, err := hex.DecodeString(strings.Trim(etag, `"`))
		if err != nil || len(raw) != md5.Size {
			return `"` + fmt.Sprintf("%d", len(partETags)) + `"`
		}
		hashes = append(hashes, raw...)
	}
	sum := md5.Sum(hashes)
	return `"` + hex.EncodeToString(sum[:]) + "-" + fmt.Sprintf("%d", len(partETags)) + `"`
}

func crc32cBase64(data []byte) string {
	checksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], checksum)
	return base64.StdEncoding.EncodeToString(buf[:])
}

func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("invalid bucket name %q", name)
	}
	if strings.Contains(name, "/") || strings.Contains(name, `\`) || strings.Contains(name, "..") {
		return fmt.Errorf("invalid bucket name %q", name)
	}
	if !isBucketNameAlnum(name[0]) || !isBucketNameAlnum(name[len(name)-1]) {
		return fmt.Errorf("invalid bucket name %q", name)
	}
	if strings.Contains(name, ".-") || strings.Contains(name, "-.") || isIPv4AddressLike(name) {
		return fmt.Errorf("invalid bucket name %q", name)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid bucket name %q", name)
	}
	return nil
}

func isBucketNameAlnum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

func isIPv4AddressLike(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if part == "" || len(part) > 3 {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func validateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key is required")
	}
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("object key contains null byte")
	}
	return nil
}

func validateUploadID(uploadID string) error {
	if len(uploadID) != 32 {
		return fmt.Errorf("invalid upload id")
	}
	for _, r := range uploadID {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return fmt.Errorf("invalid upload id")
	}
	return nil
}

func cleanMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	cleaned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cleaned[strings.ToLower(key)] = value
	}
	return cleaned
}

func cleanServerSideEncryption(encryption ServerSideEncryption) ServerSideEncryption {
	cleaned := ServerSideEncryption{
		Algorithm: strings.TrimSpace(encryption.Algorithm),
		KMSKeyID:  strings.TrimSpace(encryption.KMSKeyID),
	}
	if encryption.BucketKeyEnabled != nil {
		enabled := *encryption.BucketKeyEnabled
		cleaned.BucketKeyEnabled = &enabled
	}
	return cleaned
}

var errObjectLocked = fmt.Errorf("object is locked")

func cleanObjectRetention(retention ObjectRetention) ObjectRetention {
	return ObjectRetention{
		Mode:            strings.TrimSpace(retention.Mode),
		RetainUntilDate: strings.TrimSpace(retention.RetainUntilDate),
	}
}

func cleanObjectLegalHold(legalHold ObjectLegalHold) ObjectLegalHold {
	return ObjectLegalHold{Status: strings.TrimSpace(legalHold.Status)}
}

func objectLockPreventsDelete(object Object, now time.Time, bypassGovernance bool) bool {
	if object.LegalHold.Status == "ON" {
		return true
	}
	if object.Retention.Mode == "" || object.Retention.RetainUntilDate == "" {
		return false
	}
	if object.Retention.Mode == "GOVERNANCE" && bypassGovernance {
		return false
	}
	retainUntil, err := time.Parse(time.RFC3339, object.Retention.RetainUntilDate)
	if err != nil {
		return true
	}
	return retainUntil.After(now)
}

func defaultObjectRetention(config ObjectLockConfiguration, now time.Time) ObjectRetention {
	defaultRetention := config.Rule.DefaultRetention
	if defaultRetention.Mode == "" {
		return ObjectRetention{}
	}
	retainUntil := now
	switch {
	case defaultRetention.Days > 0:
		retainUntil = retainUntil.Add(time.Duration(defaultRetention.Days) * 24 * time.Hour)
	case defaultRetention.Years > 0:
		retainUntil = retainUntil.AddDate(defaultRetention.Years, 0, 0)
	default:
		return ObjectRetention{}
	}
	return ObjectRetention{
		Mode:            defaultRetention.Mode,
		RetainUntilDate: retainUntil.Format(time.RFC3339),
	}
}

func lifecycleExpiresObject(config LifecycleConfiguration, object Object, now time.Time) bool {
	for _, rule := range config.Rules {
		if rule.Status != "Enabled" {
			continue
		}
		prefix := rule.Filter.Prefix
		if prefix == "" {
			prefix = rule.Prefix
		}
		if prefix != "" && !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if rule.Expiration.Days != nil {
			expiresAt := object.LastModified.Add(time.Duration(*rule.Expiration.Days) * 24 * time.Hour)
			if !expiresAt.After(now) {
				return true
			}
		}
		if rule.Expiration.Date != "" {
			expiresAt, err := parseLifecycleExpirationDate(rule.Expiration.Date)
			if err == nil && !expiresAt.After(now) {
				return true
			}
		}
	}
	return false
}

func parseLifecycleExpirationDate(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	return time.Parse("2006-01-02", value)
}

var (
	errInvalidContentMD5  = fmt.Errorf("invalid content-md5")
	errContentMD5Mismatch = fmt.Errorf("content-md5 mismatch")
)

func validateContentMD5(header string, body []byte) error {
	if header == "" {
		return nil
	}
	expected, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(expected) != md5.Size {
		return errInvalidContentMD5
	}
	sum := md5.Sum(body)
	if !bytes.Equal(expected, sum[:]) {
		return errContentMD5Mismatch
	}
	return nil
}
