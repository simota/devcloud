package s3

import (
	"context"
	"encoding/xml"
	"io"
	"time"
)

const nullVersionID = "null"

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
