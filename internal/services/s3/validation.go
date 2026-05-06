package s3

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

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

var errUnsupportedLifecycleRule = fmt.Errorf("unsupported lifecycle rule")
var errUnsupportedServerSideEncryption = fmt.Errorf("unsupported server-side encryption")
var errUnsupportedSSECustomerKey = fmt.Errorf("unsupported sse-c")
var errInvalidServerSideEncryption = fmt.Errorf("invalid server-side encryption")
