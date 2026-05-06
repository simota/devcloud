package s3

import (
	"bytes"
	"context"
	"strings"
	"time"
)

func (s *Server) applyBucketLifecycle(ctx context.Context, bucket string) error {
	if err := validateBucketName(bucket); err != nil {
		return nil
	}
	_, _, err := s.store.ApplyBucketLifecycle(ctx, bucket, time.Now().UTC())
	return err
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
