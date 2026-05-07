package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

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
