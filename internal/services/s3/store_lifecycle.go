package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
