package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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
