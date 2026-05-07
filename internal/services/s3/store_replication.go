package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

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
