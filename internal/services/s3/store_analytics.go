package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

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
