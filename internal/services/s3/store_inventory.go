package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

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
