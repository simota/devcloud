package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileBucketStore struct {
	root string
}

func NewFileBucketStore(root string) *FileBucketStore {
	return &FileBucketStore{root: root}
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

var (
	errObjectLocked       = fmt.Errorf("object is locked")
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
