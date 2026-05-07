package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (s *FileBucketStore) PutObject(ctx context.Context, input PutObjectInput) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if err := validateBucketName(input.Bucket); err != nil {
		return Object{}, err
	}
	if err := validateObjectKey(input.Key); err != nil {
		return Object{}, err
	}
	bucket, ok, err := s.GetBucket(ctx, input.Bucket)
	if err != nil {
		return Object{}, err
	} else if !ok {
		return Object{}, fmt.Errorf("bucket does not exist")
	}

	body, err := io.ReadAll(input.Body)
	if err != nil {
		return Object{}, fmt.Errorf("read object body: %w", err)
	}
	if err := validateContentMD5(input.ContentMD5, body); err != nil {
		return Object{}, err
	}
	sum := md5.Sum(body)
	now := time.Now().UTC()
	object := Object{
		Bucket:             input.Bucket,
		Key:                input.Key,
		ETag:               `"` + hex.EncodeToString(sum[:]) + `"`,
		Size:               int64(len(body)),
		CreatedAt:          now,
		LastModified:       now,
		UpdatedAt:          now,
		Metageneration:     1,
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		CRC32C:             crc32cBase64(body),
		CacheControl:       input.CacheControl,
		ContentDisposition: input.ContentDisposition,
		Metadata:           cleanMetadata(input.Metadata),
		Encryption:         cleanServerSideEncryption(input.Encryption),
		Retention:          cleanObjectRetention(input.Retention),
		LegalHold:          cleanObjectLegalHold(input.LegalHold),
	}
	if object.Retention.Mode == "" {
		object.Retention = defaultObjectRetention(bucket.ObjectLockConfig, now)
	}
	switch bucket.Versioning {
	case "Enabled":
		object.VersionID = newVersionID()
	case "Suspended":
		object.VersionID = nullVersionID
	}
	if object.ContentType == "" {
		object.ContentType = "application/octet-stream"
	}

	path := s.objectPath(input.Bucket, input.Key)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return Object{}, fmt.Errorf("create object directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(path, "body"), body, 0o644); err != nil {
		return Object{}, fmt.Errorf("write object body: %w", err)
	}
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return Object{}, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(path, object, body); err != nil {
			return Object{}, err
		}
	}
	return object, nil
}

func (s *FileBucketStore) UpdateObjectMetadata(ctx context.Context, input UpdateObjectMetadataInput) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	if err := validateBucketName(input.Bucket); err != nil {
		return Object{}, false, err
	}
	if err := validateObjectKey(input.Key); err != nil {
		return Object{}, false, err
	}
	if _, ok, err := s.GetBucket(ctx, input.Bucket); err != nil {
		return Object{}, false, err
	} else if !ok {
		return Object{}, false, fmt.Errorf("bucket does not exist")
	}

	path := s.objectPath(input.Bucket, input.Key)
	var object Object
	if err := readJSONFile(filepath.Join(path, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	if input.ContentType != "" {
		object.ContentType = input.ContentType
	}
	if input.ContentEncoding != "" {
		object.ContentEncoding = input.ContentEncoding
	}
	if input.CacheControl != "" {
		object.CacheControl = input.CacheControl
	}
	if input.ContentDisposition != "" {
		object.ContentDisposition = input.ContentDisposition
	}
	if input.Metadata != nil {
		object.Metadata = cleanMetadata(input.Metadata)
	}
	if object.CreatedAt.IsZero() {
		object.CreatedAt = object.LastModified
	}
	if object.Metageneration < 1 {
		object.Metageneration = 1
	}
	object.Metageneration++
	object.UpdatedAt = time.Now().UTC()
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return Object{}, true, err
	}
	return object, true, nil
}

func (s *FileBucketStore) GetObject(ctx context.Context, bucket string, key string) (Object, []byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return Object{}, nil, false, err
	}
	if err := validateObjectKey(key); err != nil {
		return Object{}, nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return Object{}, nil, false, err
	} else if !ok {
		return Object{}, nil, false, fmt.Errorf("bucket does not exist")
	}

	path := s.objectPath(bucket, key)
	data, err := os.ReadFile(filepath.Join(path, "object.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read object metadata: %w", err)
	}
	var object Object
	if err := json.Unmarshal(data, &object); err != nil {
		return Object{}, nil, false, fmt.Errorf("decode object metadata: %w", err)
	}
	if object.DeleteMarker {
		return Object{}, nil, false, nil
	}
	body, err := os.ReadFile(filepath.Join(path, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read object body: %w", err)
	}
	return object, body, true, nil
}

func (s *FileBucketStore) GetObjectVersion(ctx context.Context, bucket string, key string, versionID string) (Object, []byte, bool, error) {
	if versionID == "" {
		return s.GetObject(ctx, bucket, key)
	}
	if err := ctx.Err(); err != nil {
		return Object{}, nil, false, err
	}
	if err := s.requireBucketAndKey(ctx, bucket, key); err != nil {
		return Object{}, nil, false, err
	}
	if versionID == nullVersionID {
		if object, body, ok, err := s.getNullObjectVersion(bucket, key); err != nil || ok {
			return object, body, ok, err
		}
	}
	path := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
	var object Object
	if err := readJSONFile(filepath.Join(path, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read object version metadata: %w", err)
	}
	if object.DeleteMarker {
		return object, nil, true, nil
	}
	body, err := os.ReadFile(filepath.Join(path, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read object version body: %w", err)
	}
	return object, body, true, nil
}

func (s *FileBucketStore) PutObjectACL(ctx context.Context, bucket string, key string, versionID string, acl string) (bool, error) {
	object, body, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ok, err
	}
	object.ACL = acl
	if object.DeleteMarker {
		body = nil
	}
	if versionID != "" {
		versionPath := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
		if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
			return true, err
		}
		return true, nil
	}
	path := s.objectPath(bucket, key)
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), object); err != nil {
		return true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(path, object, body); err != nil {
			return true, err
		}
	}
	return true, writeJSONFile(filepath.Join(path, "acl.json"), struct {
		ACL string `json:"acl"`
	}{ACL: acl})
}

func (s *FileBucketStore) GetObjectACL(ctx context.Context, bucket string, key string, versionID string) (string, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return "", ok, err
	}
	if object.ACL != "" {
		return object.ACL, true, nil
	}
	var persisted struct {
		ACL string `json:"acl"`
	}
	if err := readJSONFile(filepath.Join(s.objectPath(bucket, key), "acl.json"), &persisted); err != nil {
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

func (s *FileBucketStore) PutObjectRetention(ctx context.Context, bucket string, key string, versionID string, retention ObjectRetention) (Object, bool, error) {
	return s.updateObjectLockMetadata(ctx, bucket, key, versionID, func(object *Object) {
		object.Retention = cleanObjectRetention(retention)
	})
}

func (s *FileBucketStore) GetObjectRetention(ctx context.Context, bucket string, key string, versionID string) (ObjectRetention, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ObjectRetention{}, ok, err
	}
	return cleanObjectRetention(object.Retention), true, nil
}

func (s *FileBucketStore) PutObjectLegalHold(ctx context.Context, bucket string, key string, versionID string, legalHold ObjectLegalHold) (Object, bool, error) {
	return s.updateObjectLockMetadata(ctx, bucket, key, versionID, func(object *Object) {
		object.LegalHold = cleanObjectLegalHold(legalHold)
	})
}

func (s *FileBucketStore) GetObjectLegalHold(ctx context.Context, bucket string, key string, versionID string) (ObjectLegalHold, bool, error) {
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return ObjectLegalHold{}, ok, err
	}
	return cleanObjectLegalHold(object.LegalHold), true, nil
}

func (s *FileBucketStore) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	_, deleted, err := s.DeleteObjectWithResult(ctx, bucket, key, false)
	return deleted, err
}

func (s *FileBucketStore) DeleteObjectWithResult(ctx context.Context, bucket string, key string, bypassGovernance bool) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return Object{}, false, err
	}
	if err := validateObjectKey(key); err != nil {
		return Object{}, false, err
	}
	existingBucket, ok, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return Object{}, false, err
	} else if !ok {
		return Object{}, false, fmt.Errorf("bucket does not exist")
	}

	objectsPath := s.objectsPath(bucket)
	path := s.objectPath(bucket, key)
	if existingBucket.Versioning == "Enabled" || existingBucket.Versioning == "Suspended" {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return Object{}, false, nil
			}
			return Object{}, false, fmt.Errorf("stat object: %w", err)
		}
		current, ok, err := s.readCurrentObjectMetadata(bucket, key)
		if err != nil {
			return Object{}, false, err
		}
		if ok && !current.DeleteMarker && objectLockPreventsDelete(current, time.Now().UTC(), bypassGovernance) {
			return Object{}, false, errObjectLocked
		}
		now := time.Now().UTC()
		versionID := newVersionID()
		if existingBucket.Versioning == "Suspended" {
			versionID = nullVersionID
		}
		marker := Object{
			Bucket:       bucket,
			Key:          key,
			LastModified: now,
			UpdatedAt:    now,
			VersionID:    versionID,
			DeleteMarker: true,
		}
		if err := writeObjectMetadata(filepath.Join(path, "object.json"), marker); err != nil {
			return Object{}, false, err
		}
		if err := s.writeObjectVersion(path, marker, nil); err != nil {
			return Object{}, false, err
		}
		_ = os.Remove(filepath.Join(path, "body"))
		return marker, true, nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("stat object: %w", err)
	}
	current, ok, err := s.readCurrentObjectMetadata(bucket, key)
	if err != nil {
		return Object{}, false, err
	}
	if ok && objectLockPreventsDelete(current, time.Now().UTC(), bypassGovernance) {
		return Object{}, false, errObjectLocked
	}
	if err := os.RemoveAll(path); err != nil {
		return Object{}, false, fmt.Errorf("delete object: %w", err)
	}
	_ = os.Remove(objectsPath)
	return Object{}, true, nil
}

func (s *FileBucketStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string, bypassGovernance bool) (Object, bool, error) {
	if versionID == "" {
		return Object{}, false, fmt.Errorf("version id is required")
	}
	object, _, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	if !object.DeleteMarker && objectLockPreventsDelete(object, time.Now().UTC(), bypassGovernance) {
		return Object{}, false, errObjectLocked
	}
	if versionID == nullVersionID {
		if err := os.RemoveAll(filepath.Join(s.objectVersionsPath(bucket, key), nullVersionID)); err != nil {
			return Object{}, false, fmt.Errorf("delete object version: %w", err)
		}
		if err := s.rebuildCurrentObject(bucket, key); err != nil {
			return Object{}, false, err
		}
		return object, true, nil
	}
	if err := os.RemoveAll(filepath.Join(s.objectVersionsPath(bucket, key), versionID)); err != nil {
		return Object{}, false, fmt.Errorf("delete object version: %w", err)
	}
	if err := s.rebuildCurrentObject(bucket, key); err != nil {
		return Object{}, false, err
	}
	return object, true, nil
}

func (s *FileBucketStore) ListObjects(ctx context.Context, bucket string, prefix string) ([]Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	entries, err := os.ReadDir(s.objectsPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read objects: %w", err)
	}
	objects := make([]Object, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.objectsPath(bucket), entry.Name(), "object.json"))
		if err != nil {
			return nil, false, fmt.Errorf("read object metadata: %w", err)
		}
		var object Object
		if err := json.Unmarshal(data, &object); err != nil {
			return nil, false, fmt.Errorf("decode object metadata: %w", err)
		}
		if !object.DeleteMarker && strings.HasPrefix(object.Key, prefix) {
			objects = append(objects, object)
		}
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	return objects, true, nil
}

func (s *FileBucketStore) ListObjectVersions(ctx context.Context, bucket string, prefix string) ([]Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	entries, err := os.ReadDir(s.objectsPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read objects: %w", err)
	}
	versions := []Object{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		objectPath := filepath.Join(s.objectsPath(bucket), entry.Name())
		keyVersionsPath := filepath.Join(objectPath, "versions")
		versionEntries, err := os.ReadDir(keyVersionsPath)
		if err != nil {
			if os.IsNotExist(err) {
				object, ok, err := readObjectMetadataForVersionList(filepath.Join(objectPath, "object.json"), nullVersionID)
				if err != nil {
					return nil, false, err
				}
				if ok && strings.HasPrefix(object.Key, prefix) {
					versions = append(versions, object)
				}
				continue
			}
			return nil, false, fmt.Errorf("read object versions: %w", err)
		}
		for _, versionEntry := range versionEntries {
			if !versionEntry.IsDir() {
				continue
			}
			var object Object
			if err := readJSONFile(filepath.Join(keyVersionsPath, versionEntry.Name(), "object.json"), &object); err != nil {
				return nil, false, fmt.Errorf("read object version metadata: %w", err)
			}
			if strings.HasPrefix(object.Key, prefix) {
				versions = append(versions, object)
			}
		}
		if _, err := os.Stat(filepath.Join(keyVersionsPath, nullVersionID)); os.IsNotExist(err) {
			object, ok, err := readObjectMetadataForVersionList(filepath.Join(objectPath, "object.json"), nullVersionID)
			if err != nil {
				return nil, false, err
			}
			if ok && objectVersionID(object) == nullVersionID && strings.HasPrefix(object.Key, prefix) {
				versions = append(versions, object)
			}
		}
	}
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key == versions[j].Key {
			return versions[i].LastModified.After(versions[j].LastModified)
		}
		return versions[i].Key < versions[j].Key
	})
	return versions, true, nil
}

func (s *FileBucketStore) updateObjectLockMetadata(ctx context.Context, bucket string, key string, versionID string, update func(*Object)) (Object, bool, error) {
	object, body, ok, err := s.GetObjectVersion(ctx, bucket, key, versionID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	update(&object)
	if object.DeleteMarker {
		body = nil
	}
	if versionID != "" {
		versionPath := filepath.Join(s.objectVersionsPath(bucket, key), versionID)
		if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
			return Object{}, true, err
		}
	} else if err := writeObjectMetadata(filepath.Join(s.objectPath(bucket, key), "object.json"), object); err != nil {
		return Object{}, true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(s.objectPath(bucket, key), object, body); err != nil {
			return Object{}, true, err
		}
	}
	return object, true, nil
}

func (s *FileBucketStore) readCurrentObjectMetadata(bucket string, key string) (Object, bool, error) {
	var object Object
	if err := readJSONFile(filepath.Join(s.objectPath(bucket, key), "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	return object, true, nil
}

func (s *FileBucketStore) writeObjectVersion(objectPath string, object Object, body []byte) error {
	if object.VersionID == "" {
		return nil
	}
	versionPath := filepath.Join(objectPath, "versions", object.VersionID)
	if err := os.MkdirAll(versionPath, 0o755); err != nil {
		return fmt.Errorf("create object version directory: %w", err)
	}
	if err := writeObjectMetadata(filepath.Join(versionPath, "object.json"), object); err != nil {
		return err
	}
	if object.DeleteMarker {
		_ = os.Remove(filepath.Join(versionPath, "body"))
		return nil
	}
	if err := os.WriteFile(filepath.Join(versionPath, "body"), body, 0o644); err != nil {
		return fmt.Errorf("write object version body: %w", err)
	}
	return nil
}

func (s *FileBucketStore) getNullObjectVersion(bucket string, key string) (Object, []byte, bool, error) {
	versionPath := filepath.Join(s.objectVersionsPath(bucket, key), nullVersionID)
	var object Object
	if err := readJSONFile(filepath.Join(versionPath, "object.json"), &object); err == nil {
		if object.DeleteMarker {
			return object, nil, true, nil
		}
		body, err := os.ReadFile(filepath.Join(versionPath, "body"))
		if err != nil {
			return Object{}, nil, false, fmt.Errorf("read null object version body: %w", err)
		}
		return object, body, true, nil
	} else if !os.IsNotExist(err) {
		return Object{}, nil, false, fmt.Errorf("read null object version metadata: %w", err)
	}

	currentPath := s.objectPath(bucket, key)
	if err := readJSONFile(filepath.Join(currentPath, "object.json"), &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, false, nil
		}
		return Object{}, nil, false, fmt.Errorf("read current object metadata: %w", err)
	}
	if object.VersionID != "" && object.VersionID != nullVersionID {
		return Object{}, nil, false, nil
	}
	object.VersionID = nullVersionID
	if object.DeleteMarker {
		return object, nil, true, nil
	}
	body, err := os.ReadFile(filepath.Join(currentPath, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read current object body: %w", err)
	}
	return object, body, true, nil
}

func readObjectMetadataForVersionList(path string, defaultVersionID string) (Object, bool, error) {
	var object Object
	if err := readJSONFile(path, &object); err != nil {
		if os.IsNotExist(err) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("read object metadata: %w", err)
	}
	if object.VersionID == "" {
		object.VersionID = defaultVersionID
	}
	return object, true, nil
}

func (s *FileBucketStore) rebuildCurrentObject(bucket string, key string) error {
	path := s.objectPath(bucket, key)
	entries, err := os.ReadDir(filepath.Join(path, "versions"))
	if err != nil {
		if os.IsNotExist(err) {
			return os.RemoveAll(path)
		}
		return fmt.Errorf("read object versions: %w", err)
	}
	var latest Object
	var latestBody []byte
	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		versionPath := filepath.Join(path, "versions", entry.Name())
		var object Object
		if err := readJSONFile(filepath.Join(versionPath, "object.json"), &object); err != nil {
			return err
		}
		if found && !object.LastModified.After(latest.LastModified) {
			continue
		}
		latest = object
		found = true
		if object.DeleteMarker {
			latestBody = nil
			continue
		}
		body, err := os.ReadFile(filepath.Join(versionPath, "body"))
		if err != nil {
			return fmt.Errorf("read object version body: %w", err)
		}
		latestBody = body
	}
	if !found {
		return os.RemoveAll(path)
	}
	if err := writeObjectMetadata(filepath.Join(path, "object.json"), latest); err != nil {
		return err
	}
	bodyPath := filepath.Join(path, "body")
	if latest.DeleteMarker {
		_ = os.Remove(bodyPath)
		return nil
	}
	return os.WriteFile(bodyPath, latestBody, 0o644)
}
