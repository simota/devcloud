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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Bucket struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

type Object struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	ETag               string            `json:"etag"`
	Size               int64             `json:"size"`
	CreatedAt          time.Time         `json:"createdAt,omitempty"`
	LastModified       time.Time         `json:"lastModified"`
	UpdatedAt          time.Time         `json:"updatedAt,omitempty"`
	Metageneration     int64             `json:"metageneration,omitempty"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	CRC32C             string            `json:"crc32c,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type MultipartUpload struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	UploadID           string            `json:"uploadId"`
	CreatedAt          time.Time         `json:"createdAt"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type MultipartPart struct {
	PartNumber   int       `json:"partNumber"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
}

type PutObjectInput struct {
	Bucket             string
	Key                string
	Body               io.Reader
	ContentMD5         string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
}

type UpdateObjectMetadataInput struct {
	Bucket             string
	Key                string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
}

type CreateMultipartUploadInput struct {
	Bucket             string
	Key                string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
}

type BucketStore interface {
	CreateBucket(ctx context.Context, name string) (Bucket, bool, error)
	GetBucket(ctx context.Context, name string) (Bucket, bool, error)
	ListBuckets(ctx context.Context) ([]Bucket, error)
	DeleteBucket(ctx context.Context, name string) (bool, error)
	PutObject(ctx context.Context, input PutObjectInput) (Object, error)
	UpdateObjectMetadata(ctx context.Context, input UpdateObjectMetadataInput) (Object, bool, error)
	GetObject(ctx context.Context, bucket string, key string) (Object, []byte, bool, error)
	DeleteObject(ctx context.Context, bucket string, key string) (bool, error)
	ListObjects(ctx context.Context, bucket string, prefix string) ([]Object, bool, error)
	CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (MultipartUpload, error)
	UploadPart(ctx context.Context, bucket string, key string, uploadID string, partNumber int, body io.Reader, contentMD5 string) (MultipartPart, error)
	ListParts(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, []MultipartPart, bool, error)
	CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, partNumbers []int) (Object, bool, error)
	AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (bool, error)
	ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, bool, error)
}

type FileBucketStore struct {
	root string
}

func NewFileBucketStore(root string) *FileBucketStore {
	return &FileBucketStore{root: root}
}

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
		if entry.Name() != "bucket.json" {
			return false, fmt.Errorf("bucket is not empty")
		}
	}
	if err := os.Remove(filepath.Join(path, "bucket.json")); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("delete bucket metadata: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return false, fmt.Errorf("delete bucket directory: %w", err)
	}
	return true, nil
}

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
	if _, ok, err := s.GetBucket(ctx, input.Bucket); err != nil {
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
	body, err := os.ReadFile(filepath.Join(path, "body"))
	if err != nil {
		return Object{}, nil, false, fmt.Errorf("read object body: %w", err)
	}
	return object, body, true, nil
}

func (s *FileBucketStore) DeleteObject(ctx context.Context, bucket string, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateBucketName(bucket); err != nil {
		return false, err
	}
	if err := validateObjectKey(key); err != nil {
		return false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("bucket does not exist")
	}

	objectsPath := s.objectsPath(bucket)
	path := s.objectPath(bucket, key)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat object: %w", err)
	}
	if err := os.RemoveAll(path); err != nil {
		return false, fmt.Errorf("delete object: %w", err)
	}
	_ = os.Remove(objectsPath)
	return true, nil
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
		if strings.HasPrefix(object.Key, prefix) {
			objects = append(objects, object)
		}
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	return objects, true, nil
}

func (s *FileBucketStore) CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (MultipartUpload, error) {
	if err := ctx.Err(); err != nil {
		return MultipartUpload{}, err
	}
	if err := s.requireBucketAndKey(ctx, input.Bucket, input.Key); err != nil {
		return MultipartUpload{}, err
	}
	upload := MultipartUpload{
		Bucket:             input.Bucket,
		Key:                input.Key,
		UploadID:           newUploadID(),
		CreatedAt:          time.Now().UTC(),
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		CacheControl:       input.CacheControl,
		ContentDisposition: input.ContentDisposition,
		Metadata:           cleanMetadata(input.Metadata),
	}
	if upload.ContentType == "" {
		upload.ContentType = "application/octet-stream"
	}
	path := s.multipartUploadPath(input.Bucket, upload.UploadID)
	if err := os.MkdirAll(filepath.Join(path, "parts"), 0o755); err != nil {
		return MultipartUpload{}, fmt.Errorf("create multipart upload: %w", err)
	}
	if err := writeJSONFile(filepath.Join(path, "upload.json"), upload); err != nil {
		return MultipartUpload{}, err
	}
	return upload, nil
}

func (s *FileBucketStore) UploadPart(ctx context.Context, bucket string, key string, uploadID string, partNumber int, body io.Reader, contentMD5 string) (MultipartPart, error) {
	if err := ctx.Err(); err != nil {
		return MultipartPart{}, err
	}
	if partNumber < 1 || partNumber > 10000 {
		return MultipartPart{}, fmt.Errorf("invalid part number")
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		return MultipartPart{}, err
	}
	if !ok || upload.Key != key {
		return MultipartPart{}, fmt.Errorf("multipart upload does not exist")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return MultipartPart{}, fmt.Errorf("read multipart part: %w", err)
	}
	if err := validateContentMD5(contentMD5, data); err != nil {
		return MultipartPart{}, err
	}
	sum := md5.Sum(data)
	part := MultipartPart{
		PartNumber:   partNumber,
		ETag:         `"` + hex.EncodeToString(sum[:]) + `"`,
		Size:         int64(len(data)),
		LastModified: time.Now().UTC(),
	}
	path := s.multipartPartPath(bucket, uploadID, partNumber)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return MultipartPart{}, fmt.Errorf("create multipart part: %w", err)
	}
	if err := os.WriteFile(filepath.Join(path, "body"), data, 0o644); err != nil {
		return MultipartPart{}, fmt.Errorf("write multipart part body: %w", err)
	}
	if err := writeJSONFile(filepath.Join(path, "part.json"), part); err != nil {
		return MultipartPart{}, err
	}
	return part, nil
}

func (s *FileBucketStore) ListParts(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, []MultipartPart, bool, error) {
	if err := ctx.Err(); err != nil {
		return MultipartUpload{}, nil, false, err
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return upload, nil, ok, err
	}
	parts, err := s.readMultipartParts(bucket, uploadID)
	if err != nil {
		return MultipartUpload{}, nil, false, err
	}
	return upload, parts, true, nil
}

func (s *FileBucketStore) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadID string, partNumbers []int) (Object, bool, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, false, err
	}
	upload, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return Object{}, ok, err
	}
	var combined bytes.Buffer
	partETags := make([]string, 0, len(partNumbers))
	for _, partNumber := range partNumbers {
		var part MultipartPart
		if err := readJSONFile(filepath.Join(s.multipartPartPath(bucket, uploadID, partNumber), "part.json"), &part); err != nil {
			if os.IsNotExist(err) {
				return Object{}, true, fmt.Errorf("multipart part %d does not exist", partNumber)
			}
			return Object{}, true, fmt.Errorf("read multipart part metadata: %w", err)
		}
		body, err := os.ReadFile(filepath.Join(s.multipartPartPath(bucket, uploadID, partNumber), "body"))
		if err != nil {
			if os.IsNotExist(err) {
				return Object{}, true, fmt.Errorf("multipart part %d does not exist", partNumber)
			}
			return Object{}, true, fmt.Errorf("read multipart part: %w", err)
		}
		combined.Write(body)
		partETags = append(partETags, part.ETag)
	}
	object, err := s.PutObject(ctx, PutObjectInput{
		Bucket:             upload.Bucket,
		Key:                upload.Key,
		Body:               bytes.NewReader(combined.Bytes()),
		ContentType:        upload.ContentType,
		ContentEncoding:    upload.ContentEncoding,
		CacheControl:       upload.CacheControl,
		ContentDisposition: upload.ContentDisposition,
		Metadata:           upload.Metadata,
	})
	if err != nil {
		return Object{}, true, err
	}
	object.ETag = multipartETag(partETags)
	if err := writeObjectMetadata(filepath.Join(s.objectPath(upload.Bucket, upload.Key), "object.json"), object); err != nil {
		return Object{}, true, err
	}
	if err := os.RemoveAll(s.multipartUploadPath(bucket, uploadID)); err != nil {
		return Object{}, true, fmt.Errorf("delete completed multipart upload: %w", err)
	}
	_ = os.Remove(s.multipartPath(bucket))
	return object, true, nil
}

func (s *FileBucketStore) AbortMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, ok, err := s.getMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil || !ok {
		return ok, err
	}
	if err := os.RemoveAll(s.multipartUploadPath(bucket, uploadID)); err != nil {
		return true, fmt.Errorf("abort multipart upload: %w", err)
	}
	_ = os.Remove(s.multipartPath(bucket))
	return true, nil
}

func (s *FileBucketStore) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartUpload, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, ok, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}
	entries, err := os.ReadDir(s.multipartPath(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("read multipart uploads: %w", err)
	}
	uploads := make([]MultipartUpload, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var upload MultipartUpload
		if err := readJSONFile(filepath.Join(s.multipartPath(bucket), entry.Name(), "upload.json"), &upload); err != nil {
			return nil, false, err
		}
		uploads = append(uploads, upload)
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key == uploads[j].Key {
			return uploads[i].UploadID < uploads[j].UploadID
		}
		return uploads[i].Key < uploads[j].Key
	})
	return uploads, true, nil
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

func (s *FileBucketStore) getMultipartUpload(ctx context.Context, bucket string, key string, uploadID string) (MultipartUpload, bool, error) {
	if err := validateUploadID(uploadID); err != nil {
		return MultipartUpload{}, false, err
	}
	if err := s.requireBucketAndKey(ctx, bucket, key); err != nil {
		return MultipartUpload{}, false, err
	}
	var upload MultipartUpload
	err := readJSONFile(filepath.Join(s.multipartUploadPath(bucket, uploadID), "upload.json"), &upload)
	if err != nil {
		if os.IsNotExist(err) {
			return MultipartUpload{}, false, nil
		}
		return MultipartUpload{}, false, err
	}
	if upload.Key != key {
		return MultipartUpload{}, false, nil
	}
	return upload, true, nil
}

func (s *FileBucketStore) readMultipartParts(bucket string, uploadID string) ([]MultipartPart, error) {
	entries, err := os.ReadDir(filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read multipart parts: %w", err)
	}
	parts := make([]MultipartPart, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var part MultipartPart
		if err := readJSONFile(filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts", entry.Name(), "part.json"), &part); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
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

func (s *FileBucketStore) multipartPath(bucket string) string {
	return filepath.Join(s.bucketPath(bucket), "multipart")
}

func (s *FileBucketStore) multipartUploadPath(bucket string, uploadID string) string {
	return filepath.Join(s.multipartPath(bucket), uploadID)
}

func (s *FileBucketStore) multipartPartPath(bucket string, uploadID string, partNumber int) string {
	return filepath.Join(s.multipartUploadPath(bucket, uploadID), "parts", fmt.Sprintf("%05d", partNumber))
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

var (
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
