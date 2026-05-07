package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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
		Encryption:         cleanServerSideEncryption(input.Encryption),
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
		Encryption:         upload.Encryption,
	})
	if err != nil {
		return Object{}, true, err
	}
	object.ETag = multipartETag(partETags)
	if err := writeObjectMetadata(filepath.Join(s.objectPath(upload.Bucket, upload.Key), "object.json"), object); err != nil {
		return Object{}, true, err
	}
	if object.VersionID != "" {
		if err := s.writeObjectVersion(s.objectPath(upload.Bucket, upload.Key), object, combined.Bytes()); err != nil {
			return Object{}, true, err
		}
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
