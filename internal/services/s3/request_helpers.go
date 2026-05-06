package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func parseCopySource(source string) (bucket string, key string, versionID string, err error) {
	source = strings.TrimPrefix(source, "/")
	sourcePath, rawQuery, _ := strings.Cut(source, "?")
	source = sourcePath
	if source == "" {
		return "", "", "", fmt.Errorf("copy source is empty")
	}
	bucket, key, ok := strings.Cut(source, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", "", fmt.Errorf("copy source must include bucket and key")
	}
	decodedBucket, err := url.PathUnescape(bucket)
	if err != nil {
		return "", "", "", err
	}
	decodedKey, err := url.PathUnescape(key)
	if err != nil {
		return "", "", "", err
	}
	if rawQuery != "" {
		values, err := url.ParseQuery(rawQuery)
		if err != nil {
			return "", "", "", err
		}
		versionID = values.Get("versionId")
	}
	return decodedBucket, decodedKey, versionID, nil
}

func userMetadataFromHeaders(header http.Header) map[string]string {
	metadata := map[string]string{}
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-amz-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func serverSideEncryptionFromHeaders(header http.Header) (ServerSideEncryption, error) {
	if header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		header.Get("x-amz-server-side-encryption-customer-key-MD5") != "" {
		return ServerSideEncryption{}, errUnsupportedSSECustomerKey
	}
	algorithm := strings.TrimSpace(header.Get("x-amz-server-side-encryption"))
	kmsKeyID := strings.TrimSpace(header.Get("x-amz-server-side-encryption-aws-kms-key-id"))
	bucketKeyValue := strings.TrimSpace(header.Get("x-amz-server-side-encryption-bucket-key-enabled"))
	if algorithm == "" {
		if kmsKeyID != "" || bucketKeyValue != "" {
			return ServerSideEncryption{}, errInvalidServerSideEncryption
		}
		return ServerSideEncryption{}, nil
	}
	encryption := ServerSideEncryption{Algorithm: algorithm}
	switch algorithm {
	case "AES256":
		if kmsKeyID != "" || bucketKeyValue != "" {
			return ServerSideEncryption{}, errInvalidServerSideEncryption
		}
	case "aws:kms":
		encryption.KMSKeyID = kmsKeyID
		if bucketKeyValue != "" {
			enabled, err := strconv.ParseBool(bucketKeyValue)
			if err != nil {
				return ServerSideEncryption{}, errInvalidServerSideEncryption
			}
			encryption.BucketKeyEnabled = &enabled
		}
	default:
		return ServerSideEncryption{}, errUnsupportedServerSideEncryption
	}
	return encryption, nil
}

func hasServerSideEncryptionHeaders(header http.Header) bool {
	for _, key := range []string{
		"x-amz-server-side-encryption",
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-bucket-key-enabled",
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-customer-key",
		"x-amz-server-side-encryption-customer-key-MD5",
	} {
		if header.Get(key) != "" {
			return true
		}
	}
	return false
}

func objectLockFromHeaders(header http.Header) (ObjectRetention, ObjectLegalHold, error) {
	retention := ObjectRetention{
		Mode:            strings.TrimSpace(header.Get("x-amz-object-lock-mode")),
		RetainUntilDate: strings.TrimSpace(header.Get("x-amz-object-lock-retain-until-date")),
	}
	legalHold := ObjectLegalHold{Status: strings.TrimSpace(header.Get("x-amz-object-lock-legal-hold"))}
	if retention.Mode != "" || retention.RetainUntilDate != "" {
		if err := validateObjectRetention(retention); err != nil {
			return ObjectRetention{}, ObjectLegalHold{}, err
		}
	}
	if legalHold.Status != "" {
		if err := validateObjectLegalHold(legalHold); err != nil {
			return ObjectRetention{}, ObjectLegalHold{}, err
		}
	}
	return retention, legalHold, nil
}

func hasObjectLockHeaders(header http.Header) bool {
	for _, key := range []string{
		"x-amz-object-lock-mode",
		"x-amz-object-lock-retain-until-date",
		"x-amz-object-lock-legal-hold",
	} {
		if header.Get(key) != "" {
			return true
		}
	}
	return false
}

func bypassGovernanceRetentionFromHeaders(header http.Header) (bool, error) {
	value := strings.TrimSpace(header.Get("x-amz-bypass-governance-retention"))
	if value == "" {
		return false, nil
	}
	return strconv.ParseBool(value)
}
func readRequestBody(r *http.Request, limit int64) ([]byte, error) {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("request body exceeds limit")
	}
	return data, nil
}
func aclFromRequest(r *http.Request) (string, error) {
	if canned := strings.TrimSpace(r.Header.Get("x-amz-acl")); canned != "" {
		if !isSupportedCannedACL(canned) {
			return "", fmt.Errorf("unsupported canned acl")
		}
		return canned, nil
	}
	body, err := readRequestBody(r, 64<<10)
	if err != nil {
		return "", err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return "private", nil
	}
	var parsed accessControlPolicy
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	if parsed.CannedACL != "" {
		if !isSupportedCannedACL(parsed.CannedACL) {
			return "", fmt.Errorf("unsupported canned acl")
		}
		return parsed.CannedACL, nil
	}
	return "custom", nil
}
func isSupportedCannedACL(acl string) bool {
	switch acl {
	case "private", "public-read", "public-read-write", "authenticated-read", "bucket-owner-read", "bucket-owner-full-control":
		return true
	default:
		return false
	}
}
