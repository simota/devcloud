package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func writeServerSideEncryptionHeaders(w http.ResponseWriter, encryption ServerSideEncryption) {
	if encryption.Algorithm == "" {
		return
	}
	w.Header().Set("x-amz-server-side-encryption", encryption.Algorithm)
	if encryption.KMSKeyID != "" {
		w.Header().Set("x-amz-server-side-encryption-aws-kms-key-id", encryption.KMSKeyID)
	}
	if encryption.BucketKeyEnabled != nil {
		w.Header().Set("x-amz-server-side-encryption-bucket-key-enabled", strconv.FormatBool(*encryption.BucketKeyEnabled))
	}
}

func writeObjectLockHeaders(w http.ResponseWriter, object Object) {
	if object.Retention.Mode != "" {
		w.Header().Set("x-amz-object-lock-mode", object.Retention.Mode)
	}
	if object.Retention.RetainUntilDate != "" {
		w.Header().Set("x-amz-object-lock-retain-until-date", object.Retention.RetainUntilDate)
	}
	if object.LegalHold.Status != "" {
		w.Header().Set("x-amz-object-lock-legal-hold", object.LegalHold.Status)
	}
}

func writeServerSideEncryptionError(w http.ResponseWriter, err error) {
	if errors.Is(err, errUnsupportedSSECustomerKey) || errors.Is(err, errUnsupportedServerSideEncryption) {
		writeXMLError(w, "NotImplemented", "server-side encryption mode is not supported", http.StatusNotImplemented)
		return
	}
	writeXMLError(w, "InvalidArgument", "server-side encryption headers are invalid", http.StatusBadRequest)
}
func writeACL(w http.ResponseWriter, acl string) {
	if acl == "" {
		acl = "private"
	}
	writeXML(w, http.StatusOK, accessControlPolicy{
		XMLName: xml.Name{Local: "AccessControlPolicy"},
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: owner{
			ID:          "devcloud",
			DisplayName: "devcloud",
		},
		AccessControlList: accessControlList{
			Grants: []grant{grantForACL(acl)},
		},
		CannedACL: acl,
	})
}

func grantForACL(acl string) grant {
	permission := "FULL_CONTROL"
	if acl == "public-read" || acl == "authenticated-read" || acl == "bucket-owner-read" {
		permission = "READ"
	}
	return grant{
		Grantee: grantee{
			XmlnsXSI:    "http://www.w3.org/2001/XMLSchema-instance",
			Type:        "CanonicalUser",
			ID:          "devcloud",
			DisplayName: "devcloud",
		},
		Permission: permission,
	}
}

func writeObjectHeaders(w http.ResponseWriter, object Object) {
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	writeServerSideEncryptionHeaders(w, object.Encryption)
	writeObjectLockHeaders(w, object)
	if object.VersionID != "" {
		w.Header().Set("x-amz-version-id", object.VersionID)
	}
	if object.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", object.ContentEncoding)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	for key, value := range object.Metadata {
		w.Header().Set("x-amz-meta-"+key, value)
	}
}

func parseRange(header string, size int64) (start int64, end int64, partial bool, err error) {
	if header == "" {
		if size == 0 {
			return 0, -1, false, nil
		}
		return 0, size - 1, false, nil
	}
	if size == 0 {
		return 0, 0, false, fmt.Errorf("empty object has no satisfiable range")
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	left, right, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, fmt.Errorf("invalid range")
	}
	if left == "" {
		suffix, err := strconv.ParseInt(right, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, nil
	}
	start, err = strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, fmt.Errorf("invalid range start")
	}
	if right == "" {
		return start, size - 1, true, nil
	}
	end, err = strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range end")
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true, nil
}
func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeXMLError(w, "MethodNotAllowed", "method not allowed", http.StatusMethodNotAllowed)
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	xml.NewEncoder(w).Encode(value)
}

func writeXMLError(w http.ResponseWriter, code string, message string, status int) {
	writeXML(w, status, errorResponse{
		XMLName: xml.Name{Local: "Error"},
		Code:    code,
		Message: message,
	})
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type accessControlPolicy struct {
	XMLName           xml.Name          `xml:"AccessControlPolicy"`
	Xmlns             string            `xml:"xmlns,attr,omitempty"`
	Owner             owner             `xml:"Owner"`
	AccessControlList accessControlList `xml:"AccessControlList"`
	CannedACL         string            `xml:"CannedACL,omitempty"`
}

type accessControlList struct {
	Grants []grant `xml:"Grant"`
}

type grant struct {
	Grantee    grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type grantee struct {
	XmlnsXSI    string `xml:"xmlns:xsi,attr,omitempty"`
	Type        string `xml:"xsi:type,attr,omitempty"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

type buckets struct {
	Bucket []bucketElement `xml:"Bucket"`
}

type bucketElement struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type errorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type listInventoryConfigurationsResult struct {
	XMLName                 xml.Name                 `xml:"ListInventoryConfigurationsResult"`
	Xmlns                   string                   `xml:"xmlns,attr"`
	ContinuationToken       string                   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken   string                   `xml:"NextContinuationToken,omitempty"`
	IsTruncated             bool                     `xml:"IsTruncated"`
	InventoryConfigurations []InventoryConfiguration `xml:"InventoryConfiguration"`
}

type listAnalyticsConfigurationsResult struct {
	XMLName                 xml.Name                 `xml:"ListAnalyticsConfigurationsResult"`
	Xmlns                   string                   `xml:"xmlns,attr"`
	ContinuationToken       string                   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken   string                   `xml:"NextContinuationToken,omitempty"`
	IsTruncated             bool                     `xml:"IsTruncated"`
	AnalyticsConfigurations []AnalyticsConfiguration `xml:"AnalyticsConfiguration"`
}

type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status,omitempty"`
}
type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}
