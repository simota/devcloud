package s3

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

type objectListing struct {
	contents              []Object
	commonPrefixes        []string
	truncated             bool
	nextMarker            string
	nextContinuationToken string
}

type versionListing struct {
	versions            []Object
	truncated           bool
	nextKeyMarker       string
	nextVersionIDMarker string
}

func latestObjectVersionIDs(versions []Object) map[string]string {
	latestByKey := make(map[string]string)
	for _, object := range versions {
		if _, ok := latestByKey[object.Key]; ok {
			continue
		}
		latestByKey[object.Key] = objectVersionID(object)
	}
	return latestByKey
}

func objectVersionID(object Object) string {
	if object.VersionID == "" {
		return nullVersionID
	}
	return object.VersionID
}

func buildVersionListing(versions []Object, keyMarker string, versionIDMarker string, maxKeys int) versionListing {
	listing := versionListing{}
	if maxKeys == 0 {
		return listing
	}
	started := keyMarker == ""
	for _, object := range versions {
		versionID := objectVersionID(object)
		if !started {
			switch {
			case object.Key < keyMarker:
				continue
			case object.Key > keyMarker:
				started = true
			case versionIDMarker == "":
				continue
			case versionID == versionIDMarker:
				started = true
				continue
			default:
				continue
			}
		}
		if len(listing.versions) >= maxKeys {
			listing.truncated = true
			last := listing.versions[len(listing.versions)-1]
			listing.nextKeyMarker = last.Key
			listing.nextVersionIDMarker = objectVersionID(last)
			return listing
		}
		listing.versions = append(listing.versions, object)
	}
	return listing
}

func buildObjectListing(objects []Object, prefix string, delimiter string, marker string, maxKeys int) objectListing {
	listing := objectListing{}
	if maxKeys == 0 {
		return listing
	}
	commonPrefixes := map[string]bool{}
	count := 0
	for i := 0; i < len(objects); i++ {
		object := objects[i]
		if marker != "" && object.Key <= marker {
			continue
		}

		itemKey := object.Key
		itemIsObject := true
		lastKeyForItem := object.Key
		if delimiter != "" {
			remainder := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				itemKey = prefix + remainder[:index+len(delimiter)]
				itemIsObject = false
				for i+1 < len(objects) && strings.HasPrefix(objects[i+1].Key, itemKey) {
					i++
					lastKeyForItem = objects[i].Key
				}
				if commonPrefixes[itemKey] {
					continue
				}
			}
		}

		if count >= maxKeys {
			listing.truncated = true
			listing.nextMarker = marker
			listing.nextContinuationToken = encodeContinuationToken(marker)
			if listing.nextMarker == "" {
				listing.nextMarker = object.Key
				listing.nextContinuationToken = encodeContinuationToken(object.Key)
			}
			return listing
		}

		if itemIsObject {
			listing.contents = append(listing.contents, object)
		} else {
			commonPrefixes[itemKey] = true
			listing.commonPrefixes = append(listing.commonPrefixes, itemKey)
		}
		count++
		marker = lastKeyForItem
	}
	return listing
}

func parseMaxKeys(value string) (int, error) {
	if value == "" {
		return 1000, nil
	}
	maxKeys, err := strconv.Atoi(value)
	if err != nil || maxKeys < 0 {
		return 0, fmt.Errorf("invalid max-keys")
	}
	if maxKeys > 1000 {
		return 1000, nil
	}
	return maxKeys, nil
}

type continuationToken struct {
	LastKey string `json:"lastKey"`
}

func encodeContinuationToken(lastKey string) string {
	data, err := json.Marshal(continuationToken{LastKey: lastKey})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeContinuationToken(value string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	var token continuationToken
	if err := json.Unmarshal(data, &token); err != nil {
		return "", err
	}
	return token.LastKey, nil
}

func encodeListValue(value string, encodingType string) string {
	if encodingType != "url" || value == "" {
		return value
	}
	return awsPercentEncode(value, "~-_.")
}

type listBucketResult struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	Xmlns                 string                `xml:"xmlns,attr"`
	Name                  string                `xml:"Name"`
	Prefix                string                `xml:"Prefix"`
	Delimiter             string                `xml:"Delimiter,omitempty"`
	Marker                string                `xml:"Marker,omitempty"`
	NextMarker            string                `xml:"NextMarker,omitempty"`
	ContinuationToken     string                `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string                `xml:"NextContinuationToken,omitempty"`
	StartAfter            string                `xml:"StartAfter,omitempty"`
	KeyCount              int                   `xml:"KeyCount"`
	MaxKeys               int                   `xml:"MaxKeys"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	ListType              int                   `xml:"ListType,omitempty"`
	Contents              []objectElement       `xml:"Contents"`
	CommonPrefixes        []commonPrefixElement `xml:"CommonPrefixes"`
}

type objectElement struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixElement struct {
	Prefix string `xml:"Prefix"`
}

type listVersionsResult struct {
	XMLName             xml.Name              `xml:"ListVersionsResult"`
	Xmlns               string                `xml:"xmlns,attr"`
	Name                string                `xml:"Name"`
	Prefix              string                `xml:"Prefix"`
	KeyMarker           string                `xml:"KeyMarker,omitempty"`
	VersionIDMarker     string                `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string                `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string                `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int                   `xml:"MaxKeys"`
	IsTruncated         bool                  `xml:"IsTruncated"`
	Versions            []versionElement      `xml:"Version"`
	DeleteMarkers       []deleteMarkerElement `xml:"DeleteMarker"`
}

type versionElement struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type deleteMarkerElement struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}
