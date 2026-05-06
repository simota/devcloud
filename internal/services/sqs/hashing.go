package sqs

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"
)

func md5Hex(value string) string {
	sum := md5.Sum([]byte(value))
	return fmt.Sprintf("%x", sum)
}

func md5OfMessageAttributes(attrs map[string]messageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	var payload bytes.Buffer
	for _, name := range names {
		attr := attrs[name]
		writeMD5AttributeString(&payload, name)
		writeMD5AttributeString(&payload, attr.DataType)
		if strings.HasPrefix(strings.ToLower(attr.DataType), "binary") {
			payload.WriteByte(2)
			writeMD5AttributeBytes(&payload, decodeBinaryAttribute(attr.BinaryValue))
			continue
		}
		payload.WriteByte(1)
		writeMD5AttributeString(&payload, attr.StringValue)
	}
	sum := md5.Sum(payload.Bytes())
	return fmt.Sprintf("%x", sum)
}

func writeMD5AttributeString(buf *bytes.Buffer, value string) {
	writeMD5AttributeBytes(buf, []byte(value))
}

func writeMD5AttributeBytes(buf *bytes.Buffer, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	buf.Write(length[:])
	buf.Write(value)
}

func decodeBinaryAttribute(value string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err == nil {
		return decoded
	}
	return []byte(value)
}

func newOpaqueID(prefix string) string {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(raw[:])
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
