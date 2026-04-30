package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sigV4Algorithm = "AWS4-HMAC-SHA256"
	sigV4Service   = "s3"
)

var nowUTC = func() time.Time {
	return time.Now().UTC()
}

type signatureError struct {
	code   string
	status int
}

func (e signatureError) Error() string {
	return e.code
}

func (s *Server) verifySignature(r *http.Request) error {
	hasPresign := r.URL.Query().Get("X-Amz-Algorithm") != ""
	hasHeaderAuth := r.Header.Get("Authorization") != ""
	if !hasPresign && !hasHeaderAuth {
		if strings.EqualFold(s.config.AuthMode, "strict") {
			return signatureError{code: "AccessDenied", status: http.StatusForbidden}
		}
		return nil
	}
	if hasPresign {
		return s.verifyPresignedURL(r)
	}
	return s.verifyAuthorizationHeader(r)
}

func (s *Server) verifyPresignedURL(r *http.Request) error {
	query := r.URL.Query()
	if query.Get("X-Amz-Algorithm") != sigV4Algorithm {
		return signatureError{code: "InvalidArgument", status: http.StatusBadRequest}
	}
	credential := query.Get("X-Amz-Credential")
	accessKey, dateStamp, region, service, ok := parseCredentialScope(credential)
	if !ok {
		return signatureError{code: "AuthorizationHeaderMalformed", status: http.StatusBadRequest}
	}
	if !s.validCredential(accessKey, region, service) {
		return signatureError{code: "InvalidAccessKeyId", status: http.StatusForbidden}
	}
	amzDate, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	if err != nil {
		return signatureError{code: "AccessDenied", status: http.StatusForbidden}
	}
	expires, err := strconv.ParseInt(query.Get("X-Amz-Expires"), 10, 64)
	if err != nil || expires < 0 || expires > 604800 {
		return signatureError{code: "AccessDenied", status: http.StatusForbidden}
	}
	if nowUTC().After(amzDate.Add(time.Duration(expires) * time.Second)) {
		return signatureError{code: "AccessDenied", status: http.StatusForbidden}
	}

	signedHeaders := query.Get("X-Amz-SignedHeaders")
	if signedHeaders == "" {
		return signatureError{code: "AuthorizationHeaderMalformed", status: http.StatusBadRequest}
	}
	expectedSignature := s.signatureForRequest(r, dateStamp, region, signedHeaders, "UNSIGNED-PAYLOAD", "X-Amz-Signature")
	if !hmac.Equal([]byte(query.Get("X-Amz-Signature")), []byte(expectedSignature)) {
		return signatureError{code: "SignatureDoesNotMatch", status: http.StatusForbidden}
	}
	return nil
}

func (s *Server) verifyAuthorizationHeader(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, sigV4Algorithm+" ") {
		return signatureError{code: "AuthorizationHeaderMalformed", status: http.StatusBadRequest}
	}
	values := parseAuthParams(strings.TrimPrefix(auth, sigV4Algorithm+" "))
	credential := values["Credential"]
	signedHeaders := values["SignedHeaders"]
	signature := values["Signature"]
	accessKey, dateStamp, region, service, ok := parseCredentialScope(credential)
	if !ok || signedHeaders == "" || signature == "" {
		return signatureError{code: "AuthorizationHeaderMalformed", status: http.StatusBadRequest}
	}
	if !s.validCredential(accessKey, region, service) {
		return signatureError{code: "InvalidAccessKeyId", status: http.StatusForbidden}
	}
	if r.Header.Get("x-amz-date") == "" {
		return signatureError{code: "AuthorizationHeaderMalformed", status: http.StatusBadRequest}
	}
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	} else if err := verifyPayloadHash(r, payloadHash); err != nil {
		return err
	}
	expectedSignature := s.signatureForRequest(r, dateStamp, region, signedHeaders, payloadHash, "")
	if !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		return signatureError{code: "SignatureDoesNotMatch", status: http.StatusForbidden}
	}
	return nil
}

func (s *Server) validCredential(accessKey string, region string, service string) bool {
	configuredAccessKey := s.config.AccessKeyID
	if configuredAccessKey == "" {
		configuredAccessKey = "dev"
	}
	configuredRegion := s.config.Region
	if configuredRegion == "" {
		configuredRegion = "us-east-1"
	}
	return accessKey == configuredAccessKey && region == configuredRegion && service == sigV4Service
}

func verifyPayloadHash(r *http.Request, payloadHash string) error {
	if payloadHash == "UNSIGNED-PAYLOAD" {
		return nil
	}
	if strings.HasPrefix(payloadHash, "STREAMING-") {
		return signatureError{code: "NotImplemented", status: http.StatusNotImplemented}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return signatureError{code: "InvalidRequest", status: http.StatusBadRequest}
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	if got := sha256Hex(body); !hmac.Equal([]byte(strings.ToLower(payloadHash)), []byte(got)) {
		return signatureError{code: "XAmzContentSHA256Mismatch", status: http.StatusBadRequest}
	}
	return nil
}

func (s *Server) signatureForRequest(r *http.Request, dateStamp string, region string, signedHeaders string, payloadHash string, ignoredQueryKey string) string {
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.Path),
		canonicalQueryString(r.URL.Query(), ignoredQueryKey),
		canonicalHeaders(r, signedHeaders),
		strings.ToLower(signedHeaders),
		payloadHash,
	}, "\n")
	amzDate := r.URL.Query().Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("x-amz-date")
	}
	scope := strings.Join([]string{dateStamp, region, sigV4Service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hmacSHA256(deriveSigningKey(s.config.SecretAccessKey, dateStamp, region), stringToSign)
	return hex.EncodeToString(signature)
}

func parseCredentialScope(credential string) (accessKey string, dateStamp string, region string, service string, ok bool) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func parseAuthParams(value string) map[string]string {
	result := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			result[key] = val
		}
	}
	return result
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return awsPercentEncode(path, "/~")
}

func canonicalQueryString(values url.Values, ignoredKey string) string {
	type pair struct {
		key   string
		value string
	}
	var pairs []pair
	for key, vals := range values {
		if key == ignoredKey {
			continue
		}
		if len(vals) == 0 {
			pairs = append(pairs, pair{key: key})
			continue
		}
		for _, val := range vals {
			pairs = append(pairs, pair{key: key, value: val})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	encoded := make([]string, 0, len(pairs))
	for _, item := range pairs {
		encoded = append(encoded, awsPercentEncode(item.key, "~-_")+"="+awsPercentEncode(item.value, "~-_"))
	}
	return strings.Join(encoded, "&")
}

func canonicalHeaders(r *http.Request, signedHeaders string) string {
	var b strings.Builder
	for _, name := range strings.Split(strings.ToLower(signedHeaders), ";") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		fmt.Fprintf(&b, "%s:%s\n", name, normalizeHeaderValue(value))
	}
	return b.String()
}

func normalizeHeaderValue(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func awsPercentEncode(value string, safe string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' || strings.ContainsRune(safe, rune(c)) {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func deriveSigningKey(secret string, dateStamp string, region string) []byte {
	if secret == "" {
		secret = "dev"
	}
	dateKey := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, sigV4Service)
	return hmacSHA256(serviceKey, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(value))
	return mac.Sum(nil)
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func writeSignatureError(w http.ResponseWriter, err error) {
	if sigErr, ok := err.(signatureError); ok {
		writeXMLError(w, sigErr.code, sigErr.code, sigErr.status)
		return
	}
	writeXMLError(w, "AccessDenied", "AccessDenied", http.StatusForbidden)
}
