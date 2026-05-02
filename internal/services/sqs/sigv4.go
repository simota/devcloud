package sqs

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const (
	sigV4Algorithm = "AWS4-HMAC-SHA256"
	sigV4Service   = "sqs"
)

type signatureError struct {
	code   string
	status int
}

func (e signatureError) Error() string {
	return e.code
}

func (s *Server) verifySignature(r *http.Request) error {
	if s.config.AuthMode == "" || strings.EqualFold(s.config.AuthMode, "relaxed") {
		return nil
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" {
		return signatureError{code: "AccessDenied", status: http.StatusForbidden}
	}
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
	expected := s.signatureForRequest(r, dateStamp, region, signedHeaders, payloadHash)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return signatureError{code: "SignatureDoesNotMatch", status: http.StatusForbidden}
	}
	return nil
}

func signatureErrorDetails(err error) (string, int) {
	if sigErr, ok := err.(signatureError); ok {
		return sigErr.code, sigErr.status
	}
	return "AccessDenied", http.StatusForbidden
}

func (s *Server) validCredential(accessKey string, region string, service string) bool {
	return accessKey == defaultString(s.config.AccessKeyID, "dev") &&
		region == defaultString(s.config.Region, "us-east-1") &&
		service == sigV4Service
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
		return signatureError{code: "SignatureDoesNotMatch", status: http.StatusForbidden}
	}
	return nil
}

func (s *Server) signatureForRequest(r *http.Request, dateStamp string, region string, signedHeaders string, payloadHash string) string {
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.Path),
		canonicalQueryString(r.URL.Query()),
		canonicalHeaders(r, signedHeaders),
		strings.ToLower(signedHeaders),
		payloadHash,
	}, "\n")
	amzDate := r.Header.Get("x-amz-date")
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

func canonicalQueryString(values url.Values) string {
	type pair struct {
		key   string
		value string
	}
	pairs := []pair{}
	for key, vals := range values {
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
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(normalizeHeaderValue(value))
		b.WriteByte('\n')
	}
	return b.String()
}

func normalizeHeaderValue(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func awsPercentEncode(value string, safe string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~' || strings.ContainsRune(safe, rune(ch)) {
			b.WriteByte(ch)
			continue
		}
		b.WriteString("%")
		b.WriteString(strings.ToUpper(hex.EncodeToString([]byte{ch})))
	}
	return b.String()
}

func deriveSigningKey(secret string, dateStamp string, region string) []byte {
	secret = defaultString(secret, "dev")
	dateKey := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, sigV4Service)
	return hmacSHA256(serviceKey, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
