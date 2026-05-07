package gcs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestAuthModes(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())

	relaxed := NewServer(Config{AuthMode: "relaxed"}, store).routes()
	if rec := performRequest(relaxed, http.MethodGet, "/storage/v1/b?project=devcloud", ""); rec.Code != http.StatusOK {
		t.Fatalf("relaxed status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	oauthRelaxed := NewServer(Config{AuthMode: "oauth-relaxed"}, store).routes()
	if rec := performRequest(oauthRelaxed, http.MethodGet, "/storage/v1/b?project=devcloud", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("oauth-relaxed missing bearer status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if rec := performRequestWithHeaders(oauthRelaxed, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer local-token"}); rec.Code != http.StatusOK {
		t.Fatalf("oauth-relaxed bearer status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	bearerDev := NewServer(Config{AuthMode: "bearer-dev", BearerToken: "expected-token"}, store).routes()
	if rec := performRequestWithHeaders(bearerDev, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer wrong-token"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bearer-dev wrong token status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if rec := performRequestWithHeaders(bearerDev, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer expected-token"}); rec.Code != http.StatusOK {
		t.Fatalf("bearer-dev matching token status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func performRequest(handler http.Handler, method string, target string, body string) *httptest.ResponseRecorder {
	return performRequestWithHeaders(handler, method, target, body, nil)
}

func performRequestWithHeaders(handler http.Handler, method string, target string, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
