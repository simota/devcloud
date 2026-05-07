package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObjectLockConfigurationRetentionAndLegalHold(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	config := `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>`
	putConfig := performRequest(routes, http.MethodPut, "/demo-bucket?object-lock", strings.NewReader(config))
	if putConfig.Code != http.StatusOK {
		t.Fatalf("put object lock config status = %d, want %d; body=%s", putConfig.Code, http.StatusOK, putConfig.Body.String())
	}
	getConfig := performRequest(routes, http.MethodGet, "/demo-bucket?object-lock", nil)
	if getConfig.Code != http.StatusOK {
		t.Fatalf("get object lock config status = %d, want %d; body=%s", getConfig.Code, http.StatusOK, getConfig.Body.String())
	}
	var parsedConfig ObjectLockConfiguration
	if err := xml.NewDecoder(getConfig.Body).Decode(&parsedConfig); err != nil {
		t.Fatalf("decode object lock config: %v", err)
	}
	if parsedConfig.ObjectLockEnabled != "Enabled" || parsedConfig.Rule.DefaultRetention.Mode != "GOVERNANCE" || parsedConfig.Rule.DefaultRetention.Days != 7 {
		t.Fatalf("object lock config = %#v", parsedConfig)
	}
	if putDefault := performRequest(routes, http.MethodPut, "/demo-bucket/docs/default-retention.txt", strings.NewReader("default")); putDefault.Code != http.StatusOK {
		t.Fatalf("put default retention object status = %d; body=%s", putDefault.Code, putDefault.Body.String())
	}
	defaultHead := performRequest(routes, http.MethodHead, "/demo-bucket/docs/default-retention.txt", nil)
	if got := defaultHead.Header().Get("x-amz-object-lock-mode"); got != "GOVERNANCE" {
		t.Fatalf("default retention mode = %q, want GOVERNANCE", got)
	}
	if got := defaultHead.Header().Get("x-amz-object-lock-retain-until-date"); got == "" {
		t.Fatal("default retention missing retain until date")
	}

	retainUntil := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/locked.txt", strings.NewReader("locked body"))
	putReq.Header.Set("x-amz-object-lock-mode", "COMPLIANCE")
	putReq.Header.Set("x-amz-object-lock-retain-until-date", retainUntil)
	putReq.Header.Set("x-amz-object-lock-legal-hold", "ON")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put locked object status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("x-amz-object-lock-mode"); got != "COMPLIANCE" {
		t.Fatalf("put object lock mode = %q, want COMPLIANCE", got)
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/locked.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head locked object status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("x-amz-object-lock-retain-until-date"); got != retainUntil {
		t.Fatalf("head retain until = %q, want %q", got, retainUntil)
	}
	if got := head.Header().Get("x-amz-object-lock-legal-hold"); got != "ON" {
		t.Fatalf("head legal hold = %q, want ON", got)
	}

	getRetention := performRequest(routes, http.MethodGet, "/demo-bucket/docs/locked.txt?retention", nil)
	if getRetention.Code != http.StatusOK {
		t.Fatalf("get retention status = %d, want %d; body=%s", getRetention.Code, http.StatusOK, getRetention.Body.String())
	}
	var retention ObjectRetention
	if err := xml.NewDecoder(getRetention.Body).Decode(&retention); err != nil {
		t.Fatalf("decode retention: %v", err)
	}
	if retention.Mode != "COMPLIANCE" || retention.RetainUntilDate != retainUntil {
		t.Fatalf("retention = %#v", retention)
	}

	turnOffLegalHold := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?legal-hold", strings.NewReader(`<LegalHold><Status>OFF</Status></LegalHold>`))
	if turnOffLegalHold.Code != http.StatusOK {
		t.Fatalf("put legal hold off status = %d, want %d; body=%s", turnOffLegalHold.Code, http.StatusOK, turnOffLegalHold.Body.String())
	}
	getLegalHold := performRequest(routes, http.MethodGet, "/demo-bucket/docs/locked.txt?legal-hold", nil)
	if getLegalHold.Code != http.StatusOK {
		t.Fatalf("get legal hold status = %d, want %d; body=%s", getLegalHold.Code, http.StatusOK, getLegalHold.Body.String())
	}
	var legalHold ObjectLegalHold
	if err := xml.NewDecoder(getLegalHold.Body).Decode(&legalHold); err != nil {
		t.Fatalf("decode legal hold: %v", err)
	}
	if legalHold.Status != "OFF" {
		t.Fatalf("legal hold = %#v", legalHold)
	}
}

func TestObjectLockPreventsDeleteUntilRetentionExpires(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	retainUntil := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt", strings.NewReader("locked body")); put.Code != http.StatusOK {
		t.Fatalf("put object status = %d; body=%s", put.Code, put.Body.String())
	}
	body := `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?retention", strings.NewReader(body)); putRetention.Code != http.StatusOK {
		t.Fatalf("put retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}

	deleteLocked := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/locked.txt", nil)
	if deleteLocked.Code != http.StatusForbidden {
		t.Fatalf("delete locked status = %d, want %d; body=%s", deleteLocked.Code, http.StatusForbidden, deleteLocked.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(deleteLocked.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode locked delete error: %v", err)
	}
	if parsed.Code != "AccessDenied" {
		t.Fatalf("locked delete code = %q, want AccessDenied", parsed.Code)
	}

	if putExpiredRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/locked.txt?retention", strings.NewReader(`<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2000-01-01T00:00:00Z</RetainUntilDate></Retention>`)); putExpiredRetention.Code != http.StatusOK {
		t.Fatalf("put expired retention status = %d; body=%s", putExpiredRetention.Code, putExpiredRetention.Body.String())
	}
	deleteExpired := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/locked.txt", nil)
	if deleteExpired.Code != http.StatusNoContent {
		t.Fatalf("delete expired retention status = %d, want %d; body=%s", deleteExpired.Code, http.StatusNoContent, deleteExpired.Body.String())
	}
}

func TestObjectLockBypassGovernanceRetentionOnlyBypassesGovernance(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	retainUntil := "2099-01-01T00:00:00Z"
	for _, key := range []string{"governance.txt", "compliance.txt", "legal-hold.txt"} {
		if put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/"+key, strings.NewReader(key)); put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}
	governance := `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/governance.txt?retention", strings.NewReader(governance)); putRetention.Code != http.StatusOK {
		t.Fatalf("put governance retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}
	compliance := `<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>` + retainUntil + `</RetainUntilDate></Retention>`
	if putRetention := performRequest(routes, http.MethodPut, "/demo-bucket/docs/compliance.txt?retention", strings.NewReader(compliance)); putRetention.Code != http.StatusOK {
		t.Fatalf("put compliance retention status = %d; body=%s", putRetention.Code, putRetention.Body.String())
	}
	if putLegalHold := performRequest(routes, http.MethodPut, "/demo-bucket/docs/legal-hold.txt?legal-hold", strings.NewReader(`<LegalHold><Status>ON</Status></LegalHold>`)); putLegalHold.Code != http.StatusOK {
		t.Fatalf("put legal hold status = %d; body=%s", putLegalHold.Code, putLegalHold.Body.String())
	}

	invalidReq := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/governance.txt", nil)
	invalidReq.Header.Set("x-amz-bypass-governance-retention", "not-bool")
	invalidRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid bypass header status = %d, want %d; body=%s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}

	bypassReq := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/governance.txt", nil)
	bypassReq.Header.Set("x-amz-bypass-governance-retention", "true")
	bypassRec := httptest.NewRecorder()
	routes.ServeHTTP(bypassRec, bypassReq)
	if bypassRec.Code != http.StatusNoContent {
		t.Fatalf("bypass governance delete status = %d, want %d; body=%s", bypassRec.Code, http.StatusNoContent, bypassRec.Body.String())
	}

	for _, key := range []string{"compliance.txt", "legal-hold.txt"} {
		req := httptest.NewRequest(http.MethodDelete, "/demo-bucket/docs/"+key, nil)
		req.Header.Set("x-amz-bypass-governance-retention", "true")
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("bypass %s delete status = %d, want %d; body=%s", key, rec.Code, http.StatusForbidden, rec.Body.String())
		}
	}
}

