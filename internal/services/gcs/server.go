package gcs

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	s3svc "devcloud/internal/services/s3"
)

type Config struct {
	Addr              string
	Project           string
	Location          string
	AuthMode          string
	BearerToken       string
	UploadSessionPath string
}

type Server struct {
	config   Config
	store    s3svc.BucketStore
	mu       sync.Mutex
	sessions map[string]resumableSession
}

func NewServer(cfg Config, store s3svc.BucketStore) *Server {
	server := &Server{config: cfg, store: store, sessions: map[string]resumableSession{}}
	server.loadResumableSessions()
	return server
}

type resumableSession struct {
	Bucket             string
	Name               string
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
	Preconditions      objectPreconditions
	CreatedAt          time.Time
	ReceivedBytes      int64
}

type objectPreconditions struct {
	IfGenerationMatch        string
	IfGenerationNotMatch     string
	IfMetagenerationMatch    string
	IfMetagenerationNotMatch string
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) routes() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "backendError", "gcs service is disabled")
		return
	}
	w.Header().Set("Server", "devcloud-gcs")
	if !s.authorize(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="devcloud-gcs"`)
		writeError(w, http.StatusUnauthorized, "authError", "invalid authentication credentials")
		return
	}

	switch {
	case r.URL.Path == "/upload/storage/v1/b" || strings.HasPrefix(r.URL.EscapedPath(), "/upload/storage/v1/b/"):
		s.handleUpload(w, r)
	case strings.HasPrefix(r.URL.EscapedPath(), "/download/storage/v1/b/"):
		s.handleDownload(w, r)
	case r.URL.Path == "/storage/v1/b":
		s.handleBuckets(w, r)
	case strings.HasPrefix(r.URL.EscapedPath(), "/storage/v1/b/"):
		s.handleBucketOrObject(w, r)
	default:
		writeError(w, http.StatusNotFound, "notFound", "not found")
	}
}

func (s *Server) authorize(r *http.Request) bool {
	mode := strings.ToLower(strings.TrimSpace(s.config.AuthMode))
	switch mode {
	case "", "off", "relaxed":
		return true
	case "oauth-relaxed":
		token := bearerTokenFromRequest(r)
		return token != ""
	case "bearer-dev":
		token := bearerTokenFromRequest(r)
		expected := strings.TrimSpace(s.config.BearerToken)
		if token == "" || expected == "" {
			return false
		}
		return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
	default:
		return false
	}
}

func bearerTokenFromRequest(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	scheme, token, ok := strings.Cut(auth, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}
