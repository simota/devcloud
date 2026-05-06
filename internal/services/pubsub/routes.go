package pubsub

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"
)

func (s *Server) Run(ctx context.Context) error {
	listenerCount := s.listenerCount()
	errCh := make(chan error, listenerCount)
	if s.config.EnablePush {
		go s.pushWorker(ctx)
	}
	go func() { errCh <- s.runGRPCListener(ctx) }()
	if s.config.RESTEnabled {
		go func() { errCh <- s.runRESTServer(ctx) }()
	}

	for i := 0; i < listenerCount; i++ {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err == nil || errors.Is(err, context.Canceled) {
				continue
			}
			return err
		}
	}
	return nil
}

func (s *Server) listenerCount() int {
	if s.config.RESTEnabled {
		return 2
	}
	return 1
}

func (s *Server) runGRPCListener(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return err
	}
	server := s.newGRPCServer()

	go func() {
		<-ctx.Done()
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			server.Stop()
		}
	}()

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

func (s *Server) grpcRoutes() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "devcloud-pubsub-grpc")
		switch r.URL.Path {
		case "/healthz":
			if r.Method != http.MethodGet {
				methodNotAllowed(w, "GET")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"service":  "pubsub",
				"status":   "running",
				"protocol": "grpc",
			})
		case "/readyz":
			if r.Method != http.MethodGet {
				methodNotAllowed(w, "GET")
				return
			}
			if s.loadErr != nil {
				writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"service":  "pubsub",
				"status":   "running",
				"protocol": "grpc",
			})
		default:
			writeError(w, http.StatusNotFound, "NOT_FOUND", "grpc service not implemented")
		}
	})
}

func (s *Server) runRESTServer(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.RESTAddr,
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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-pubsub")
	if !s.authorize(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="devcloud-pubsub"`)
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid authentication credentials")
		return
	}
	if s.loadErr != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	switch {
	case r.URL.Path == "/healthz" || r.URL.Path == "/readyz":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"service":  "pubsub",
			"status":   "running",
			"protocol": "rest",
		})
	case isTopicPublishPath(r.URL.EscapedPath()):
		s.handlePublish(w, r)
	case isTopicIAMPath(r.URL.EscapedPath()):
		s.handleTopicIAM(w, r)
	case isSubscriptionPullPath(r.URL.EscapedPath()):
		s.handlePull(w, r)
	case isSubscriptionAcknowledgePath(r.URL.EscapedPath()):
		s.handleAcknowledge(w, r)
	case isSubscriptionModifyAckDeadlinePath(r.URL.EscapedPath()):
		s.handleModifyAckDeadline(w, r)
	case isSubscriptionModifyPushConfigPath(r.URL.EscapedPath()):
		s.handleModifyPushConfig(w, r)
	case isSubscriptionDetachPath(r.URL.EscapedPath()):
		s.handleDetachSubscription(w, r)
	case isSubscriptionIAMPath(r.URL.EscapedPath()):
		s.handleSubscriptionIAM(w, r)
	case isTopicsCollectionPath(r.URL.EscapedPath()):
		s.handleTopics(w, r)
	case isTopicSubscriptionsPath(r.URL.EscapedPath()):
		s.handleTopicSubscriptions(w, r)
	case isTopicSnapshotsPath(r.URL.EscapedPath()):
		s.handleTopicSnapshots(w, r)
	case isTopicPath(r.URL.EscapedPath()):
		s.handleTopic(w, r)
	case isSnapshotsCollectionPath(r.URL.EscapedPath()):
		s.handleSnapshots(w, r)
	case isSnapshotPath(r.URL.EscapedPath()):
		s.handleSnapshot(w, r)
	case isSchemasValidateMessagePath(r.URL.EscapedPath()):
		s.handleSchemaValidateMessage(w, r)
	case isSchemasCollectionPath(r.URL.EscapedPath()):
		s.handleSchemas(w, r)
	case isSchemaPath(r.URL.EscapedPath()):
		s.handleSchema(w, r)
	case isSubscriptionsCollectionPath(r.URL.EscapedPath()):
		s.handleSubscriptions(w, r)
	case isSubscriptionSeekPath(r.URL.EscapedPath()):
		s.handleSeek(w, r)
	case isSubscriptionPath(r.URL.EscapedPath()):
		s.handleSubscription(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
	}
}

func (s *Server) authorize(r *http.Request) bool {
	mode := strings.ToLower(strings.TrimSpace(s.config.AuthMode))
	switch mode {
	case "", "off", "relaxed":
		return true
	case "oauth-relaxed":
		return bearerTokenFromRequest(r) != ""
	case "bearer-dev", "strict":
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
