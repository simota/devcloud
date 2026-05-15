package applicationautoscaling

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const targetPrefix = "AnyScaleFrontendService."

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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handle(w, r)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-application-autoscaling")
	if s.loadErr != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to load application-autoscaling state")
		return
	}
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "ResourceNotFoundException", "not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "ValidationException", "method not allowed")
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" && !strings.HasPrefix(contentType, "application/x-amz-json-1.1") {
		writeError(w, http.StatusBadRequest, "ValidationException", "unsupported content type")
		return
	}
	if err := s.verifySignature(r); err != nil {
		writeSignatureError(w, err)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	if !strings.HasPrefix(target, targetPrefix) {
		writeError(w, http.StatusBadRequest, "UnknownOperationException", "unknown operation")
		return
	}

	switch strings.TrimPrefix(target, targetPrefix) {
	case "RegisterScalableTarget":
		s.handleRegisterScalableTarget(w, r)
	case "DescribeScalableTargets":
		s.handleDescribeScalableTargets(w, r)
	case "DeregisterScalableTarget":
		s.handleDeregisterScalableTarget(w, r)
	case "PutScalingPolicy":
		s.handlePutScalingPolicy(w, r)
	case "DescribeScalingPolicies":
		s.handleDescribeScalingPolicies(w, r)
	case "DeleteScalingPolicy":
		s.handleDeleteScalingPolicy(w, r)
	case "DescribeScalingActivities":
		s.handleDescribeScalingActivities(w, r)
	case "PutScheduledAction":
		s.handlePutScheduledAction(w, r)
	case "DescribeScheduledActions":
		s.handleDescribeScheduledActions(w, r)
	case "DeleteScheduledAction":
		s.handleDeleteScheduledAction(w, r)
	case "TagResource":
		s.handleTagResource(w, r)
	case "UntagResource":
		s.handleUntagResource(w, r)
	case "ListTagsForResource":
		s.handleListTagsForResource(w, r)
	default:
		writeError(w, http.StatusBadRequest, "UnknownOperationException", "unknown operation")
	}
}

func decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	if err := json.NewDecoder(r.Body).Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "SerializationException", "invalid json request")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, name string, message string) {
	w.Header().Set("X-Amzn-Errortype", name)
	writeJSON(w, status, map[string]string{
		"__type":  "com.amazonaws.application-autoscaling.v20160206#" + name,
		"message": message,
	})
}
