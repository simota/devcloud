package sqs

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *Server) routes() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-sqs")
	if s.loadErr != nil {
		writeJSONError(w, "InternalError", "failed to load sqs state", http.StatusInternalServerError)
		return
	}
	if r.URL.Path == "" {
		r.URL.Path = "/"
	}
	if !isRootPath(r.URL.Path) && queueNameFromURL(r.URL.Path) == "" {
		writeProtocolError(w, protocolFromRequest(r), "InvalidAddress", "SQS endpoint path is invalid", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		writeJSONError(w, "InvalidAction", "method is not supported", http.StatusMethodNotAllowed)
		return
	}
	authProtocol := protocolFromRequest(r)
	if err := s.verifySignature(r); err != nil {
		code, status := signatureErrorDetails(err)
		writeProtocolError(w, authProtocol, code, err.Error(), status)
		return
	}

	protocol, operation, err := s.detectOperation(r)
	if err != nil {
		code := "InvalidAction"
		status := http.StatusBadRequest
		var requestErr sqsRequestError
		if errors.As(err, &requestErr) {
			code = requestErr.Code
			status = requestErr.Status
		}
		writeProtocolError(w, protocol, code, err.Error(), status)
		return
	}
	switch operation {
	case "ListQueues":
		s.handleListQueues(w, r, protocol)
	case "CreateQueue":
		s.handleCreateQueue(w, r, protocol)
	case "GetQueueUrl":
		s.handleGetQueueURL(w, r, protocol)
	case "GetQueueAttributes":
		s.handleGetQueueAttributes(w, r, protocol)
	case "SetQueueAttributes":
		s.handleSetQueueAttributes(w, r, protocol)
	case "DeleteQueue":
		s.handleDeleteQueue(w, r, protocol)
	case "PurgeQueue":
		s.handlePurgeQueue(w, r, protocol)
	case "TagQueue":
		s.handleTagQueue(w, r, protocol)
	case "UntagQueue":
		s.handleUntagQueue(w, r, protocol)
	case "ListQueueTags":
		s.handleListQueueTags(w, r, protocol)
	case "ListDeadLetterSourceQueues":
		s.handleListDeadLetterSourceQueues(w, r, protocol)
	case "StartMessageMoveTask":
		s.handleStartMessageMoveTask(w, r, protocol)
	case "ListMessageMoveTasks":
		s.handleListMessageMoveTasks(w, r, protocol)
	case "CancelMessageMoveTask":
		s.handleCancelMessageMoveTask(w, r, protocol)
	case "AddPermission":
		s.handleAddPermission(w, r, protocol)
	case "RemovePermission":
		s.handleRemovePermission(w, r, protocol)
	case "SendMessage":
		s.handleSendMessage(w, r, protocol)
	case "SendMessageBatch":
		s.handleSendMessageBatch(w, r, protocol)
	case "ReceiveMessage":
		s.handleReceiveMessage(w, r, protocol)
	case "DeleteMessage":
		s.handleDeleteMessage(w, r, protocol)
	case "DeleteMessageBatch":
		s.handleDeleteMessageBatch(w, r, protocol)
	case "ChangeMessageVisibility":
		s.handleChangeMessageVisibility(w, r, protocol)
	case "ChangeMessageVisibilityBatch":
		s.handleChangeMessageVisibilityBatch(w, r, protocol)
	default:
		writeProtocolError(w, protocol, "InvalidAction", "operation is not implemented", http.StatusBadRequest)
	}
}

func (s *Server) detectOperation(r *http.Request) (protocolKind, string, error) {
	target := r.Header.Get("X-Amz-Target")
	if strings.HasPrefix(target, "AmazonSQS.") {
		return protocolJSON, strings.TrimPrefix(target, "AmazonSQS."), nil
	}
	if r.Method == http.MethodGet {
		if err := validateQueryAPIVersion(r.URL.Query().Get("Version")); err != nil {
			return protocolQuery, "", err
		}
		return protocolQuery, r.URL.Query().Get("Action"), nil
	}
	if strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return protocolQuery, "", err
		}
		if err := validateQueryAPIVersion(r.Form.Get("Version")); err != nil {
			return protocolQuery, "", err
		}
		return protocolQuery, r.Form.Get("Action"), nil
	}
	if strings.Contains(r.Header.Get("Content-Type"), "application/x-amz-json-1.0") {
		return protocolJSON, "", errors.New("missing X-Amz-Target")
	}
	return protocolQuery, "", errors.New("missing SQS action")
}

func validateQueryAPIVersion(version string) error {
	if version == "" {
		return sqsRequestError{
			Code:    "MissingParameter",
			Message: "Version is required for SQS Query protocol",
			Status:  http.StatusBadRequest,
		}
	}
	if version != sqsAPIVersion {
		return sqsRequestError{
			Code:    "InvalidParameterValue",
			Message: "Version must be " + sqsAPIVersion,
			Status:  http.StatusBadRequest,
		}
	}
	return nil
}

func isRootPath(path string) bool {
	return path == "" || path == "/"
}
