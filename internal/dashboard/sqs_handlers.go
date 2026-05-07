package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
)

func (s *Server) handleSQSStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	region := defaultString(s.config.SQSRegion, "us-east-1")
	queueCount := 0
	if s.sqs != nil {
		snapshot := s.sqs.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		region = snapshot.Region
		queueCount = len(snapshot.Queues)
	}
	writeJSON(w, map[string]any{
		"service":     "sqs",
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.SQSEndpoint, "http://127.0.0.1:9324"),
		"region":      region,
		"authMode":    defaultString(s.config.SQSAuthMode, "relaxed"),
		"storagePath": defaultString(s.config.SQSStoragePath, ".devcloud/data/sqs"),
		"queueCount":  queueCount,
	})
}

func (s *Server) handleSQSQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		methodNotAllowed(w, "GET, POST")
		return
	}
	if s.sqs == nil {
		http.Error(w, "sqs service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodPost {
		s.forwardSQSDashboardOperation(w, r, "CreateQueue", "", "")
		return
	}
	snapshot := s.sqs.Snapshot()
	writeJSON(w, map[string]any{
		"queues": snapshot.Queues,
	})
}

func (s *Server) handleSQSQueue(w http.ResponseWriter, r *http.Request) {
	if s.sqs == nil {
		http.Error(w, "sqs service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/sqs/queues/")
	if err != nil {
		http.Error(w, "invalid sqs queue path", http.StatusBadRequest)
		return
	}
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	queueName := parts[0]
	detail, found := s.sqs.QueueDetailSnapshot(queueName)
	if !found {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, map[string]any{
			"queue": detail.Queue,
		})
		return
	}
	switch parts[1] {
	case "messages":
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			methodNotAllowed(w, "GET, POST")
			return
		}
		if r.Method == http.MethodPost {
			s.forwardSQSDashboardOperation(w, r, "SendMessage", queueName, detail.Queue.URL)
			return
		}
		writeJSON(w, map[string]any{
			"queueName": queueName,
			"messages":  detail.Messages,
		})
	case "receive":
		s.forwardSQSDashboardOperation(w, r, "ReceiveMessage", queueName, detail.Queue.URL)
	case "delete":
		s.forwardSQSDashboardOperation(w, r, "DeleteMessage", queueName, detail.Queue.URL)
	case "visibility":
		s.forwardSQSDashboardOperation(w, r, "ChangeMessageVisibility", queueName, detail.Queue.URL)
	case "leases":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, map[string]any{
			"queueName": queueName,
			"leases":    detail.Leases,
		})
	case "dlq":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		dlq, found := s.sqs.DeadLetterSnapshot(queueName)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{
			"queueName":              queueName,
			"deadLetterQueue":        dlq.DeadLetterQueue,
			"deadLetterSourceQueues": dlq.DeadLetterSourceQueues,
		})
	case "purge":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		if !s.sqs.PurgeQueueByName(queueName) {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

type dashboardSQSOperationRequest struct {
	Input json.RawMessage `json:"input"`
}

func (s *Server) forwardSQSDashboardOperation(w http.ResponseWriter, r *http.Request, operation string, queueName string, queueURL string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request dashboardSQSOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid json request", http.StatusBadRequest)
		return
	}
	input, err := normalizeSQSDashboardInput(request.Input, queueName, queueURL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPost
	req.URL = &url.URL{Path: "/"}
	req.RequestURI = ""
	req.Body = io.NopCloser(bytes.NewReader(input))
	req.ContentLength = int64(len(input))
	req.Header = make(http.Header)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+operation)
	s.sqs.ServeHTTP(w, req)
}

func normalizeSQSDashboardInput(raw json.RawMessage, queueName string, queueURL string) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("input is required")
	}
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, errors.New("input must be valid JSON")
	}
	if input == nil {
		return nil, errors.New("input must be a JSON object")
	}
	if queueName != "" {
		if existing, ok := input["QueueName"]; ok {
			if existingName, ok := existing.(string); !ok || existingName != queueName {
				return nil, errors.New("input QueueName must match the selected queue")
			}
		}
	}
	if queueURL != "" {
		if existing, ok := input["QueueUrl"]; ok {
			if existingURL, ok := existing.(string); !ok || existingURL != queueURL {
				return nil, errors.New("input QueueUrl must match the selected queue")
			}
		} else {
			input["QueueUrl"] = queueURL
		}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, errors.New("input could not be encoded")
	}
	return encoded, nil
}
