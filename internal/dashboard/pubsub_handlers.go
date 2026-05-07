package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (s *Server) handlePubSubStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	project := defaultString(s.config.PubSubProject, "devcloud")
	topicCount := 0
	subscriptionCount := 0
	if s.pubsub != nil {
		snapshot := s.pubsub.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		project = snapshot.Project
		topicCount = len(snapshot.Topics)
		subscriptionCount = len(snapshot.Subscriptions)
	}
	writeJSON(w, map[string]any{
		"service":           "pubsub",
		"status":            status,
		"running":           running,
		"grpcEndpoint":      defaultString(s.config.PubSubGRPCEndpoint, "127.0.0.1:8085"),
		"restEndpoint":      defaultString(s.config.PubSubRESTEndpoint, "http://127.0.0.1:8086"),
		"project":           project,
		"storagePath":       defaultString(s.config.PubSubStoragePath, ".devcloud/data/pubsub"),
		"topicCount":        topicCount,
		"subscriptionCount": subscriptionCount,
	})
}

func (s *Server) handlePubSubProjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project := defaultString(s.config.PubSubProject, "devcloud")
	status := "disabled"
	running := false
	if s.pubsub != nil {
		snapshot := s.pubsub.Snapshot()
		project = snapshot.Project
		status = snapshot.Status
		running = snapshot.Running
	}
	writeJSON(w, map[string]any{
		"projects": []map[string]any{
			{
				"project": project,
				"status":  status,
				"running": running,
			},
		},
	})
}

func (s *Server) handlePubSubTopics(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.pubsub.Snapshot()
		writeJSON(w, map[string]any{
			"project": snapshot.Project,
			"topics":  snapshot.Topics,
		})
	case http.MethodPost:
		var request struct {
			TopicID string `json:"topicId"`
			Name    string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		topicID := dashboardPubSubResourceID(firstNonEmpty(request.TopicID, request.Name))
		if topicID == "" {
			http.Error(w, "topicId is required", http.StatusBadRequest)
			return
		}
		s.forwardPubSubTopicCreate(w, r, topicID)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handlePubSubTopic(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/topics/")
	if err != nil {
		http.Error(w, "invalid pubsub topic path", http.StatusBadRequest)
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "publish":
			s.forwardPubSubTopicAction(w, r, parts[0], "publish")
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	snapshot := s.pubsub.Snapshot()
	name := "projects/" + snapshot.Project + "/topics/" + parts[0]
	for _, topic := range snapshot.Topics {
		if topic.Name == name {
			writeJSON(w, map[string]any{
				"project": snapshot.Project,
				"topic":   topic,
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) forwardPubSubTopicAction(w http.ResponseWriter, r *http.Request, topicID string, action string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	s.forwardPubSubRequest(w, r, http.MethodPost, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/topics/"+url.PathEscape(topicID)+":"+action)
}

func (s *Server) forwardPubSubTopicCreate(w http.ResponseWriter, r *http.Request, topicID string) {
	s.forwardPubSubRequest(w, r, http.MethodPut, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/topics/"+url.PathEscape(topicID))
}

func (s *Server) pubSubProject() string {
	project := defaultString(s.config.PubSubProject, "devcloud")
	if s.pubsub != nil {
		project = s.pubsub.Snapshot().Project
	}
	return project
}

func (s *Server) forwardPubSubRequest(w http.ResponseWriter, r *http.Request, method string, path string) {
	req := r.Clone(r.Context())
	req.Method = method
	req.URL = &url.URL{
		Path:     path,
		RawQuery: r.URL.RawQuery,
	}
	req.RequestURI = ""
	req.Body = r.Body
	req.Header = r.Header.Clone()
	s.pubsub.ServeHTTP(w, req)
}

func (s *Server) handlePubSubSubscriptions(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.pubsub.Snapshot()
		writeJSON(w, map[string]any{
			"project":       snapshot.Project,
			"subscriptions": snapshot.Subscriptions,
		})
	case http.MethodPost:
		var request struct {
			SubscriptionID     string `json:"subscriptionId"`
			Name               string `json:"name"`
			TopicID            string `json:"topicId"`
			Topic              string `json:"topic"`
			AckDeadlineSeconds int    `json:"ackDeadlineSeconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		subscriptionID := dashboardPubSubResourceID(firstNonEmpty(request.SubscriptionID, request.Name))
		topicID := dashboardPubSubResourceID(firstNonEmpty(request.TopicID, request.Topic))
		if subscriptionID == "" || topicID == "" {
			http.Error(w, "subscriptionId and topicId are required", http.StatusBadRequest)
			return
		}
		s.forwardPubSubSubscriptionCreate(w, r, subscriptionID, topicID, request.AckDeadlineSeconds)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handlePubSubSubscription(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/subscriptions/")
	if err != nil {
		http.Error(w, "invalid pubsub subscription path", http.StatusBadRequest)
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "pull":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "pull")
		case "ack":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "acknowledge")
		case "modifyAckDeadline":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "modifyAckDeadline")
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	snapshot := s.pubsub.Snapshot()
	name := "projects/" + snapshot.Project + "/subscriptions/" + parts[0]
	for _, subscription := range snapshot.Subscriptions {
		if subscription.Name == name {
			writeJSON(w, map[string]any{
				"project":      snapshot.Project,
				"subscription": subscription,
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) forwardPubSubSubscriptionAction(w http.ResponseWriter, r *http.Request, subscriptionID string, action string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	s.forwardPubSubRequest(w, r, http.MethodPost, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/subscriptions/"+url.PathEscape(subscriptionID)+":"+action)
}

func (s *Server) forwardPubSubSubscriptionCreate(w http.ResponseWriter, r *http.Request, subscriptionID string, topicID string, ackDeadlineSeconds int) {
	project := s.pubSubProject()
	body := map[string]any{
		"topic": "projects/" + project + "/topics/" + topicID,
	}
	if ackDeadlineSeconds > 0 {
		body["ackDeadlineSeconds"] = ackDeadlineSeconds
	}
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "invalid subscription request", http.StatusBadRequest)
		return
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPut
	req.URL = &url.URL{
		Path:     "/v1/projects/" + url.PathEscape(project) + "/subscriptions/" + url.PathEscape(subscriptionID),
		RawQuery: r.URL.RawQuery,
	}
	req.RequestURI = ""
	req.Body = io.NopCloser(strings.NewReader(string(data)))
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", "application/json")
	s.pubsub.ServeHTTP(w, req)
}

func dashboardPubSubResourceID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "/") {
		parts := strings.Split(value, "/")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return value
}

func (s *Server) handlePubSubMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/messages/")
	if err != nil {
		http.Error(w, "invalid pubsub message path", http.StatusBadRequest)
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	message, found := s.pubsub.MessageSnapshot(parts[0])
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"message": message,
	})
}
