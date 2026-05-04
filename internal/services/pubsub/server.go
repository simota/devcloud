package pubsub

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	GRPCAddr                  string
	RESTAddr                  string
	Project                   string
	AuthMode                  string
	BearerToken               string
	StoragePath               string
	MessageStoragePath        string
	RESTEnabled               bool
	DefaultAckDeadlineSeconds int
	MessageRetentionSeconds   int
	MaxAckDeadlineSeconds     int
	MaxPullMessages           int
	PullWaitTimeout           time.Duration
	StreamingPullDisabled     bool
	EnablePush                bool
}

type Server struct {
	config        Config
	mu            sync.Mutex
	topics        map[string]topicResource
	subscriptions map[string]subscriptionResource
	snapshots     map[string]snapshotResource
	schemas       map[string]schemaResource
	messages      map[string]pubsubMessage
	deliveries    map[string][]deliveryRecord
	nextMessageID uint64
	nextAckID     uint64
	now           func() time.Time
	loadErr       error
}

type Snapshot struct {
	Status        string                 `json:"status"`
	Running       bool                   `json:"running"`
	Project       string                 `json:"project"`
	Topics        []TopicSnapshot        `json:"topics"`
	Subscriptions []SubscriptionSnapshot `json:"subscriptions"`
}

type TopicSnapshot struct {
	Name              string `json:"name"`
	SubscriptionCount int    `json:"subscriptionCount"`
	CreatedAt         string `json:"createdAt,omitempty"`
	UpdatedAt         string `json:"updatedAt,omitempty"`
}

type SubscriptionSnapshot struct {
	Name                      string             `json:"name"`
	Topic                     string             `json:"topic"`
	Labels                    map[string]string  `json:"labels,omitempty"`
	CreatedAt                 string             `json:"createdAt,omitempty"`
	UpdatedAt                 string             `json:"updatedAt,omitempty"`
	AckDeadlineSeconds        int                `json:"ackDeadlineSeconds"`
	EnableMessageOrdering     bool               `json:"enableMessageOrdering,omitempty"`
	EnableExactlyOnceDelivery bool               `json:"enableExactlyOnceDelivery,omitempty"`
	RetainAckedMessages       bool               `json:"retainAckedMessages,omitempty"`
	MessageRetentionDuration  string             `json:"messageRetentionDuration,omitempty"`
	ExpirationPolicy          map[string]any     `json:"expirationPolicy,omitempty"`
	Filter                    string             `json:"filter,omitempty"`
	DeadLetterPolicy          map[string]any     `json:"deadLetterPolicy,omitempty"`
	RetryPolicy               map[string]any     `json:"retryPolicy,omitempty"`
	PushConfig                map[string]any     `json:"pushConfig,omitempty"`
	BacklogMessages           int                `json:"backlogMessages"`
	InFlightMessages          int                `json:"inFlightMessages"`
	TotalRetainedMessages     int                `json:"totalRetainedMessages"`
	MaxDeliveryAttemptSeen    int                `json:"maxDeliveryAttemptSeen"`
	RecentDeliveries          []DeliverySnapshot `json:"recentDeliveries,omitempty"`
}

type DeliverySnapshot struct {
	MessageID        string `json:"messageId"`
	Subscription     string `json:"subscription,omitempty"`
	PublishTime      string `json:"publishTime,omitempty"`
	OrderingKey      string `json:"orderingKey,omitempty"`
	State            string `json:"state"`
	LeaseDeadline    string `json:"leaseDeadline,omitempty"`
	NextDeliveryTime string `json:"nextDeliveryTime,omitempty"`
	DeliveryAttempt  int    `json:"deliveryAttempt"`
}

type MessageSnapshot struct {
	MessageID     string             `json:"messageId"`
	PublishTime   string             `json:"publishTime,omitempty"`
	OrderingKey   string             `json:"orderingKey,omitempty"`
	Subscriptions []DeliverySnapshot `json:"subscriptions,omitempty"`
}

func NewServer(cfg Config) *Server {
	server := &Server{
		config:        cfg,
		topics:        map[string]topicResource{},
		subscriptions: map[string]subscriptionResource{},
		snapshots:     map[string]snapshotResource{},
		schemas:       map[string]schemaResource{},
		messages:      map[string]pubsubMessage{},
		deliveries:    map[string][]deliveryRecord{},
		now:           time.Now,
	}
	if server.config.GRPCAddr == "" {
		server.config.GRPCAddr = "127.0.0.1:8085"
	}
	if server.config.RESTAddr == "" {
		server.config.RESTAddr = "127.0.0.1:8086"
	}
	if server.config.DefaultAckDeadlineSeconds <= 0 {
		server.config.DefaultAckDeadlineSeconds = 10
	}
	if server.config.MessageRetentionSeconds <= 0 {
		server.config.MessageRetentionSeconds = 604800
	}
	if server.config.MaxAckDeadlineSeconds <= 0 {
		server.config.MaxAckDeadlineSeconds = 600
	}
	if server.config.MaxPullMessages <= 0 {
		server.config.MaxPullMessages = 1000
	}
	if server.config.PullWaitTimeout <= 0 {
		server.config.PullWaitTimeout = time.Second
	}
	if cfg.StoragePath != "" || cfg.MessageStoragePath != "" {
		server.loadErr = server.loadResources()
	}
	return server
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.expireLeasesLocked(now)
	s.cleanupRetainedMessagesLocked(now)
	topicNames := make([]string, 0, len(s.topics))
	for name := range s.topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	topics := make([]TopicSnapshot, 0, len(topicNames))
	for _, name := range topicNames {
		count := 0
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name {
				count++
			}
		}
		topic := s.topics[name]
		topics = append(topics, TopicSnapshot{
			Name:              name,
			SubscriptionCount: count,
			CreatedAt:         topic.CreatedAt,
			UpdatedAt:         topic.UpdatedAt,
		})
	}

	subscriptionNames := make([]string, 0, len(s.subscriptions))
	for name := range s.subscriptions {
		subscriptionNames = append(subscriptionNames, name)
	}
	sort.Strings(subscriptionNames)

	subscriptions := make([]SubscriptionSnapshot, 0, len(subscriptionNames))
	for _, name := range subscriptionNames {
		subscription := s.subscriptions[name]
		snapshot := SubscriptionSnapshot{
			Name:                      subscription.Name,
			Topic:                     subscription.Topic,
			Labels:                    copyStringMap(subscription.Labels),
			CreatedAt:                 subscription.CreatedAt,
			UpdatedAt:                 subscription.UpdatedAt,
			AckDeadlineSeconds:        subscription.AckDeadlineSeconds,
			EnableMessageOrdering:     subscription.EnableMessageOrdering,
			EnableExactlyOnceDelivery: subscription.EnableExactlyOnceDelivery,
			RetainAckedMessages:       subscription.RetainAckedMessages,
			MessageRetentionDuration:  subscription.MessageRetentionDuration,
			ExpirationPolicy:          copyAnyMap(subscription.ExpirationPolicy),
			Filter:                    subscription.Filter,
			DeadLetterPolicy:          copyAnyMap(subscription.DeadLetterPolicy),
			RetryPolicy:               copyAnyMap(subscription.RetryPolicy),
			PushConfig:                safePushConfigSnapshot(subscription.PushConfig),
		}
		for _, delivery := range s.deliveries[name] {
			if delivery.Acked {
				continue
			}
			snapshot.TotalRetainedMessages++
			if delivery.DeliveryAttempt > snapshot.MaxDeliveryAttemptSeen {
				snapshot.MaxDeliveryAttemptSeen = delivery.DeliveryAttempt
			}
			if delivery.LeaseDeadline.After(now) {
				snapshot.InFlightMessages++
			} else {
				snapshot.BacklogMessages++
			}
			snapshot.RecentDeliveries = append(snapshot.RecentDeliveries, s.deliverySnapshotLocked(delivery, now))
		}
		if len(snapshot.RecentDeliveries) > 20 {
			snapshot.RecentDeliveries = snapshot.RecentDeliveries[len(snapshot.RecentDeliveries)-20:]
		}
		subscriptions = append(subscriptions, snapshot)
	}

	return Snapshot{
		Status:        "running",
		Running:       true,
		Project:       defaultString(s.config.Project, "devcloud"),
		Topics:        topics,
		Subscriptions: subscriptions,
	}
}

func (s *Server) deliverySnapshotLocked(delivery deliveryRecord, now time.Time) DeliverySnapshot {
	state := "backlog"
	leaseDeadline := ""
	nextDeliveryTime := ""
	if delivery.LeaseDeadline.After(now) {
		state = "in-flight"
		leaseDeadline = delivery.LeaseDeadline.UTC().Format(time.RFC3339Nano)
	} else if delivery.NextDeliveryTime.After(now) {
		state = "delayed"
		nextDeliveryTime = delivery.NextDeliveryTime.UTC().Format(time.RFC3339Nano)
	}
	message := s.messages[delivery.MessageID]
	return DeliverySnapshot{
		MessageID:        delivery.MessageID,
		PublishTime:      message.PublishTime,
		OrderingKey:      message.OrderingKey,
		State:            state,
		LeaseDeadline:    leaseDeadline,
		NextDeliveryTime: nextDeliveryTime,
		DeliveryAttempt:  delivery.DeliveryAttempt,
	}
}

func (s *Server) MessageSnapshot(messageID string) (MessageSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	message, found := s.messages[messageID]
	if !found {
		return MessageSnapshot{}, false
	}
	snapshot := MessageSnapshot{
		MessageID:   message.MessageID,
		PublishTime: message.PublishTime,
		OrderingKey: message.OrderingKey,
	}
	for subscriptionName, deliveries := range s.deliveries {
		for _, delivery := range deliveries {
			if delivery.MessageID != messageID || delivery.Acked {
				continue
			}
			deliverySnapshot := s.deliverySnapshotLocked(delivery, now)
			deliverySnapshot.Subscription = subscriptionName
			snapshot.Subscriptions = append(snapshot.Subscriptions, deliverySnapshot)
		}
	}
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		return snapshot.Subscriptions[i].Subscription < snapshot.Subscriptions[j].Subscription
	})
	return snapshot, true
}

type topicResource struct {
	Name                     string            `json:"name"`
	Labels                   map[string]string `json:"labels,omitempty"`
	CreatedAt                string            `json:"createdAt,omitempty"`
	UpdatedAt                string            `json:"updatedAt,omitempty"`
	MessageRetentionDuration string            `json:"messageRetentionDuration,omitempty"`
	SchemaSettings           map[string]any    `json:"schemaSettings,omitempty"`
	KMSKeyName               string            `json:"kmsKeyName,omitempty"`
}

type subscriptionResource struct {
	Name                      string            `json:"name"`
	Topic                     string            `json:"topic"`
	Detached                  bool              `json:"detached,omitempty"`
	Labels                    map[string]string `json:"labels,omitempty"`
	CreatedAt                 string            `json:"createdAt,omitempty"`
	UpdatedAt                 string            `json:"updatedAt,omitempty"`
	AckDeadlineSeconds        int               `json:"ackDeadlineSeconds,omitempty"`
	EnableMessageOrdering     bool              `json:"enableMessageOrdering,omitempty"`
	EnableExactlyOnceDelivery bool              `json:"enableExactlyOnceDelivery,omitempty"`
	RetainAckedMessages       bool              `json:"retainAckedMessages,omitempty"`
	MessageRetentionDuration  string            `json:"messageRetentionDuration,omitempty"`
	ExpirationPolicy          map[string]any    `json:"expirationPolicy,omitempty"`
	Filter                    string            `json:"filter,omitempty"`
	DeadLetterPolicy          map[string]any    `json:"deadLetterPolicy,omitempty"`
	RetryPolicy               map[string]any    `json:"retryPolicy,omitempty"`
	PushConfig                map[string]any    `json:"pushConfig,omitempty"`
}

type snapshotResource struct {
	Name         string            `json:"name"`
	Topic        string            `json:"topic,omitempty"`
	Subscription string            `json:"subscription,omitempty"`
	ExpireTime   string            `json:"expireTime,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Deliveries   []deliveryRecord  `json:"deliveries,omitempty"`
}

type schemaResource struct {
	Name               string                   `json:"name"`
	Type               string                   `json:"type,omitempty"`
	Definition         string                   `json:"definition,omitempty"`
	RevisionID         string                   `json:"revisionId,omitempty"`
	RevisionCreateTime string                   `json:"revisionCreateTime,omitempty"`
	Revisions          []schemaRevisionResource `json:"revisions,omitempty"`
}

type schemaRevisionResource struct {
	Type               string `json:"type,omitempty"`
	Definition         string `json:"definition,omitempty"`
	RevisionID         string `json:"revisionId,omitempty"`
	RevisionCreateTime string `json:"revisionCreateTime,omitempty"`
}

type pubsubMessage struct {
	Data        string            `json:"data,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type deliveryRecord struct {
	MessageID        string
	AckID            string
	LeaseDeadline    time.Time
	NextDeliveryTime time.Time
	DeliveryAttempt  int
	Acked            bool
}

type resourceFile struct {
	Topics        []topicResource             `json:"topics"`
	Subscriptions []subscriptionResource      `json:"subscriptions"`
	Snapshots     []snapshotResource          `json:"snapshots,omitempty"`
	Schemas       []schemaResource            `json:"schemas,omitempty"`
	Messages      []pubsubMessage             `json:"messages,omitempty"`
	Deliveries    map[string][]deliveryRecord `json:"deliveries,omitempty"`
	NextMessageID uint64                      `json:"nextMessageId,omitempty"`
	NextAckID     uint64                      `json:"nextAckId,omitempty"`
}

type messageStateFile struct {
	Messages      []pubsubMessage             `json:"messages,omitempty"`
	Deliveries    map[string][]deliveryRecord `json:"deliveries,omitempty"`
	NextMessageID uint64                      `json:"nextMessageId,omitempty"`
	NextAckID     uint64                      `json:"nextAckId,omitempty"`
}

var resourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+%-]{0,254}$`)
var attributeComparisonFilterPattern = regexp.MustCompile(`^attributes\.([A-Za-z0-9_.-]+)\s*(!=|=)\s*"([^"]*)"$`)
var attributePrefixFilterPattern = regexp.MustCompile(`^hasPrefix\(\s*attributes\.([A-Za-z0-9_.-]+)\s*,\s*"([^"]*)"\s*\)$`)

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

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	topics := make([]topicResource, 0, len(s.topics))
	for _, topic := range s.topics {
		if resourceProject(topic.Name) == project {
			topics = append(topics, topic)
		}
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(topics), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"topics": topics[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	project, topicID, ok := topicNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	switch r.Method {
	case http.MethodPut:
		var request topicResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Name != "" && request.Name != name {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic name does not match request path")
			return
		}
		if err := validateTopicMetadata(request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		now := s.now().UTC().Format(time.RFC3339Nano)
		s.mu.Lock()
		defer s.mu.Unlock()
		topic := topicResource{
			Name:                     name,
			Labels:                   copyStringMap(request.Labels),
			CreatedAt:                now,
			UpdatedAt:                now,
			MessageRetentionDuration: request.MessageRetentionDuration,
			SchemaSettings:           copyAnyMap(request.SchemaSettings),
			KMSKeyName:               request.KMSKeyName,
		}
		if _, exists := s.topics[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "topic already exists")
			return
		}
		s.topics[name] = topic
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.topics, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, topic)
	case http.MethodPatch:
		s.handleTopicPatch(w, r, name)
	case http.MethodGet:
		s.mu.Lock()
		topic, found := s.topics[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		writeJSON(w, http.StatusOK, topic)
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.topics[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name && !subscription.Detached {
				writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "topic has attached subscriptions")
				return
			}
		}
		delete(s.topics, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, PATCH, DELETE")
	}
}

func (s *Server) handleTopicPatch(w http.ResponseWriter, r *http.Request, name string) {
	patch, presentFields, err := decodeTopicPatch(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if patch.Name != "" && patch.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic name does not match request path")
		return
	}
	if err := validateTopicMetadata(patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	topic, found := s.topics[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	if hasPatchField(presentFields, "labels") {
		topic.Labels = copyStringMap(patch.Labels)
	}
	if hasPatchField(presentFields, "messageRetentionDuration") {
		topic.MessageRetentionDuration = patch.MessageRetentionDuration
	}
	if hasPatchField(presentFields, "schemaSettings") {
		topic.SchemaSettings = copyAnyMap(patch.SchemaSettings)
	}
	if hasPatchField(presentFields, "kmsKeyName") {
		topic.KMSKeyName = patch.KMSKeyName
	}
	topic.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.topics[name] = topic
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, topic)
}

func (s *Server) handleTopicSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project, topicID, ok := topicSubscriptionsParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.topics[name]; !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	subscriptions := make([]string, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		if subscription.Topic == name && !subscription.Detached {
			subscriptions = append(subscriptions, subscription.Name)
		}
	}
	sort.Strings(subscriptions)
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(subscriptions), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subscriptions[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleTopicIAM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, topicID, action, ok := topicActionParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	name := topicName(project, topicID)
	s.mu.Lock()
	_, found := s.topics[name]
	s.mu.Unlock()
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	s.handleIAMAction(w, r, action)
}

func (s *Server) handleTopicSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project, topicID, ok := topicSnapshotsParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.topics[name]; !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	now := s.now().UTC()
	snapshots := make([]string, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		if snapshot.Topic == name && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot.Name)
		}
	}
	sort.Strings(snapshots)
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(snapshots), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSubscriptionIAM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, action, ok := subscriptionAnyActionParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	_, found := s.subscriptions[name]
	s.mu.Unlock()
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	s.handleIAMAction(w, r, action)
}

func (s *Server) handleIAMAction(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case "getIamPolicy":
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "bindings": []any{}})
	case "setIamPolicy":
		var request struct {
			Policy map[string]any `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Policy == nil {
			request.Policy = map[string]any{"version": 1, "bindings": []any{}}
		}
		writeJSON(w, http.StatusOK, request.Policy)
	case "testIamPermissions":
		var request struct {
			Permissions []string `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"permissions": request.Permissions})
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
	}
}

func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	subscriptions := make([]subscriptionResource, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		if resourceProject(subscription.Name) == project {
			subscriptions = append(subscriptions, subscription)
		}
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Name < subscriptions[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(subscriptions), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subscriptions[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	snapshots := make([]snapshotResource, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		if resourceProject(snapshot.Name) == project && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot.public())
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(snapshots), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	if r.Method == http.MethodPost {
		s.handleSchemaCreate(w, r, project)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET, POST")
		return
	}
	schemaView, ok := parseSchemaView(w, r)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	schemas := make([]schemaResource, 0, len(s.schemas))
	for _, schema := range s.schemas {
		if resourceProject(schema.Name) == project {
			schemas = append(schemas, schema.public(schemaView))
		}
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Name < schemas[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(schemas), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"schemas": schemas[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSchemaCreate(w http.ResponseWriter, r *http.Request, project string) {
	schemaID := strings.TrimSpace(r.URL.Query().Get("schemaId"))
	if schemaID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schemaId is required")
		return
	}
	if !validResourceID(schemaID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
		return
	}
	name := schemaName(project, schemaID)
	var request schemaResource
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.Name != "" && request.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name does not match request path")
		return
	}
	if request.Type != "" && !validSchemaType(request.Type) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
		return
	}
	if err := validateSchemaDefinition(request.Type, request.Definition); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	request.Name = name
	if request.RevisionID == "" {
		request.RevisionID = "1"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.schemas[name]; exists {
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "schema already exists")
		return
	}
	s.schemas[name] = request
	if err := s.saveResourcesLocked(); err != nil {
		delete(s.schemas, name)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, request)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	project, schemaID, ok := schemaNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := schemaName(project, schemaID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(schemaID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
		return
	}
	switch r.Method {
	case http.MethodPut:
		var request schemaResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Name != "" && request.Name != name {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name does not match request path")
			return
		}
		if request.Type != "" && !validSchemaType(request.Type) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
			return
		}
		if err := validateSchemaDefinition(request.Type, request.Definition); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		request.Name = name
		if request.RevisionID == "" {
			request.RevisionID = "1"
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.schemas[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "schema already exists")
			return
		}
		s.schemas[name] = request
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.schemas, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, request)
	case http.MethodGet:
		schemaView, ok := parseSchemaView(w, r)
		if !ok {
			return
		}
		s.mu.Lock()
		schema, found := s.schemas[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		writeJSON(w, http.StatusOK, schema.public(schemaView))
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.schemas[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		delete(s.schemas, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (s *Server) handleSchemaValidateMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, ok := schemasValidateMessageParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		Name     string         `json:"name"`
		Schema   schemaResource `json:"schema"`
		Message  string         `json:"message"`
		Encoding string         `json:"encoding"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	hasInlineSchema := !emptySchemaResource(request.Schema)
	if request.Name == "" && !hasInlineSchema {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "schema name or inline schema is required")
		return
	}
	if request.Name != "" && hasInlineSchema {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "only one of schema name or inline schema may be set")
		return
	}
	if request.Encoding != "" && !validSchemaEncoding(request.Encoding) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema encoding")
		return
	}
	if request.Message != "" {
		message, err := decodeBase64Bytes(request.Message)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message must be base64 encoded")
			return
		}
		if !validSchemaMessageData(message, request.Encoding) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message is invalid for schema encoding")
			return
		}
	}
	if request.Name != "" {
		if !validFullSchemaName(request.Name) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
			return
		}
		if resourceProject(request.Name) != project {
			writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "schema belongs to a different project")
			return
		}
		s.mu.Lock()
		_, found := s.schemas[request.Name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "schema not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	if request.Schema.Name != "" {
		if !validFullSchemaName(request.Schema.Name) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema name")
			return
		}
		if resourceProject(request.Schema.Name) != project {
			writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "schema belongs to a different project")
			return
		}
	}
	if !validSchemaType(request.Schema.Type) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema type")
		return
	}
	if err := validateSchemaDefinition(request.Schema.Type, request.Schema.Definition); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	project, snapshotID, ok := snapshotNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := snapshotName(project, snapshotID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(snapshotID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid snapshot name")
		return
	}
	switch r.Method {
	case http.MethodPut:
		var request struct {
			Subscription string `json:"subscription"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if !validFullSubscriptionName(request.Subscription) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.snapshots[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "snapshot already exists")
			return
		}
		subscription, found := s.subscriptions[request.Subscription]
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		snapshot := snapshotResource{
			Name:         name,
			Topic:        subscription.Topic,
			Subscription: subscription.Name,
			ExpireTime:   s.now().UTC().Add(7 * 24 * time.Hour).Format(time.RFC3339Nano),
			Deliveries:   snapshotDeliveries(s.deliveries[subscription.Name]),
		}
		s.snapshots[name] = snapshot
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.snapshots, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, snapshot.public())
	case http.MethodGet:
		s.mu.Lock()
		snapshot, found := s.snapshots[name]
		s.mu.Unlock()
		if !found || snapshotExpired(snapshot, s.now().UTC()) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
			return
		}
		writeJSON(w, http.StatusOK, snapshot.public())
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.snapshots[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
			return
		}
		delete(s.snapshots, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (s *Server) handleSubscription(w http.ResponseWriter, r *http.Request) {
	project, subscriptionID, ok := subscriptionNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := subscriptionName(project, subscriptionID)
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	switch r.Method {
	case http.MethodPut:
		var request subscriptionResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Topic == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "subscription topic is required")
			return
		}
		if !validFullTopicName(request.Topic) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
			return
		}
		if request.AckDeadlineSeconds < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
			return
		}
		if request.AckDeadlineSeconds == 0 {
			request.AckDeadlineSeconds = s.config.DefaultAckDeadlineSeconds
		}
		if request.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
			return
		}
		if err := validateSubscriptionFilter(request.Filter); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateSubscriptionMetadata(request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateDeadLetterPolicy(request.DeadLetterPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateRetryPolicy(request.RetryPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validatePushConfig(request.PushConfig); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		now := s.now().UTC().Format(time.RFC3339Nano)
		request.Name = name
		request.CreatedAt = now
		request.UpdatedAt = now

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.subscriptions[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "subscription already exists")
			return
		}
		if _, found := s.topics[request.Topic]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		if !s.deadLetterTopicExistsLocked(request.DeadLetterPolicy) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "dead-letter topic not found")
			return
		}
		request.Labels = copyStringMap(request.Labels)
		s.subscriptions[name] = request
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.subscriptions, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, request)
	case http.MethodGet:
		s.mu.Lock()
		subscription, found := s.subscriptions[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		writeJSON(w, http.StatusOK, subscription)
	case http.MethodPatch:
		s.handleSubscriptionPatch(w, r, name)
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.subscriptions[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		delete(s.subscriptions, name)
		delete(s.deliveries, name)
		for snapshotName, snapshot := range s.snapshots {
			if snapshot.Subscription == name {
				delete(s.snapshots, snapshotName)
			}
		}
		s.cleanupUnreferencedMessagesLocked()
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, PATCH, DELETE")
	}
}

func (s *Server) handleSeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "seek")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	var request struct {
		Snapshot string `json:"snapshot"`
		Time     string `json:"time"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.Snapshot == "" && request.Time == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "snapshot or time is required")
		return
	}
	if request.Snapshot != "" && request.Time != "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "only one of snapshot or time may be set")
		return
	}
	var seekTime time.Time
	if request.Time != "" {
		parsed, err := time.Parse(time.RFC3339Nano, request.Time)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, request.Time)
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid seek time")
			return
		}
		seekTime = parsed.UTC()
	}
	if request.Snapshot != "" && !validFullSnapshotName(request.Snapshot) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid snapshot name")
		return
	}
	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if request.Time != "" {
		s.deliveries[name] = s.seekDeliveriesByTimeLocked(subscription, seekTime)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	snapshot, found := s.snapshots[request.Snapshot]
	if !found || snapshotExpired(snapshot, s.now().UTC()) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
		return
	}
	if snapshot.Subscription != name {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "snapshot belongs to a different subscription")
		return
	}
	s.deliveries[name] = snapshotDeliveries(snapshot.Deliveries)
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) seekDeliveriesByTimeLocked(subscription subscriptionResource, seekTime time.Time) []deliveryRecord {
	records := s.deliveries[subscription.Name]
	replayed := make([]deliveryRecord, 0, len(records))
	for _, delivery := range records {
		message, found := s.messages[delivery.MessageID]
		if !found {
			continue
		}
		publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
		if err != nil || publishedAt.Before(seekTime) {
			continue
		}
		replayed = append(replayed, deliveryRecord{MessageID: delivery.MessageID})
	}
	return replayed
}

func (s *Server) handleSubscriptionPatch(w http.ResponseWriter, r *http.Request, name string) {
	patch, presentFields, err := decodeSubscriptionPatch(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if patch.Name != "" && patch.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "subscription name does not match request path")
		return
	}
	if patch.AckDeadlineSeconds < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
		return
	}
	if patch.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
		return
	}
	if hasPatchField(presentFields, "filter") {
		if err := validateSubscriptionFilter(patch.Filter); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "messageRetentionDuration") || hasPatchField(presentFields, "expirationPolicy") {
		if err := validateSubscriptionMetadata(patch); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "deadLetterPolicy") {
		if err := validateDeadLetterPolicy(patch.DeadLetterPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "retryPolicy") {
		if err := validateRetryPolicy(patch.RetryPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "pushConfig") {
		if err := validatePushConfig(patch.PushConfig); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if hasPatchField(presentFields, "deadLetterPolicy") && !s.deadLetterTopicExistsLocked(patch.DeadLetterPolicy) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "dead-letter topic not found")
		return
	}
	if hasPatchField(presentFields, "topic") && patch.Topic != "" && patch.Topic != subscription.Topic {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription topic cannot be changed")
		return
	}
	if hasPatchField(presentFields, "labels") {
		subscription.Labels = copyStringMap(patch.Labels)
	}
	if hasPatchField(presentFields, "ackDeadlineSeconds") {
		if patch.AckDeadlineSeconds == 0 {
			subscription.AckDeadlineSeconds = s.config.DefaultAckDeadlineSeconds
		} else {
			subscription.AckDeadlineSeconds = patch.AckDeadlineSeconds
		}
	}
	if hasPatchField(presentFields, "enableMessageOrdering") {
		subscription.EnableMessageOrdering = patch.EnableMessageOrdering
	}
	if hasPatchField(presentFields, "enableExactlyOnceDelivery") {
		subscription.EnableExactlyOnceDelivery = patch.EnableExactlyOnceDelivery
	}
	if hasPatchField(presentFields, "retainAckedMessages") {
		subscription.RetainAckedMessages = patch.RetainAckedMessages
	}
	if hasPatchField(presentFields, "messageRetentionDuration") {
		subscription.MessageRetentionDuration = patch.MessageRetentionDuration
	}
	if hasPatchField(presentFields, "expirationPolicy") {
		subscription.ExpirationPolicy = copyAnyMap(patch.ExpirationPolicy)
	}
	if hasPatchField(presentFields, "filter") {
		subscription.Filter = patch.Filter
	}
	if hasPatchField(presentFields, "deadLetterPolicy") {
		subscription.DeadLetterPolicy = copyAnyMap(patch.DeadLetterPolicy)
	}
	if hasPatchField(presentFields, "retryPolicy") {
		subscription.RetryPolicy = copyAnyMap(patch.RetryPolicy)
	}
	if hasPatchField(presentFields, "pushConfig") {
		subscription.PushConfig = copyAnyMap(patch.PushConfig)
	}
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, subscription)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, topicID, ok := topicPublishParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	name := topicName(project, topicID)
	var request struct {
		Messages []struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			OrderingKey string            `json:"orderingKey"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if len(request.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "messages are required")
		return
	}
	for _, message := range request.Messages {
		if err := validatePublishMessage(message.Data, message.Attributes); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	topic, found := s.topics[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	for _, message := range request.Messages {
		if err := validateMessageAgainstTopicSchemaSettings(message.Data, topic.SchemaSettings); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	messageIDs := make([]string, 0, len(request.Messages))
	for _, incoming := range request.Messages {
		s.nextMessageID++
		messageID := strconv.FormatUint(s.nextMessageID, 10)
		message := pubsubMessage{
			Data:        incoming.Data,
			Attributes:  copyStringMap(incoming.Attributes),
			MessageID:   messageID,
			PublishTime: now,
			OrderingKey: incoming.OrderingKey,
		}
		s.messages[messageID] = message
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name && !subscription.Detached && subscriptionMatchesMessage(subscription, message) {
				s.deliveries[subscription.Name] = append(s.deliveries[subscription.Name], deliveryRecord{MessageID: messageID})
			}
		}
		messageIDs = append(messageIDs, messageID)
	}
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messageIds": messageIDs})
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "pull")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		MaxMessages       int   `json:"maxMessages"`
		ReturnImmediately *bool `json:"returnImmediately"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.MaxMessages <= 0 {
		request.MaxMessages = 1
	}
	if request.MaxMessages > s.config.MaxPullMessages {
		request.MaxMessages = s.config.MaxPullMessages
	}

	name := subscriptionName(project, subscriptionID)
	if request.ReturnImmediately != nil && !*request.ReturnImmediately {
		s.waitForPullAvailability(r.Context(), name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if subscription.Detached {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription is detached")
		return
	}
	if subscriptionPushEndpoint(subscription) != "" {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription is configured for push delivery")
		return
	}
	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	s.expireLeasesLocked(now)
	ackDeadline := subscription.AckDeadlineSeconds
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	received := make([]map[string]any, 0, request.MaxMessages)
	deliveries := s.deliveries[name]
	blockedOrderingKeys := map[string]struct{}{}
	if subscription.EnableMessageOrdering {
		for _, delivery := range deliveries {
			if delivery.Acked || !delivery.LeaseDeadline.After(now) {
				continue
			}
			message, found := s.messages[delivery.MessageID]
			if !found || message.OrderingKey == "" {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
	}
	for i := range deliveries {
		if len(received) >= request.MaxMessages {
			break
		}
		if deliveries[i].Acked || deliveries[i].LeaseDeadline.After(now) {
			continue
		}
		if deliveries[i].NextDeliveryTime.After(now) {
			if subscription.EnableMessageOrdering {
				if message, found := s.messages[deliveries[i].MessageID]; found && message.OrderingKey != "" {
					blockedOrderingKeys[message.OrderingKey] = struct{}{}
				}
			}
			continue
		}
		message, found := s.messages[deliveries[i].MessageID]
		if !found {
			continue
		}
		if s.deadLetterDeliveryLocked(subscription, &deliveries[i], message, now) {
			continue
		}
		if subscription.EnableMessageOrdering && message.OrderingKey != "" {
			if _, blocked := blockedOrderingKeys[message.OrderingKey]; blocked {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
		s.nextAckID++
		deliveries[i].AckID = fmt.Sprintf("%s-%d", deliveries[i].MessageID, s.nextAckID)
		deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadline) * time.Second)
		deliveries[i].NextDeliveryTime = time.Time{}
		deliveries[i].DeliveryAttempt++
		received = append(received, map[string]any{
			"ackId": deliveries[i].AckID,
			"message": map[string]any{
				"data":        message.Data,
				"attributes":  message.Attributes,
				"messageId":   message.MessageID,
				"publishTime": message.PublishTime,
				"orderingKey": message.OrderingKey,
			},
			"deliveryAttempt": deliveries[i].DeliveryAttempt,
		})
	}
	s.deliveries[name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	if len(received) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"receivedMessages": received})
}

func (s *Server) waitForPullAvailability(ctx context.Context, subscriptionName string) {
	deadline := time.NewTimer(s.config.PullWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.pullMayReturn(subscriptionName) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) pushWorker(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				delivered, _ := s.deliverPush(ctx, client)
				if !delivered {
					break
				}
			}
		}
	}
}

func (s *Server) deliverPush(ctx context.Context, client *http.Client) (bool, error) {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	delivery, ok, err := s.nextPushDelivery()
	if !ok || err != nil {
		return ok, err
	}
	body, err := json.Marshal(map[string]any{
		"message": map[string]any{
			"data":            delivery.Message.Data,
			"attributes":      delivery.Message.Attributes,
			"messageId":       delivery.Message.MessageID,
			"message_id":      delivery.Message.MessageID,
			"publishTime":     delivery.Message.PublishTime,
			"publish_time":    delivery.Message.PublishTime,
			"orderingKey":     delivery.Message.OrderingKey,
			"ordering_key":    delivery.Message.OrderingKey,
			"deliveryAttempt": delivery.Attempt,
		},
		"subscription":    delivery.SubscriptionName,
		"deliveryAttempt": delivery.Attempt,
	})
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.Endpoint, bytes.NewReader(body))
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, nil
	}
	defer response.Body.Close()
	s.finishPushDelivery(delivery, response.StatusCode >= 200 && response.StatusCode < 300)
	return true, nil
}

type pushDelivery struct {
	SubscriptionName string
	Endpoint         string
	Message          pubsubMessage
	Attempt          int
}

func (s *Server) nextPushDelivery() (pushDelivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	s.expireLeasesLocked(now)
	subscriptionNames := make([]string, 0, len(s.subscriptions))
	for name := range s.subscriptions {
		subscriptionNames = append(subscriptionNames, name)
	}
	sort.Strings(subscriptionNames)
	for _, subscriptionName := range subscriptionNames {
		subscription := s.subscriptions[subscriptionName]
		endpoint := subscriptionPushEndpoint(subscription)
		if endpoint == "" || subscription.Detached {
			continue
		}
		deliveries := s.deliveries[subscriptionName]
		for i := range deliveries {
			if deliveries[i].Acked || deliveries[i].LeaseDeadline.After(now) || deliveries[i].NextDeliveryTime.After(now) {
				continue
			}
			message, found := s.messages[deliveries[i].MessageID]
			if !found {
				continue
			}
			if s.deadLetterDeliveryLocked(subscription, &deliveries[i], message, now) {
				continue
			}
			ackDeadline := subscription.AckDeadlineSeconds
			if ackDeadline <= 0 {
				ackDeadline = s.config.DefaultAckDeadlineSeconds
			}
			deliveries[i].DeliveryAttempt++
			deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadline) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
			s.deliveries[subscriptionName] = deliveries
			if err := s.saveResourcesLocked(); err != nil {
				return pushDelivery{}, false, err
			}
			return pushDelivery{
				SubscriptionName: subscriptionName,
				Endpoint:         endpoint,
				Message:          message,
				Attempt:          deliveries[i].DeliveryAttempt,
			}, true, nil
		}
	}
	return pushDelivery{}, false, nil
}

func (s *Server) finishPushDelivery(delivery pushDelivery, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[delivery.SubscriptionName]
	if !found {
		return
	}
	now := s.now().UTC()
	deliveries := s.deliveries[delivery.SubscriptionName]
	for i := range deliveries {
		if deliveries[i].MessageID != delivery.Message.MessageID || deliveries[i].DeliveryAttempt != delivery.Attempt || deliveries[i].Acked {
			continue
		}
		deliveries[i].LeaseDeadline = time.Time{}
		if success {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].NextDeliveryTime = now.Add(s.subscriptionRetryBackoffLocked(delivery.SubscriptionName, deliveries[i].DeliveryAttempt))
		}
		break
	}
	s.deliveries[delivery.SubscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	_ = s.saveResourcesLocked()
}

func (s *Server) pullMayReturn(subscriptionName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[subscriptionName]
	if !found || subscription.Detached || subscriptionPushEndpoint(subscription) != "" {
		return true
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	blockedOrderingKeys := map[string]struct{}{}
	if subscription.EnableMessageOrdering {
		for _, delivery := range s.deliveries[subscriptionName] {
			if delivery.Acked || !delivery.LeaseDeadline.After(now) {
				continue
			}
			message, found := s.messages[delivery.MessageID]
			if !found || message.OrderingKey == "" {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
	}
	for _, delivery := range s.deliveries[subscriptionName] {
		if delivery.Acked || delivery.LeaseDeadline.After(now) || delivery.NextDeliveryTime.After(now) {
			if subscription.EnableMessageOrdering && delivery.NextDeliveryTime.After(now) {
				if message, found := s.messages[delivery.MessageID]; found && message.OrderingKey != "" {
					blockedOrderingKeys[message.OrderingKey] = struct{}{}
				}
			}
			continue
		}
		message, found := s.messages[delivery.MessageID]
		if !found {
			continue
		}
		if subscription.EnableMessageOrdering && message.OrderingKey != "" {
			if _, blocked := blockedOrderingKeys[message.OrderingKey]; blocked {
				continue
			}
		}
		return true
	}
	return false
}

func (s *Server) handleAcknowledge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "acknowledge")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	s.updateAckDeadlines(w, r, project, subscriptionID, true)
}

func (s *Server) handleModifyAckDeadline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "modifyAckDeadline")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	s.updateAckDeadlines(w, r, project, subscriptionID, false)
}

func (s *Server) handleModifyPushConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "modifyPushConfig")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		PushConfig map[string]any `json:"pushConfig"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if err := validatePushConfig(request.PushConfig); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	subscription.PushConfig = copyAnyMap(request.PushConfig)
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleDetachSubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "detach")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	subscription.Detached = true
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	delete(s.deliveries, name)
	for snapshotName, snapshot := range s.snapshots {
		if snapshot.Subscription == name {
			delete(s.snapshots, snapshotName)
		}
	}
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) updateAckDeadlines(w http.ResponseWriter, r *http.Request, project string, subscriptionID string, acknowledge bool) {
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		AckIDs             []string `json:"ackIds"`
		AckDeadlineSeconds int      `json:"ackDeadlineSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if len(request.AckIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	for _, ackID := range request.AckIDs {
		if strings.TrimSpace(ackID) == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackIds must not contain empty values")
			return
		}
	}
	if !acknowledge && request.AckDeadlineSeconds < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
		return
	}
	if !acknowledge && request.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
		return
	}

	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	ackIDs := map[string]struct{}{}
	for _, ackID := range request.AckIDs {
		ackIDs[ackID] = struct{}{}
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	deliveries := s.deliveries[name]
	for i := range deliveries {
		if _, ok := ackIDs[deliveries[i].AckID]; !ok || deliveries[i].Acked {
			continue
		}
		if acknowledge {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			continue
		}
		if request.AckDeadlineSeconds == 0 {
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].LeaseDeadline = now.Add(time.Duration(request.AckDeadlineSeconds) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
		}
	}
	s.deliveries[name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func isTopicsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "topics" && parts[2] != ""
}

func isTopicPublishPath(path string) bool {
	_, _, ok := topicPublishParts(path)
	return ok
}

func isTopicIAMPath(path string) bool {
	_, _, action, ok := topicActionParts(path)
	return ok && isIAMAction(action)
}

func isTopicPath(path string) bool {
	_, _, ok := topicNameParts(path)
	return ok
}

func isTopicSubscriptionsPath(path string) bool {
	_, _, ok := topicSubscriptionsParts(path)
	return ok
}

func isTopicSnapshotsPath(path string) bool {
	_, _, ok := topicSnapshotsParts(path)
	return ok
}

func isSubscriptionsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "subscriptions" && parts[2] != ""
}

func isSnapshotsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "snapshots" && parts[2] != ""
}

func isSchemasCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "schemas" && parts[2] != ""
}

func isSchemasValidateMessagePath(path string) bool {
	_, ok := schemasValidateMessageParts(path)
	return ok
}

func isSubscriptionPullPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "pull")
	return ok
}

func isSubscriptionAcknowledgePath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "acknowledge")
	return ok
}

func isSubscriptionModifyAckDeadlinePath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "modifyAckDeadline")
	return ok
}

func isSubscriptionModifyPushConfigPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "modifyPushConfig")
	return ok
}

func isSubscriptionDetachPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "detach")
	return ok
}

func isSubscriptionIAMPath(path string) bool {
	_, _, action, ok := subscriptionAnyActionParts(path)
	return ok && isIAMAction(action)
}

func isSubscriptionSeekPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "seek")
	return ok
}

func isSubscriptionPath(path string) bool {
	_, _, ok := subscriptionNameParts(path)
	return ok
}

func isSnapshotPath(path string) bool {
	_, _, ok := snapshotNameParts(path)
	return ok
}

func isSchemaPath(path string) bool {
	_, _, ok := schemaNameParts(path)
	return ok
}

func pathParts(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		unescaped, err := url.PathUnescape(part)
		if err != nil {
			parts[i] = "\x00"
			continue
		}
		parts[i] = strings.TrimSpace(unescaped)
	}
	return parts
}

func topicNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func topicPublishParts(path string) (string, string, bool) {
	project, topicID, action, ok := topicActionParts(path)
	if !ok || action != "publish" {
		return "", "", false
	}
	return project, topicID, true
}

func topicActionParts(path string) (string, string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[2] == "" {
		return "", "", "", false
	}
	topicID, action, ok := strings.Cut(parts[4], ":")
	if !ok || topicID == "" || action == "" {
		return "", "", "", false
	}
	return parts[2], topicID, action, true
}

func topicSubscriptionsParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 6 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[5] != "subscriptions" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func topicSnapshotsParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 6 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[5] != "snapshots" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func subscriptionActionParts(path string, wantAction string) (string, string, bool) {
	project, subscriptionID, action, ok := subscriptionAnyActionParts(path)
	if !ok || action != wantAction {
		return "", "", false
	}
	return project, subscriptionID, true
}

func subscriptionAnyActionParts(path string) (string, string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "subscriptions" || parts[2] == "" {
		return "", "", "", false
	}
	subscriptionID, action, ok := strings.Cut(parts[4], ":")
	if !ok || subscriptionID == "" || action == "" {
		return "", "", "", false
	}
	return parts[2], subscriptionID, action, true
}

func subscriptionNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "subscriptions" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func snapshotNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "snapshots" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func schemaNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "schemas" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func schemasValidateMessageParts(path string) (string, bool) {
	parts := pathParts(path)
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "projects" || parts[2] == "" || parts[3] != "schemas:validateMessage" {
		return "", false
	}
	return parts[2], true
}

func validResourceID(id string) bool {
	return resourceIDPattern.MatchString(id)
}

func validProjectID(id string) bool {
	return resourceIDPattern.MatchString(id)
}

func validFullTopicName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "topics" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSubscriptionName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "subscriptions" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSnapshotName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "snapshots" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validFullSchemaName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[2] == "schemas" && validProjectID(parts[1]) && validResourceID(parts[3])
}

func validSchemaType(schemaType string) bool {
	switch schemaType {
	case "", "TYPE_UNSPECIFIED", "PROTOCOL_BUFFER", "AVRO":
		return true
	default:
		return false
	}
}

func validSchemaEncoding(encoding string) bool {
	switch encoding {
	case "", "ENCODING_UNSPECIFIED", "JSON", "BINARY":
		return true
	default:
		return false
	}
}

func validSchemaMessageData(message []byte, encoding string) bool {
	if len(message) == 0 {
		return true
	}
	switch encoding {
	case "JSON":
		return json.Valid(message)
	default:
		return true
	}
}

func validateSchemaDefinition(schemaType string, definition string) error {
	if strings.TrimSpace(definition) == "" {
		return nil
	}
	switch schemaType {
	case "AVRO":
		var decoded any
		if err := json.Unmarshal([]byte(definition), &decoded); err != nil {
			return errors.New("avro schema definition must be valid json")
		}
		if _, ok := decoded.(map[string]any); !ok {
			return errors.New("avro schema definition must be a json object")
		}
	}
	return nil
}

func emptySchemaResource(schema schemaResource) bool {
	return schema.Name == "" &&
		schema.Type == "" &&
		schema.Definition == "" &&
		schema.RevisionID == "" &&
		schema.RevisionCreateTime == "" &&
		len(schema.Revisions) == 0
}

func isIAMAction(action string) bool {
	switch action {
	case "getIamPolicy", "setIamPolicy", "testIamPermissions":
		return true
	default:
		return false
	}
}

func parseSchemaView(w http.ResponseWriter, r *http.Request) (string, bool) {
	view := strings.TrimSpace(r.URL.Query().Get("view"))
	switch view {
	case "", "FULL", "BASIC":
		return view, true
	default:
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid schema view")
		return "", false
	}
}

func validateDeadLetterPolicy(policy map[string]any) error {
	if len(policy) == 0 {
		return nil
	}
	rawTopic, ok := policy["deadLetterTopic"]
	if !ok {
		return fmt.Errorf("deadLetterPolicy.deadLetterTopic is required")
	}
	topic, ok := rawTopic.(string)
	if !ok || !validFullTopicName(topic) {
		return fmt.Errorf("invalid deadLetterPolicy.deadLetterTopic")
	}
	maxAttempts, ok := deadLetterMaxDeliveryAttempts(policy)
	if !ok {
		return fmt.Errorf("deadLetterPolicy.maxDeliveryAttempts is required")
	}
	if maxAttempts < 5 || maxAttempts > 100 {
		return fmt.Errorf("deadLetterPolicy.maxDeliveryAttempts must be between 5 and 100")
	}
	return nil
}

func (s *Server) deadLetterTopicExistsLocked(policy map[string]any) bool {
	if len(policy) == 0 {
		return true
	}
	_, found := s.topics[deadLetterTopic(policy)]
	return found
}

func validateTopicMetadata(topic topicResource) error {
	if strings.TrimSpace(topic.MessageRetentionDuration) != "" {
		if _, err := parseGoogleDuration(topic.MessageRetentionDuration); err != nil {
			return fmt.Errorf("messageRetentionDuration must be a non-negative duration")
		}
	}
	if len(topic.SchemaSettings) > 0 {
		rawSchema, ok := topic.SchemaSettings["schema"]
		if !ok {
			return fmt.Errorf("schemaSettings.schema is required")
		}
		schema, ok := rawSchema.(string)
		if !ok || !validFullSchemaName(schema) {
			return fmt.Errorf("invalid schemaSettings.schema")
		}
		if rawEncoding, ok := topic.SchemaSettings["encoding"]; ok {
			encoding, ok := rawEncoding.(string)
			if !ok || !validSchemaEncoding(encoding) {
				return fmt.Errorf("invalid schemaSettings.encoding")
			}
		}
	}
	return nil
}

func validateSubscriptionMetadata(subscription subscriptionResource) error {
	if strings.TrimSpace(subscription.MessageRetentionDuration) != "" {
		if _, err := parseGoogleDuration(subscription.MessageRetentionDuration); err != nil {
			return fmt.Errorf("messageRetentionDuration must be a non-negative duration")
		}
	}
	if len(subscription.ExpirationPolicy) > 0 {
		rawTTL, ok := subscription.ExpirationPolicy["ttl"]
		if !ok {
			return fmt.Errorf("expirationPolicy.ttl is required")
		}
		ttl, ok := rawTTL.(string)
		if !ok || strings.TrimSpace(ttl) == "" {
			return fmt.Errorf("expirationPolicy.ttl must be a duration string")
		}
		if _, err := parseGoogleDuration(ttl); err != nil {
			return fmt.Errorf("expirationPolicy.ttl must be a non-negative duration")
		}
	}
	return nil
}

func parseGoogleDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty duration")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("invalid duration")
	}
	return duration, nil
}

func validateSubscriptionFilter(filter string) error {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return nil
	}
	if !attributeComparisonFilterPattern.MatchString(filter) && !attributePrefixFilterPattern.MatchString(filter) {
		return fmt.Errorf("unsupported subscription filter")
	}
	return nil
}

func validateRetryPolicy(policy map[string]any) error {
	if len(policy) == 0 {
		return nil
	}
	minimum, hasMinimum, err := retryPolicyDuration(policy, "minimumBackoff")
	if err != nil {
		return err
	}
	maximum, hasMaximum, err := retryPolicyDuration(policy, "maximumBackoff")
	if err != nil {
		return err
	}
	if hasMinimum && hasMaximum && minimum > maximum {
		return fmt.Errorf("retryPolicy.minimumBackoff must be less than or equal to retryPolicy.maximumBackoff")
	}
	return nil
}

func validatePushConfig(config map[string]any) error {
	if len(config) == 0 {
		return nil
	}
	rawEndpoint, ok := config["pushEndpoint"]
	if !ok {
		return nil
	}
	endpoint, ok := rawEndpoint.(string)
	if !ok || strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("pushConfig.pushEndpoint must be an http or https URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("pushConfig.pushEndpoint must not include user info")
	}
	return nil
}

func validatePublishMessage(data string, attributes map[string]string) error {
	if data == "" && len(attributes) == 0 {
		return fmt.Errorf("message data or attributes are required")
	}
	if data != "" {
		if _, err := decodeBase64Bytes(data); err != nil {
			return fmt.Errorf("message data must be base64 encoded")
		}
	}
	for key := range attributes {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("message attributes must not contain empty keys")
		}
	}
	return nil
}

func validateMessageAgainstTopicSchemaSettings(data string, schemaSettings map[string]any) error {
	if len(schemaSettings) == 0 {
		return nil
	}
	encoding, _ := schemaSettings["encoding"].(string)
	if encoding == "" {
		return nil
	}
	message, err := decodeBase64Bytes(data)
	if err != nil {
		return fmt.Errorf("message data must be base64 encoded")
	}
	if !validSchemaMessageData(message, encoding) {
		return fmt.Errorf("message is invalid for topic schema encoding")
	}
	return nil
}

func decodeBase64Bytes(value string) ([]byte, error) {
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
	} {
		decoded, err := encoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid base64")
}

func retryPolicyDuration(policy map[string]any, field string) (time.Duration, bool, error) {
	raw, ok := policy[field]
	if !ok {
		return 0, false, nil
	}
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return 0, false, fmt.Errorf("retryPolicy.%s must be a duration string", field)
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 0, false, fmt.Errorf("retryPolicy.%s must be a non-negative duration", field)
	}
	return duration, true, nil
}

func subscriptionMatchesMessage(subscription subscriptionResource, message pubsubMessage) bool {
	filter := strings.TrimSpace(subscription.Filter)
	if filter == "" {
		return true
	}
	if match := attributeComparisonFilterPattern.FindStringSubmatch(filter); len(match) == 4 {
		value := message.Attributes[match[1]]
		if match[2] == "!=" {
			return value != match[3]
		}
		return value == match[3]
	}
	match := attributePrefixFilterPattern.FindStringSubmatch(filter)
	if len(match) != 3 {
		return false
	}
	return strings.HasPrefix(message.Attributes[match[1]], match[2])
}

func subscriptionPushEndpoint(subscription subscriptionResource) string {
	rawEndpoint, ok := subscription.PushConfig["pushEndpoint"]
	if !ok {
		return ""
	}
	endpoint, _ := rawEndpoint.(string)
	return strings.TrimSpace(endpoint)
}

func safePushConfigSnapshot(config map[string]any) map[string]any {
	if len(config) == 0 {
		return nil
	}
	safe := map[string]any{}
	if endpoint, ok := config["pushEndpoint"].(string); ok && strings.TrimSpace(endpoint) != "" {
		safe["pushEndpoint"] = strings.TrimSpace(endpoint)
	}
	if len(safe) == 0 {
		return nil
	}
	return safe
}

func deadLetterMaxDeliveryAttempts(policy map[string]any) (int, bool) {
	raw, ok := policy["maxDeliveryAttempts"]
	if !ok {
		return 0, false
	}
	switch value := raw.(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		if value != float64(int(value)) {
			return 0, false
		}
		return int(value), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func deadLetterTopic(policy map[string]any) string {
	if len(policy) == 0 {
		return ""
	}
	topic, _ := policy["deadLetterTopic"].(string)
	return topic
}

func topicName(project string, topicID string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, topicID)
}

func subscriptionName(project string, subscriptionID string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", project, subscriptionID)
}

func snapshotName(project string, snapshotID string) string {
	return fmt.Sprintf("projects/%s/snapshots/%s", project, snapshotID)
}

func schemaName(project string, schemaID string) string {
	return fmt.Sprintf("projects/%s/schemas/%s", project, schemaID)
}

func (s *Server) generatedSubscriptionNameLocked(project string) string {
	if !validProjectID(project) {
		project = defaultString(s.config.Project, "devcloud")
	}
	for i := 1; ; i++ {
		name := subscriptionName(project, fmt.Sprintf("devcloud-auto-sub-%d", i))
		if _, exists := s.subscriptions[name]; !exists {
			return name
		}
	}
}

func (s *Server) generatedSnapshotNameLocked(project string) string {
	if !validProjectID(project) {
		project = defaultString(s.config.Project, "devcloud")
	}
	for i := 1; ; i++ {
		name := snapshotName(project, fmt.Sprintf("devcloud-auto-snapshot-%d", i))
		if _, exists := s.snapshots[name]; !exists {
			return name
		}
	}
}

func resourceProject(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

func parseListPagination(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	query := r.URL.Query()
	pageSize := 0
	if raw := query.Get("pageSize"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be non-negative")
			return 0, 0, false
		}
		pageSize = parsed
	}
	start := 0
	if raw := query.Get("pageToken"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageToken is invalid")
			return 0, 0, false
		}
		start = parsed
	}
	return start, pageSize, true
}

func pageBounds(total int, start int, pageSize int) (int, string) {
	if start > total {
		start = total
	}
	end := total
	if pageSize > 0 && start+pageSize < total {
		end = start + pageSize
		return end, strconv.Itoa(end)
	}
	return end, ""
}

func copyStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	copied := make(map[string]string, len(value))
	for k, v := range value {
		copied[k] = v
	}
	return copied
}

func copyAnyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	copied := make(map[string]any, len(value))
	for k, v := range value {
		copied[k] = v
	}
	return copied
}

func (snapshot snapshotResource) public() snapshotResource {
	snapshot.Deliveries = nil
	return snapshot
}

func (schema schemaResource) public(view string) schemaResource {
	if view == "BASIC" {
		schema.Definition = ""
	}
	return schema
}

func snapshotExpired(snapshot snapshotResource, now time.Time) bool {
	if strings.TrimSpace(snapshot.ExpireTime) == "" {
		return false
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, snapshot.ExpireTime)
	if err != nil {
		expiresAt, err = time.Parse(time.RFC3339, snapshot.ExpireTime)
	}
	return err == nil && !expiresAt.After(now)
}

func snapshotDeliveries(deliveries []deliveryRecord) []deliveryRecord {
	if len(deliveries) == 0 {
		return nil
	}
	copied := make([]deliveryRecord, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Acked {
			continue
		}
		delivery.AckID = ""
		delivery.LeaseDeadline = time.Time{}
		delivery.NextDeliveryTime = time.Time{}
		copied = append(copied, delivery)
	}
	return copied
}

func decodeTopicPatch(r *http.Request) (topicResource, map[string]struct{}, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return topicResource{}, nil, err
	}
	topicBody := body
	if raw, ok := body["topic"]; ok {
		if err := json.Unmarshal(raw, &topicBody); err != nil {
			return topicResource{}, nil, err
		}
	}
	var patch topicResource
	data, err := json.Marshal(topicBody)
	if err != nil {
		return topicResource{}, nil, err
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return topicResource{}, nil, err
	}

	fields, err := topicUpdateMaskFields(r, body, topicBody)
	if err != nil {
		return topicResource{}, nil, err
	}
	return patch, fields, nil
}

func topicUpdateMaskFields(r *http.Request, body map[string]json.RawMessage, topicBody map[string]json.RawMessage) (map[string]struct{}, error) {
	if raw := r.URL.Query().Get("updateMask"); raw != "" {
		return parseTopicUpdateMask(raw)
	}
	if raw, ok := body["updateMask"]; ok {
		var mask string
		if err := json.Unmarshal(raw, &mask); err == nil {
			return parseTopicUpdateMask(mask)
		}
		var structured struct {
			Paths []string `json:"paths"`
		}
		if err := json.Unmarshal(raw, &structured); err != nil {
			return nil, err
		}
		return parseTopicUpdateMask(strings.Join(structured.Paths, ","))
	}
	fields := map[string]struct{}{}
	for field := range topicBody {
		normalized, ok := normalizeTopicPatchField(field)
		if ok {
			fields[normalized] = struct{}{}
		}
	}
	return fields, nil
}

func parseTopicUpdateMask(mask string) (map[string]struct{}, error) {
	fields := map[string]struct{}{}
	for _, raw := range strings.Split(mask, ",") {
		field := strings.TrimSpace(raw)
		if field == "" {
			continue
		}
		normalized, ok := normalizeTopicPatchField(field)
		if !ok {
			return nil, fmt.Errorf("unsupported topic update field %q", field)
		}
		fields[normalized] = struct{}{}
	}
	return fields, nil
}

func normalizeTopicPatchField(field string) (string, bool) {
	field = strings.TrimPrefix(field, "topic.")
	switch field {
	case "name":
		return "name", true
	case "labels":
		return "labels", true
	case "messageRetentionDuration", "message_retention_duration":
		return "messageRetentionDuration", true
	case "schemaSettings", "schema_settings":
		return "schemaSettings", true
	case "kmsKeyName", "kms_key_name":
		return "kmsKeyName", true
	default:
		return "", false
	}
}

func decodeSubscriptionPatch(r *http.Request) (subscriptionResource, map[string]struct{}, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return subscriptionResource{}, nil, err
	}
	subscriptionBody := body
	if raw, ok := body["subscription"]; ok {
		if err := json.Unmarshal(raw, &subscriptionBody); err != nil {
			return subscriptionResource{}, nil, err
		}
	}
	var patch subscriptionResource
	data, err := json.Marshal(subscriptionBody)
	if err != nil {
		return subscriptionResource{}, nil, err
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return subscriptionResource{}, nil, err
	}

	fields, err := subscriptionUpdateMaskFields(r, body, subscriptionBody)
	if err != nil {
		return subscriptionResource{}, nil, err
	}
	return patch, fields, nil
}

func subscriptionUpdateMaskFields(r *http.Request, body map[string]json.RawMessage, subscriptionBody map[string]json.RawMessage) (map[string]struct{}, error) {
	if raw := r.URL.Query().Get("updateMask"); raw != "" {
		return parseSubscriptionUpdateMask(raw)
	}
	if raw, ok := body["updateMask"]; ok {
		var mask string
		if err := json.Unmarshal(raw, &mask); err == nil {
			return parseSubscriptionUpdateMask(mask)
		}
		var structured struct {
			Paths []string `json:"paths"`
		}
		if err := json.Unmarshal(raw, &structured); err != nil {
			return nil, err
		}
		return parseSubscriptionUpdateMask(strings.Join(structured.Paths, ","))
	}
	fields := map[string]struct{}{}
	for field := range subscriptionBody {
		normalized, ok := normalizeSubscriptionPatchField(field)
		if ok {
			fields[normalized] = struct{}{}
		}
	}
	return fields, nil
}

func parseSubscriptionUpdateMask(mask string) (map[string]struct{}, error) {
	fields := map[string]struct{}{}
	for _, raw := range strings.Split(mask, ",") {
		field := strings.TrimSpace(raw)
		if field == "" {
			continue
		}
		normalized, ok := normalizeSubscriptionPatchField(field)
		if !ok {
			return nil, fmt.Errorf("unsupported subscription update field %q", field)
		}
		fields[normalized] = struct{}{}
	}
	return fields, nil
}

func normalizeSubscriptionPatchField(field string) (string, bool) {
	field = strings.TrimPrefix(field, "subscription.")
	switch field {
	case "name":
		return "name", true
	case "topic":
		return "topic", true
	case "labels":
		return "labels", true
	case "ackDeadlineSeconds", "ack_deadline_seconds":
		return "ackDeadlineSeconds", true
	case "enableMessageOrdering", "enable_message_ordering":
		return "enableMessageOrdering", true
	case "enableExactlyOnceDelivery", "enable_exactly_once_delivery":
		return "enableExactlyOnceDelivery", true
	case "retainAckedMessages", "retain_acked_messages":
		return "retainAckedMessages", true
	case "messageRetentionDuration", "message_retention_duration":
		return "messageRetentionDuration", true
	case "expirationPolicy", "expiration_policy":
		return "expirationPolicy", true
	case "filter":
		return "filter", true
	case "deadLetterPolicy", "dead_letter_policy":
		return "deadLetterPolicy", true
	case "retryPolicy", "retry_policy":
		return "retryPolicy", true
	case "pushConfig", "push_config":
		return "pushConfig", true
	default:
		return "", false
	}
}

func hasPatchField(fields map[string]struct{}, field string) bool {
	_, ok := fields[field]
	return ok
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func compactAckedDeliveries(deliveries []deliveryRecord, retainAcked bool) []deliveryRecord {
	if retainAcked {
		return deliveries
	}
	if len(deliveries) == 0 {
		return deliveries
	}
	kept := deliveries[:0]
	for _, delivery := range deliveries {
		if !delivery.Acked {
			kept = append(kept, delivery)
		}
	}
	return kept
}

func (s *Server) expireLeasesLocked(now time.Time) {
	for subscription, deliveries := range s.deliveries {
		changed := false
		for i := range deliveries {
			if deliveries[i].Acked || deliveries[i].LeaseDeadline.IsZero() || deliveries[i].LeaseDeadline.After(now) {
				continue
			}
			deliveries[i].AckID = ""
			nextDeliveryTime := deliveries[i].LeaseDeadline
			deliveries[i].LeaseDeadline = time.Time{}
			if backoff := s.subscriptionRetryBackoffLocked(subscription, deliveries[i].DeliveryAttempt); backoff > 0 {
				deliveries[i].NextDeliveryTime = nextDeliveryTime.Add(backoff)
			}
			changed = true
		}
		if changed {
			s.deliveries[subscription] = deliveries
		}
	}
}

func (s *Server) subscriptionRetryBackoffLocked(subscriptionName string, deliveryAttempt int) time.Duration {
	subscription, found := s.subscriptions[subscriptionName]
	if !found {
		return 0
	}
	minimum, ok, err := retryPolicyDuration(subscription.RetryPolicy, "minimumBackoff")
	if err != nil || !ok {
		return 0
	}
	maximum, hasMaximum, err := retryPolicyDuration(subscription.RetryPolicy, "maximumBackoff")
	if err != nil {
		return minimum
	}
	backoff := minimum
	for attempt := 1; attempt < deliveryAttempt; attempt++ {
		if hasMaximum && backoff >= maximum {
			return maximum
		}
		if backoff > time.Duration(1<<62) {
			if hasMaximum {
				return maximum
			}
			return backoff
		}
		backoff *= 2
	}
	if hasMaximum && backoff > maximum {
		return maximum
	}
	return backoff
}

func (s *Server) deadLetterDeliveryLocked(subscription subscriptionResource, delivery *deliveryRecord, message pubsubMessage, now time.Time) bool {
	maxAttempts, ok := deadLetterMaxDeliveryAttempts(subscription.DeadLetterPolicy)
	if !ok || delivery.DeliveryAttempt < maxAttempts {
		return false
	}
	topic := deadLetterTopic(subscription.DeadLetterPolicy)
	if topic == "" {
		return false
	}
	if _, found := s.topics[topic]; !found {
		return false
	}

	s.nextMessageID++
	deadLetterMessageID := strconv.FormatUint(s.nextMessageID, 10)
	deadLetterMessage := pubsubMessage{
		Data:        message.Data,
		Attributes:  copyStringMap(message.Attributes),
		MessageID:   deadLetterMessageID,
		PublishTime: now.UTC().Format(time.RFC3339Nano),
		OrderingKey: message.OrderingKey,
	}
	s.messages[deadLetterMessageID] = deadLetterMessage
	for _, candidate := range s.subscriptions {
		if candidate.Topic == topic && !candidate.Detached {
			s.deliveries[candidate.Name] = append(s.deliveries[candidate.Name], deliveryRecord{MessageID: deadLetterMessageID})
		}
	}
	delivery.Acked = true
	delivery.AckID = ""
	delivery.LeaseDeadline = time.Time{}
	delivery.NextDeliveryTime = time.Time{}
	return true
}

func (s *Server) cleanupRetainedMessagesLocked(now time.Time) {
	s.cleanupExpiredSnapshotsLocked(now)
	if s.config.MessageRetentionSeconds <= 0 || len(s.messages) == 0 {
		return
	}
	for subscription, deliveries := range s.deliveries {
		retention := s.subscriptionMessageRetentionLocked(subscription)
		cutoff := now.Add(-retention)
		kept := deliveries[:0]
		for _, delivery := range deliveries {
			message, found := s.messages[delivery.MessageID]
			if !found {
				continue
			}
			publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
			if err != nil || publishedAt.Before(cutoff) {
				continue
			}
			kept = append(kept, delivery)
		}
		if len(kept) == 0 {
			delete(s.deliveries, subscription)
		} else {
			s.deliveries[subscription] = kept
		}
	}
	globalCutoff := now.Add(-time.Duration(s.config.MessageRetentionSeconds) * time.Second)
	for name, snapshot := range s.snapshots {
		kept := snapshot.Deliveries[:0]
		for _, delivery := range snapshot.Deliveries {
			message, found := s.messages[delivery.MessageID]
			if !found {
				continue
			}
			publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
			if err != nil || publishedAt.Before(globalCutoff) {
				continue
			}
			kept = append(kept, delivery)
		}
		snapshot.Deliveries = kept
		s.snapshots[name] = snapshot
	}
	s.cleanupUnreferencedMessagesLocked()
}

func (s *Server) cleanupExpiredSnapshotsLocked(now time.Time) {
	for name, snapshot := range s.snapshots {
		if snapshotExpired(snapshot, now) {
			delete(s.snapshots, name)
		}
	}
}

func (s *Server) subscriptionMessageRetentionLocked(subscriptionName string) time.Duration {
	fallback := time.Duration(s.config.MessageRetentionSeconds) * time.Second
	subscription, found := s.subscriptions[subscriptionName]
	if !found || strings.TrimSpace(subscription.MessageRetentionDuration) == "" {
		if !found {
			return fallback
		}
		topic, topicFound := s.topics[subscription.Topic]
		if !topicFound || strings.TrimSpace(topic.MessageRetentionDuration) == "" {
			return fallback
		}
		retention, err := parseGoogleDuration(topic.MessageRetentionDuration)
		if err != nil || retention <= 0 {
			return fallback
		}
		return retention
	}
	retention, err := parseGoogleDuration(subscription.MessageRetentionDuration)
	if err != nil || retention <= 0 {
		return fallback
	}
	return retention
}

func (s *Server) cleanupUnreferencedMessagesLocked() {
	if len(s.messages) == 0 {
		return
	}
	referenced := map[string]struct{}{}
	for subscriptionName, deliveries := range s.deliveries {
		subscription := s.subscriptions[subscriptionName]
		for _, delivery := range deliveries {
			if delivery.Acked && !subscription.RetainAckedMessages {
				continue
			}
			referenced[delivery.MessageID] = struct{}{}
		}
	}
	for _, snapshot := range s.snapshots {
		for _, delivery := range snapshot.Deliveries {
			referenced[delivery.MessageID] = struct{}{}
		}
	}
	for id := range s.messages {
		if _, ok := referenced[id]; !ok {
			delete(s.messages, id)
		}
	}
}

func (s *Server) loadResources() error {
	if s.config.StoragePath != "" {
		data, err := os.ReadFile(s.resourceFilePath())
		if errors.Is(err, os.ErrNotExist) {
			if s.config.MessageStoragePath == "" {
				return nil
			}
		} else if err != nil {
			return err
		} else {
			var file resourceFile
			if err := json.Unmarshal(data, &file); err != nil {
				return err
			}
			s.loadResourceFile(file)
		}
	}

	if s.config.MessageStoragePath == "" {
		return nil
	}
	messageData, err := os.ReadFile(s.messageStateFilePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var messageFile messageStateFile
	if err := json.Unmarshal(messageData, &messageFile); err != nil {
		return err
	}
	s.loadMessageStateFile(messageFile)
	return nil
}

func (s *Server) loadResourceFile(file resourceFile) {
	for _, topic := range file.Topics {
		if topic.Name != "" {
			s.topics[topic.Name] = topic
		}
	}
	for _, subscription := range file.Subscriptions {
		if subscription.Name != "" {
			s.subscriptions[subscription.Name] = subscription
		}
	}
	for _, snapshot := range file.Snapshots {
		if snapshot.Name != "" {
			s.snapshots[snapshot.Name] = snapshot
		}
	}
	for _, schema := range file.Schemas {
		if schema.Name != "" {
			s.schemas[schema.Name] = schema
		}
	}
	for _, message := range file.Messages {
		if message.MessageID != "" {
			s.messages[message.MessageID] = message
			if id, err := strconv.ParseUint(message.MessageID, 10, 64); err == nil && id > s.nextMessageID {
				s.nextMessageID = id
			}
		}
	}
	for subscription, deliveries := range file.Deliveries {
		if subscription != "" {
			s.deliveries[subscription] = deliveries
		}
	}
	if file.NextMessageID > s.nextMessageID {
		s.nextMessageID = file.NextMessageID
	}
	s.nextAckID = file.NextAckID
}

func (s *Server) loadMessageStateFile(file messageStateFile) {
	s.messages = map[string]pubsubMessage{}
	s.deliveries = map[string][]deliveryRecord{}
	s.nextMessageID = 0
	for _, message := range file.Messages {
		if message.MessageID != "" {
			s.messages[message.MessageID] = message
			if id, err := strconv.ParseUint(message.MessageID, 10, 64); err == nil && id > s.nextMessageID {
				s.nextMessageID = id
			}
		}
	}
	for subscription, deliveries := range file.Deliveries {
		if subscription != "" {
			s.deliveries[subscription] = deliveries
		}
	}
	if file.NextMessageID > s.nextMessageID {
		s.nextMessageID = file.NextMessageID
	}
	s.nextAckID = file.NextAckID
}

func (s *Server) saveResourcesLocked() error {
	if s.config.StoragePath == "" && s.config.MessageStoragePath == "" {
		s.cleanupUnreferencedMessagesLocked()
		return nil
	}
	s.cleanupRetainedMessagesLocked(s.now().UTC())
	s.cleanupUnreferencedMessagesLocked()
	if s.config.StoragePath != "" {
		if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
			return err
		}
	}
	topics := make([]topicResource, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	subscriptions := make([]subscriptionResource, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Name < subscriptions[j].Name })
	snapshots := make([]snapshotResource, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	schemas := make([]schemaResource, 0, len(s.schemas))
	for _, schema := range s.schemas {
		schemas = append(schemas, schema)
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Name < schemas[j].Name })
	messages := make([]pubsubMessage, 0, len(s.messages))
	for _, message := range s.messages {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].MessageID < messages[j].MessageID })
	deliveries := make(map[string][]deliveryRecord, len(s.deliveries))
	for subscription, records := range s.deliveries {
		if len(records) > 0 {
			deliveries[subscription] = append([]deliveryRecord(nil), records...)
		}
	}
	includeMessageState := s.config.MessageStoragePath == ""
	data, err := json.MarshalIndent(s.resourceFileForSave(topics, subscriptions, snapshots, schemas, messages, deliveries, includeMessageState), "", "  ")
	if err != nil {
		return err
	}
	if s.config.StoragePath != "" {
		if err := writeJSONFileAtomically(s.resourceFilePath(), data); err != nil {
			return err
		}
	}
	if s.config.MessageStoragePath == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.MessageStoragePath, 0o755); err != nil {
		return err
	}
	messageData, err := json.MarshalIndent(messageStateFile{
		Messages:      messages,
		Deliveries:    deliveries,
		NextMessageID: s.nextMessageID,
		NextAckID:     s.nextAckID,
	}, "", "  ")
	if err != nil {
		return err
	}
	return writeJSONFileAtomically(s.messageStateFilePath(), messageData)
}

func (s *Server) resourceFileForSave(topics []topicResource, subscriptions []subscriptionResource, snapshots []snapshotResource, schemas []schemaResource, messages []pubsubMessage, deliveries map[string][]deliveryRecord, includeMessageState bool) resourceFile {
	file := resourceFile{
		Topics:        topics,
		Subscriptions: subscriptions,
		Snapshots:     snapshots,
		Schemas:       schemas,
	}
	if includeMessageState {
		file.Messages = messages
		file.Deliveries = deliveries
		file.NextMessageID = s.nextMessageID
		file.NextAckID = s.nextAckID
	}
	return file
}

func writeJSONFileAtomically(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Server) resourceFilePath() string {
	return filepath.Join(s.config.StoragePath, "resources.json")
}

func (s *Server) messageStateFilePath() string {
	return filepath.Join(s.config.MessageStoragePath, "pubsub.json")
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

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"status":  code,
			"message": message,
		},
	})
}
