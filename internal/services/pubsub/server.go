package pubsub

import (
	"sync"
	"time"

	"devcloud/internal/events"
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
	config         Config
	mu             sync.Mutex
	topics         map[string]topicResource
	subscriptions  map[string]subscriptionResource
	snapshots      map[string]snapshotResource
	schemas        map[string]schemaResource
	messages       map[string]pubsubMessage
	deliveries     map[string][]deliveryRecord
	nextMessageID  uint64
	nextAckID      uint64
	now            func() time.Time
	loadErr        error
	eventPublisher events.Publisher
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
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
