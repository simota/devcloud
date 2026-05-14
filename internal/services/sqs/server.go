package sqs

import (
	"path/filepath"
	"strings"
	"sync"

	"devcloud/internal/events"
)

type Config struct {
	Addr                            string
	Region                          string
	AccountID                       string
	QueueURLHost                    string
	AuthMode                        string
	AccessKeyID                     string
	SecretAccessKey                 string
	StoragePath                     string
	MaxQueues                       int
	MaxMessageBytes                 int64
	MaxReceiveBatchSize             int
	DefaultVisibilityTimeoutSeconds int
	DefaultDelaySeconds             int
	DefaultMessageRetentionSeconds  int
	DefaultReceiveWaitTimeSeconds   int
}

type Server struct {
	config         Config
	mu             sync.Mutex
	queues         map[string]*queueState
	moveTasks      map[string]moveTaskState
	waitCh         chan struct{}
	loadErr        error
	eventPublisher events.Publisher
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
}

func NewServer(cfg Config) *Server {
	if storagePath := strings.TrimSpace(cfg.StoragePath); storagePath != "" {
		if absolutePath, err := filepath.Abs(storagePath); err == nil {
			cfg.StoragePath = absolutePath
		}
	}
	server := &Server{
		config:    cfg,
		queues:    map[string]*queueState{},
		moveTasks: map[string]moveTaskState{},
		waitCh:    make(chan struct{}),
	}
	if cfg.StoragePath != "" {
		server.loadErr = server.load()
	}
	return server
}
