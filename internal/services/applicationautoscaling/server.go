package applicationautoscaling

import (
	"sync"

	"devcloud/internal/events"
)

type Config struct {
	Addr            string
	Region          string
	AccountID       string
	AuthMode        string
	AccessKeyID     string
	SecretAccessKey string
	StoragePath     string
}

type Server struct {
	config           Config
	mu               sync.Mutex
	scalableTargets  map[string]scalableTarget
	scalingPolicies  map[string]scalingPolicy
	scheduledActions map[string]scheduledAction
	tags             map[string]map[string]string
	loadErr          error
	eventPublisher   events.Publisher
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
}

func NewServer(cfg Config) *Server {
	server := &Server{
		config:           cfg,
		scalableTargets:  map[string]scalableTarget{},
		scalingPolicies:  map[string]scalingPolicy{},
		scheduledActions: map[string]scheduledAction{},
		tags:             map[string]map[string]string{},
	}
	if cfg.StoragePath != "" {
		server.loadErr = server.load()
	}
	return server
}
