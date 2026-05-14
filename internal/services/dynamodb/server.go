package dynamodb

import (
	"sync"

	"devcloud/internal/events"
)

type Config struct {
	Addr            string
	Region          string
	AuthMode        string
	AccessKeyID     string
	SecretAccessKey string
	StoragePath     string
	MaxItemBytes    int64
	MaxTables       int
}

type Server struct {
	config         Config
	mu             sync.Mutex
	tables         map[string]*tableState
	backups        map[string]backupDescription
	backupTables   map[string]tableDescription
	backupItems    map[string]map[string]item
	loadErr        error
	eventPublisher events.Publisher
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
}

func NewServer(cfg Config) *Server {
	server := &Server{
		config:       cfg,
		tables:       map[string]*tableState{},
		backups:      map[string]backupDescription{},
		backupTables: map[string]tableDescription{},
		backupItems:  map[string]map[string]item{},
	}
	if cfg.StoragePath != "" {
		server.loadErr = server.load()
	}
	return server
}
