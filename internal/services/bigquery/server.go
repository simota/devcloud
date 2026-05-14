package bigquery

import (
	"devcloud/internal/events"
	s3svc "devcloud/internal/services/s3"
)

type Config struct {
	Addr             string
	Project          string
	Location         string
	AuthMode         string
	BearerToken      string
	StoragePath      string
	MaxRowsPerTable  int64
	MaxRequestBytes  int64
	MaxResultRows    int
	DefaultLegacySQL bool
	ObjectStore      s3svc.BucketStore
}

type Server struct {
	config         Config
	eventPublisher events.Publisher
}

func NewServer(cfg Config) *Server {
	return &Server{config: cfg}
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
}
