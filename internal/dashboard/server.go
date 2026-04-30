package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"devcloud/internal/services/mail"
	"devcloud/internal/storage/mailstore"
)

type Config struct {
	Addr string
}

type Server struct {
	config Config
	store  mailstore.Store
}

func NewServer(cfg Config, store mailstore.Store) *Server {
	return &Server{config: cfg, store: store}
}

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
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/messages/", s.handleMessage)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, indexHTML)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		result, err := s.store.List(r.Context(), mail.ListMessagesInput{Limit: 100})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, result)
	case http.MethodDelete:
		if err := s.store.DeleteAll(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	id, raw, ok := parseMessagePath(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if id == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if raw {
			rc, ok, err := s.store.GetRaw(r.Context(), id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				http.NotFound(w, r)
				return
			}
			defer rc.Close()
			w.Header().Set("Content-Type", "message/rfc822")
			io.Copy(w, rc)
			return
		}
		message, ok, err := s.store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, message)
	case http.MethodDelete:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		if err := s.store.Delete(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		methodNotAllowed(w, "GET, DELETE")
	}
}

func parseMessagePath(path string) (id string, raw bool, ok bool) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", false, false
	}
	if strings.Contains(path, "/") {
		id, suffix, found := strings.Cut(path, "/")
		return id, suffix == "raw", found && suffix == "raw"
	}
	return path, false, true
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(value)
}
