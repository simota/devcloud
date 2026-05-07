package dashboard

import (
	"io"
	"net/http"
	"strings"

	"devcloud/internal/services/mail"
)

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
