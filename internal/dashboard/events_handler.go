package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.eventBus == nil {
		http.Error(w, "event bus not initialised", http.StatusServiceUnavailable)
		return
	}

	// Parse topic filter from query string.
	var topics []string
	if raw := r.URL.Query().Get("topics"); raw != "" {
		for _, t := range strings.Split(raw, ",") {
			if t = strings.TrimSpace(t); t != "" {
				topics = append(topics, t)
			}
		}
	}

	// Set SSE response headers.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flush := func() {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	// Send initial "ready" event so the client knows the stream is live.
	fmt.Fprint(w, "event: ready\ndata: {}\n\n")
	flush()

	ch, cancel := s.eventBus.Subscribe(64, topics)
	defer cancel()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flush()
		case e, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(e)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Service, data)
			flush()
		}
	}
}
