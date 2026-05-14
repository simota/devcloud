package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// handleEvents upgrades the request to a WebSocket and streams events from
// the in-process bus to the client. We chose WebSocket over SSE so the long
// lived stream doesn't consume one of the browser's six HTTP/1.1 connection
// slots and doesn't keep the page in a perpetual "loading" state for
// devtools / lighthouse instrumentation.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.eventBus == nil {
		http.Error(w, "event bus not initialised", http.StatusServiceUnavailable)
		return
	}

	// Optional topic filter from query string (kept for parity with the
	// previous SSE handler so existing test fixtures continue to work).
	var topics []string
	if raw := r.URL.Query().Get("topics"); raw != "" {
		for _, t := range strings.Split(raw, ",") {
			if t = strings.TrimSpace(t); t != "" {
				topics = append(topics, t)
			}
		}
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// devcloud only ever serves the dashboard on localhost, so the
		// browser's same-origin check is the only auth surface we need.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	ctx, cancelCtx := context.WithCancel(r.Context())
	defer cancelCtx()

	// Drain client-sent frames so close frames are observed promptly.
	go func() {
		for {
			if _, _, err := conn.Reader(ctx); err != nil {
				cancelCtx()
				return
			}
		}
	}()

	ch, cancelSub := s.eventBus.Subscribe(64, topics)
	defer cancelSub()

	// Initial ready frame lets the client confirm the stream is live.
	if err := writeWebsocketJSON(ctx, conn, map[string]any{"type": "ready"}); err != nil {
		return
	}

	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "bye")
			return
		case <-pingTicker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				return
			}
		case e, ok := <-ch:
			if !ok {
				return
			}
			if err := writeWebsocketJSON(ctx, conn, e); err != nil {
				return
			}
		}
	}
}

func writeWebsocketJSON(ctx context.Context, c *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Write(writeCtx, websocket.MessageText, data); err != nil {
		// A normal close while we're mid-write is expected; report only
		// genuine I/O failures.
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}
