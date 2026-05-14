package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"devcloud/internal/events"
)

func newEventTestServer(t *testing.T) (*Server, *events.Bus) {
	t.Helper()
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	bus := events.NewBus()
	server.SetEventBus(bus)
	return server, bus
}

func dialEventsWS(t *testing.T, ctx context.Context, baseURL, query string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(baseURL, "http") + "/api/events"
	if query != "" {
		url += "?" + query
	}
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	return conn
}

func readEventFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) map[string]any {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("websocket read: %v", err)
	}
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("websocket frame %q is not JSON: %v", data, err)
	}
	return frame
}

func TestHandleEventsNoBus(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	// No event bus set → 503 over plain HTTP, no upgrade.
	rec := performRequest(server.routes(), http.MethodGet, "/api/events")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleEventsMethodNotAllowed(t *testing.T) {
	server, _ := newEventTestServer(t)
	rec := performRequest(server.routes(), http.MethodPost, "/api/events")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleEventsReadyAndEvent(t *testing.T) {
	server, bus := newEventTestServer(t)
	ts := httptest.NewServer(server.routes())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn := dialEventsWS(t, ctx, ts.URL, "")
	t.Cleanup(func() { conn.CloseNow() })

	// First frame is the ready marker.
	ready := readEventFrame(t, ctx, conn)
	if ready["type"] != "ready" {
		t.Fatalf("expected ready frame, got %v", ready)
	}

	bus.Publish(events.Event{
		Type:    "s3.object.put",
		Service: "s3",
		Payload: map[string]any{"bucket": "my-bucket", "key": "foo.txt"},
	})

	frame := readEventFrame(t, ctx, conn)
	if frame["service"] != "s3" || frame["type"] != "s3.object.put" {
		t.Fatalf("expected s3.object.put frame, got %v", frame)
	}
}

func TestHandleEventsCleansUpOnClientDisconnect(t *testing.T) {
	server, bus := newEventTestServer(t)
	ts := httptest.NewServer(server.routes())
	t.Cleanup(ts.Close)

	if got := bus.SubscriberCount(); got != 0 {
		t.Fatalf("expected 0 subscribers before connect, got %d", got)
	}

	// Simulate the user rapidly switching service pages: open and tear
	// down several WebSocket connections in quick succession.
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn := dialEventsWS(t, ctx, ts.URL, "")
		readEventFrame(t, ctx, conn) // drain "ready"
		conn.Close(websocket.StatusNormalClosure, "")
		cancel()
	}

	// After all clients have disconnected, the bus must drain back to zero.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got := bus.SubscriberCount(); got == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscribers leaked after disconnect: %d still registered", bus.SubscriberCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHandleEventsTopicFilter(t *testing.T) {
	server, bus := newEventTestServer(t)
	ts := httptest.NewServer(server.routes())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn := dialEventsWS(t, ctx, ts.URL, "topics=mail")
	t.Cleanup(func() { conn.CloseNow() })

	ready := readEventFrame(t, ctx, conn)
	if ready["type"] != "ready" {
		t.Fatalf("expected ready frame, got %v", ready)
	}

	// Publish an S3 event — must NOT arrive (filtered server-side).
	bus.Publish(events.Event{Type: "s3.object.put", Service: "s3"})
	// Publish a mail event — MUST arrive.
	bus.Publish(events.Event{Type: "mail.received", Service: "mail"})

	frame := readEventFrame(t, ctx, conn)
	if frame["service"] != "mail" {
		t.Fatalf("expected mail event after filter, got %v", frame)
	}
}
