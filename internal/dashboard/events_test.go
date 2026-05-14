package dashboard

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"devcloud/internal/events"
)

type sseTextLine struct{ text string }

func newEventTestServer(t *testing.T) (*Server, *events.Bus) {
	t.Helper()
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	bus := events.NewBus()
	server.SetEventBus(bus)
	return server, bus
}

// waitForLine reads from lines until a line containing substr is found or timeout elapses.
func waitForLine(t *testing.T, lines <-chan sseTextLine, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("SSE stream closed before line containing %q arrived", substr)
			}
			if strings.Contains(l.text, substr) {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for line containing %q", substr)
		}
	}
}

// sseLines opens a GET request to url, reads SSE lines into a channel, and
// stops when ctx is cancelled. The channel is closed when the goroutine exits.
func sseLines(ctx context.Context, url string) <-chan sseTextLine {
	ch := make(chan sseTextLine, 64)
	go func() {
		defer close(ch)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			select {
			case ch <- sseTextLine{sc.Text()}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func TestHandleEventsNoBus(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	// No event bus set → 503.
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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	lines := sseLines(ctx, ts.URL+"/api/events")

	// Wait for the "ready" event line.
	waitForLine(t, lines, "event: ready", 3*time.Second)

	// Publish an event through the bus.
	bus.Publish(events.Event{
		Type:    "s3.object.put",
		Service: "s3",
		Payload: map[string]any{"bucket": "my-bucket", "key": "foo.txt"},
	})

	// Wait for the "event: s3" line.
	waitForLine(t, lines, "event: s3", 3*time.Second)

	// Cancel SSE connection before closing the server so ts.Close() doesn't hang.
	cancel()
}

func TestHandleEventsTopicFilter(t *testing.T) {
	server, bus := newEventTestServer(t)
	ts := httptest.NewServer(server.routes())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	lines := sseLines(ctx, ts.URL+"/api/events?topics=mail")

	// Drain the ready event.
	waitForLine(t, lines, "event: ready", 3*time.Second)

	// Publish an S3 event — must NOT arrive (filtered out).
	bus.Publish(events.Event{Type: "s3.object.put", Service: "s3"})
	// Publish a mail event — MUST arrive.
	bus.Publish(events.Event{Type: "mail.received", Service: "mail"})

	deadline := time.After(3 * time.Second)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("SSE stream closed before mail event arrived")
			}
			if strings.Contains(l.text, "event: s3") {
				t.Fatal("received s3 event despite mail-only topic filter")
			}
			if strings.Contains(l.text, "event: mail") {
				cancel() // clean up SSE before ts.Close()
				return   // success
			}
		case <-deadline:
			t.Fatal("timeout waiting for mail event")
		}
	}
}
