package events_test

import (
	"testing"
	"time"

	"devcloud/internal/events"
)

func TestBusPublishSubscribe(t *testing.T) {
	bus := events.NewBus()
	ch, cancel := bus.Subscribe(8, nil)
	defer cancel()

	e := events.Event{Type: "mail.received", Service: "mail"}
	bus.Publish(e)

	select {
	case got := <-ch:
		if got.Type != e.Type {
			t.Fatalf("expected type %q, got %q", e.Type, got.Type)
		}
		if got.Service != e.Service {
			t.Fatalf("expected service %q, got %q", e.Service, got.Service)
		}
		if got.Timestamp.IsZero() {
			t.Fatal("expected non-zero timestamp")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestBusTopicFilter(t *testing.T) {
	bus := events.NewBus()

	// Subscribe only to "s3"
	ch, cancel := bus.Subscribe(8, []string{"s3"})
	defer cancel()

	// Publish a "mail" event — should not arrive.
	bus.Publish(events.Event{Type: "mail.received", Service: "mail"})
	// Publish an "s3" event — should arrive.
	bus.Publish(events.Event{Type: "s3.object.put", Service: "s3"})

	select {
	case got := <-ch:
		if got.Service != "s3" {
			t.Fatalf("expected s3 event, got service=%q", got.Service)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for s3 event")
	}

	// Ensure no second event arrives.
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra event: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBusCancel(t *testing.T) {
	bus := events.NewBus()
	_, cancel := bus.Subscribe(8, nil)
	// Cancel twice — must not panic.
	cancel()
	cancel()

	// After cancel, publish should not block or panic.
	bus.Publish(events.Event{Type: "test", Service: "test"})
}

func TestBusBufferOverflowDrop(t *testing.T) {
	bus := events.NewBus()
	// Buffer size 1.
	ch, cancel := bus.Subscribe(1, nil)
	defer cancel()

	// Publish 3 events; the second and third should be dropped silently.
	for i := 0; i < 3; i++ {
		bus.Publish(events.Event{Type: "s3.object.put", Service: "s3"})
	}

	// At most 1 event should be in the channel.
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			if count > 1 {
				t.Fatalf("expected at most 1 buffered event, got %d", count)
			}
			return
		}
	}
}

func TestEmitNilSafe(t *testing.T) {
	// Must not panic when publisher is nil.
	events.Emit(nil, events.Event{Type: "test", Service: "test"})
}

func TestNoopPublisher(t *testing.T) {
	var p events.NoopPublisher
	// Must not panic.
	p.Publish(events.Event{Type: "test", Service: "test"})
}
