package events

import (
	"sync"
	"time"
)

// Event is a lightweight notification sent to SSE subscribers.
// Payloads must never contain credentials, authorization headers,
// message bodies, or object payloads.
type Event struct {
	Type      string         `json:"type"`
	Service   string         `json:"service"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// Publisher is the write-only side of the bus. Services and handlers
// accept this interface so they can be tested with NoopPublisher.
type Publisher interface {
	Publish(Event)
}

// Bus is a simple fan-out pub/sub bus. Publish is non-blocking;
// a subscriber whose channel is full simply drops that event.
type Bus struct {
	mu   sync.RWMutex
	subs map[uint64]*subscription
	next uint64
}

type subscription struct {
	ch     chan Event
	topics map[string]struct{} // empty == subscribe to all
}

// NewBus returns an initialized Bus.
func NewBus() *Bus {
	return &Bus{subs: map[uint64]*subscription{}}
}

// Publish fans out an event to all matching subscribers.
// If e.Timestamp is zero, it is set to time.Now().
// The call is always non-blocking.
func (b *Bus) Publish(e Event) {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		if len(sub.topics) > 0 {
			if _, ok := sub.topics[e.Service]; !ok {
				continue
			}
		}
		// Non-blocking send: drop this event for a slow subscriber.
		select {
		case sub.ch <- e:
		default:
		}
	}
}

// Subscribe returns a receive-only channel and a cancel function.
// buffer controls the channel buffer size; topics is an optional
// allowlist of service names (empty = all services).
// Calling cancel more than once is safe.
func (b *Bus) Subscribe(buffer int, topics []string) (<-chan Event, func()) {
	topicSet := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		if t != "" {
			topicSet[t] = struct{}{}
		}
	}
	ch := make(chan Event, buffer)
	sub := &subscription{ch: ch, topics: topicSet}

	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = sub
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel
}

// Emit calls p.Publish(e) only when p is non-nil.
func Emit(p Publisher, e Event) {
	if p != nil {
		p.Publish(e)
	}
}

// NoopPublisher satisfies Publisher with no side effects. Useful in tests.
type NoopPublisher struct{}

func (NoopPublisher) Publish(Event) {}
