package server

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
)

// Event is one named SSE message the dashboard's HTMX SSE extension
// listens for (`hx-trigger="sse:account-update"` etc.). Data is the
// already-rendered HTML fragment HTMX will swap into the DOM.
type Event struct {
	Name string
	Data []byte
}

// Broadcaster fans out events to any number of subscribed SSE
// connections. Drop policy is "slow subscriber loses messages, never
// blocks the broadcaster" — every subscriber has a small buffered
// channel; pushes that would block the channel skip that subscriber.
type Broadcaster struct {
	mu          sync.RWMutex
	subscribers map[uint64]chan Event
	nextID      atomic.Uint64
}

// NewBroadcaster returns an empty broadcaster ready for use.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subscribers: make(map[uint64]chan Event)}
}

// Subscribe registers a new listener. The returned cancel closes the
// channel and removes the subscriber; call it from a defer in the
// SSE handler so disconnects are cleaned up promptly.
func (b *Broadcaster) Subscribe() (<-chan Event, func()) {
	id := b.nextID.Add(1)
	ch := make(chan Event, 16)
	b.mu.Lock()
	b.subscribers[id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if existing, ok := b.subscribers[id]; ok {
			delete(b.subscribers, id)
			close(existing)
		}
		b.mu.Unlock()
	}
}

// Broadcast sends ev to every subscriber. Slow subscribers whose
// buffer is full silently drop the event.
func (b *Broadcaster) Broadcast(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// Subscriber too slow — drop.
		}
	}
}

// Len reports the number of active subscribers (for diagnostics).
func (b *Broadcaster) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

// ServeSSE implements GET /api/events. It writes Content-Type +
// no-cache, flushes immediately so the browser sees the open, then
// forwards every event from the subscription channel as
// `event: <name>\ndata: <data>\n\n`. Returns cleanly when the client
// disconnects or the context is cancelled.
func (b *Broadcaster) ServeSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Comment frame so the EventSource fires onopen immediately even
	// before any real event lands.
	_, _ = w.Write([]byte(":hello\n\n"))
	flusher.Flush()

	sub, cancel := b.Subscribe()
	defer cancel()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub:
			if !ok {
				return
			}
			if _, err := w.Write([]byte("event: " + ev.Name + "\ndata: ")); err != nil {
				return
			}
			if _, err := w.Write(ev.Data); err != nil {
				return
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// guardSSE returns errClientGone if r.Context is already cancelled.
// Currently unused but kept for future health probes.
var errClientGone = errors.New("server: SSE client gone")

func clientStillConnected(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errClientGone
	default:
		return nil
	}
}

var _ = clientStillConnected // tucked away for later use
