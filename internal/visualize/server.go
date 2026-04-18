package visualize

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/higino/eulantir/internal/engine"
)

// statusEvent is the JSON payload sent to SSE clients on each task completion.
type statusEvent struct {
	NodeID string           `json:"nodeID"`
	Status engine.RunStatus `json:"status"`
}

// Server serves the pipeline visualization dashboard and streams live task
// status updates to connected browsers over Server-Sent Events.
type Server struct {
	mu        sync.RWMutex
	graph     GraphData
	clientsMu sync.Mutex
	clients   map[chan string]struct{}
}

// NewServer creates a Server backed by the given graph.
func NewServer(graph GraphData) *Server {
	return &Server{
		graph:   graph,
		clients: make(map[chan string]struct{}),
	}
}

// Push updates the in-memory graph and broadcasts a status event to all
// connected SSE clients. Safe to call from multiple goroutines.
func (s *Server) Push(result engine.TaskResult) {
	s.mu.Lock()
	s.graph.ApplyResult(result)
	s.mu.Unlock()

	payload, _ := json.Marshal(statusEvent{NodeID: result.NodeID, Status: result.Status})

	s.clientsMu.Lock()
	for ch := range s.clients {
		select {
		case ch <- string(payload):
		default: // drop if the client is too slow
		}
	}
	s.clientsMu.Unlock()
}

// Done sends a sentinel "done" event so the browser can close the SSE stream.
func (s *Server) Done() {
	s.clientsMu.Lock()
	for ch := range s.clients {
		select {
		case ch <- `{"status":"done"}`:
		default:
		}
	}
	s.clientsMu.Unlock()
}

// ServeHTTP implements http.Handler. Routes: / → dashboard, /events → SSE.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/events":
		s.handleSSE(w, r)
	default:
		s.handleIndex(w, r)
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	g := s.graph
	s.mu.RUnlock()

	html, err := RenderHTML(g, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(html)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering if present

	ch := make(chan string, 32)
	s.clientsMu.Lock()
	s.clients[ch] = struct{}{}
	s.clientsMu.Unlock()

	defer func() {
		s.clientsMu.Lock()
		delete(s.clients, ch)
		s.clientsMu.Unlock()
	}()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
