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
	mu      sync.Mutex
	graph   GraphData
	clients map[chan string]struct{}
}

// NewServer creates a Server backed by the given graph.
func NewServer(graph GraphData) *Server {
	return &Server{
		graph:   graph,
		clients: make(map[chan string]struct{}),
	}
}

// Push updates the in-memory graph and broadcasts a status event to all
// connected SSE clients atomically. Safe to call from multiple goroutines.
func (s *Server) Push(result engine.TaskResult) {
	payload, _ := json.Marshal(statusEvent{NodeID: result.NodeID, Status: result.Status})

	s.mu.Lock()
	s.graph.ApplyResult(result)
	for ch := range s.clients {
		select {
		case ch <- string(payload):
		default: // drop if the client is too slow
		}
	}
	s.mu.Unlock()
}

// Done sends a sentinel "done" event so the browser can close the SSE stream.
func (s *Server) Done() {
	s.mu.Lock()
	for ch := range s.clients {
		select {
		case ch <- `{"status":"done"}`:
		default:
		}
	}
	s.mu.Unlock()
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
	s.mu.Lock()
	g := s.graph
	s.mu.Unlock()

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

	// Replay current node states and register the client atomically so no
	// Push() event can slip between replay and registration.
	s.mu.Lock()
	for _, n := range s.graph.Nodes {
		if payload, err := json.Marshal(statusEvent{NodeID: n.ID, Status: n.Status}); err == nil {
			ch <- string(payload)
		}
	}
	s.clients[ch] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
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
