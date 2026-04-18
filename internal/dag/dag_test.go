package dag

import (
	"testing"
)

func TestTopologicalSort_Linear(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "a"})
	d.AddNode(Node{ID: "b"})
	d.AddNode(Node{ID: "c"})
	d.AddEdge("a", "b")
	d.AddEdge("b", "c")

	sorted, err := d.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(sorted))
	}
	// a must come before b, b before c
	pos := make(map[string]int)
	for i, n := range sorted {
		pos[n.ID] = i
	}
	if pos["a"] >= pos["b"] {
		t.Errorf("expected a before b")
	}
	if pos["b"] >= pos["c"] {
		t.Errorf("expected b before c")
	}
}

func TestTopologicalSort_Diamond(t *testing.T) {
	//   a
	//  / \
	// b   c
	//  \ /
	//   d
	d := New()
	for _, id := range []string{"a", "b", "c", "d"} {
		d.AddNode(Node{ID: id})
	}
	d.AddEdge("a", "b")
	d.AddEdge("a", "c")
	d.AddEdge("b", "d")
	d.AddEdge("c", "d")

	sorted, err := d.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pos := make(map[string]int)
	for i, n := range sorted {
		pos[n.ID] = i
	}
	if pos["a"] >= pos["b"] || pos["a"] >= pos["c"] {
		t.Errorf("a must come before b and c")
	}
	if pos["b"] >= pos["d"] || pos["c"] >= pos["d"] {
		t.Errorf("b and c must come before d")
	}
}

func TestTopologicalSort_Cycle(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "a"})
	d.AddNode(Node{ID: "b"})
	d.AddNode(Node{ID: "c"})
	d.AddEdge("a", "b")
	d.AddEdge("b", "c")
	d.AddEdge("c", "a") // cycle

	_, err := d.TopologicalSort()
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestTopologicalSort_SingleNode(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "only"})

	sorted, err := d.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 1 || sorted[0].ID != "only" {
		t.Errorf("unexpected result: %+v", sorted)
	}
}

func TestAddNode_Duplicate(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "a"})
	if err := d.AddNode(Node{ID: "a"}); err == nil {
		t.Fatal("expected error for duplicate node, got nil")
	}
}

func TestPredecessors(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "a"})
	d.AddNode(Node{ID: "b"})
	d.AddNode(Node{ID: "c"})
	d.AddEdge("a", "c")
	d.AddEdge("b", "c")

	preds := d.Predecessors("c")
	if len(preds) != 2 {
		t.Errorf("expected 2 predecessors of c, got %d", len(preds))
	}
}

func TestSuccessors(t *testing.T) {
	d := New()
	d.AddNode(Node{ID: "a"})
	d.AddNode(Node{ID: "b"})
	d.AddNode(Node{ID: "c"})
	d.AddEdge("a", "b")
	d.AddEdge("a", "c")

	succs := d.Successors("a")
	if len(succs) != 2 {
		t.Errorf("expected 2 successors of a, got %d", len(succs))
	}
}
