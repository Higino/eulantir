package dag

import (
	"fmt"

	"github.com/higino/eulantir/internal/config"
)

// Build constructs and validates a DAG from a slice of TaskConfigs.
// It adds all nodes first, then all edges, then runs a topological sort
// to confirm there are no cycles. Returns the sorted execution order.
func Build(tasks []config.TaskConfig) (*DAG, []Node, error) {
	d := New()

	// pass 1: add all nodes
	for _, t := range tasks {
		node := Node{
			ID:           t.ID,
			ConnectorRef: t.Connector,
			TransformRef: t.Transform,
			DependsOn:    t.DependsOn,
		}
		if err := d.AddNode(node); err != nil {
			return nil, nil, fmt.Errorf("add node: %w", err)
		}
	}

	// pass 2: add edges (dependency → dependent)
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			// edge goes from dep → t.ID (dep must finish before t starts)
			if err := d.AddEdge(dep, t.ID); err != nil {
				return nil, nil, fmt.Errorf("add edge %s→%s: %w", dep, t.ID, err)
			}
		}
	}

	// pass 3: topological sort — also detects cycles
	sorted, err := d.TopologicalSort()
	if err != nil {
		return nil, nil, err
	}

	return d, sorted, nil
}
