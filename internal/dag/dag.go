package dag

import "errors"

// Node is a single task in the pipeline DAG.
type Node struct {
	ID           string
	ConnectorRef string // name of the ConnectorConfig this node uses
	TransformRef string // optional: name of the TransformConfig to apply
	DependsOn    []string
}

// DAG is a directed acyclic graph of pipeline nodes.
type DAG struct {
	nodes map[string]Node
	edges map[string][]string // node ID → list of successor IDs
}

// New returns an empty DAG.
func New() *DAG {
	return &DAG{
		nodes: make(map[string]Node),
		edges: make(map[string][]string),
	}
}

// AddNode registers a node. Returns an error if the ID is already present.
func (d *DAG) AddNode(n Node) error {
	if _, exists := d.nodes[n.ID]; exists {
		return errors.New("duplicate node id: " + n.ID)
	}
	d.nodes[n.ID] = n
	if _, ok := d.edges[n.ID]; !ok {
		d.edges[n.ID] = nil
	}
	return nil
}

// AddEdge records that `from` must complete before `to` starts.
func (d *DAG) AddEdge(fromID, toID string) error {
	if _, ok := d.nodes[fromID]; !ok {
		return errors.New("unknown node: " + fromID)
	}
	if _, ok := d.nodes[toID]; !ok {
		return errors.New("unknown node: " + toID)
	}
	d.edges[fromID] = append(d.edges[fromID], toID)
	return nil
}

// TopologicalSort returns nodes in execution order using Kahn's algorithm.
// Returns an error if the graph contains a cycle.
func (d *DAG) TopologicalSort() ([]Node, error) {
	inDegree := make(map[string]int, len(d.nodes))
	for id := range d.nodes {
		inDegree[id] = 0
	}
	for _, successors := range d.edges {
		for _, s := range successors {
			inDegree[s]++
		}
	}

	// seed queue with nodes that have no dependencies
	queue := []string{}
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []Node
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		sorted = append(sorted, d.nodes[id])

		for _, successor := range d.edges[id] {
			inDegree[successor]--
			if inDegree[successor] == 0 {
				queue = append(queue, successor)
			}
		}
	}

	if len(sorted) != len(d.nodes) {
		return nil, errors.New("pipeline DAG contains a cycle — check depends_on fields")
	}
	return sorted, nil
}

// Predecessors returns nodes that must complete before nodeID.
func (d *DAG) Predecessors(nodeID string) []Node {
	var preds []Node
	for id, successors := range d.edges {
		for _, s := range successors {
			if s == nodeID {
				preds = append(preds, d.nodes[id])
				break
			}
		}
	}
	return preds
}

// Successors returns nodes that depend on nodeID.
func (d *DAG) Successors(nodeID string) []Node {
	var succs []Node
	for _, id := range d.edges[nodeID] {
		succs = append(succs, d.nodes[id])
	}
	return succs
}
