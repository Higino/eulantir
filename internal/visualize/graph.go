package visualize

import (
	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/engine"
)

// NodeKind classifies a task by its role in the pipeline.
type NodeKind string

const (
	KindSource    NodeKind = "source"
	KindSink      NodeKind = "sink"
	KindTransform NodeKind = "transform"
)

// GraphNode is one task node in the visualization.
type GraphNode struct {
	ID     string           `json:"id"`
	Label  string           `json:"label"`
	Kind   NodeKind         `json:"kind"`
	Status engine.RunStatus `json:"status"`
}

// GraphEdge is a directed dependency between two task nodes.
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// GraphData is the complete graph representation passed to the HTML template.
type GraphData struct {
	PipelineName string      `json:"pipelineName"`
	Nodes        []GraphNode `json:"nodes"`
	Edges        []GraphEdge `json:"edges"`
}

// sinkTypes mirrors the heuristic in engine/executor.go.
var sinkTypes = map[string]bool{
	"postgres": true, "csv-sink": true, "kafka-sink": true, "s3-sink": true,
}

// Build converts a PipelineConfig into a GraphData with all nodes in StatusPending.
func Build(cfg *config.PipelineConfig) GraphData {
	connTypes := make(map[string]string, len(cfg.Connectors))
	for _, c := range cfg.Connectors {
		connTypes[c.Name] = c.Type
	}

	nodes := make([]GraphNode, 0, len(cfg.Tasks))
	var edges []GraphEdge

	for _, t := range cfg.Tasks {
		kind := KindTransform
		label := t.ID
		if t.Connector != "" {
			ct := connTypes[t.Connector]
			if sinkTypes[ct] {
				kind = KindSink
			} else {
				kind = KindSource
			}
			label = t.ID + "\n[" + ct + "]"
		} else if t.Transform != "" {
			label = t.ID + "\n[transform]"
		}

		nodes = append(nodes, GraphNode{
			ID:     t.ID,
			Label:  label,
			Kind:   kind,
			Status: engine.StatusPending,
		})

		for _, dep := range t.DependsOn {
			edges = append(edges, GraphEdge{From: dep, To: t.ID})
		}
	}

	return GraphData{
		PipelineName: cfg.Name,
		Nodes:        nodes,
		Edges:        edges,
	}
}

// ApplyResult updates the status of the matching node in-place.
func (g *GraphData) ApplyResult(result engine.TaskResult) {
	for i, n := range g.Nodes {
		if n.ID == result.NodeID {
			g.Nodes[i].Status = result.Status
			return
		}
	}
}
