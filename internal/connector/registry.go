package connector

import (
	"context"
	"fmt"
	"sort"
)

// SourceFactory creates a Source from a raw config map.
type SourceFactory func(ctx context.Context, cfg map[string]any) (Source, error)

// SinkFactory creates a Sink from a raw config map.
type SinkFactory func(ctx context.Context, cfg map[string]any) (Sink, error)

// ConnectorInfo describes a registered connector type.
type ConnectorInfo struct {
	Type        string
	Description string
	ConfigKeys  []string
}

// Registry holds all registered source and sink factories.
type Registry struct {
	sources map[string]SourceFactory
	sinks   map[string]SinkFactory
	info    map[string]ConnectorInfo
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		sources: make(map[string]SourceFactory),
		sinks:   make(map[string]SinkFactory),
		info:    make(map[string]ConnectorInfo),
	}
}

// RegisterSource registers a source factory under the given type name.
func (r *Registry) RegisterSource(info ConnectorInfo, fn SourceFactory) {
	r.sources[info.Type] = fn
	r.info[info.Type] = info
}

// RegisterSink registers a sink factory under the given type name.
func (r *Registry) RegisterSink(info ConnectorInfo, fn SinkFactory) {
	r.sinks[info.Type] = fn
	r.info[info.Type] = info
}

// BuildSource instantiates a Source for the given type and config.
func (r *Registry) BuildSource(typ string, cfg map[string]any) (Source, error) {
	factory, ok := r.sources[typ]
	if !ok {
		return nil, fmt.Errorf("no source connector registered for type %q", typ)
	}
	src, err := factory(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("init source %q: %w", typ, err)
	}
	return src, nil
}

// BuildSink instantiates a Sink for the given type and config.
func (r *Registry) BuildSink(typ string, cfg map[string]any) (Sink, error) {
	factory, ok := r.sinks[typ]
	if !ok {
		return nil, fmt.Errorf("no sink connector registered for type %q", typ)
	}
	sink, err := factory(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("init sink %q: %w", typ, err)
	}
	return sink, nil
}

// IsSink reports whether the given connector type is registered as a sink.
func (r *Registry) IsSink(typ string) bool {
	_, ok := r.sinks[typ]
	return ok
}

// List returns all registered connector types sorted alphabetically.
func (r *Registry) List() []ConnectorInfo {
	list := make([]ConnectorInfo, 0, len(r.info))
	for _, info := range r.info {
		list = append(list, info)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Type < list[j].Type })
	return list
}

// Default is the process-wide connector registry.
// Built-in connectors register themselves via init() in their packages.
var Default = NewRegistry()
