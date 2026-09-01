package backplane

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

// NodeKind identifies the role of a node in a Backplane graph.
type NodeKind string

const (
	NodeModule   NodeKind = "module"
	NodeResource NodeKind = "resource"
	NodeTopic    NodeKind = "topic"
)

// EdgeKind identifies how two declarations are connected: a resource feeding
// a module, a module publishing to a topic, a topic delivering to a declared
// subscriber, or a topic projected through a Latest.
type EdgeKind string

const (
	EdgeResource  EdgeKind = "resource"
	EdgePublish   EdgeKind = "publish"
	EdgeSubscribe EdgeKind = "subscribe"
	EdgeLatest    EdgeKind = "latest"
)

// Node is a module, caller-provided resource, or typed topic. IDs are unique
// within a graph; labels are human-readable and may repeat, for example when
// the same function is registered twice.
type Node struct {
	ID    string
	Kind  NodeKind
	Label string
}

// Edge is a directed relationship between two nodes, referenced by ID.
type Edge struct {
	From string
	To   string
	Kind EdgeKind
}

// Graph is the application topology derived from the module signatures — the
// same declarations Run executes. It is deterministic for a given Backplane
// and building it has no side effects and needs no resources.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

// Graph returns the declared topology without binding resources or starting
// any module.
func (b *Backplane) Graph() Graph {
	graph := Graph{}
	moduleIDs := make([]string, len(b.modules))
	resourceTypes := make(map[reflect.Type]struct{})
	topicTypes := make(map[reflect.Type]struct{})

	for index, m := range b.modules {
		id := fmt.Sprintf("module:%d", index)
		moduleIDs[index] = id
		graph.Nodes = append(graph.Nodes, Node{ID: id, Kind: NodeModule, Label: qualifiedModuleName(m.fn)})
		for _, p := range m.params {
			if p.kind == resourceParameter {
				resourceTypes[p.typeOf] = struct{}{}
			} else {
				topicTypes[p.topicType] = struct{}{}
			}
		}
	}

	resourceIDs := make(map[reflect.Type]string, len(resourceTypes))
	for index, resourceType := range sortedTypes(resourceTypes) {
		id := fmt.Sprintf("resource:%d", index)
		resourceIDs[resourceType] = id
		graph.Nodes = append(graph.Nodes, Node{ID: id, Kind: NodeResource, Label: resourceType.String()})
	}

	topicIDs := make(map[reflect.Type]string, len(topicTypes))
	for index, topicType := range sortedTypes(topicTypes) {
		id := fmt.Sprintf("topic:%d", index)
		topicIDs[topicType] = id
		graph.Nodes = append(graph.Nodes, Node{ID: id, Kind: NodeTopic, Label: topicType.String()})
	}

	for moduleIndex, m := range b.modules {
		moduleID := moduleIDs[moduleIndex]
		for _, p := range m.params {
			switch p.kind {
			case resourceParameter:
				graph.Edges = append(graph.Edges, Edge{From: resourceIDs[p.typeOf], To: moduleID, Kind: EdgeResource})
			case publisherParameter:
				graph.Edges = append(graph.Edges, Edge{From: moduleID, To: topicIDs[p.topicType], Kind: EdgePublish})
			case subscriberParameter:
				graph.Edges = append(graph.Edges, Edge{From: topicIDs[p.topicType], To: moduleID, Kind: EdgeSubscribe})
			case latestParameter:
				graph.Edges = append(graph.Edges, Edge{From: topicIDs[p.topicType], To: moduleID, Kind: EdgeLatest})
			}
		}
	}

	return graph
}

// Include returns the selected modules, their published topics, and every
// transitive dependency needed to feed them. Downstream consumers of the
// selected modules' outputs are deliberately excluded. Selectors may be
// package-qualified or short module names; short names must be unambiguous.
// Calling Include without selectors returns the complete graph.
func (g Graph) Include(selectors ...string) (Graph, error) {
	if len(selectors) == 0 {
		return g, nil
	}

	selected := make(map[string]bool)
	for _, selector := range selectors {
		moduleIDs, err := g.resolveModules(selector)
		if err != nil {
			return Graph{}, err
		}
		for _, id := range moduleIDs {
			selected[id] = true
		}
	}

	incoming := make(map[string][]int)
	outgoing := make(map[string][]int)
	for index, edge := range g.Edges {
		incoming[edge.To] = append(incoming[edge.To], index)
		outgoing[edge.From] = append(outgoing[edge.From], index)
	}

	keptNodes := make(map[string]bool)
	keptEdges := make(map[int]bool)
	queue := make([]string, 0, len(selected))
	for id := range selected {
		keptNodes[id] = true
		queue = append(queue, id)
	}

	visited := make(map[string]bool)
	for len(queue) > 0 {
		moduleID := queue[0]
		queue = queue[1:]
		if visited[moduleID] {
			continue
		}
		visited[moduleID] = true

		for _, edgeIndex := range incoming[moduleID] {
			edge := g.Edges[edgeIndex]
			switch edge.Kind {
			case EdgeResource:
				keptNodes[edge.From] = true
				keptEdges[edgeIndex] = true
			case EdgeSubscribe, EdgeLatest:
				keptNodes[edge.From] = true
				keptEdges[edgeIndex] = true
				for _, publisherEdgeIndex := range incoming[edge.From] {
					publisherEdge := g.Edges[publisherEdgeIndex]
					if publisherEdge.Kind != EdgePublish {
						continue
					}
					keptNodes[publisherEdge.From] = true
					keptEdges[publisherEdgeIndex] = true
					queue = append(queue, publisherEdge.From)
				}
			}
		}
	}

	for moduleID := range selected {
		for _, edgeIndex := range outgoing[moduleID] {
			edge := g.Edges[edgeIndex]
			if edge.Kind != EdgePublish {
				continue
			}
			keptNodes[edge.To] = true
			keptEdges[edgeIndex] = true
		}
	}

	result := Graph{}
	for _, node := range g.Nodes {
		if keptNodes[node.ID] {
			result.Nodes = append(result.Nodes, node)
		}
	}
	for index, edge := range g.Edges {
		if keptEdges[index] {
			result.Edges = append(result.Edges, edge)
		}
	}
	return result, nil
}

func (g Graph) resolveModules(selector string) ([]string, error) {
	var exact []string
	shortMatches := make(map[string][]string)
	var available []string
	for _, node := range g.Nodes {
		if node.Kind != NodeModule {
			continue
		}
		available = append(available, node.Label)
		if node.Label == selector {
			exact = append(exact, node.ID)
		}
		if shortModuleName(node.Label) == selector {
			shortMatches[node.Label] = append(shortMatches[node.Label], node.ID)
		}
	}
	if len(exact) > 0 {
		return exact, nil
	}
	if len(shortMatches) == 1 {
		for _, ids := range shortMatches {
			return ids, nil
		}
	}
	if len(shortMatches) > 1 {
		matches := slices.Sorted(maps.Keys(shortMatches))
		return nil, fmt.Errorf("module %q is ambiguous; matches %s", selector, strings.Join(matches, ", "))
	}
	slices.Sort(available)
	return nil, fmt.Errorf("module %q not found; available modules: %s", selector, strings.Join(available, ", "))
}

func shortModuleName(name string) string {
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		return name[dot+1:]
	}
	return name
}

func sortedTypes(set map[reflect.Type]struct{}) []reflect.Type {
	types := make([]reflect.Type, 0, len(set))
	for typeOf := range set {
		types = append(types, typeOf)
	}
	slices.SortFunc(types, func(a, b reflect.Type) int {
		return strings.Compare(typeIdentity(a), typeIdentity(b))
	})
	return types
}

func typeIdentity(typeOf reflect.Type) string {
	return typeOf.PkgPath() + "\x00" + typeOf.String()
}
