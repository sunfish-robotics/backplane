package backplane

import (
	"fmt"
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
		graph.Nodes = append(graph.Nodes, Node{ID: id, Kind: NodeModule, Label: m.name})
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

// Mermaid renders the graph as a Mermaid flowchart: modules as rectangles,
// resources as cylinders, topics as hexagons, with latest-wins projections
// labelled on their edges.
func (g Graph) Mermaid() string {
	var output strings.Builder
	output.WriteString("flowchart LR\n")

	mermaidIDs := make(map[string]string, len(g.Nodes))
	for index, node := range g.Nodes {
		id := fmt.Sprintf("n%d", index)
		mermaidIDs[node.ID] = id
		label := escapeMermaid(node.Label)
		switch node.Kind {
		case NodeModule:
			fmt.Fprintf(&output, "  %s[\"%s\"]\n", id, label)
		case NodeResource:
			fmt.Fprintf(&output, "  %s[(\"%s\")]\n", id, label)
		case NodeTopic:
			fmt.Fprintf(&output, "  %s{{\"%s\"}}\n", id, label)
		}
	}

	for _, edge := range g.Edges {
		from := mermaidIDs[edge.From]
		to := mermaidIDs[edge.To]
		if edge.Kind == EdgeLatest {
			fmt.Fprintf(&output, "  %s -->|latest| %s\n", from, to)
		} else {
			fmt.Fprintf(&output, "  %s --> %s\n", from, to)
		}
	}

	return output.String()
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

// escapeMermaid neutralises characters that break quoted Mermaid labels.
// Mermaid uses HTML-entity style escapes, not backslashes; '#' is escaped
// first so the replacement codes themselves survive.
func escapeMermaid(value string) string {
	value = strings.ReplaceAll(value, "#", "#35;")
	value = strings.ReplaceAll(value, "\\", "#92;")
	value = strings.ReplaceAll(value, "\"", "#quot;")
	return strings.ReplaceAll(value, "\n", " ")
}
