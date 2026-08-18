package backplane

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

// NodeKind identifies the role of a node in a Backplane graph.
type NodeKind string

const (
	NodeModule   NodeKind = "module"
	NodeResource NodeKind = "resource"
	NodeTopic    NodeKind = "topic"
)

// EdgeKind identifies how two declarations are connected.
type EdgeKind string

const (
	EdgeResource  EdgeKind = "resource"
	EdgePublish   EdgeKind = "publish"
	EdgeSubscribe EdgeKind = "subscribe"
	EdgeLatest    EdgeKind = "latest"
)

// Node is a module, caller-provided resource, or typed topic.
type Node struct {
	ID    string
	Kind  NodeKind
	Label string
}

// Edge is a directed relationship between two nodes.
type Edge struct {
	From string
	To   string
	Kind EdgeKind
}

// Graph is the side-effect-free topology derived from module signatures.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

// Graph returns the declared topology without binding resources or starting modules.
func (b *Backplane) Graph() Graph {
	graph := Graph{}
	moduleIDs := make([]string, len(b.modules))
	resourceTypes := make(map[reflect.Type]struct{})
	topicTypes := make(map[reflect.Type]struct{})

	for index, m := range b.modules {
		id := fmt.Sprintf("module:%d", index)
		moduleIDs[index] = id
		graph.Nodes = append(graph.Nodes, Node{
			ID:    id,
			Kind:  NodeModule,
			Label: moduleName(m.fn),
		})
		for _, p := range m.params {
			if p.kind == resourceParameter {
				resourceTypes[p.typeOf] = struct{}{}
			} else {
				topicTypes[p.topicType] = struct{}{}
			}
		}
	}

	resources := sortedTypes(resourceTypes)
	resourceIDs := make(map[reflect.Type]string, len(resources))
	for index, resourceType := range resources {
		id := fmt.Sprintf("resource:%d", index)
		resourceIDs[resourceType] = id
		graph.Nodes = append(graph.Nodes, Node{ID: id, Kind: NodeResource, Label: resourceType.String()})
	}

	topics := sortedTypes(topicTypes)
	topicIDs := make(map[reflect.Type]string, len(topics))
	for index, topicType := range topics {
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

// Mermaid renders the graph as a Mermaid flowchart.
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

func moduleName(function reflect.Value) string {
	definition := runtime.FuncForPC(function.Pointer())
	if definition == nil {
		return function.Type().String()
	}

	name := strings.TrimSuffix(definition.Name(), "-fm")
	if slash := strings.LastIndex(name, "/"); slash >= 0 {
		name = name[slash+1:]
	}
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	return name
}

func sortedTypes(set map[reflect.Type]struct{}) []reflect.Type {
	types := make([]reflect.Type, 0, len(set))
	for typeOf := range set {
		types = append(types, typeOf)
	}
	sort.Slice(types, func(i, j int) bool {
		return typeIdentity(types[i]) < typeIdentity(types[j])
	})
	return types
}

func typeIdentity(typeOf reflect.Type) string {
	return typeOf.PkgPath() + "\x00" + typeOf.String()
}

func escapeMermaid(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return strings.ReplaceAll(value, "\n", " ")
}
