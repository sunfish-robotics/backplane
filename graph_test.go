package backplane

import (
	"context"
	"strings"
	"testing"
)

type graphStore interface {
	Save(string) error
}

type graphQueueChanged struct{}
type graphAssignmentReady struct{}

func graphSyncBackend(context.Context, graphStore, chan<- graphQueueChanged) error {
	return nil
}

func graphScheduleJobs(context.Context, graphStore, <-chan graphQueueChanged, chan<- graphAssignmentReady) error {
	return nil
}

func graphServeHTTP(context.Context, *Latest[graphQueueChanged], <-chan graphAssignmentReady) error {
	return nil
}

func TestGraphComesFromDeclarationsWithoutResources(t *testing.T) {
	application, err := New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	graph := application.Graph()
	if len(graph.Nodes) != 6 {
		t.Fatalf("Graph() returned %d nodes, want 6: %#v", len(graph.Nodes), graph.Nodes)
	}
	if len(graph.Edges) != 7 {
		t.Fatalf("Graph() returned %d edges, want 7: %#v", len(graph.Edges), graph.Edges)
	}

	store := findNode(t, graph, NodeResource, "backplane.graphStore")
	syncBackend := findNode(t, graph, NodeModule, "graphSyncBackend")
	queueChanged := findNode(t, graph, NodeTopic, "backplane.graphQueueChanged")
	scheduleJobs := findNode(t, graph, NodeModule, "graphScheduleJobs")
	serveHTTP := findNode(t, graph, NodeModule, "graphServeHTTP")

	assertEdge(t, graph, store.ID, syncBackend.ID, EdgeResource)
	assertEdge(t, graph, syncBackend.ID, queueChanged.ID, EdgePublish)
	assertEdge(t, graph, queueChanged.ID, scheduleJobs.ID, EdgeSubscribe)
	assertEdge(t, graph, queueChanged.ID, serveHTTP.ID, EdgeLatest)
}

func TestGraphRendersMermaid(t *testing.T) {
	application, err := New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mermaid := application.Graph().Mermaid()
	for _, want := range []string{
		"flowchart LR",
		"graphSyncBackend",
		"graphScheduleJobs",
		"graphServeHTTP",
		"backplane.graphStore",
		"backplane.graphQueueChanged",
		"latest",
	} {
		if !strings.Contains(mermaid, want) {
			t.Fatalf("Mermaid() output does not contain %q:\n%s", want, mermaid)
		}
	}
}

func findNode(t *testing.T, graph Graph, kind NodeKind, label string) Node {
	t.Helper()
	for _, node := range graph.Nodes {
		if node.Kind == kind && node.Label == label {
			return node
		}
	}
	t.Fatalf("node %s %q not found in %#v", kind, label, graph.Nodes)
	return Node{}
}

func assertEdge(t *testing.T, graph Graph, from, to string, kind EdgeKind) {
	t.Helper()
	for _, edge := range graph.Edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return
		}
	}
	t.Fatalf("edge %s -%s-> %s not found in %#v", from, kind, to, graph.Edges)
}
