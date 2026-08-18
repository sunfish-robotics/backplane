package backplane_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Michael-F-Bryan/backplane"
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

func graphServeHTTP(context.Context, *backplane.Latest[graphQueueChanged], <-chan graphAssignmentReady) error {
	return nil
}

func TestGraphComesFromDeclarationsWithoutResources(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
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

	store := findNode(t, graph, backplane.NodeResource, "backplane_test.graphStore")
	syncBackend := findNode(t, graph, backplane.NodeModule, "graphSyncBackend")
	queueChanged := findNode(t, graph, backplane.NodeTopic, "backplane_test.graphQueueChanged")
	scheduleJobs := findNode(t, graph, backplane.NodeModule, "graphScheduleJobs")
	serveHTTP := findNode(t, graph, backplane.NodeModule, "graphServeHTTP")

	assertEdge(t, graph, store.ID, syncBackend.ID, backplane.EdgeResource)
	assertEdge(t, graph, syncBackend.ID, queueChanged.ID, backplane.EdgePublish)
	assertEdge(t, graph, queueChanged.ID, scheduleJobs.ID, backplane.EdgeSubscribe)
	assertEdge(t, graph, queueChanged.ID, serveHTTP.ID, backplane.EdgeLatest)
}

func TestGraphIsDeterministic(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, second := application.Graph(), application.Graph()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Graph() is not deterministic:\n%#v\n%#v", first, second)
	}
	if first.Mermaid() != second.Mermaid() {
		t.Fatal("Mermaid() is not deterministic")
	}
}

func TestGraphKeepsDuplicateModulesDistinct(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphSyncBackend, graphScheduleJobs)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	graph := application.Graph()
	var duplicates []backplane.Node
	seenIDs := make(map[string]bool)
	for _, node := range graph.Nodes {
		if seenIDs[node.ID] {
			t.Fatalf("Graph() reused node ID %q", node.ID)
		}
		seenIDs[node.ID] = true
		if node.Kind == backplane.NodeModule && node.Label == "graphSyncBackend" {
			duplicates = append(duplicates, node)
		}
	}
	if len(duplicates) != 2 {
		t.Fatalf("Graph() has %d graphSyncBackend nodes, want 2 honest duplicates: %#v", len(duplicates), graph.Nodes)
	}
}

func TestGraphRendersMermaid(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mermaid := application.Graph().Mermaid()
	for _, want := range []string{
		"flowchart LR",
		"graphSyncBackend",
		"graphScheduleJobs",
		"graphServeHTTP",
		"backplane_test.graphStore",
		"backplane_test.graphQueueChanged",
		"-->|latest|",
	} {
		if !strings.Contains(mermaid, want) {
			t.Fatalf("Mermaid() output does not contain %q:\n%s", want, mermaid)
		}
	}
}

func TestMermaidEscapesQuotedLabels(t *testing.T) {
	// The struct tag puts quotes and backslashes into the topic type's name.
	publish := func(_ context.Context, tagged chan<- struct {
		V string `json:"v"`
	}) error {
		return nil
	}
	application, err := backplane.New(publish)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mermaid := application.Graph().Mermaid()
	if !strings.Contains(mermaid, "#quot;") {
		t.Fatalf("Mermaid() did not escape quotes:\n%s", mermaid)
	}
	if strings.Contains(mermaid, `\"`) {
		t.Fatalf("Mermaid() left a raw quote inside a label:\n%s", mermaid)
	}
}

func findNode(t *testing.T, graph backplane.Graph, kind backplane.NodeKind, label string) backplane.Node {
	t.Helper()
	for _, node := range graph.Nodes {
		if node.Kind == kind && node.Label == label {
			return node
		}
	}
	t.Fatalf("node %s %q not found in %#v", kind, label, graph.Nodes)
	return backplane.Node{}
}

func assertEdge(t *testing.T, graph backplane.Graph, from, to string, kind backplane.EdgeKind) {
	t.Helper()
	for _, edge := range graph.Edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return
		}
	}
	t.Fatalf("edge %s -%s-> %s not found in %#v", from, kind, to, graph.Edges)
}
