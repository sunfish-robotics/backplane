package backplane_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/sunfish-robotics/backplane"
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
	syncBackend := findNode(t, graph, backplane.NodeModule, "backplane_test.graphSyncBackend")
	queueChanged := findNode(t, graph, backplane.NodeTopic, "backplane_test.graphQueueChanged")
	scheduleJobs := findNode(t, graph, backplane.NodeModule, "backplane_test.graphScheduleJobs")
	serveHTTP := findNode(t, graph, backplane.NodeModule, "backplane_test.graphServeHTTP")

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
		if node.Kind == backplane.NodeModule && node.Label == "backplane_test.graphSyncBackend" {
			duplicates = append(duplicates, node)
		}
	}
	if len(duplicates) != 2 {
		t.Fatalf("Graph() has %d graphSyncBackend nodes, want 2 honest duplicates: %#v", len(duplicates), graph.Nodes)
	}
}

func TestGraphIncludeWithoutSelectorsReturnsEverything(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	graph := application.Graph()

	selected, err := graph.Include()
	if err != nil {
		t.Fatalf("Include() error = %v", err)
	}
	if !reflect.DeepEqual(selected, graph) {
		t.Fatalf("Include() = %#v, want complete graph %#v", selected, graph)
	}
}

func TestGraphIncludeKeepsSelectedOutputsAndTransitiveDependencies(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs, graphServeHTTP)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	graph, err := application.Graph().Include("graphScheduleJobs")
	if err != nil {
		t.Fatalf("Include() error = %v", err)
	}

	store := findNode(t, graph, backplane.NodeResource, "backplane_test.graphStore")
	syncBackend := findNode(t, graph, backplane.NodeModule, "backplane_test.graphSyncBackend")
	queueChanged := findNode(t, graph, backplane.NodeTopic, "backplane_test.graphQueueChanged")
	scheduleJobs := findNode(t, graph, backplane.NodeModule, "backplane_test.graphScheduleJobs")
	assignmentReady := findNode(t, graph, backplane.NodeTopic, "backplane_test.graphAssignmentReady")

	assertEdge(t, graph, store.ID, syncBackend.ID, backplane.EdgeResource)
	assertEdge(t, graph, syncBackend.ID, queueChanged.ID, backplane.EdgePublish)
	assertEdge(t, graph, queueChanged.ID, scheduleJobs.ID, backplane.EdgeSubscribe)
	assertEdge(t, graph, scheduleJobs.ID, assignmentReady.ID, backplane.EdgePublish)

	for _, node := range graph.Nodes {
		if node.Label == "backplane_test.graphServeHTTP" {
			t.Fatalf("Include() retained downstream module %#v", node)
		}
	}
}

func TestGraphIncludeAcceptsQualifiedModuleNames(t *testing.T) {
	application, err := backplane.New(graphSyncBackend, graphScheduleJobs)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	graph, err := application.Graph().Include("backplane_test.graphSyncBackend")
	if err != nil {
		t.Fatalf("Include() error = %v", err)
	}
	findNode(t, graph, backplane.NodeModule, "backplane_test.graphSyncBackend")
	for _, node := range graph.Nodes {
		if node.Label == "backplane_test.graphScheduleJobs" {
			t.Fatalf("Include() retained downstream module %#v", node)
		}
	}
}

func TestGraphIncludeUnionsRepeatedSelectors(t *testing.T) {
	graph := backplane.Graph{
		Nodes: []backplane.Node{
			{ID: "module:0", Kind: backplane.NodeModule, Label: "alpha.Run"},
			{ID: "module:1", Kind: backplane.NodeModule, Label: "beta.Run"},
			{ID: "topic:0", Kind: backplane.NodeTopic, Label: "alpha.Output"},
			{ID: "topic:1", Kind: backplane.NodeTopic, Label: "beta.Output"},
		},
		Edges: []backplane.Edge{
			{From: "module:0", To: "topic:0", Kind: backplane.EdgePublish},
			{From: "module:1", To: "topic:1", Kind: backplane.EdgePublish},
		},
	}

	selected, err := graph.Include("alpha.Run", "beta.Run")
	if err != nil {
		t.Fatalf("Include() error = %v", err)
	}
	if !reflect.DeepEqual(selected, graph) {
		t.Fatalf("Include() = %#v, want union %#v", selected, graph)
	}
}

func TestGraphIncludeRejectsAmbiguousShortNames(t *testing.T) {
	graph := backplane.Graph{Nodes: []backplane.Node{
		{ID: "module:0", Kind: backplane.NodeModule, Label: "alpha.Run"},
		{ID: "module:1", Kind: backplane.NodeModule, Label: "beta.Run"},
	}}

	_, err := graph.Include("Run")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("Include() error = %v, want ambiguous selector", err)
	}
}

func TestGraphIncludeRejectsUnknownModules(t *testing.T) {
	application, err := backplane.New(graphSyncBackend)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = application.Graph().Include("missing")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Include() error = %v, want module not found", err)
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
