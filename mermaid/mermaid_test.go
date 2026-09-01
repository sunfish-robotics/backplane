package mermaid

import (
	"context"
	"strings"
	"testing"

	"github.com/sunfish-robotics/backplane"
)

func TestRenderProducesDeterministicDataflow(t *testing.T) {
	graph := exampleGraph()
	first, second := Render(graph), Render(graph)
	if first != second {
		t.Fatalf("Render() is not deterministic:\n%s\n%s", first, second)
	}

	for _, want := range []string{
		"flowchart TB",
		"app.syncBackend",
		"app.serveHTTP",
		"app.queueChanged",
		"-.->|latest|",
		"classDef module",
		"classDef topic",
	} {
		if !strings.Contains(first, want) {
			t.Fatalf("Render() output does not contain %q:\n%s", want, first)
		}
	}
	if strings.Contains(first, "app.graphStore") {
		t.Fatalf("Render() included resources by default:\n%s", first)
	}
}

func TestRenderWithCanIncludeResources(t *testing.T) {
	diagram := RenderWith(exampleGraph(), Options{Resources: true})
	for _, want := range []string{
		`("app.graphStore")`,
		"classDef resource",
	} {
		if !strings.Contains(diagram, want) {
			t.Fatalf("RenderWith() output does not contain %q:\n%s", want, diagram)
		}
	}
}

func TestRenderWithCanRenderLeftToRight(t *testing.T) {
	diagram := RenderWith(exampleGraph(), Options{Direction: LeftToRight})
	if !strings.HasPrefix(diagram, "flowchart LR\n") {
		t.Fatalf("RenderWith() did not render left to right:\n%s", diagram)
	}
}

func TestRenderShortensImportPathsInsideGenericTypes(t *testing.T) {
	graph := backplane.Graph{Nodes: []backplane.Node{{
		ID:    "topic:0",
		Kind:  backplane.NodeTopic,
		Label: "mavlink.MessageOf[*github.com/bluenviron/gomavlib/v3/pkg/dialects/common.MessageAttitude]",
	}}}

	diagram := Render(graph)
	if !strings.Contains(diagram, "mavlink.MessageOf[*common.MessageAttitude]") {
		t.Fatalf("Render() did not shorten the generic type label:\n%s", diagram)
	}
	if strings.Contains(diagram, "github.com/bluenviron") {
		t.Fatalf("Render() retained an import path in the display label:\n%s", diagram)
	}
}

func TestRenderEscapesQuotedLabels(t *testing.T) {
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

	diagram := Render(application.Graph())
	if !strings.Contains(diagram, "#quot;") {
		t.Fatalf("Render() did not escape quotes:\n%s", diagram)
	}
	if strings.Contains(diagram, `\"`) {
		t.Fatalf("Render() left a raw quote inside a label:\n%s", diagram)
	}
}

func TestEscape(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{`plain`, `plain`},
		{`say "hi"`, `say #quot;hi#quot;`},
		{`a\b`, `a#92;b`},
		{"line\nbreak", "line break"},
		// '#' must be escaped first so replacement codes survive intact.
		{`#quot;`, `#35;quot;`},
	}
	for _, tt := range tests {
		if got := escape(tt.in); got != tt.want {
			t.Errorf("escape(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func exampleGraph() backplane.Graph {
	return backplane.Graph{
		Nodes: []backplane.Node{
			{ID: "module:0", Kind: backplane.NodeModule, Label: "app.syncBackend"},
			{ID: "module:1", Kind: backplane.NodeModule, Label: "app.serveHTTP"},
			{ID: "resource:0", Kind: backplane.NodeResource, Label: "app.graphStore"},
			{ID: "topic:0", Kind: backplane.NodeTopic, Label: "app.queueChanged"},
		},
		Edges: []backplane.Edge{
			{From: "resource:0", To: "module:0", Kind: backplane.EdgeResource},
			{From: "module:0", To: "topic:0", Kind: backplane.EdgePublish},
			{From: "topic:0", To: "module:1", Kind: backplane.EdgeLatest},
		},
	}
}
