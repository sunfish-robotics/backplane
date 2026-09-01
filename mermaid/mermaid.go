// Package mermaid renders backplane graphs as Mermaid flowcharts.
package mermaid

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/sunfish-robotics/backplane"
)

// Direction controls the direction of a rendered flowchart.
type Direction string

const (
	TopToBottom Direction = "TB"
	BottomToTop Direction = "BT"
	LeftToRight Direction = "LR"
	RightToLeft Direction = "RL"
)

// Options controls optional rendering details. The zero value renders a
// top-to-bottom dataflow without caller-provided resources.
type Options struct {
	// Direction defaults to TopToBottom.
	Direction Direction
	// Resources includes caller-provided resources and their dependency edges.
	Resources bool
}

// Render renders graph as a top-to-bottom Mermaid dataflow. Modules are
// rectangles, topics are hexagons, resources are omitted, and latest-wins
// projections use labelled dashed edges.
func Render(graph backplane.Graph) string {
	return RenderWith(graph, Options{})
}

// RenderWith renders graph as a Mermaid flowchart using options.
func RenderWith(graph backplane.Graph, options Options) string {
	var output strings.Builder
	direction := options.Direction
	if direction == "" {
		direction = TopToBottom
	}
	fmt.Fprintf(&output, "flowchart %s\n", direction)

	mermaidIDs := make(map[string]string, len(graph.Nodes))
	classes := make(map[backplane.NodeKind][]string)
	for index, node := range graph.Nodes {
		if node.Kind == backplane.NodeResource && !options.Resources {
			continue
		}
		id := fmt.Sprintf("n%d", index)
		mermaidIDs[node.ID] = id
		classes[node.Kind] = append(classes[node.Kind], id)
		label := escape(shortenImportPaths(node.Label))
		switch node.Kind {
		case backplane.NodeModule:
			fmt.Fprintf(&output, "  %s[\"%s\"]\n", id, label)
		case backplane.NodeResource:
			fmt.Fprintf(&output, "  %s(\"%s\")\n", id, label)
		case backplane.NodeTopic:
			fmt.Fprintf(&output, "  %s{{\"%s\"}}\n", id, label)
		}
	}

	for _, edge := range graph.Edges {
		from, fromVisible := mermaidIDs[edge.From]
		to, toVisible := mermaidIDs[edge.To]
		if !fromVisible || !toVisible {
			continue
		}
		if edge.Kind == backplane.EdgeLatest {
			fmt.Fprintf(&output, "  %s -.->|latest| %s\n", from, to)
		} else {
			fmt.Fprintf(&output, "  %s --> %s\n", from, to)
		}
	}

	output.WriteString("  classDef module fill:#e8f1ff,stroke:#2563eb,color:#111827\n")
	output.WriteString("  classDef resource fill:#f8fafc,stroke:#64748b,color:#111827\n")
	output.WriteString("  classDef topic fill:#f5f3ff,stroke:#7c3aed,color:#111827\n")
	for _, kind := range []backplane.NodeKind{
		backplane.NodeModule,
		backplane.NodeResource,
		backplane.NodeTopic,
	} {
		if len(classes[kind]) > 0 {
			fmt.Fprintf(&output, "  class %s %s\n", strings.Join(classes[kind], ","), kind)
		}
	}

	return output.String()
}

var importPath = regexp.MustCompile(`(?:[[:alnum:]_.~+-]+/)+([[:alnum:]_]+)\.`)

func shortenImportPaths(value string) string {
	return importPath.ReplaceAllString(value, "$1.")
}

// escape neutralises characters that break quoted Mermaid labels. Mermaid uses
// HTML-entity style escapes, not backslashes; '#' is escaped first so the
// replacement codes themselves survive.
func escape(value string) string {
	value = strings.ReplaceAll(value, "#", "#35;")
	value = strings.ReplaceAll(value, "\\", "#92;")
	value = strings.ReplaceAll(value, "\"", "#quot;")
	return strings.ReplaceAll(value, "\n", " ")
}
