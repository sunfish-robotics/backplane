package mermaid_test

import (
	"context"
	"fmt"

	"github.com/sunfish-robotics/backplane"
	"github.com/sunfish-robotics/backplane/mermaid"
)

type queueChanged struct{}
type graphStore struct{}

func syncBackend(context.Context, *graphStore, chan<- queueChanged) error {
	return nil
}

func serveHTTP(context.Context, <-chan queueChanged) error {
	return nil
}

func ExampleRender() {
	application, err := backplane.New(syncBackend, serveHTTP)
	if err != nil {
		panic(err)
	}

	fmt.Print(mermaid.Render(application.Graph()))
	// Output:
	// flowchart TB
	//   n0["mermaid_test.syncBackend"]
	//   n1["mermaid_test.serveHTTP"]
	//   n3{{"mermaid_test.queueChanged"}}
	//   n0 --> n3
	//   n3 --> n1
	//   classDef module fill:#e8f1ff,stroke:#2563eb,color:#111827
	//   classDef resource fill:#f8fafc,stroke:#64748b,color:#111827
	//   classDef topic fill:#f5f3ff,stroke:#7c3aed,color:#111827
	//   class n0,n1 module
	//   class n3 topic
}
