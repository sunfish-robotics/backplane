package backplane_test

import (
	"context"
	"fmt"

	"github.com/sunfish-robotics/backplane"
)

// A compact print-farm agent: the backend queues jobs, a scheduler assigns
// them to a printer, a runner executes them, and a recorder keeps history.

type jobQueued struct{ ID string }

type assignmentReady struct{ Job, Printer string }

type jobFinished struct{ Job string }

// jobStore stands in for a durable store; backplane binds it as a resource
// because it is neither a channel nor a Latest.
type jobStore struct{ queued []string }

func syncBackend(_ context.Context, store *jobStore, queued chan<- jobQueued) error {
	for _, id := range store.queued {
		queued <- jobQueued{ID: id}
	}
	return nil
}

func scheduleJobs(_ context.Context, queued <-chan jobQueued, assignments chan<- assignmentReady) error {
	for job := range queued {
		assignments <- assignmentReady{Job: job.ID, Printer: "printer-1"}
	}
	return nil
}

func runJobs(_ context.Context, assignments <-chan assignmentReady, finished chan<- jobFinished) error {
	for assignment := range assignments {
		finished <- jobFinished{Job: assignment.Job}
	}
	return nil
}

func recordHistory(_ context.Context, finished <-chan jobFinished) error {
	for outcome := range finished {
		fmt.Printf("printed %s\n", outcome.Job)
	}
	return nil
}

func Example() {
	application, err := backplane.New(syncBackend, scheduleJobs, runJobs, recordHistory)
	if err != nil {
		panic(err)
	}

	store := &jobStore{queued: []string{"benchy", "bracket"}}
	if err := application.Run(context.Background(), store); err != nil {
		panic(err)
	}
	// Output:
	// printed benchy
	// printed bracket
}

func ExampleBackplane_Graph() {
	application, err := backplane.New(syncBackend, scheduleJobs, runJobs, recordHistory)
	if err != nil {
		panic(err)
	}

	fmt.Print(application.Graph().Mermaid())
	// Output:
	// flowchart TB
	//   n0["backplane_test.syncBackend"]
	//   n1["backplane_test.scheduleJobs"]
	//   n2["backplane_test.runJobs"]
	//   n3["backplane_test.recordHistory"]
	//   n5{{"backplane_test.assignmentReady"}}
	//   n6{{"backplane_test.jobFinished"}}
	//   n7{{"backplane_test.jobQueued"}}
	//   n0 --> n7
	//   n7 --> n1
	//   n1 --> n5
	//   n5 --> n2
	//   n2 --> n6
	//   n6 --> n3
	//   classDef module fill:#e8f1ff,stroke:#2563eb,color:#111827
	//   classDef resource fill:#f8fafc,stroke:#64748b,color:#111827
	//   classDef topic fill:#f5f3ff,stroke:#7c3aed,color:#111827
	//   class n0,n1,n2,n3 module
	//   class n5,n6,n7 topic
}

func ExampleLatest() {
	type printerState struct {
		Name, Status string
	}

	application, err := backplane.New(
		func(_ context.Context, states chan<- printerState) error {
			states <- printerState{Name: "printer-1", Status: "idle"}
			return nil
		},
		func(ctx context.Context, states *backplane.Latest[printerState]) error {
			for state := range states.Watch(ctx) {
				fmt.Printf("%s is %s\n", state.Name, state.Status)
			}
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	if err := application.Run(context.Background()); err != nil {
		panic(err)
	}
	// Output:
	// printer-1 is idle
}
