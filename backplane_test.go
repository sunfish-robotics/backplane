package backplane_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sunfish-robotics/backplane"
)

type namedResource interface {
	Name() string
}

type postgresStore string

func (s postgresStore) Name() string { return string(s) }

type memoryStore string

func (s memoryStore) Name() string { return string(s) }

var errBoom = errors.New("boom")

func failingModule(context.Context) error { return errBoom }

func TestNewRejectsInvalidModuleSignatures(t *testing.T) {
	var nilModule func(context.Context) error

	tests := []struct {
		name   string
		module any
	}{
		{"not a function", 42},
		{"untyped nil", nil},
		{"typed nil function", nilModule},
		{"missing context", func() error { return nil }},
		{"context is not first", func(int, context.Context) error { return nil }},
		{"context appears twice", func(context.Context, context.Context) error { return nil }},
		{"no results", func(context.Context) {}},
		{"non-error result", func(context.Context) int { return 0 }},
		{"too many results", func(context.Context) (int, error) { return 0, nil }},
		{"variadic", func(context.Context, ...int) error { return nil }},
		{"bidirectional channel", func(context.Context, chan int) error { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := backplane.New(tt.module); err == nil {
				t.Fatal("New() accepted an invalid module signature")
			}
		})
	}
}

func TestNewRejectsConsumersWithoutPublisher(t *testing.T) {
	type message struct{}

	subscriber := func(context.Context, <-chan message) error { return nil }
	if _, err := backplane.New(subscriber); err == nil {
		t.Fatal("New() accepted a subscriber whose topic has no publisher")
	}

	watcher := func(context.Context, *backplane.Latest[message]) error { return nil }
	if _, err := backplane.New(watcher); err == nil {
		t.Fatal("New() accepted a Latest whose topic has no publisher")
	}
}

func TestRunConnectsTypedPublishersAndSubscribers(t *testing.T) {
	type jobQueued struct {
		ID string
	}

	var got []jobQueued
	application, err := backplane.New(
		func(_ context.Context, jobs chan<- jobQueued) error {
			jobs <- jobQueued{ID: "job-123"}
			return nil
		},
		func(_ context.Context, jobs <-chan jobQueued) error {
			for job := range jobs {
				got = append(got, job)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "job-123" {
		t.Fatalf("subscriber received %#v", got)
	}
}

func TestPublishReturnsBeforeSubscriberAcceptsValue(t *testing.T) {
	type update int

	sendReturned := make(chan struct{})
	application, err := backplane.New(
		func(_ context.Context, updates chan<- update) error {
			updates <- 42
			close(sendReturned)
			return nil
		},
		func(ctx context.Context, updates <-chan update) error {
			select {
			case <-sendReturned:
				// The topic pump has accepted the value, but this subscriber has
				// deliberately not accepted it yet.
			case <-ctx.Done():
				return errors.New("publish waited for downstream delivery")
			}
			if got := <-updates; got != 42 {
				return fmt.Errorf("subscriber received %d, want 42", got)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := application.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestEverySubscriberReceivesValuesFromEveryPublisher(t *testing.T) {
	type queueChanged string

	firstResult := make(chan []queueChanged, 1)
	secondResult := make(chan []queueChanged, 1)
	subscriber := func(result chan<- []queueChanged) func(context.Context, <-chan queueChanged) error {
		return func(_ context.Context, changes <-chan queueChanged) error {
			var received []queueChanged
			for change := range changes {
				received = append(received, change)
			}
			result <- received
			return nil
		}
	}

	application, err := backplane.New(
		func(_ context.Context, changes chan<- queueChanged) error {
			changes <- "backend"
			return nil
		},
		func(_ context.Context, changes chan<- queueChanged) error {
			changes <- "operator"
			return nil
		},
		subscriber(firstResult),
		subscriber(secondResult),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := []queueChanged{"backend", "operator"}
	for index, result := range []chan []queueChanged{firstResult, secondResult} {
		got := <-result
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("subscriber %d received %v, want %v", index, got, want)
		}
	}
}

func TestTopicClosesOnlyAfterEveryPublisherFinishes(t *testing.T) {
	type note string

	release := make(chan struct{})
	application, err := backplane.New(
		func(_ context.Context, notes chan<- note) error {
			notes <- "first"
			return nil
		},
		func(_ context.Context, notes chan<- note) error {
			<-release
			notes <- "second"
			return nil
		},
		func(_ context.Context, notes <-chan note) error {
			if got := <-notes; got != "first" {
				return errors.New("expected the eager publisher's value first")
			}
			// The eager publisher has finished; the topic must stay open for
			// the remaining publisher.
			close(release)
			if got := <-notes; got != "second" {
				return errors.New("topic closed before every publisher finished")
			}
			if _, open := <-notes; open {
				return errors.New("topic stayed open after every publisher finished")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestSuccessfulModuleDoesNotCancelSiblings(t *testing.T) {
	type report int

	finished := make(chan struct{})
	var got report
	application, err := backplane.New(
		func(context.Context) error {
			close(finished)
			return nil
		},
		// The remaining modules complete a full round trip through the topic
		// after the first module has finished; if its nil return had cancelled
		// the group, the value would be dropped during draining.
		func(_ context.Context, reports chan<- report) error {
			<-finished
			reports <- 42
			return nil
		},
		func(_ context.Context, reports <-chan report) error {
			for value := range reports {
				got = value
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got != 42 {
		t.Fatalf("subscriber received %d after a sibling completed, want 42", got)
	}
}

func TestPublisherOutlivesFinishedSubscriber(t *testing.T) {
	type tick int

	application, err := backplane.New(
		func(_ context.Context, ticks chan<- tick) error {
			// Bare sends: once the subscriber returns, its abandoned
			// subscription must not block these forever.
			ticks <- 1
			ticks <- 2
			ticks <- 3
			return nil
		},
		func(_ context.Context, ticks <-chan tick) error {
			<-ticks
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunCancelsSiblingModulesAfterFirstError(t *testing.T) {
	siblingStarted := make(chan struct{})
	siblingStopped := make(chan struct{})

	application, err := backplane.New(
		func(ctx context.Context) error {
			select {
			case <-siblingStarted:
				return errBoom
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		func(ctx context.Context) error {
			close(siblingStarted)
			<-ctx.Done()
			close(siblingStopped)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want %v", err, errBoom)
	}
	select {
	case <-siblingStopped:
	default:
		t.Fatal("Run() returned before the cancelled sibling stopped")
	}
}

func TestRunReturnsFirstModuleErrorAndNamesTheModule(t *testing.T) {
	errLater := errors.New("shutdown error")

	// The sibling errors only after cancellation, which only follows the
	// first error, so errBoom is deterministically first.
	application, err := backplane.New(
		failingModule,
		func(ctx context.Context) error {
			<-ctx.Done()
			return errLater
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = application.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want the first error %v", err, errBoom)
	}
	if errors.Is(err, errLater) {
		t.Fatalf("Run() error = %v, must not be the later error", err)
	}
	if !strings.Contains(err.Error(), "failingModule") {
		t.Fatalf("Run() error %q does not name the failing module", err)
	}
}

func TestParentCancellationStopsRun(t *testing.T) {
	started := make(chan struct{})
	application, err := backplane.New(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	if err := application.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestParentCancellationReturnedWhenModulesStopCleanly(t *testing.T) {
	started := make(chan struct{})
	application, err := backplane.New(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	if err := application.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestCancellationUnblocksBlockedPublisherAndSubscriber(t *testing.T) {
	type measurement int
	type heartbeat struct{}

	publisherBlocked := make(chan struct{})
	application, err := backplane.New(
		// Publisher blocked in a bare send: the first value occupies the pump
		// (its subscriber is busy elsewhere), so the second send blocks.
		func(_ context.Context, measurements chan<- measurement) error {
			measurements <- 1
			close(publisherBlocked)
			measurements <- 2
			return nil
		},
		// Subscriber blocked receiving a topic that never produces a value.
		func(_ context.Context, measurements <-chan measurement, heartbeats <-chan heartbeat) error {
			<-heartbeats
			for range measurements {
			}
			return nil
		},
		func(ctx context.Context, heartbeats chan<- heartbeat) error {
			<-ctx.Done()
			return nil
		},
		func(ctx context.Context) error {
			<-publisherBlocked
			return errBoom
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Run returning at all proves both blocked modules unwound.
	if err := application.Run(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want %v", err, errBoom)
	}
}

func TestTopicToleratesModuleClosingItsPublisherChannel(t *testing.T) {
	type update string

	release := make(chan struct{})
	var got update
	application, err := backplane.New(
		// Closing the channel is a fault backplane tolerates: the topic must
		// not complete until this module actually returns, and the sibling
		// publisher must be unaffected.
		func(_ context.Context, updates chan<- update) error {
			close(updates)
			<-release
			return nil
		},
		func(_ context.Context, updates chan<- update) error {
			updates <- "still-delivered"
			return nil
		},
		func(_ context.Context, updates <-chan update) error {
			got = <-updates
			close(release)
			if _, open := <-updates; open {
				return errors.New("received a value after every publisher finished")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got != "still-delivered" {
		t.Fatalf("subscriber received %q, want %q", got, "still-delivered")
	}
}

func TestRunValidatesResourcesBeforeStartingAnyModule(t *testing.T) {
	type config struct{}
	started := false

	tests := []struct {
		name      string
		resources []any
	}{
		{"missing resource", nil},
		{"typed nil resource", []any{(*config)(nil)}},
		{"untyped nil resource", []any{nil}},
		{"duplicate resources", []any{&config{}, &config{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			application, err := backplane.New(
				func(context.Context) error {
					started = true
					return nil
				},
				func(context.Context, *config) error {
					started = true
					return nil
				},
			)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := application.Run(context.Background(), tt.resources...); err == nil {
				t.Fatal("Run() accepted an invalid resource binding")
			}
			if started {
				t.Fatal("a module started before every binding was validated")
			}
		})
	}
}

func TestRunBindsConcreteResourcesToInterfaces(t *testing.T) {
	var got namedResource
	application, err := backplane.New(func(_ context.Context, store namedResource) error {
		got = store
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	want := postgresStore("postgres")
	if err := application.Run(context.Background(), want); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got != want {
		t.Fatalf("module received %v, want %v", got, want)
	}
}

func TestRunRejectsAmbiguousResourceBindings(t *testing.T) {
	application, err := backplane.New(func(context.Context, namedResource) error { return nil })
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = application.Run(context.Background(), postgresStore("a"), memoryStore("b"))
	if err == nil {
		t.Fatal("Run() accepted two resources satisfying the same parameter")
	}
	for _, want := range []string{"postgresStore", "memoryStore"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Run() error %q does not mention %s", err, want)
		}
	}
}

func TestRunRejectsUnusedResources(t *testing.T) {
	application, err := backplane.New(func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = application.Run(context.Background(), postgresStore("orphan"))
	if err == nil || !strings.Contains(err.Error(), "not used") {
		t.Fatalf("Run() error = %v, want an unused-resource error", err)
	}
}

type contextKey struct{}

func TestRunInjectsContextAndResources(t *testing.T) {
	type config struct {
		name string
	}

	want := &config{name: "print-farm"}
	var gotContext context.Context
	var gotConfig *config

	application, err := backplane.New(func(ctx context.Context, cfg *config) error {
		gotContext = ctx
		gotConfig = cfg
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.WithValue(context.Background(), contextKey{}, "marker")
	if err := application.Run(ctx, want); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if gotContext == nil || gotContext.Value(contextKey{}) != "marker" {
		t.Fatal("module did not receive a context derived from the caller's context")
	}
	if gotConfig != want {
		t.Fatalf("module received config %p, want %p", gotConfig, want)
	}
}

func TestBackplaneCanRunMoreThanOnce(t *testing.T) {
	type ping struct{}

	total := 0
	application, err := backplane.New(
		func(_ context.Context, pings chan<- ping) error {
			pings <- ping{}
			return nil
		},
		func(_ context.Context, pings <-chan ping) error {
			for range pings {
				total++
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for run := 0; run < 2; run++ {
		if err := application.Run(context.Background()); err != nil {
			t.Fatalf("Run() %d error = %v", run, err)
		}
	}
	if total != 2 {
		t.Fatalf("subscriber received %d values across two runs, want 2", total)
	}
}
