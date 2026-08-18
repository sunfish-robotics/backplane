package backplane

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

type namedResource interface {
	Name() string
}

type concreteResource string

func (r concreteResource) Name() string { return string(r) }

func TestNewRejectsSubscriberWithoutPublisher(t *testing.T) {
	type message struct{}
	_, err := New(func(context.Context, <-chan message) error { return nil })
	if err == nil {
		t.Fatal("New() accepted a subscriber whose topic has no publisher")
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

	application, err := New(
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

func TestRunConnectsTypedPublishersAndSubscribers(t *testing.T) {
	type jobQueued struct {
		ID string
	}

	var got []jobQueued
	application, err := New(
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

func TestSuccessfulModuleDoesNotCancelSiblings(t *testing.T) {
	siblingStarted := make(chan struct{})

	application, err := New(
		func(context.Context) error { return nil },
		func(ctx context.Context) error {
			close(siblingStarted)
			<-ctx.Done()
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- application.Run(ctx) }()

	select {
	case <-siblingStarted:
	case <-time.After(time.Second):
		t.Fatal("sibling did not start")
	}

	select {
	case err := <-result:
		t.Fatalf("Run() returned after a different module completed successfully: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run() error after parent cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after parent cancellation")
	}
}

func TestRunCancelsSiblingModulesAfterFirstError(t *testing.T) {
	want := errors.New("printer connection failed")
	siblingStarted := make(chan struct{})
	siblingStopped := make(chan struct{})

	application, err := New(
		func(ctx context.Context) error {
			select {
			case <-siblingStarted:
				return want
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := application.Run(ctx); !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want %v", err, want)
	}

	select {
	case <-siblingStopped:
	default:
		t.Fatal("Run() returned before the cancelled sibling stopped")
	}
}

func TestRunRejectsTypedNilResourcesBeforeStartingModules(t *testing.T) {
	type config struct{}
	var resource *config
	started := false

	application, err := New(func(context.Context, *config) error {
		started = true
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background(), resource); err == nil {
		t.Fatal("Run() accepted a typed nil resource")
	}
	if started {
		t.Fatal("module started with a typed nil resource")
	}
}

func TestRunBindsConcreteResourcesToInterfaces(t *testing.T) {
	var got namedResource
	application, err := New(func(_ context.Context, resource namedResource) error {
		got = resource
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	want := concreteResource("postgres")
	if err := application.Run(context.Background(), want); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got != want {
		t.Fatalf("module received %v, want %v", got, want)
	}
}

func TestRunValidatesEveryResourceBeforeStartingModules(t *testing.T) {
	type config struct{}
	started := false

	application, err := New(
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

	if err := application.Run(context.Background()); err == nil {
		t.Fatal("Run() succeeded without a required resource")
	}
	if started {
		t.Fatal("a module started before all resource bindings were validated")
	}
}

func TestRunInjectsContextAndResources(t *testing.T) {
	type config struct {
		name string
	}

	want := &config{name: "print-farm"}
	var gotContext context.Context
	var gotConfig *config

	application, err := New(func(ctx context.Context, cfg *config) error {
		gotContext = ctx
		gotConfig = cfg
		return nil
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.WithValue(context.Background(), struct{}{}, "marker")
	if err := application.Run(ctx, want); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if gotContext == nil || gotContext.Value(struct{}{}) != "marker" {
		t.Fatal("module did not receive a context derived from the caller's context")
	}
	if gotConfig != want {
		t.Fatalf("module received config %p, want %p", gotConfig, want)
	}
}

func TestNewRejectsInvalidModuleSignatures(t *testing.T) {
	var nilModule func(context.Context) error

	tests := []struct {
		name   string
		module any
	}{
		{"not a function", 42},
		{"nil function", nilModule},
		{"missing context", func() error { return nil }},
		{"context is not first", func(int, context.Context) error { return nil }},
		{"missing error result", func(context.Context) {}},
		{"too many results", func(context.Context) (error, error) { return nil, nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.module); err == nil {
				t.Fatal("New() succeeded with an invalid module signature")
			}
		})
	}
}
