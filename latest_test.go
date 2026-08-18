package backplane

import (
	"context"
	"testing"
	"time"
)

func TestLatestWatcherUsesLatestWinsDelivery(t *testing.T) {
	type printerState int

	watcherReady := make(chan struct{})
	topicFinished := make(chan struct{})
	var (
		got   printerState
		count int
	)

	application, err := New(
		func(_ context.Context, states chan<- printerState) error {
			<-watcherReady
			states <- 1
			states <- 2
			states <- 3
			return nil
		},
		func(_ context.Context, states <-chan printerState) error {
			for range states {
			}
			close(topicFinished)
			return nil
		},
		func(ctx context.Context, states *Latest[printerState]) error {
			updates := states.Watch(ctx)
			close(watcherReady)
			<-topicFinished
			for state := range updates {
				got = state
				count++
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

	if got != 3 || count != 1 {
		t.Fatalf("slow watcher received %d values ending at %d, want one value ending at 3", count, got)
	}
}

func TestLatestRetainsTheFinalTopicValue(t *testing.T) {
	type printerState string

	var (
		got       printerState
		receivedAt time.Time
		ok        bool
	)

	application, err := New(
		func(_ context.Context, states chan<- printerState) error {
			states <- "idle"
			states <- "printing"
			return nil
		},
		func(ctx context.Context, states *Latest[printerState]) error {
			for range states.Watch(ctx) {
			}
			got, receivedAt, ok = states.Load()
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !ok {
		t.Fatal("Load() reported no value")
	}
	if got != "printing" {
		t.Fatalf("Load() value = %q, want %q", got, "printing")
	}
	if receivedAt.IsZero() {
		t.Fatal("Load() returned a zero arrival time")
	}
}
