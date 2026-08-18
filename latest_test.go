package backplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Michael-F-Bryan/backplane"
)

func TestLatestZeroValueHoldsNothing(t *testing.T) {
	var latest backplane.Latest[int]

	value, receivedAt, ok := latest.Load()
	if ok || value != 0 || !receivedAt.IsZero() {
		t.Fatalf("Load() = %v, %v, %v; want zero value, zero time, false", value, receivedAt, ok)
	}

	// A watcher on an empty Latest closes on context cancellation without
	// ever delivering a value.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range latest.Watch(ctx) {
		t.Fatal("Watch() delivered a value that was never published")
	}
}

func TestLatestRetainsTheFinalTopicValue(t *testing.T) {
	type printerState string

	var states *backplane.Latest[printerState]
	application, err := backplane.New(
		func(_ context.Context, updates chan<- printerState) error {
			updates <- "idle"
			updates <- "printing"
			return nil
		},
		func(_ context.Context, latest *backplane.Latest[printerState]) error {
			states = latest
			return nil
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Run has returned, so the topic completed and the projection holds the
	// final value even though the observing module returned immediately.
	got, receivedAt, ok := states.Load()
	if !ok {
		t.Fatal("Load() reported no value")
	}
	if got != "printing" {
		t.Fatalf("Load() value = %q, want %q", got, "printing")
	}
	if receivedAt.IsZero() {
		t.Fatal("Load() returned a zero arrival time")
	}
	if receivedAt.After(time.Now()) {
		t.Fatalf("Load() arrival time %v is in the future", receivedAt)
	}

	// A watcher attached after completion still gets the final value, then a
	// closed channel.
	var deliveries int
	for state := range states.Watch(context.Background()) {
		if state != "printing" {
			t.Fatalf("late Watch() delivered %q, want %q", state, "printing")
		}
		deliveries++
	}
	if deliveries != 1 {
		t.Fatalf("late Watch() delivered %d values, want 1", deliveries)
	}
}

func TestLatestWatcherUsesLatestWinsDelivery(t *testing.T) {
	type printerState int

	watcherReady := make(chan struct{})
	topicFinished := make(chan struct{})
	var (
		got   printerState
		count int
	)

	application, err := backplane.New(
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
		func(ctx context.Context, states *backplane.Latest[printerState]) error {
			updates := states.Watch(ctx)
			close(watcherReady)
			// Only start reading after every value has been published: the
			// slow watcher must see just the newest value.
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

func TestLatestWatcherClosesOnContextCancel(t *testing.T) {
	type farmState int
	errStop := errors.New("observed enough")

	application, err := backplane.New(
		func(ctx context.Context, states chan<- farmState) error {
			states <- 1
			<-ctx.Done() // keep the topic open so only the watch context can close the watcher
			return nil
		},
		func(ctx context.Context, states *backplane.Latest[farmState]) error {
			watchContext, cancelWatch := context.WithCancel(ctx)
			defer cancelWatch()
			updates := states.Watch(watchContext)
			if got := <-updates; got != 1 {
				return errors.New("watcher missed the published value")
			}
			cancelWatch()
			for range updates {
			}
			return errStop
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := application.Run(context.Background()); !errors.Is(err, errStop) {
		t.Fatalf("Run() error = %v, want %v", err, errStop)
	}
}

func TestLatestWatcherClosesWhenTopicCompletes(t *testing.T) {
	type farmState int

	var got farmState
	application, err := backplane.New(
		func(_ context.Context, states chan<- farmState) error {
			states <- 7
			return nil
		},
		// Watching with context.Background proves topic completion alone
		// closes the watcher; the loop would otherwise never terminate.
		func(_ context.Context, states *backplane.Latest[farmState]) error {
			for state := range states.Watch(context.Background()) {
				got = state
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
	if got != 7 {
		t.Fatalf("watcher received %d, want 7", got)
	}
}

func TestLatestIsSharedBetweenModules(t *testing.T) {
	type farmState int

	captured := make(chan *backplane.Latest[farmState], 2)
	observer := func(_ context.Context, states *backplane.Latest[farmState]) error {
		captured <- states
		return nil
	}

	application, err := backplane.New(
		func(_ context.Context, states chan<- farmState) error {
			states <- 1
			return nil
		},
		observer,
		observer,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := application.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if first, second := <-captured, <-captured; first != second {
		t.Fatal("modules observing the same topic received different Latest instances")
	}
}
