package backplane

import (
	"context"
	"reflect"
	"sync"
	"time"
)

var latestProjectionType = reflect.TypeFor[latestProjection]()

// latestProjection is the untyped view used to recognise *Latest[T] during
// signature inspection.
type latestProjection interface {
	messageType() reflect.Type
}

// latestFactory bridges signature reflection back into a method where T is
// known, allowing the runtime to use NewLatest rather than constructing or
// mutating a projection through a second path.
type latestFactory interface {
	newLatest() (latestProjection, latestInput)
}

// latestInput is the runtime-owned write side of the channel consumed by a
// Latest. It is deliberately private: modules only receive the projection.
type latestInput interface {
	offer(reflect.Value)
	closeAndWait()
}

type typedLatestInput[T any] struct {
	updates chan T
	done    <-chan struct{}
}

func (input *typedLatestInput[T]) offer(value reflect.Value) {
	update := value.Interface().(T)
	select {
	case input.updates <- update:
	default:
		// Runtime inputs hold one pending value. Replace it instead of
		// backpressuring the topic; this projection is latest-wins by design.
		select {
		case <-input.updates:
		default:
		}
		input.updates <- update
	}
}

func (input *typedLatestInput[T]) closeAndWait() {
	close(input.updates)
	<-input.done
}

// Latest is a lossy, latest-wins projection of a topic: it retains the most
// recently published value and its arrival time. A module declares *Latest[T]
// instead of <-chan T when it needs current state, or change notifications
// that must never backpressure the topic.
//
// The runtime creates one Latest per topic with [NewLatest] and hands the same
// instance to every module that declares it. External callers can use NewLatest
// to project a channel they own. Latest itself exposes no operation that can
// publish a value or complete the projection. The zero value holds no value and
// stays empty forever.
type Latest[T any] struct {
	mu         sync.Mutex
	value      T
	receivedAt time.Time
	hasValue   bool
	closed     bool
	done       chan struct{} // created by NewLatest or first Watch, closed with the source
	watchers   map[chan T]struct{}
}

// NewLatest projects updates from a channel into a Latest. It starts a
// goroutine that consumes updates until the channel closes. Each value is
// timestamped when that goroutine receives it; closing updates closes every
// watcher after all buffered values have been projected. NewLatest never closes
// the supplied channel.
//
// Projection is asynchronous: a send or close returning does not by itself
// guarantee that Load has observed the value. Use Watch when synchronisation is
// required. NewLatest panics if updates is nil.
func NewLatest[T any](updates <-chan T) *Latest[T] {
	if updates == nil {
		panic("backplane: NewLatest called with nil update channel")
	}

	latest := &Latest[T]{done: make(chan struct{})}

	go func() {
		for value := range updates {
			latest.store(value, time.Now())
		}
		latest.close()
	}()
	return latest
}

// Load returns the most recent value, its arrival time, and whether any value
// has been observed yet.
func (l *Latest[T]) Load() (value T, receivedAt time.Time, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.value, l.receivedAt, l.hasValue
}

// Watch returns a channel that converges on the most recent value: the
// current value (if any) is delivered immediately, and each newer value
// overwrites any undelivered one, so a slow watcher misses intermediate
// values rather than backpressuring the source. The channel closes when the
// source completes or ctx is cancelled, whichever comes first. For a runtime
// projection the source completes with its topic; for a projection made with
// NewLatest it completes when the updates channel closes. Watching after the
// source has completed yields the final value, then a closed channel.
func (l *Latest[T]) Watch(ctx context.Context) <-chan T {
	watcher := make(chan T, 1)

	l.mu.Lock()
	if l.hasValue {
		watcher <- l.value
	}
	if l.closed {
		close(watcher)
		l.mu.Unlock()
		return watcher
	}
	if l.watchers == nil {
		l.watchers = make(map[chan T]struct{})
	}
	if l.done == nil {
		l.done = make(chan struct{})
	}
	l.watchers[watcher] = struct{}{}
	done := l.done
	l.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			l.mu.Lock()
			if _, registered := l.watchers[watcher]; registered {
				delete(l.watchers, watcher)
				close(watcher)
			}
			l.mu.Unlock()
		case <-done:
			// close() already closed every registered watcher.
		}
	}()
	return watcher
}

func (*Latest[T]) messageType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (*Latest[T]) newLatest() (latestProjection, latestInput) {
	updates := make(chan T, 1)
	latest := NewLatest((<-chan T)(updates))
	return latest, &typedLatestInput[T]{updates: updates, done: latest.done}
}

func (l *Latest[T]) store(value T, receivedAt time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.value = value
	l.receivedAt = receivedAt
	l.hasValue = true
	for watcher := range l.watchers {
		select {
		case watcher <- value:
		default:
			// The watcher has an undelivered value: replace it. Nothing else
			// sends on watcher, so after the drain the send cannot block.
			select {
			case <-watcher:
			default:
			}
			watcher <- value
		}
	}
}

func (l *Latest[T]) close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return
	}
	l.closed = true
	if l.done != nil {
		close(l.done)
	}
	for watcher := range l.watchers {
		delete(l.watchers, watcher)
		close(watcher)
	}
}

// latestMessageType reports the carried message type when parameterType is a
// *Latest[T]; used during signature inspection.
func latestMessageType(parameterType reflect.Type) (reflect.Type, bool) {
	if parameterType.Kind() != reflect.Pointer || !parameterType.Implements(latestProjectionType) {
		return nil, false
	}
	projection := reflect.New(parameterType.Elem()).Interface().(latestProjection)
	return projection.messageType(), true
}

func newLatestProjection(parameterType reflect.Type) (latestProjection, latestInput) {
	factory := reflect.New(parameterType.Elem()).Interface().(latestFactory)
	return factory.newLatest()
}
