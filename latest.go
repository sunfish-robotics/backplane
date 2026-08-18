package backplane

import (
	"context"
	"reflect"
	"sync"
	"time"
)

var latestProjectionType = reflect.TypeFor[latestProjection]()

type latestProjection interface {
	messageType() reflect.Type
	publish(reflect.Value, time.Time)
	close()
}

// Latest is a non-blocking projection of a topic's most recently observed value.
//
// A module declares *Latest[T] instead of <-chan T when it needs current state
// or lossy, latest-wins updates rather than backpressured delivery of every value.
type Latest[T any] struct {
	mu         sync.Mutex
	value      T
	receivedAt time.Time
	hasValue   bool
	closed     bool
	watchers   map[chan T]struct{}
}

// Load returns the latest value, its arrival time, and whether a value has been
// observed yet.
func (l *Latest[T]) Load() (value T, receivedAt time.Time, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.value, l.receivedAt, l.hasValue
}

// Watch returns a size-one stream which always converges on the latest value.
// A slow watcher may miss intermediate values. The channel closes when the
// topic finishes or ctx is cancelled.
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
	l.watchers[watcher] = struct{}{}
	l.mu.Unlock()

	go func() {
		<-ctx.Done()
		l.mu.Lock()
		if _, exists := l.watchers[watcher]; exists {
			delete(l.watchers, watcher)
			close(watcher)
		}
		l.mu.Unlock()
	}()

	return watcher
}

func (*Latest[T]) messageType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (l *Latest[T]) publish(value reflect.Value, receivedAt time.Time) {
	typedValue := value.Interface().(T)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.value = typedValue
	l.receivedAt = receivedAt
	l.hasValue = true
	for watcher := range l.watchers {
		select {
		case watcher <- typedValue:
		default:
			select {
			case <-watcher:
			default:
			}
			watcher <- typedValue
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
	for watcher := range l.watchers {
		delete(l.watchers, watcher)
		close(watcher)
	}
}

func newLatestProjection(parameterType reflect.Type) (latestProjection, bool) {
	if parameterType.Kind() != reflect.Pointer || !parameterType.Implements(latestProjectionType) {
		return nil, false
	}

	projection := reflect.New(parameterType.Elem()).Interface().(latestProjection)
	return projection, true
}
