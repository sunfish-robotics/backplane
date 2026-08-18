package backplane

import (
	"context"
	"reflect"
	"sync"
	"time"
)

var latestProjectionType = reflect.TypeFor[latestProjection]()

// latestProjection is the untyped view of a *Latest[T] used by the runtime to
// detect the parameter, feed it values, and close it when its topic completes.
type latestProjection interface {
	messageType() reflect.Type
	publish(value reflect.Value, receivedAt time.Time)
	close()
}

// Latest is a lossy, latest-wins projection of a topic: it retains the most
// recently published value and its arrival time. A module declares *Latest[T]
// instead of <-chan T when it needs current state, or change notifications
// that must never backpressure the topic.
//
// The runtime creates one Latest per topic and hands the same instance to
// every module that declares it. The zero value holds no value and stays
// empty forever; only the runtime populates a Latest.
type Latest[T any] struct {
	mu         sync.Mutex
	value      T
	receivedAt time.Time
	hasValue   bool
	closed     bool
	done       chan struct{} // created on first Watch, closed with the topic
	watchers   map[chan T]struct{}
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
// values rather than backpressuring the topic. The channel closes when the
// topic completes or ctx is cancelled, whichever comes first. Watching after
// the topic has completed yields the final value, then a closed channel.
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
			// The watcher has an undelivered value: replace it. Nothing else
			// sends on watcher, so after the drain the send cannot block.
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
	return newLatestProjection(parameterType).messageType(), true
}

func newLatestProjection(parameterType reflect.Type) latestProjection {
	return reflect.New(parameterType.Elem()).Interface().(latestProjection)
}
