package backplane

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"time"
)

// topic carries every value published to one exact Go type from its
// publishers to its subscribers and, when declared, its Latest projection.
//
// Each publisher parameter gets its own unbuffered channel so that a module
// wrongly closing its channel cannot poison the other publishers; the topic
// completes when every publishing module has returned, not when a channel
// closes. Backplane never closes a publisher channel itself.
type topic struct {
	messageType reflect.Type
	inputs      []reflect.Value
	subscribers []subscription
	latest      latestProjection

	mu        sync.Mutex
	remaining int           // publisher parameters whose module has not returned
	done      chan struct{} // closed once every publishing module has returned
}

// subscription is one declared <-chan T parameter. moduleDone is closed when
// the owning module returns, so an abandoned subscription stops receiving
// deliveries instead of backpressuring the topic forever.
type subscription struct {
	channel    reflect.Value
	moduleDone reflect.Value
	dead       bool
}

func newTopic(messageType reflect.Type) *topic {
	return &topic{messageType: messageType, done: make(chan struct{})}
}

func (t *topic) addPublisher(parameterType reflect.Type) reflect.Value {
	input := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, t.messageType), 0)
	t.inputs = append(t.inputs, input)
	t.remaining++
	return input.Convert(parameterType)
}

func (t *topic) addSubscriber(parameterType reflect.Type, moduleDone chan struct{}) reflect.Value {
	channel := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, t.messageType), 0)
	t.subscribers = append(t.subscribers, subscription{
		channel:    channel,
		moduleDone: reflect.ValueOf(moduleDone),
	})
	return channel.Convert(parameterType)
}

func (t *topic) addLatest(parameterType reflect.Type) reflect.Value {
	if t.latest == nil {
		t.latest = newLatestProjection(parameterType)
	}
	return reflect.ValueOf(t.latest)
}

func (t *topic) publisherDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.remaining--
	if t.remaining == 0 {
		close(t.done)
	}
}

// pump receives every published value and fans it out to subscribers and the
// Latest projection. It returns once every publishing module has returned.
// After cancellation it keeps draining but drops the values, so a publisher
// blocked in a bare send can still unwind during shutdown.
func (t *topic) pump(ctx context.Context) {
	defer t.finish()

	topicDone := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.done)}
	contextDone := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	inputs := slices.Clone(t.inputs)
	cancelled := false

	for {
		cases := make([]reflect.SelectCase, 0, len(inputs)+2)
		cases = append(cases, topicDone, contextDone)
		if cancelled {
			cases[1].Chan = reflect.Value{} // a zero Chan is never ready
		}
		for _, input := range inputs {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: input})
		}

		chosen, value, ok := reflect.Select(cases)
		switch {
		case chosen == 0: // every publisher has returned
			return
		case chosen == 1:
			cancelled = true
		case !ok: // a module closed its publisher channel: stop receiving from it
			inputs = slices.Delete(inputs, chosen-2, chosen-1)
		case cancelled: // cancellation interrupts delivery: drain and drop
		default:
			if t.latest != nil {
				t.latest.publish(value, time.Now())
			}
			cancelled = !t.deliver(value, contextDone)
		}
	}
}

// deliver hands value to every live subscriber, blocking until each accepts
// it. It reports false if the context was cancelled mid-delivery, in which
// case the remaining subscribers miss the value.
func (t *topic) deliver(value reflect.Value, contextDone reflect.SelectCase) bool {
	for index := range t.subscribers {
		sub := &t.subscribers[index]
		if sub.dead {
			continue
		}
		chosen, _, _ := reflect.Select([]reflect.SelectCase{
			{Dir: reflect.SelectSend, Chan: sub.channel, Send: value},
			{Dir: reflect.SelectRecv, Chan: sub.moduleDone},
			contextDone,
		})
		switch chosen {
		case 1: // the subscribing module returned: drop its subscription
			sub.dead = true
		case 2:
			return false
		}
	}
	return true
}

func (t *topic) finish() {
	for _, sub := range t.subscribers {
		sub.channel.Close()
	}
	if t.latest != nil {
		t.latest.close()
	}
}
