package backplane

import (
	"context"
	"reflect"
	"sync"
	"time"
)

type runtimeTopic struct {
	input       reflect.Value
	subscribers []reflect.Value
	latest      latestProjection

	mu                 sync.Mutex
	remainingPublishers int
}

func newRuntimeTopic(messageType reflect.Type, publishers int) *runtimeTopic {
	channelType := reflect.ChanOf(reflect.BothDir, messageType)
	return &runtimeTopic{
		input:               reflect.MakeChan(channelType, 0),
		remainingPublishers: publishers,
	}
}

func (t *runtimeTopic) publisher(parameterType reflect.Type) reflect.Value {
	return t.input.Convert(parameterType)
}

func (t *runtimeTopic) subscriber(parameterType reflect.Type) reflect.Value {
	channelType := reflect.ChanOf(reflect.BothDir, parameterType.Elem())
	channel := reflect.MakeChan(channelType, 0)
	t.subscribers = append(t.subscribers, channel)
	return channel.Convert(parameterType)
}

func (t *runtimeTopic) latestValue(parameterType reflect.Type) reflect.Value {
	if t.latest == nil {
		projection, ok := newLatestProjection(parameterType)
		if !ok {
			panic("backplane: invalid Latest parameter")
		}
		t.latest = projection
	}
	return reflect.ValueOf(t.latest).Convert(parameterType)
}

func (t *runtimeTopic) publisherDone() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.remainingPublishers--
	if t.remainingPublishers == 0 {
		t.input.Close()
	}
}

func (t *runtimeTopic) run(ctx context.Context) error {
	defer func() {
		for _, subscriber := range t.subscribers {
			subscriber.Close()
		}
		if t.latest != nil {
			t.latest.close()
		}
	}()

	contextDone := reflect.ValueOf(ctx.Done())
	for {
		chosen, value, open := reflect.Select([]reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: t.input},
			{Dir: reflect.SelectRecv, Chan: contextDone},
		})
		if chosen == 1 || !open {
			return nil
		}
		if t.latest != nil {
			t.latest.publish(value, time.Now())
		}

		for _, subscriber := range t.subscribers {
			chosen, _, _ := reflect.Select([]reflect.SelectCase{
				{Dir: reflect.SelectSend, Chan: subscriber, Send: value},
				{Dir: reflect.SelectRecv, Chan: contextDone},
			})
			if chosen == 1 {
				return nil
			}
		}
	}
}
