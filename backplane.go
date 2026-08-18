package backplane

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/sync/errgroup"
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

// Backplane is an immutable declaration of an application's modules.
type Backplane struct {
	modules []module
}

type module struct {
	fn     reflect.Value
	params []parameter
}

type parameterKind uint8

const (
	resourceParameter parameterKind = iota
	publisherParameter
	subscriberParameter
	latestParameter
)

type parameter struct {
	kind      parameterKind
	typeOf    reflect.Type
	topicType reflect.Type
}

// New declares an application without starting its modules.
func New(moduleFunctions ...any) (*Backplane, error) {
	modules := make([]module, 0, len(moduleFunctions))
	for index, function := range moduleFunctions {
		m, err := inspectModule(function)
		if err != nil {
			return nil, fmt.Errorf("module %d: %w", index, err)
		}
		modules = append(modules, m)
	}

	if err := validateTopics(modules); err != nil {
		return nil, err
	}

	return &Backplane{modules: modules}, nil
}

func validateTopics(modules []module) error {
	publishers := make(map[reflect.Type]bool)
	for _, m := range modules {
		for _, p := range m.params {
			if p.kind == publisherParameter {
				publishers[p.topicType] = true
			}
		}
	}

	for _, m := range modules {
		for _, p := range m.params {
			if (p.kind == subscriberParameter || p.kind == latestParameter) && !publishers[p.topicType] {
				return fmt.Errorf("topic %s has a subscriber but no publisher", p.topicType)
			}
		}
	}

	return nil
}

func inspectModule(function any) (module, error) {
	value := reflect.ValueOf(function)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return module{}, errors.New("must be a function")
	}
	if value.IsNil() {
		return module{}, errors.New("must not be nil")
	}

	ty := value.Type()
	if ty.NumIn() == 0 || ty.In(0) != contextType {
		return module{}, errors.New("first parameter must be context.Context")
	}
	params := make([]parameter, 0, ty.NumIn()-1)
	for index := 1; index < ty.NumIn(); index++ {
		parameterType := ty.In(index)
		if parameterType == contextType {
			return module{}, errors.New("context.Context may only appear as the first parameter")
		}

		p := parameter{kind: resourceParameter, typeOf: parameterType}
		if projection, ok := newLatestProjection(parameterType); ok {
			p.kind = latestParameter
			p.topicType = projection.messageType()
		} else if parameterType.Kind() == reflect.Chan {
			p.topicType = parameterType.Elem()
			switch parameterType.ChanDir() {
			case reflect.SendDir:
				p.kind = publisherParameter
			case reflect.RecvDir:
				p.kind = subscriberParameter
			default:
				return module{}, fmt.Errorf("parameter %d must use a directional channel", index)
			}
		}
		params = append(params, p)
	}
	if ty.NumOut() != 1 || ty.Out(0) != errorType {
		return module{}, errors.New("must return exactly one error")
	}

	return module{fn: value, params: params}, nil
}

// Run binds resources and invokes every declared module.
func (b *Backplane) Run(ctx context.Context, resources ...any) error {
	values := make(map[reflect.Type]reflect.Value, len(resources))
	for index, resource := range resources {
		value := reflect.ValueOf(resource)
		if !value.IsValid() || isNil(value) {
			return fmt.Errorf("resource %d is nil", index)
		}
		if _, exists := values[value.Type()]; exists {
			return fmt.Errorf("duplicate resource %s", value.Type())
		}
		values[value.Type()] = value
	}

	publisherCounts := make(map[reflect.Type]int)
	topics := make(map[reflect.Type]*runtimeTopic)
	for _, m := range b.modules {
		for _, p := range m.params {
			if p.kind == publisherParameter {
				publisherCounts[p.topicType]++
			}
			if p.kind != resourceParameter {
				topics[p.topicType] = nil
			}
		}
	}
	for messageType := range topics {
		topics[messageType] = newRuntimeTopic(messageType, publisherCounts[messageType])
	}

	group, groupContext := errgroup.WithContext(ctx)
	type invocation struct {
		module     module
		args       []reflect.Value
		publishers []*runtimeTopic
	}

	invocations := make([]invocation, 0, len(b.modules))
	for _, m := range b.modules {
		invocation := invocation{
			module: m,
			args:   []reflect.Value{reflect.ValueOf(groupContext)},
		}
		for _, p := range m.params {
			switch p.kind {
			case resourceParameter:
				value, err := resolveResource(values, p.typeOf)
				if err != nil {
					return err
				}
				invocation.args = append(invocation.args, value)
			case publisherParameter:
				topic := topics[p.topicType]
				invocation.args = append(invocation.args, topic.publisher(p.typeOf))
				invocation.publishers = append(invocation.publishers, topic)
			case subscriberParameter:
				invocation.args = append(invocation.args, topics[p.topicType].subscriber(p.typeOf))
			case latestParameter:
				invocation.args = append(invocation.args, topics[p.topicType].latestValue(p.typeOf))
			}
		}
		invocations = append(invocations, invocation)
	}

	for _, topic := range topics {
		group.Go(func() error { return topic.run(groupContext) })
	}
	for _, invocation := range invocations {
		group.Go(func() error {
			defer func() {
				for _, topic := range invocation.publishers {
					topic.publisherDone()
				}
			}()

			result := invocation.module.fn.Call(invocation.args)[0].Interface()
			if result == nil {
				return nil
			}
			return result.(error)
		})
	}

	return group.Wait()
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func resolveResource(values map[reflect.Type]reflect.Value, parameter reflect.Type) (reflect.Value, error) {
	if value, exists := values[parameter]; exists {
		return value, nil
	}

	var match reflect.Value
	for _, value := range values {
		if !value.Type().AssignableTo(parameter) {
			continue
		}
		if match.IsValid() {
			return reflect.Value{}, fmt.Errorf("multiple resources satisfy %s", parameter)
		}
		match = value
	}
	if match.IsValid() {
		return match, nil
	}

	return reflect.Value{}, fmt.Errorf("missing resource %s", parameter)
}
