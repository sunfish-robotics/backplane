package backplane

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"golang.org/x/sync/errgroup"
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

// Backplane is an immutable declaration of an application's modules, built by
// New. It can be inspected with Graph and executed with Run, both of which
// derive everything from the same module signatures.
type Backplane struct {
	modules []module
}

type module struct {
	name   string
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

// parameter is one declared dependency of a module. typeOf is the exact
// parameter type; topicType is the carried message type for topic-backed
// parameters and nil for resources.
type parameter struct {
	kind      parameterKind
	typeOf    reflect.Type
	topicType reflect.Type
}

// New inspects each module function's signature and records its declared
// resources and topics without executing any module code. It rejects invalid
// module signatures and subscriptions to topics that no module publishes.
func New(moduleFunctions ...any) (*Backplane, error) {
	modules := make([]module, 0, len(moduleFunctions))
	for index, function := range moduleFunctions {
		m, err := inspectModule(function)
		if err != nil {
			if m.name != "" {
				return nil, fmt.Errorf("module %d (%s): %w", index, m.name, err)
			}
			return nil, fmt.Errorf("module %d: %w", index, err)
		}
		modules = append(modules, m)
	}
	if err := validateTopics(modules); err != nil {
		return nil, err
	}
	return &Backplane{modules: modules}, nil
}

func inspectModule(function any) (module, error) {
	value := reflect.ValueOf(function)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return module{}, errors.New("must be a function")
	}
	if value.IsNil() {
		return module{}, errors.New("must be a non-nil function")
	}

	m := module{name: moduleName(value), fn: value}
	ty := value.Type()
	if ty.IsVariadic() {
		return m, errors.New("must not be variadic")
	}
	if ty.NumOut() != 1 || ty.Out(0) != errorType {
		return m, errors.New("must return exactly one error")
	}
	if ty.NumIn() == 0 || ty.In(0) != contextType {
		return m, errors.New("first parameter must be a context.Context")
	}
	for index := 1; index < ty.NumIn(); index++ {
		p, err := inspectParameter(ty.In(index))
		if err != nil {
			return m, fmt.Errorf("parameter %d: %w", index, err)
		}
		m.params = append(m.params, p)
	}
	return m, nil
}

func inspectParameter(parameterType reflect.Type) (parameter, error) {
	if parameterType == contextType {
		return parameter{}, errors.New("context.Context may only appear as the first parameter")
	}
	if messageType, ok := latestMessageType(parameterType); ok {
		return parameter{kind: latestParameter, typeOf: parameterType, topicType: messageType}, nil
	}
	if parameterType.Kind() == reflect.Chan {
		p := parameter{typeOf: parameterType, topicType: parameterType.Elem()}
		switch parameterType.ChanDir() {
		case reflect.SendDir:
			p.kind = publisherParameter
		case reflect.RecvDir:
			p.kind = subscriberParameter
		default:
			return parameter{}, errors.New("channels must be directional: chan<- T publishes, <-chan T subscribes")
		}
		return p, nil
	}
	return parameter{kind: resourceParameter, typeOf: parameterType}, nil
}

func validateTopics(modules []module) error {
	published := make(map[reflect.Type]bool)
	for _, m := range modules {
		for _, p := range m.params {
			if p.kind == publisherParameter {
				published[p.topicType] = true
			}
		}
	}
	for _, m := range modules {
		for _, p := range m.params {
			if (p.kind == subscriberParameter || p.kind == latestParameter) && !published[p.topicType] {
				return fmt.Errorf("module %s consumes topic %s, but no module publishes it", m.name, p.topicType)
			}
		}
	}
	return nil
}

// Run binds the supplied resources to the declared module parameters,
// validates every binding, then starts all modules concurrently under one
// errgroup. It returns once every module has returned, reporting the first
// module error. See the package documentation for the lifecycle contract.
func (b *Backplane) Run(ctx context.Context, resources ...any) error {
	bindings, err := indexResources(resources)
	if err != nil {
		return err
	}

	group, groupContext := errgroup.WithContext(ctx)
	contextValue := reflect.ValueOf(groupContext)

	topics := make(map[reflect.Type]*topic)
	topicFor := func(messageType reflect.Type) *topic {
		t := topics[messageType]
		if t == nil {
			t = newTopic(messageType)
			topics[messageType] = t
		}
		return t
	}

	type invocation struct {
		module    module
		args      []reflect.Value
		done      chan struct{} // closed when the module returns; created if it subscribes
		published []*publisherEndpoint
	}
	invocations := make([]invocation, 0, len(b.modules))
	for _, m := range b.modules {
		inv := invocation{module: m, args: make([]reflect.Value, 0, len(m.params)+1)}
		inv.args = append(inv.args, contextValue)
		for _, p := range m.params {
			switch p.kind {
			case resourceParameter:
				value, err := bindings.resolve(p.typeOf)
				if err != nil {
					return fmt.Errorf("module %s: %w", m.name, err)
				}
				inv.args = append(inv.args, value)
			case publisherParameter:
				t := topicFor(p.topicType)
				channel, publisher := t.addPublisher(p.typeOf)
				inv.args = append(inv.args, channel)
				inv.published = append(inv.published, publisher)
			case subscriberParameter:
				if inv.done == nil {
					inv.done = make(chan struct{})
				}
				inv.args = append(inv.args, topicFor(p.topicType).addSubscriber(p.typeOf, inv.done))
			case latestParameter:
				inv.args = append(inv.args, topicFor(p.topicType).addLatest(p.typeOf))
			}
		}
		invocations = append(invocations, inv)
	}
	if err := bindings.unused(); err != nil {
		return err
	}

	for _, t := range topics {
		group.Go(func() error {
			t.pump(groupContext)
			return nil
		})
	}
	for _, inv := range invocations {
		group.Go(func() error {
			defer func() {
				if inv.done != nil {
					close(inv.done)
				}
				for _, publisher := range inv.published {
					publisher.complete()
				}
			}()
			result := inv.module.fn.Call(inv.args)[0].Interface()
			if result == nil {
				return nil
			}
			return fmt.Errorf("module %s: %w", inv.module.name, result.(error))
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

// resourceSet holds the values passed to Run in declaration order so that
// binding errors are deterministic and unused values can be reported.
type resourceSet struct {
	values []reflect.Value
	used   []bool
	byType map[reflect.Type]int
}

func indexResources(resources []any) (*resourceSet, error) {
	set := &resourceSet{
		values: make([]reflect.Value, 0, len(resources)),
		used:   make([]bool, len(resources)),
		byType: make(map[reflect.Type]int, len(resources)),
	}
	for index, resource := range resources {
		value := reflect.ValueOf(resource)
		if !value.IsValid() || isNil(value) {
			return nil, fmt.Errorf("resource %d is nil", index)
		}
		if _, exists := set.byType[value.Type()]; exists {
			return nil, fmt.Errorf("duplicate resource %s", value.Type())
		}
		set.byType[value.Type()] = index
		set.values = append(set.values, value)
	}
	return set, nil
}

func (s *resourceSet) resolve(parameterType reflect.Type) (reflect.Value, error) {
	if index, ok := s.byType[parameterType]; ok {
		s.used[index] = true
		return s.values[index], nil
	}
	matched := -1
	for index, value := range s.values {
		if !value.Type().AssignableTo(parameterType) {
			continue
		}
		if matched >= 0 {
			return reflect.Value{}, fmt.Errorf("both %s and %s satisfy resource %s",
				s.values[matched].Type(), value.Type(), parameterType)
		}
		matched = index
	}
	if matched < 0 {
		return reflect.Value{}, fmt.Errorf("missing resource %s", parameterType)
	}
	s.used[matched] = true
	return s.values[matched], nil
}

func (s *resourceSet) unused() error {
	for index, used := range s.used {
		if !used {
			return fmt.Errorf("resource %d (%s) is not used by any module", index, s.values[index].Type())
		}
	}
	return nil
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// moduleName reports a short human-readable name for a module function,
// e.g. "ScheduleJobs" or "Server.Run". It is best-effort: closures come out
// as names like "main.func1" and duplicates are allowed.
func moduleName(function reflect.Value) string {
	definition := runtime.FuncForPC(function.Pointer())
	if definition == nil {
		return function.Type().String()
	}

	name := strings.TrimSuffix(definition.Name(), "-fm")
	if slash := strings.LastIndex(name, "/"); slash >= 0 {
		name = name[slash+1:]
	}
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	return name
}
