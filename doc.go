// Package backplane is a small in-process application runtime: it runs a
// fixed set of modules — ordinary Go functions — concurrently, wiring them
// together through typed topics declared by their signatures.
//
// # Modules
//
// A module is any function of the form
//
//	func(ctx context.Context, ...dependencies) error
//
// context.Context must be the first parameter and appear exactly once, and
// error must be the only result. Every other parameter declares a dependency:
//
//	chan<- T    publish to the topic carrying T
//	<-chan T    subscribe to the topic carrying T
//	*Latest[T]  observe the most recent value published to T
//	other types a resource supplied by the caller of Run
//
// New records these declarations without calling any module, so the topology
// can be inspected (see [Backplane.Graph]) before anything runs. The module
// set is fixed at that point: there is no runtime registration or spawn API.
// A module that needs short-lived workers starts its own goroutines, joins
// them before returning, and folds their failures into its returned error.
//
// # Resources
//
// The caller constructs resources, passes them to Run, and cleans them up
// afterwards; backplane binds already-created values and never constructs or
// tears anything down. Each resource parameter binds to exactly one supplied
// value, preferring an exact type match and otherwise requiring exactly one
// assignable value, so a concrete type can satisfy an interface parameter.
// Nil, duplicate, ambiguous, missing, and unused resources are all rejected
// before any module starts.
//
// # Topics and delivery
//
// A topic is identified by the exact Go type flowing through it. Any number
// of modules may publish or subscribe to the same type, and every subscriber
// receives every published value. Delivery is in-process, memory-only, and
// backpressured. The topic pump holds at most one value in flight: a send
// returns when the pump accepts the value, which may be before every subscriber
// accepts it, but the pump cannot accept another publication while a slow
// subscriber blocks delivery. Modules must therefore treat every send as
// potentially blocking. There is no durability, replay, retry, or
// acknowledgement; if work must survive a crash, persist it first and use the
// topic as a wake-up.
//
// A module may close a publisher channel when it has finished publishing. A
// publisher completes when its channel is closed or its module returns,
// whichever happens first, and a topic completes once every publisher has
// completed. Backplane then closes the subscriber channels, so ranging over a
// subscription is the natural consumption loop. Subject to cancellation,
// values accepted before the final publisher completes are delivered before
// subscribers close.
//
// Closing one publisher does not affect other publishers on the topic. Sending
// after closing a publisher panics, as usual in Go. Modules must not close
// subscriber channels or use any channel after returning. A module that returns
// while values are still flowing simply stops participating: its subscriptions
// are dropped so it cannot backpressure the topic from beyond the grave.
//
// # Lifecycle
//
// Run starts every module in one [golang.org/x/sync/errgroup.Group].
// Returning nil completes only that module; the first non-nil error cancels
// the shared context, and cancelling the context passed to Run stops the
// runtime. Run waits for every module to return. It reports the first module
// error, wrapped with the module's name; if no module fails, it returns the
// caller's context error, which is nil after natural completion. After
// cancellation, publishes are drained and dropped so blocked senders can
// unwind, but every module is still responsible for observing ctx and returning
// promptly. Panics are programmer faults and are not recovered.
//
// # Latest
//
// [Latest] is the deliberate boundary between streams and state: it retains
// the newest value and its arrival time, and its watchers get latest-wins
// delivery that may skip intermediate values but never backpressures the
// source. Use a subscriber when every value matters, a Latest when only the
// current value does. The runtime projects topics through [NewLatest]; callers
// outside the runtime can use the same constructor with a channel they own,
// without gaining write or close methods on Latest itself.
package backplane
