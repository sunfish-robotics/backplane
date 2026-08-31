# Changelog

## [0.3.0](https://github.com/sunfish-robotics/backplane/compare/v0.2.0...v0.3.0) (2026-08-31)


### Features

* make Mermaid graphs easier to read ([36f44c2](https://github.com/sunfish-robotics/backplane/commit/36f44c2c69ca731d4aa9e7bce29d9a8fd1a00fe4))
* make Mermaid graphs easier to read ([260a895](https://github.com/sunfish-robotics/backplane/commit/260a895551e0c60b291acac3c413795cec9435a1))

## [0.2.0](https://github.com/sunfish-robotics/backplane/compare/v0.1.0...v0.2.0) (2026-08-24)


### Features

* complete topics when publishers close ([43d1dbd](https://github.com/sunfish-robotics/backplane/commit/43d1dbddf5ae601a271167b45f70825a94339773))


### Bug Fixes

* return caller cancellation after clean shutdown ([59b8599](https://github.com/sunfish-robotics/backplane/commit/59b8599cf0ee0cc9583a07a265abff68486457b6))

## 0.1.0 (2026-08-21)

Initial public release of Backplane, a small in-process application runtime for Go.

### Runtime

- Declare modules as ordinary `func(context.Context, ...dependencies) error` functions. Channel directions declare typed publishers and subscribers; other parameters declare caller-owned resources.
- Validate module signatures, topic producers, and resource bindings before any module starts. Concrete resources can satisfy interface parameters when the match is unambiguous.
- Run a fixed module set concurrently under one shared context. The first module error cancels its siblings, waits for them to return, and reports the failing module by name.

### Topics and state

- Fan values from multiple publishers to every live subscriber of the same exact Go type. The topic pump accepts at most one value at a time, so slow subscribers apply backpressure to subsequent publications.
- Close subscriptions after every publisher has returned, remove subscriptions whose module has already stopped, and drain and drop publications during cancellation so blocked publishers can unwind.
- Provide `Latest[T]` for current-state consumers, with timestamped `Load` access and non-backpressuring, latest-wins `Watch` notifications.

### Topology and documentation

- Derive a deterministic graph of modules, resources, and topics from the same declarations used at runtime, without starting modules or constructing resources.
- Render the topology as a Mermaid flowchart.
- Document the lifecycle, ownership, and delivery contracts with package documentation and executable examples.
