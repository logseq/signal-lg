# signal-lg

`signal-lg` is a small, statically typed incremental runtime written entirely in
LG. It provides the Bonsai-lite kernel used by LUI:

- batched state and deterministic stabilization;
- reactive constants, multi-input `map`, `cutoff`, and explicit disposal;
- FIFO effects and monotonic generations;
- typed subscriptions and idempotent cleanup;
- scoped state slots and mount/unmount lifecycle;
- local `switch` scopes;
- keyed item signals that retain child identity across `Insert`, `Remove`, and
  `Move` patches.

The dependency graph stays statically typed. Each `signal<value>` stores only
callbacks accepting that same `value` type; the scheduler stores only
zero-argument tasks. There is no universal dynamic value or unsafe cast.

## Test

```sh
opam exec -- dune runtest
```

The test alias compiles the same LG sources for Native and Melange and runs the
shared `clojure.test` suite on both targets.
