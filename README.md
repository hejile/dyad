# dyad

A rust async runtime leveraging temporal borrow. Try to find right high level abstractions for async rust.

dyad introduces `TaskGroup` as its core abstraction.

A `TaskGroup` consists of a shared state and several tasks. Tasks in a `TaskGroup` can read or write the shared state, and get notified if the shared state is modified.

A `TaskGroup` is a single schedule unit. The runtime continues executing other ready tasks within the same `TaskGroup` before switching to other `TaskGroup`.

Tasks in `TaskGroup` use `Predicate` as core mechanism for notification.

Tasks can also share state with `LocalState`.