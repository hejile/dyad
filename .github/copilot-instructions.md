# dyad: a Custom Rust Async Runtime

## Core Architecture: The "TaskGroup" Model
The fundamental unit of abstraction is not an async fn, but a TaskGroup.

Definition: A TaskGroup is a collection of tasks that share a single, mutable state (S).

Isolation: Each TaskGroup owns its state. Tasks within the group communicate strictly by modifying and observing this shared state.

Concurrency: The runtime is single-threaded. There are NO threads, Arc, Mutex, or RwLock involved in the core logic. Use Rc and RefCell.

Wake-up mechanism: each task registers one or more functions that take a ref to the shared state and return a bool; if they return true the task is woken. Run these functions whenever the shared state changes.

Scheduling: a TaskGroup acts as a scheduling unit — if a task in a TaskGroup is woken, after it runs the runtime runs any other tasks in that TaskGroup that need to run before scheduling other TaskGroups. This keeps the shared state's cache hot; modern CPUs are mainly bottlenecked by memory latency while computation is very fast.