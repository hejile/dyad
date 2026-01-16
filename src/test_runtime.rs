use crate::executor::Executor;
use crate::task_group::TaskGroup;
use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::task::Waker;

thread_local! {
    static TEST_RUNTIME_CONTEXT: RefCell<Option<TestRuntimeContext>> = RefCell::new(None);
}

pub fn test(f: impl Future<Output = ()> + 'static) {
    let mut runtime = TestRuntime::new();
    let mut task_group = TaskGroup::new(());
    task_group.spawn(async move |_dh| {
        f.await;
    });
    runtime.spawn_task_group(task_group);
    runtime.run();
}

struct SleepEntry {
    wake_at_cycle: u64,
    waker: Waker,
}

impl PartialEq for SleepEntry {
    fn eq(&self, other: &Self) -> bool {
        self.wake_at_cycle == other.wake_at_cycle
    }
}

impl Eq for SleepEntry {}

impl PartialOrd for SleepEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SleepEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (earliest wake time first)
        other.wake_at_cycle.cmp(&self.wake_at_cycle)
    }
}

struct TestRuntimeContext {
    current_cycle: u64,
    sleep_heap: BinaryHeap<SleepEntry>,
}

impl TestRuntimeContext {
    fn new() -> Self {
        TestRuntimeContext {
            current_cycle: 0,
            sleep_heap: BinaryHeap::new(),
        }
    }

    fn register_sleep(&mut self, cycles: u64, waker: Waker) {
        self.sleep_heap.push(SleepEntry {
            wake_at_cycle: self.current_cycle + cycles,
            waker,
        });
    }

    fn advance_cycle(&mut self) {
        self.current_cycle += 1;
        
        // Wake all tasks whose sleep time has elapsed
        while let Some(entry) = self.sleep_heap.peek() {
            if entry.wake_at_cycle <= self.current_cycle {
                let entry = self.sleep_heap.pop().unwrap();
                entry.waker.wake();
            } else {
                break;
            }
        }
    }
}

pub async fn sleep_cycles(cycles: u64) {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct SleepFuture {
        cycle_to_wake: u64,
        registered: bool,
    }

    impl Future for SleepFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            TEST_RUNTIME_CONTEXT.with(|ctx| {
                let mut context = ctx.borrow_mut();
                let context = context.as_mut().unwrap();

                if context.current_cycle >= self.cycle_to_wake {
                    Poll::Ready(())
                } else {
                    if !self.registered {
                        context.register_sleep(self.cycle_to_wake - context.current_cycle, cx.waker().clone());
                        self.registered = true;
                    }
                    Poll::Pending
                }
            })
        }
    }

    let current_cycle = TEST_RUNTIME_CONTEXT.with(|ctx| {
        ctx.borrow().as_ref().unwrap().current_cycle
    });
    SleepFuture {
        cycle_to_wake: current_cycle + cycles,
        registered: false,
    }
    .await
}

pub struct TestRuntime {
    pub executor: Executor,
    context: Option<TestRuntimeContext>,
}

impl TestRuntime {
    pub fn new() -> Self {
        let executor = Executor::new(|| {
            // No-op notifier for test runtime
        });
        TestRuntime {
            executor,
            context: Some(TestRuntimeContext::new())
        }
    }

    pub fn run(&mut self) {
        self.run_cycles(0);
    }

    pub fn run_cycles(&mut self, cycles: u64) {
        let old_value = TEST_RUNTIME_CONTEXT.with(|ctx| {
            let old = ctx.borrow_mut().take();
            *ctx.borrow_mut() = self.context.take();
            old
        });

        let range = 0.. if cycles == 0 { u64::MAX } else { cycles };
        for _ in range {
            if self.executor.is_empty() {
                break;
            }
            self.run_one_cycle();
        }

        TEST_RUNTIME_CONTEXT.with(|ctx| {
            self.context = ctx.borrow_mut().take();
            *ctx.borrow_mut() = old_value;
        });
    }

    pub fn current_cycle(&self) -> u64 {
        TEST_RUNTIME_CONTEXT.with(|ctx| {
            ctx.borrow().as_ref().unwrap().current_cycle
        })
    }

    pub fn run_one_cycle(&mut self) {
        TEST_RUNTIME_CONTEXT.with(|ctx| {
            let mut context = ctx.borrow_mut();
            let context = context.as_mut().unwrap();
            context.advance_cycle();
            println!("cycle {}", context.current_cycle);
        });
        self.executor.run();
    }

    pub fn spawn_task_group<S: 'static>(&mut self, task_group: TaskGroup<S>) {
        self.executor.add_task_group(task_group);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_group::TaskGroup;

    #[test]
    fn test_sleep_cycles() {
        let mut runtime = TestRuntime::new();

        struct SharedState {
            msgs: Vec<String>,
            sender_done: bool,
        }

        let mut tg = TaskGroup::new(SharedState {
            msgs: Vec::new(),
            sender_done: false,
        });
        tg.spawn(async |dh| {
            for i in 0..5 {
                dh.modify(|s| {
                    s.msgs.push(format!("count {}", i));
                });
                sleep_cycles(3).await;
            }
            dh.modify(|s| {
                s.sender_done = true;
            });
        });

        tg.spawn(async |dh| {
            let msg_available = dh.add_predicate(|s| !s.msgs.is_empty());
            let sender_done = dh.add_predicate(|s| s.sender_done);

            let mut count = 0;
            loop {
                count += 1;
                println!("receiver waiting at iteration {}", count);
                dh.wait_until(&msg_available | &sender_done).await;
                if sender_done.is_ready(&dh) {
                    println!("Sender done");
                    break;
                }
                if msg_available.is_ready(&dh) {
                    let msgs = dh.modify(|s| std::mem::take(&mut s.msgs));
                    for msg in msgs {
                        println!("Received: {}", msg);
                    }
                }
            }
        });

        runtime.executor.add_task_group(tg);
        runtime.run_cycles(20);
        assert!(
            runtime.executor.is_empty(),
            "Test runtime did not complete in expected cycles"
        );
    }

    #[test]
    fn test_test() {
        test(async {
            println!("Hello from test runtime!");
            sleep_cycles(2).await;
            println!("Woke up after 2 cycles!");
        });
    }
}