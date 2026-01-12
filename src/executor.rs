use core::task;
use std::{
    cell::{Cell, RefCell, UnsafeCell}, future, marker::PhantomData, pin::Pin, rc::Rc, sync::{Arc, atomic::AtomicBool}, task::{Poll, Wake, Waker}
};

use concurrent_queue::ConcurrentQueue;
use slab::Slab;

use crate::task_group::{self, Task, TaskGroup, UntypedTaskGroup};

pub struct Executor {
    task_groups: Slab<TaskGroupEntry>,
    shared: Arc<ExecutorShared>,
}

impl Executor {
    pub fn new(notifier: impl Fn() + Send + Sync + 'static) -> Self {
        Executor {
            task_groups: Slab::new(),
            shared: Arc::new(ExecutorShared {
                queue: ConcurrentQueue::unbounded(),
                notifier: ExecutorNotifier::new(notifier),
            }),
        }
    }

    pub fn add_task_group<S>(&mut self, mut task_group: TaskGroup<S>) -> usize
    where
        S: 'static,
    {
        let slab_entry = self.task_groups.vacant_entry();
        let task_group_index = slab_entry.key();
        let mut task_group: UntypedTaskGroup = Box::new(task_group);
        task_group.attached_to_executor(self.shared.clone(), task_group_index);

        let entry = TaskGroupEntry {
            task_group,
        };
        slab_entry.insert(entry);
        task_group_index
    }

    pub fn run(&mut self) {
        use std::sync::atomic::Ordering;

        let mut waked_task_groups = Vec::new();
        loop {
            while let Ok((task_group_index, task_index)) = self.shared.queue.pop() {
                let entry = &mut self.task_groups[task_group_index];
                entry.task_group.wake_task(task_index);

                if !waked_task_groups.contains(&task_group_index) {
                    waked_task_groups.push(task_group_index);
                }
            }

            if waked_task_groups.is_empty() {
                // 进入“空闲”前，先清掉 notifed；然后立刻二次确认队列，避免并发 push 导致丢通知
                self.shared.notifier.notifed.store(false, Ordering::Release);

                if self.shared.queue.is_empty() {
                    break;
                }

                // 队列里已经有新东西（可能是在 notifed=true 时 push 的），继续跑
                // （可选）把 notifed 置回 true，避免别的线程在我们继续运行期间反复触发外部 notifier
                self.shared.notifier.notifed.store(true, Ordering::Release);
                continue;
            }

            for task_group_index in waked_task_groups.drain(..) {
                let entry = &mut self.task_groups[task_group_index];
                entry.task_group.schedule();
                if !entry.task_group.is_live() {
                    self.task_groups.remove(task_group_index);
                }
            }
        }
    }

    pub fn task_group_count(&self) -> usize {
        self.task_groups.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.task_groups.is_empty()
    }
}

pub(crate) struct ExecutorShared {
    queue: ConcurrentQueue<(usize, usize)>,
    notifier: ExecutorNotifier,
}

impl ExecutorShared {
    pub(crate) fn schedule_task(&self, task_group_index: usize, task_index: usize) {
        self.queue.push((task_group_index, task_index)).unwrap();
        self.notifier.notify();
    }
}

struct ExecutorNotifier {
    notifier: Box<dyn Fn() + Send + Sync>,
    notifed: AtomicBool,
}

impl ExecutorNotifier {
    fn new<F>(notifier: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        ExecutorNotifier {
            notifier: Box::new(notifier),
            notifed: AtomicBool::new(false),
        }
    }

    fn notify(&self) {
        if !self.notifed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            (self.notifier)();
        }
    }
}

struct TaskGroupEntry {
    task_group: UntypedTaskGroup,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime() {
        let mut runtime = Executor::new(|| {
            // Notifier logic here
        });

        runtime.run();
    }
}