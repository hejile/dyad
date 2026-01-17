use std::sync::{Arc, atomic::AtomicBool};

use concurrent_queue::ConcurrentQueue;
use slab::Slab;

use crate::task_group::{TaskGroup, UntypedTaskGroup};

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

    pub fn add_task_group<S>(&mut self, task_group: TaskGroup<S>) -> usize
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
                self.shared.notifier.notifed.store(false, Ordering::Release);

                if self.shared.queue.is_empty() {
                    break;
                }

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