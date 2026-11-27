use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::marker::PhantomData;
use std::ops::{BitAnd, BitOr, Not};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use slab::Slab;

use crate::context::DyadContext;
use crate::executor::ExecutorShared;
use crate::{ChildDyadContext, task_group};

pub(crate) struct TaskGroup<S, C> {
    tasks: Slab<TaskEntry>,
    shared: Rc<RefCell<TaskGroupShared<S>>>,
    context: DyadContext<C>,
    executor_shared: Option<Arc<ExecutorShared>>,
    task_group_id: Option<usize>,
}

struct TaskGroupShared<S> {
    state: S,
    state_changed: bool,
    queue: TaskGroupQueue,
    new_tasks: Vec<(
        Pin<Box<dyn Future<Output = ()> + 'static>>,
        Rc<RefCell<usize>>,
    )>,
    new_task_groups: Vec<Box<dyn TaskGroupTrait>>,
    predicates: Vec<Option<fn(&S)->bool>>,
}

impl<S: 'static, C: 'static> TaskGroup<S, C> {
    pub(crate) fn new(tg: impl IntoTaskGroup<S, C>, context: DyadContext<C>) -> Self {
        let (state, f) = tg.into_task_group();
        let task_group = Self::create(state, context);
        let mut spawner = DyadSpawner {
            task_group,
        };
        f(&mut spawner);
        spawner.task_group
    }

    fn create(state: S, context: DyadContext<C>) -> Self {
        TaskGroup {
            tasks: Slab::new(),
            shared: Rc::new(RefCell::new(TaskGroupShared {
                state,
                state_changed: false,
                queue: TaskGroupQueue::new(),
                new_tasks: Vec::new(),
                new_task_groups: Vec::new(),
                predicates: Vec::new(),
            })),
            context,
            executor_shared: None,
            task_group_id: None,
        }
    }

    /// Spawns a task and returns its ID within this group.
    /// The ID remains valid while the task is alive and may be reused after completion.
    fn spawn<F>(&mut self, task: F) -> usize
    where
        F: for<'a> AsyncFnOnce(&'a mut DyadHandle<S, C>) + 'static,
    {
        let entry = self.tasks.vacant_entry();
        let task_index = entry.key();

        let mut shared = self.shared.clone();
        let context = self.context.clone();
        let future = async move {
            let mut dyad_handle = DyadHandle {
                task_group_shared: &mut shared,
                task_index,
                context,
                phantom: PhantomData,
            };
            task(&mut dyad_handle).await;
        };

        let task = Task {
            future: Box::pin(future),
        };

        let waker = if let Some(executor_shared) = &self.executor_shared {
            Waker::from(Arc::new(TaskWaker {
                executor_shared: executor_shared.clone(),
                task_group_id: self.task_group_id.unwrap(),
                task_index,
            }))
        } else {
            Waker::noop().clone()
        };
        entry.insert(TaskEntry { task, waker });
        task_index
    }

    pub(crate) fn get_handle(&self) -> TaskGroupHandle<S> {
        TaskGroupHandle {
            task_group_shared: self.shared.clone(),
        }
    }
}

pub(crate) trait TaskGroupTrait {
    fn schedule(&mut self);
    fn wake_task(&mut self, task_index: usize);
    fn is_live(&self) -> bool;
    fn attached_to_executor(&mut self, executor_shared: Arc<ExecutorShared>, task_group_id: usize);
    fn take_new_task_groups(&mut self) -> Vec<Box<dyn TaskGroupTrait>>;
}

impl<S, C> TaskGroupTrait for TaskGroup<S, C> {
    fn schedule(&mut self) {
        let mut last_chunk_index = None;
        let mut dead_tasks = Vec::new();

        loop {
            dead_tasks.clear();
            let mut shared_ref_mut = self.shared.borrow_mut();
            let shared = &mut *shared_ref_mut;
            if shared.queue.is_empty() {
                break;
            }
            let bitmap_chunk = shared.queue.take_bitmap_chunk(last_chunk_index);
            last_chunk_index = Some(bitmap_chunk.chunk_index);
            std::mem::drop(shared_ref_mut);

            for task_index in bitmap_chunk {
                let task_entry = match self.tasks.get_mut(task_index) {
                    None => continue, // task has gone
                    Some(task_entry) => task_entry,
                };
                let mut cx = Context::from_waker(&task_entry.waker);
                match task_entry.task.future.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(()) => {
                        dead_tasks.push(task_index);
                    }
                }
            }

            let mut shared_ref_mut = self.shared.borrow_mut();
            let shared = &mut *shared_ref_mut;

            if shared.state_changed {
                for (task_index, predicate) in shared.predicates.iter().enumerate() {
                    if let Some(predicate) = predicate {
                        if predicate(&shared.state) {
                            shared.queue.push(task_index);
                        }
                    }
                }
                shared.state_changed = false;
            }

            for task_index in dead_tasks.drain(..) {
                self.tasks.remove(task_index);
            }

            let mut new_tasks = std::mem::take(&mut shared.new_tasks);
            for (future, task_index_ref_cell) in new_tasks.drain(..) {
                let entry = self.tasks.vacant_entry();
                let task_index = entry.key();
                task_index_ref_cell.replace(task_index);
                let waker = Waker::from(Arc::new(TaskWaker {
                    executor_shared: self.executor_shared.as_ref().unwrap().clone(),
                    task_group_id: self.task_group_id.unwrap(),
                    task_index,
                }));
                let task = Task { future };
                entry.insert(TaskEntry { task, waker });
                shared.queue.push(task_index);
            }
        }
    }

    fn wake_task(&mut self, task_index: usize) {
        let shared = &mut *self.shared.borrow_mut();
        shared.queue.push(task_index);
    }

    fn is_live(&self) -> bool {
        !self.tasks.is_empty()
    }

    fn attached_to_executor(&mut self, executor_shared: Arc<ExecutorShared>, task_group_id: usize) {
        for (task_index, entry) in self.tasks.iter_mut() {
            let waker = Waker::from(Arc::new(TaskWaker {
                executor_shared: executor_shared.clone(),
                task_group_id,
                task_index,
            }));
            entry.waker = waker;
        }

        self.executor_shared = Some(executor_shared);
        self.task_group_id = Some(task_group_id);

        // Newly-attached task groups should poll all existing tasks at least once.
        // Do this by scheduling them through the executor's global queue; the executor
        // will then schedule this task group on the next `Executor::run()`.
        let executor_shared = self.executor_shared.as_ref().unwrap().clone();
        for task_index in self.tasks.iter().map(|(task_index, _)| task_index) {
            executor_shared.schedule_task(task_group_id, task_index);
        }
    }
    
    fn take_new_task_groups(&mut self) -> Vec<Box<dyn TaskGroupTrait>> {
        std::mem::take(&mut self.shared.borrow_mut().new_task_groups)
    }
}

// erase TaskGroup type to store in executor slab
pub(crate) type UntypedTaskGroup = Box<dyn TaskGroupTrait>;

/// Handle passed into tasks to interact with the task group
/// 'task lifetime ensures that the handle does not outlive the task
pub struct DyadHandle<'task, S, C> {
    task_group_shared: &'task mut Rc<RefCell<TaskGroupShared<S>>>,
    task_index: usize,
    context: DyadContext<C>,
    phantom: PhantomData<*mut &'task ()>, // Invariant over 'task
}

impl<'task, S: 'static, C: 'static> DyadHandle<'task, S, C> {
    pub fn spawn_task_group<NS: 'static, TG: IntoTaskGroup<NS, C>>(&mut self, tg: TG) -> TG::Handle{
        let task_group = TaskGroup::new(tg, self.context.clone());
        let handle = task_group.get_handle();
        self.task_group_shared.borrow_mut().new_task_groups.push(Box::new(task_group));
        handle.into()
    }

    pub fn spawn_task_group_with_context<NS, NC, TG>(&mut self, tg: TG, mut c: NC) -> TG::Handle
    where
        NS: 'static,
        NC: 'static + ChildDyadContext<Parent = C>,
        TG: IntoTaskGroup<NS, NC>
    {
        c.parent_holder_mut().set_parent(self.context.clone());
        let task_group = TaskGroup::new(tg, DyadContext::new(c));
        let handle = task_group.get_handle();
        self.task_group_shared.borrow_mut().new_task_groups.push(Box::new(task_group));
        handle.into()
    }

    pub fn spawn<F>(&mut self, f: F)
    where
        F: for<'a> AsyncFnOnce(&'a mut DyadHandle<'_, S, C>) + 'static,
    {
        let mut task_group_shared = self.task_group_shared.clone();
        let task_index_ref_cell = Rc::new(RefCell::new(0usize));
        let task_index = task_index_ref_cell.clone();
        let context = self.context.clone();
        let task_future = async move {
            let task_index = *task_index.borrow();
            let mut dyad_handle = DyadHandle {
                task_group_shared: &mut task_group_shared,
                task_index,
                context,
                phantom: PhantomData,
            };
            f(&mut dyad_handle).await;
        };
        self.task_group_shared
            .borrow_mut()
            .new_tasks
            .push((Box::pin(task_future), task_index_ref_cell));
    }

    pub fn modify<R, F: FnOnce(&mut S) -> R>(&mut self, f: F) -> R {
        let shared = &mut *self.task_group_shared.borrow_mut();
        let r = f(&mut shared.state);
        shared.state_changed = true;
        r
    }

    pub fn modify_with_context<R, F: FnOnce(&mut S, &C) -> R>(&mut self, f: F) -> R {
        let shared = &mut *self.task_group_shared.borrow_mut();
        let r = f(&mut shared.state, &self.context);
        shared.state_changed = true;
        r
    }

    pub fn access<R, F: FnOnce(&S) -> R>(&self, f: F) -> R {
        let shared = self.task_group_shared.borrow();
        f(&shared.state)
    }

    pub fn access_with_context<R, F: FnOnce(&S, &C) -> R>(&self, f: F) -> R {
        let shared = self.task_group_shared.borrow();
        f(&shared.state, &self.context)
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub async fn wait_until(&mut self, predicate: fn(&S) -> bool) {
        let mut shared = self.task_group_shared.borrow_mut();
        let task_index = self.task_index;
        if shared.predicates.len() <= task_index {
            shared.predicates.resize_with(task_index + 1, || None);
        }
        shared.predicates[task_index] = Some(predicate);
        drop(shared);

        struct WaitUntil<'task, 'a, S, C> {
            handle: &'a mut DyadHandle<'task, S, C>,
        }

        impl<'task, 'a, S, C> Future for WaitUntil<'task, 'a, S, C> {
            type Output = ();
        
            fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                let handle = &self.as_mut().handle;
                let task_index = handle.task_index;
                let shared_ref = handle.task_group_shared.borrow();
                let predicate = shared_ref.predicates[task_index].as_ref().unwrap();
                let ready = predicate(&shared_ref.state);
                if ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }

        impl<'task, 'a, S, C> Drop for WaitUntil<'task, 'a, S, C> {
            fn drop(&mut self) {
                let task_index = self.handle.task_index;
                self.handle.task_group_shared.borrow_mut().predicates[task_index] = None;
            }
        }

        WaitUntil { handle: self }.await;
    }
}

pub struct TaskEntry {
    task: Task,
    waker: Waker,
}

pub struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

struct TaskWaker {
    executor_shared: Arc<ExecutorShared>,
    task_group_id: usize,
    task_index: usize,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.executor_shared
            .schedule_task(self.task_group_id, self.task_index);
    }
}

// optimize for small task groups
enum TaskGroupQueue {
    // Bitmap: each bit represents whether task[i] is ready
    // Supports up to 64 tasks per group
    Bitmap(u64),
    // For >64 tasks: Vec of 64-bit chunks + count of set bits
    // we expect most task groups to be small, so this is less common and so performance is not a concern
    Large(Vec<u64>, usize),
}

impl TaskGroupQueue {
    pub fn new() -> Self {
        TaskGroupQueue::Bitmap(0)
    }

    pub fn push(&mut self, task_index: usize) {
        match self {
            TaskGroupQueue::Bitmap(map) => {
                if task_index < 64 {
                    *map |= 1u64 << task_index;
                } else {
                    // Upgrade to Large when needed
                    let mut chunks = vec![0u64; task_index / 64 + 1];
                    chunks[0] = *map;
                    let count = map.count_ones() as usize;
                    let chunk_idx = task_index / 64;
                    let bit_idx = task_index % 64;
                    chunks[chunk_idx] |= 1u64 << bit_idx;
                    *self = TaskGroupQueue::Large(chunks, count + 1);
                }
            }
            TaskGroupQueue::Large(chunks, count) => {
                let chunk_idx = task_index as usize / 64;
                let bit_idx = task_index % 64;

                // Grow if needed
                if chunk_idx >= chunks.len() {
                    chunks.resize(chunk_idx + 1, 0);
                }

                let was_set = chunks[chunk_idx] & (1u64 << bit_idx) != 0;
                chunks[chunk_idx] |= 1u64 << bit_idx;
                if !was_set {
                    *count += 1;
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            TaskGroupQueue::Bitmap(map) => *map == 0,
            TaskGroupQueue::Large(_, count) => *count == 0,
        }
    }

    pub fn take_bitmap_chunk(&mut self, last_chunk_index: Option<usize>) -> BitmapIter {
        match self {
            TaskGroupQueue::Bitmap(map) => {
                let value = *map;
                *map = 0;
                BitmapIter {
                    bitmap: value,
                    chunk_index: 0,
                }
            }
            TaskGroupQueue::Large(chunks, count) => {
                if *count == 0 {
                    return BitmapIter {
                        bitmap: 0,
                        chunk_index: 0,
                    };
                }
                let last_chunk_index = last_chunk_index.unwrap_or(chunks.len() - 1);
                let chunks_len = chunks.len();
                let (s1, s2) = chunks.split_at_mut(last_chunk_index + 1);
                let iter = s2.into_iter().chain(s1.into_iter());
                for (i, chunk) in iter.enumerate() {
                    let actual_chunk_index = (last_chunk_index + 1 + i) % chunks_len;
                    let value = *chunk;
                    if value != 0 {
                        *chunk = 0;
                        let ones = value.count_ones();
                        *count -= ones as usize;
                        return BitmapIter {
                            bitmap: value,
                            chunk_index: actual_chunk_index,
                        };
                    }
                }
                unreachable!()
            }
        }
    }
}

struct BitmapIter {
    bitmap: u64,
    chunk_index: usize,
}

impl Iterator for BitmapIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bitmap == 0 {
            return None;
        }
        let index = self.bitmap.trailing_zeros() as usize;
        self.bitmap &= !(1 << index);
        Some(index + self.chunk_index * 64)
    }
}

pub struct DyadSpawner<S, C> {
    task_group: TaskGroup<S, C>,
}

impl<S: 'static, C: 'static> DyadSpawner<S, C> {
    pub fn spawn_task<F>(&mut self, f: F)
    where
        F: for<'a> AsyncFnOnce(&'a mut DyadHandle<S, C>) + 'static,
    {
        self.task_group.spawn(f);
    }
    
    pub fn spawn_task_group<NS: 'static, TG: IntoTaskGroup<NS, C>>(&mut self, tg: TG) -> TG::Handle {
        let task_group = TaskGroup::new(tg, self.task_group.context.clone());
        let handle = task_group.get_handle();
        self.task_group.shared.borrow_mut().new_task_groups.push(Box::new(task_group));
        handle.into()
    }

    pub fn spawn_task_group_with_context<NS, NC, TG>(&mut self, tg: TG, mut c: NC) -> TG::Handle
    where
        NS: 'static,
        NC: 'static + ChildDyadContext<Parent = C>,
        TG: IntoTaskGroup<NS, NC>
    {
        c.parent_holder_mut().set_parent(self.task_group.context.clone());
        let task_group = TaskGroup::new(tg, DyadContext::new(c));
        let handle = task_group.get_handle();
        self.task_group.shared.borrow_mut().new_task_groups.push(Box::new(task_group));
        handle.into()
    }
}

pub trait IntoTaskGroup<S: 'static, C: 'static> {
    type Handle : From<TaskGroupHandle<S>>;
    const SHUTDOWN_ON_LAST_HANDLE_DROP: bool;
    fn into_task_group(self) -> (S, impl FnOnce(&mut DyadSpawner<S, C>));
}

impl<S: 'static, C: 'static, F> IntoTaskGroup<S, C> for (S, F)
where
    F: FnOnce(&mut DyadSpawner<S, C>),
{
    type Handle = ();
    const SHUTDOWN_ON_LAST_HANDLE_DROP: bool = false;

    fn into_task_group(self) -> (S, impl FnOnce(&mut DyadSpawner<S, C>)) {
        self
    }
}

#[must_use]
pub struct TaskGroupHandle<S> {
    task_group_shared: Rc<RefCell<TaskGroupShared<S>>>,
}

impl<S> TaskGroupHandle<S> {

}

impl<S> From<TaskGroupHandle<S>> for () {
    fn from(_: TaskGroupHandle<S>) -> Self {
        ()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_runtime::{sleep_cycles, TestRuntime};

    #[test]
    fn test_task_group_queue() {
        let mut queue = TaskGroupQueue::new();
        queue.push(1);
        queue.push(2);
        queue.push(3);
        queue.push(1); // duplicate push

        let values: Vec<usize> = queue.take_bitmap_chunk(None).collect();
        assert_eq!(values, [1, 2, 3]);

        for i in 0..256 {
            queue.push(i);
        }
        assert!(!queue.is_empty());
        let values: Vec<usize> = queue.take_bitmap_chunk(Some(0)).collect();
        assert_eq!(values, (64..128).collect::<Vec<usize>>());
        let values: Vec<usize> = queue.take_bitmap_chunk(Some(1)).collect();
        assert_eq!(values, (128..192).collect::<Vec<usize>>());
        let values: Vec<usize> = queue.take_bitmap_chunk(Some(2)).collect();
        assert_eq!(values, (192..256).collect::<Vec<usize>>());
        let values: Vec<usize> = queue.take_bitmap_chunk(None).collect();
        assert_eq!(values, (0..64).collect::<Vec<usize>>());
        assert!(queue.is_empty());
    }

    #[test]
    fn test_basic_task_group1() {
        let mut runtime = TestRuntime::new();

        struct SharedState {
            counter: usize,
            threshold: usize,
        }

        impl SharedState {
            fn new() -> Self {
                Self {
                    counter: 0,
                    threshold: 0,
                }
            }
        }

        impl IntoTaskGroup<Self, ()> for SharedState {
            
            fn into_task_group(self) -> (Self, impl FnOnce(&mut DyadSpawner<Self, ()>)) {
                (self, |spawner| {
                    spawner.spawn_task(async |dh| {
                        dh.modify(|s| s.threshold = 5);
                        dh.spawn(async move |dh| {
                            for _ in 0..10 {
                                sleep_cycles(1).await;
                                dh.modify(|s| s.counter += 1);
                            }
                        });
                        dh.wait_until(|s| s.counter >= s.threshold).await;
                        println!("Predicate satisfied: counter >= 5");
                    });
                })
            }
            
            type Handle = ();
            
            const SHUTDOWN_ON_LAST_HANDLE_DROP: bool = false;
        }

        runtime.executor.add_task_group(SharedState::new(), ());
        runtime.run();
    }
}
