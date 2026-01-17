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

use crate::executor::ExecutorShared;

pub struct TaskGroup<S> {
    tasks: Slab<TaskEntry>,
    shared: Rc<RefCell<TaskGroupShared<S>>>,
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
    // pridicates slab is expected to be small.
    predicates: Slab<PredicateEntry<S>>,
    local_states: Slab<LocalStateEntry>,
    // should indexed with task_index, at most one pred_expr for one task
    // we put it here for cache locality and access from DyadHandle
    pred_exprs: Vec<Option<PredExpr>>,
    dead_predicates: Rc<RefCell<Vec<usize>>>,
    dead_local_states: Rc<RefCell<Vec<usize>>>,
    changed_local_states: Rc<RefCell<Vec<usize>>>,
}

impl<S> TaskGroupShared<S> {
    pub fn eval_pred_expr(&self, pred_expr: &PredExpr) -> bool {
        match pred_expr {
            PredExpr::And(lhs, rhs) => {
                self.eval_pred_expr(lhs) && self.eval_pred_expr(rhs)
            }
            PredExpr::Or(lhs, rhs) => {
                self.eval_pred_expr(lhs) || self.eval_pred_expr(rhs)
            }
            PredExpr::Not(inner) => {
                !self.eval_pred_expr(inner)
            }
            PredExpr::Leaf(predicate_index) => {
                if let Some(predicate_entry) = self.predicates.get(*predicate_index) {
                    (predicate_entry.func)(&self.state)
                } else {
                    false
                }
            }
        }
    }
}

impl<S: 'static> TaskGroup<S> {
    pub fn new(state: S) -> Self {
        TaskGroup {
            tasks: Slab::new(),
            shared: Rc::new(RefCell::new(TaskGroupShared {
                state,
                state_changed: false,
                queue: TaskGroupQueue::new(),
                new_tasks: Vec::new(),
                predicates: Slab::new(),
                local_states: Slab::new(),
                pred_exprs: Vec::new(),
                dead_local_states: Rc::new(RefCell::new(Vec::new())),
                dead_predicates: Rc::new(RefCell::new(Vec::new())),
                changed_local_states: Rc::new(RefCell::new(Vec::new())),
            })),
            executor_shared: None,
            task_group_id: None,
        }
    }

    pub fn spawn<F>(&mut self, task: F)
    where
        F: for<'a> AsyncFnOnce(&'a mut DyadHandle<S>) + 'static,
    {
        let entry = self.tasks.vacant_entry();
        let task_index = entry.key();

        let mut shared = self.shared.clone();
        let future = async move {
            let mut dyad_handle = DyadHandle {
                task_group_shared: &mut shared,
                task_index,
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
    }
}

pub(crate) trait TaskGroupTrait {
    fn schedule(&mut self);
    fn wake_task(&mut self, task_index: usize);
    fn is_live(&self) -> bool;
    fn attached_to_executor(&mut self, executor_shared: Arc<ExecutorShared>, task_group_id: usize);
}

impl<S> TaskGroupTrait for TaskGroup<S> {
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
                for (task_index, pred_expr) in shared.pred_exprs.iter().enumerate() {
                    if let Some(pred_expr) = pred_expr {
                        if shared.eval_pred_expr(pred_expr) {
                            shared.queue.push(task_index);
                        }
                    }                    
                }
                shared.state_changed = false;
            } else {
                for local_state_index in shared.changed_local_states.borrow_mut().drain(..) {
                    if let Some(local_state_entry) = shared.local_states.get(local_state_index) {
                        let predicates_using = local_state_entry.predicates_using.borrow();
                        for &predicate_index in predicates_using.iter() {
                            if let Some(predicate_entry) = shared.predicates.get(predicate_index) {
                                let task_index = predicate_entry.task_index;
                                if let Some(pred_expr) = &shared.pred_exprs.get(task_index).and_then(|e| e.as_ref()) {
                                    if shared.eval_pred_expr(pred_expr) {
                                        shared.queue.push(task_index);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Remove predicates belonging to dead tasks
            shared.predicates.retain(
                |_, predicate_entry| !dead_tasks.contains(&predicate_entry.task_index));

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

        {
            let shared = &mut *self.shared.borrow_mut();
            // Clean up dead predicates
            for predicate_index in shared.dead_predicates.borrow_mut().drain(..) {
                // Predicates can be removed through multiple paths:
                // - explicit handle drop queues the index into `dead_predicates`
                // - task exit removes all predicates belonging to the dead task via `retain`
                // So this cleanup must be idempotent.
                if shared.predicates.contains(predicate_index) {
                    shared.predicates.remove(predicate_index);
                }
            }
            // Clean up dead local states
            for local_state_index in shared.dead_local_states.borrow_mut().drain(..) {
                // it's possible same local index is added multiple times
                // handle drop -> bind with key -> handle drop again
                if let Some(entry) = shared.local_states.get(local_state_index) {
                    if Rc::strong_count(&entry.state) == 1 {
                        // no other references
                        shared.local_states.remove(local_state_index);
                    }
                }
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
}

// erase TaskGroup type to store in executor slab
pub(crate) type UntypedTaskGroup = Box<dyn TaskGroupTrait>;

pub struct TaskGroupHandle {
    phantom: PhantomData<*mut ()>,
}

/// Handle passed into tasks to interact with the task group
/// 'task lifetime ensures that the handle does not outlive the task
pub struct DyadHandle<'task, S> {
    task_group_shared: &'task mut Rc<RefCell<TaskGroupShared<S>>>,
    task_index: usize,
    phantom: PhantomData<*mut &'task ()>, // Invariant over 'task
}

impl<'task, S: 'static> DyadHandle<'task, S> {
    pub fn spawn<F>(&mut self, f: F)
    where
        F: for<'a> AsyncFnOnce(&'a mut DyadHandle<'_, S>) + 'static,
    {
        let mut task_group_shared = self.task_group_shared.clone();
        let task_index_ref_cell = Rc::new(RefCell::new(0usize));
        let task_index = task_index_ref_cell.clone();
        let task_future = async move {
            let task_index = *task_index.borrow();
            let mut dyad_handle = DyadHandle {
                task_group_shared: &mut task_group_shared,
                task_index,
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

    pub fn access<R, F: FnOnce(&S) -> R>(&self, f: F) -> R {
        let shared = self.task_group_shared.borrow();
        f(&shared.state)
    }

    pub fn add_predicate<F>(&mut self, predicate: F) -> PredicateHandle<'task>
    where
        F: Fn(&S) -> bool + 'static,
    {
        let mut shared = self.task_group_shared.borrow_mut();
        let predicate_index = shared.predicates.insert(PredicateEntry {
            func: Box::new(predicate),
            task_index: self.task_index,
            is_active: Cell::new(true),
        });
        PredicateHandle {
            predicate_index,
            dead_predicates: shared.dead_predicates.clone(),
            phantom: PhantomData,
        }
    }

    pub fn add_predicate_with_state<F>(&mut self, mut predicate: F) -> PredicateHandle<'task>
    where
        F: FnMut(&mut LocalStateInPredicateBinder) -> Box<dyn Fn(&S) -> bool> + 'static,
    {
        let shared = &mut *self.task_group_shared.borrow_mut();
        let entry = shared.predicates.vacant_entry();
        let predicate_index = entry.key();
        let mut binder = LocalStateInPredicateBinder {
            local_states: &shared.local_states,
            dead_local_states: &shared.dead_local_states,
            predicate_index,
        };
        let predicate_func = predicate(&mut binder);
        entry.insert(PredicateEntry {
            func: predicate_func,
            task_index: self.task_index,
            is_active: Cell::new(true),
        });
        PredicateHandle {
            predicate_index,
            dead_predicates: shared.dead_predicates.clone(),
            phantom: PhantomData,
        }
    }

    pub fn create_local_state<T: 'static>(&mut self, state: T) -> LocalStateHandle<'task, T> {
        let mut shared = self.task_group_shared.borrow_mut();
        let local_state_index = shared.local_states.insert(LocalStateEntry {
            state: Rc::new(RefCell::new(state)),
            predicates_using: Rc::new(RefCell::new(Vec::new())),
        });
        LocalStateHandle {
            local_state_index,
            state: shared
                .local_states
                .get(local_state_index)
                .unwrap()
                .state
                .clone(),
            dead_local_states: shared.dead_local_states.clone(),
            changed_local_states: shared.changed_local_states.clone(),
            phantom: PhantomData,
        }
    }

    pub fn bind_local_state<T>(&mut self, key: LocalStateKey<T>) -> LocalStateHandle<'task, T> {
        let shared = &mut *self.task_group_shared.borrow_mut();
        let local_state_entry = &shared.local_states[key.local_state_index];
        LocalStateHandle {
            local_state_index: key.local_state_index,
            state: local_state_entry.state.clone(),
            dead_local_states: shared.dead_local_states.clone(),
            changed_local_states: shared.changed_local_states.clone(),
            phantom: PhantomData,
        }
    }

    pub async fn wait_until(&mut self, pred_expr: impl Into<PredExpr>) {
        let pred_expr = pred_expr.into();
        let mut shared = self.task_group_shared.borrow_mut();
        let task_index = self.task_index;
        if shared.pred_exprs.len() <= task_index {
            shared.pred_exprs.resize_with(task_index + 1, || None);
        }
        shared.pred_exprs[task_index] = Some(pred_expr);
        drop(shared);
        poll_fn(|_| {
            let mut shared_ref_mut = self.task_group_shared.borrow_mut();
            let pred_expr = shared_ref_mut.pred_exprs[task_index].as_ref().unwrap();
            let ready = shared_ref_mut.eval_pred_expr(pred_expr);
            if ready {
                shared_ref_mut.pred_exprs[task_index] = None;
                drop(shared_ref_mut);
                Poll::Ready(())
            } else {
                drop(shared_ref_mut);
                Poll::Pending
            }
        }).await;
    }
}

struct PredicateEntry<S> {
    func: Box<dyn Fn(&S) -> bool>,
    task_index: usize,
    is_active: Cell<bool>,
}

pub struct PredicateHandle<'task> {
    predicate_index: usize,
    dead_predicates: Rc<RefCell<Vec<usize>>>,
    phantom: PhantomData<*mut &'task ()>, // Invariant over 'task
}

impl<'task> PredicateHandle<'task> {
    pub fn is_ready<S>(&self, dyad_handle: &DyadHandle<S>) -> bool {
        let shared = dyad_handle.task_group_shared.borrow();
        let predicate_entry = &shared.predicates[self.predicate_index];
        (predicate_entry.func)(&shared.state)
    }

    pub fn set_active<S>(&self, dyad_handle: &mut DyadHandle<S>, is_active: bool) {
        let shared = dyad_handle.task_group_shared.borrow_mut();
        shared.predicates[self.predicate_index]
            .is_active
            .set(is_active);
    }

    pub fn is_active<S>(&self, dyad_handle: &DyadHandle<S>) -> bool {
        let shared = dyad_handle.task_group_shared.borrow();
        shared.predicates[self.predicate_index].is_active.get()
    }
}

impl<'task> Drop for PredicateHandle<'task> {
    fn drop(&mut self) {
        self.dead_predicates.borrow_mut().push(self.predicate_index);
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

struct LocalStateEntry {
    state: Rc<RefCell<dyn Any>>,
    predicates_using: Rc<RefCell<Vec<usize>>>,
}

pub struct LocalStateHandle<'task, T> {
    local_state_index: usize,
    state: Rc<RefCell<dyn Any>>,
    dead_local_states: Rc<RefCell<Vec<usize>>>,
    changed_local_states: Rc<RefCell<Vec<usize>>>,
    phantom: PhantomData<*mut &'task T>, // Invariant over 'task
}

pub struct LocalStateInPredictateHandle<T> {
    local_state_index: usize,
    state: Rc<RefCell<dyn Any>>,
    dead_local_states: Rc<RefCell<Vec<usize>>>,
    phantom: PhantomData<*mut T>,
}

impl<T: Any> LocalStateInPredictateHandle<T> {
    pub fn access<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let state = self.state.borrow();
        let typed_state = state.downcast_ref::<T>().expect("LocalState type mismatch");
        f(typed_state)
    }
}

impl<T> Drop for LocalStateInPredictateHandle<T> {
    fn drop(&mut self) {
        // there is always at least one Rc reference from the local_states slab
        if Rc::strong_count(&self.state) == 2 {
            self.dead_local_states
                .borrow_mut()
                .push(self.local_state_index);
        }
    }
}

impl<'task, T: Any> LocalStateHandle<'task, T> {
    pub fn access<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let state = self.state.borrow();
        let typed_state = state.downcast_ref::<T>().expect("LocalState type mismatch");
        f(typed_state)
    }

    pub fn modify<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.changed_local_states
            .borrow_mut()
            .push(self.local_state_index);
        let mut state = self.state.borrow_mut();
        let typed_state = state.downcast_mut::<T>().expect("LocalState type mismatch");
        f(typed_state)
    }

    pub fn key(&self) -> LocalStateKey<T> {
        LocalStateKey {
            local_state_index: self.local_state_index,
            phantom: PhantomData,
        }
    }
}

impl<'task, T> Drop for LocalStateHandle<'task, T> {
    fn drop(&mut self) {
        // there is always at least one Rc reference from the local_states slab
        if Rc::strong_count(&self.state) == 2 {
            self.dead_local_states
                .borrow_mut()
                .push(self.local_state_index);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LocalStateKey<T> {
    local_state_index: usize,
    phantom: PhantomData<Rc<T>>,
}

pub struct LocalStateInPredicateBinder<'a> {
    local_states: &'a Slab<LocalStateEntry>,
    dead_local_states: &'a Rc<RefCell<Vec<usize>>>,
    predicate_index: usize,
}

impl<'a> LocalStateInPredicateBinder<'a> {
    pub fn bind<T>(&mut self, key: LocalStateKey<T>) -> LocalStateInPredictateHandle<T> {
        let local_state_entry = self
            .local_states
            .get(key.local_state_index)
            .expect("LocalState dropped");
        local_state_entry
            .predicates_using
            .borrow_mut()
            .push(self.predicate_index);
        LocalStateInPredictateHandle {
            state: local_state_entry.state.clone(),
            local_state_index: key.local_state_index,
            dead_local_states: self.dead_local_states.clone(),
            phantom: PhantomData,
        }
    }
}

pub enum PredExpr {
    And(Box<PredExpr>, Box<PredExpr>),
    Or(Box<PredExpr>, Box<PredExpr>),
    Not(Box<PredExpr>),
    Leaf(usize),
}

impl BitAnd for PredExpr {
    type Output = PredExpr;

    fn bitand(self, rhs: Self) -> Self::Output {
        PredExpr::And(Box::new(self), Box::new(rhs))
    }
}

impl BitAnd<&PredicateHandle<'_>> for PredExpr {
    type Output = PredExpr;

    fn bitand(self, rhs: &PredicateHandle<'_>) -> Self::Output {
        PredExpr::And(Box::new(self), Box::new(PredExpr::Leaf(rhs.predicate_index)))
    }
}

impl BitAnd<PredExpr> for &PredicateHandle<'_> {
    type Output = PredExpr;

    fn bitand(self, rhs: PredExpr) -> Self::Output {
        PredExpr::And(Box::new(PredExpr::Leaf(self.predicate_index)), Box::new(rhs))
    }
}

impl BitAnd<&PredicateHandle<'_>> for &PredicateHandle<'_> {
    type Output = PredExpr;

    fn bitand(self, rhs: &PredicateHandle<'_>) -> Self::Output {
        PredExpr::And(
            Box::new(PredExpr::Leaf(self.predicate_index)),
            Box::new(PredExpr::Leaf(rhs.predicate_index)),
        )
    }
}

impl BitOr for PredExpr {
    type Output = PredExpr;

    fn bitor(self, rhs: Self) -> Self::Output {
        PredExpr::Or(Box::new(self), Box::new(rhs))
    }
}

impl BitOr<&PredicateHandle<'_>> for PredExpr {
    type Output = PredExpr;

    fn bitor(self, rhs: &PredicateHandle<'_>) -> Self::Output {
        PredExpr::Or(Box::new(self), Box::new(PredExpr::Leaf(rhs.predicate_index)))
    }
}

impl BitOr<PredExpr> for &PredicateHandle<'_> {
    type Output = PredExpr;

    fn bitor(self, rhs: PredExpr) -> Self::Output {
        PredExpr::Or(Box::new(PredExpr::Leaf(self.predicate_index)), Box::new(rhs))
    }
}

impl BitOr<&PredicateHandle<'_>> for &PredicateHandle<'_> {
    type Output = PredExpr;

    fn bitor(self, rhs: &PredicateHandle<'_>) -> Self::Output {
        PredExpr::Or(
            Box::new(PredExpr::Leaf(self.predicate_index)),
            Box::new(PredExpr::Leaf(rhs.predicate_index)),
        )
    }
}

impl Not for PredExpr {
    type Output = PredExpr;

    fn not(self) -> Self::Output {
        PredExpr::Not(Box::new(self))
    }
}

impl Not for &PredicateHandle<'_> {
    type Output = PredExpr;

    fn not(self) -> Self::Output {
        PredExpr::Not(Box::new(PredExpr::Leaf(self.predicate_index)))
    }
}

impl From<&PredicateHandle<'_>> for PredExpr {
    fn from(handle: &PredicateHandle<'_>) -> Self {
        PredExpr::Leaf(handle.predicate_index)
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
    fn test_local_state() {
        let mut runtime = TestRuntime::new();

        struct SharedState {
            counter: usize,
        }

        let mut tg = TaskGroup::new(SharedState { counter: 0 });

        tg.spawn(async |dh| {
            let local_state = dh.create_local_state::<usize>(0usize);
            let local_state_key = local_state.key();
            let p1 = dh.add_predicate_with_state(move |binder| {
                let local_handle = binder.bind::<usize>(local_state_key);
                Box::new(move |s: &SharedState| {
                    let local_value = local_handle.access(|v| *v);
                    s.counter < local_value
                })
            });
            dh.spawn(async move |dh| {
                let mut local_state = dh.bind_local_state(local_state_key);

                sleep_cycles(10).await;
                local_state.modify(|v| *v = 5);
            });
            dh.wait_until(&p1).await;
            println!("Predicate satisfied: counter < local_state");
        });

        runtime.executor.add_task_group(tg);
        runtime.run();
    }
}
