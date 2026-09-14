use std::cell::{Cell, RefCell};
use std::future::{pending, poll_fn, Future};
use std::pin::pin;
use std::rc::{self, Rc};
use std::task::{Poll, Waker};

use paste::paste;

/// Observable value
pub struct Ov<T> {
    inner: OvInner<T>,
}

impl<T> Ov<T> {
    pub fn with_ref<R>(&self, f: impl FnOnce(&T) -> R) -> R{
        match &self.inner {
            OvInner::Const(v) => f(v),
            OvInner::State(s) => s.with_ref(f),
            OvInner::Computed(c) => c.with_ref(f),
        }
    }
    pub async fn wait_change(&mut self) {
        match &mut self.inner {
            OvInner::Const(v) => {
                let _: () = pending().await;
            },
            OvInner::State(s) => s.wait_change().await,
            OvInner::Computed(c) => c.wait_change().await,
        }
    }

    pub fn new_from_const(value: T) -> Self {
        Self {
            inner: OvInner::Const(Rc::new(value)),
        }
    }

    pub fn new_computed<A, D, F>(d: D, f: F) -> Self
        where
            D: IntoOvTuple<A>,
            (D::OvTuple, F): IntoComputed<T>
    {
        (d.into_ov_tuple(), f).into_computed()
    }
}

impl<T: Copy> Ov<T> {
    pub fn get(&self) -> T {
        self.with_ref(|v| *v)
    }
}

impl<T: 'static> Ov<T> {
    pub fn map<R: 'static, F: 'static>(self, f: F) -> Ov<R>
        where
            F: FnMut(&T) -> R
    {
        Ov::new_computed(self, f)
    }
}

impl<T> Clone for Ov<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

enum OvInner<T> {
    Const(Rc<T>),
    State(OvStateHandle<T>),
    Computed(OvComputedHandle<T>),
}

impl<T> Clone for OvInner<T> {
    fn clone(&self) -> Self {
        match self {
            OvInner::Const(v) => OvInner::Const(v.clone()),
            OvInner::State(s) => {
                let new_id = s.shared.borrow_mut().allocate_state_handle();
                OvInner::State(OvStateHandle {
                    shared: s.shared.clone(),
                    index: new_id,
                })
            },
            OvInner::Computed(c) => {
                let new_id = c.shared.state_shared.borrow_mut().allocate_state_handle();
                OvInner::Computed(OvComputedHandle {
                    shared: c.shared.clone(),
                    index: new_id,
                })
            }
        }
    }
}

pub struct OvMutator<T> {
    inner: OvMutatorInner<T>,
}

enum OvMutatorInner<T> {
    State(OvStateHandle<T>),
}

impl<T> OvMutator<T> {
    pub fn with_ref<R>(&self, f: impl FnOnce(&T) -> R) -> R{
        match &self.inner {
            OvMutatorInner::State(s) => s.with_ref(f),
        }
    }
    pub async fn wait_change(&mut self) {
        match &mut self.inner {
            OvMutatorInner::State(s) => s.wait_change().await,
        }
    }
    pub fn with_mut<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R {
        match &mut self.inner {
            OvMutatorInner::State(s) => s.mutate(f),
        }
    }
}

impl<T: PartialEq> OvMutator<T> {
    /// Replace the value if it is not equal to the new value.
    pub fn replace_if_not_equal(&mut self, new_value: T) {
        match &mut self.inner {
            OvMutatorInner::State(s) => s.try_mutate(|v|
                if *v != new_value {
                    *v = new_value;
                    Some(())
                } else {
                    None
                }
            ),
        };
    }
}

struct OvStateHandle<T> {
    shared: Rc<RefCell<OvStateShared<T>>>,
    index: usize,
}

impl<T> OvStateHandle<T> {
    fn with_ref<R>(&self, f: impl FnOnce(&T) -> R) -> R{
        OvStateShared::with_ref_at(&self.shared, self.index, f)
    }
    async fn wait_change(&mut self) {
        OvStateShared::wait_change_at(&self.shared, self.index).await;
    }
    fn mutate<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut guard = self.shared.borrow_mut();
        let r = f(&mut guard.state);
        guard.wakeup();
        let handle_data = &mut guard.handles[self.index].assert_ov_handle();
        handle_data.state.set(HandleState::Consumed);
        r
    }
    // f should return None if no change is needed.
    // for internal use. if f mutate the state but return None, the observer will not be notified.
    fn try_mutate<R>(&mut self, f: impl FnOnce(&mut T) -> Option<R>) -> Option<R> {
        let mut guard = self.shared.borrow_mut();
        let r = f(&mut guard.state)?;
        guard.wakeup();
        let handle_data = &mut guard.handles[self.index].assert_ov_handle();
        handle_data.state.set(HandleState::Consumed);
        Some(r)
    }
}

struct OvStateShared<T> {
    state: T,
    handles: Vec<PerHandleData>,
}

impl <T> OvStateShared<T> {
    fn new(state: T) -> Self {
        Self {
            state,
            handles: Vec::new(),
        }
    }

    fn wakeup(&self) {
        for handle_data  in &self.handles {
            handle_data.notify_changed();
        }
    }

    fn allocate_state_handle(&mut self) -> usize {
        let id = self.handles.len();
        self.handles.push(PerHandleData::new_ov_handle());
        id
    }

    async fn wait_change_at(shared: &RefCell<OvStateShared<T>>, index: usize) {
        let mut first_run = true;
        poll_fn(|cx| {
            if first_run{
                first_run = false;
                let mut guard = shared.borrow_mut();
                let handle_data = &mut guard.handles[index].assert_ov_handle_mut();

                match handle_data.state.get() {
                    HandleState::Changed => {
                        return Poll::Ready(());
                    },
                    HandleState::Consumed => {
                        handle_data.state.set(HandleState::Waiting);
                    },
                    HandleState::Waiting => { },
                }
            }
            let mut guard = shared.borrow_mut();
            let handle_data = &mut guard.handles[index].assert_ov_handle_mut();
            match handle_data.state.get() {
                HandleState::Changed => Poll::Ready(()),
                HandleState::Waiting => {
                    handle_data.waker.clone_from(cx.waker());
                    Poll::Pending
                },
                HandleState::Consumed => unreachable!(),
            }
        }).await;
    }

    fn with_ref_at<R>(shared: &RefCell<OvStateShared<T>>, index: usize, f: impl FnOnce(&T) -> R) -> R {
        let guard = shared.borrow();
        if let PerHandleData::Ov(handle_data) = &guard.handles[index] {
            if handle_data.state.get() == HandleState::Changed {
                handle_data.state.set(HandleState::Consumed);
            }
        }
        f(&guard.state)
    }
}

enum PerHandleData {
    Empty,
    Ov(OvHandleData),
    Computed(rc::Weak<dyn ComputedNotifier>),
}

impl PerHandleData {
    fn new_empty() -> Self {
        Self::Empty
    }

    fn new_ov_handle() -> Self {
        Self::Ov(OvHandleData::new())
    }

    fn notify_changed(&self) {
        match self {
            PerHandleData::Empty => {},
            PerHandleData::Ov(s) => s.notify_changed(),
            PerHandleData::Computed(c) => c.upgrade().unwrap().notify(),
        }
    }
    fn assert_ov_handle(&self) -> &OvHandleData {
        match self {
            PerHandleData::Ov(s) => s,
            _ => panic!("not a state handle"),
        }
    }
    fn assert_ov_handle_mut(&mut self) -> &mut OvHandleData {
        match self {
            PerHandleData::Ov(s) => s,
            _ => panic!("not a state handle"),
        }
    }
    fn assert_computed_handle(&self) -> &rc::Weak<dyn ComputedNotifier> {
        match self {
            PerHandleData::Computed(c) => c,
            _ => panic!("not a computed handle"),
        }
    }
    fn assert_computed_handle_mut(&mut self) -> &mut rc::Weak<dyn ComputedNotifier> {
        match self {
            PerHandleData::Computed(c) => c,
            _ => panic!("not a computed handle"),
        }
    }
    fn change_to_computed_handle(&mut self, computed_shared: rc::Weak<dyn ComputedNotifier>) {
        *self = PerHandleData::Computed(computed_shared);
    }
}

struct OvHandleData {
    state: Cell<HandleState>,
    waker: Waker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum  HandleState {
    Changed,
    Waiting,
    Consumed,
}

impl OvHandleData {
    fn new() -> Self {
        Self {
            state: Cell::new(HandleState::Consumed),
            waker: Waker::noop().clone(),
        }
    }
    async fn update_waker(&mut self) {
        poll_fn(|cx| {
            let new_waker = cx.waker();
            if !new_waker.will_wake(&self.waker) {
                self.waker = new_waker.clone();
            }
            Poll::Ready(())
        }).await;
    }

    fn notify_changed(&self) {
        if self.state.get() == HandleState::Waiting {
            self.waker.wake_by_ref();
        }
        self.state.set(HandleState::Changed);
    }
}

pub struct Os<T> {
    shared: Rc<RefCell<OvStateShared<T>>>,
}

impl<T> Os<T> {
    pub fn new(state: T) -> Self {
        Self {
            shared: Rc::new(RefCell::new(OvStateShared::new(state))),
        }
    }

    pub fn observe(&self) -> Ov<T> {
        let index = self.shared.borrow_mut().allocate_state_handle();
        Ov {
            inner: OvInner::State(OvStateHandle {
                shared: self.shared.clone(),
                index,
            }),
        }
    }

    pub fn mutator(&mut self) -> OvMutator<T> {
        let index = self.shared.borrow_mut().allocate_state_handle();
        OvMutator {
            inner: OvMutatorInner::State(OvStateHandle {
                shared: self.shared.clone(),
                index,
            }),
        }
    }
}

impl<T: 'static> Os<T> {
    pub fn map<R: 'static, F: 'static>(&self, f: F) -> Ov<R>
        where
            F: FnMut(&T) -> R
    {
        self.observe().map(f)
    }
}

// different from ov because of passive reaction to change.
enum ComputedSource<T> {
    Const(Rc<T>),
    State(OvStateHandle<T>),
    Computed(OvComputedHandle<T>),
}

impl<T> ComputedSource<T> {
    fn new_from_ov(ov: Ov<T>, computed_notifier: rc::Weak<dyn ComputedNotifier>) -> Self {
        match ov.inner {
            OvInner::Const(v) => ComputedSource::Const(v),
            OvInner::State(s) => {
                s.shared.borrow_mut().handles[s.index].change_to_computed_handle(computed_notifier);
                ComputedSource::State(s)
            },
            OvInner::Computed(c) => {
                c.shared.state_shared.borrow_mut().handles[c.index].change_to_computed_handle(computed_notifier);
                ComputedSource::Computed(c)
            },
        }
    }

    fn with_ref<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        match self {
            ComputedSource::Const(v) => f(v),
            ComputedSource::State(s) => s.with_ref(f),
            ComputedSource::Computed(c) => c.with_ref(f),
        }
    }
}

struct OvComputedHandle<T> {
    shared: Rc<OvComputedShared<T>>,
    index: usize,
}

impl<T> OvComputedHandle<T> {
    fn with_ref<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let shared = &*self.shared;
        if shared.dirty.get() {
            let new_state = (shared.recompute.borrow_mut())();
            shared.state_shared.borrow_mut().state = new_state;
            shared.dirty.set(false);
        }
        OvStateShared::with_ref_at(&shared.state_shared, self.index, f)
    }
    async fn wait_change(&mut self) {
        OvStateShared::wait_change_at(&self.shared.state_shared, self.index).await;
    }
}

struct OvComputedShared<T> {
    // Fn is enough for most cases, use FnMut to give users more flexibility.
    recompute: RefCell<Box<dyn FnMut() -> T>>,
    state_shared: RefCell<OvStateShared<T>>,
    dirty: Cell<bool>,
}

impl<T> OvComputedShared<T> {
    fn new(f: Box<dyn FnMut() -> T>, state: T) -> Self {
        Self {
            recompute: RefCell::new(f),
            state_shared: RefCell::new(OvStateShared::new(state)),
            dirty: Cell::new(false),
        }
    }

    fn wakeup(&self) {
        let state_shared = self.state_shared.borrow();
        for handle_data  in &state_shared.handles {
            handle_data.notify_changed();
        }
    }

    fn allocate_observer(&self) -> usize {
        self.state_shared.borrow_mut().allocate_state_handle()
    }
}

trait ComputedNotifier {
    fn notify(&self);
}

impl<T> ComputedNotifier for OvComputedShared<T> {
    fn notify(&self) {
        self.dirty.set(true);
        self.state_shared.borrow().wakeup();
    }
}


pub enum OvOrMutator<T> {
    Ov(Ov<T>),
    Mutator(OvMutator<T>),
    Pair(Ov<T>, OvMutator<T>),
}

pub trait IntoOv<T> {
    fn into_ov(self) -> Ov<T>;
}

impl<T> IntoOv<T> for Ov<T> {
    fn into_ov(self) -> Ov<T> {
        self
    }
}

impl<T> IntoOv<T> for &Os<T> {
    fn into_ov(self) -> Ov<T> {
        self.observe()
    }
}

impl<T> IntoOv<T> for &Ov<T> {
    fn into_ov(self) -> Ov<T> {
        (*self).clone()
    }
}

trait IntoOvTuple<T> {
    type OvTuple;
    fn into_ov_tuple(self) -> Self::OvTuple;
}

impl<T0: 'static, O0> IntoOvTuple<T0> for O0
    where
        O0: IntoOv<T0>,
{
    type OvTuple = Ov<T0>;
    fn into_ov_tuple(self) -> Self::OvTuple {
        self.into_ov()
    }
}

impl<T0: 'static, O0> IntoOvTuple<(T0,)> for (O0,)
    where
        O0: IntoOv<T0>,
{
    type OvTuple = (Ov<T0>,);
    fn into_ov_tuple(self) -> Self::OvTuple {
        (self.0.into_ov(),)
    }
}

impl<T0: 'static, T1: 'static, O0, O1> IntoOvTuple<(T0, T1)> for (O0, O1)
    where
        O0: IntoOv<T0>,
        O1: IntoOv<T1>,
{
    type OvTuple = (Ov<T0>, Ov<T1>);
    fn into_ov_tuple(self) -> Self::OvTuple {
        (self.0.into_ov(), self.1.into_ov())
    }
}

pub(crate) trait Watchable {
    type Target<'a>;
    async fn wait_change(&mut self);
    fn with_ref<R>(&self, f: impl for<'a> FnOnce(Self::Target<'a>) -> R) -> R;
}

impl<T: 'static> Watchable for Ov<T> {
    type Target<'a> = &'a T;
    async fn wait_change(&mut self) {
        self.wait_change().await;
    }
    fn with_ref<R>(&self, f: impl for<'a> FnOnce(Self::Target<'a>) -> R) -> R {
        self.with_ref(f)
    }
}

impl<T: 'static> Watchable for (Ov<T>,) {
    type Target<'a> = (&'a T,);
    async fn wait_change(&mut self) {
        self.0.wait_change().await;
    }
    fn with_ref<R>(&self, f: impl for<'a> FnOnce(Self::Target<'a>) -> R) -> R {
        self.0.with_ref(|v| f((v,)))
    }
}

macro_rules! wait_change_tuple {
    ($t:expr, $($index:expr),*) => {
        paste!{
            $(
                let [<fut $index>] = ($t.$index.wait_change());
                let mut [<pinned_fut $index>] = pin!([<fut $index>]);
            )*
            poll_fn(move |cx| {
                $(
                    if [<pinned_fut $index>].as_mut().poll(cx).is_ready() {
                        return Poll::Ready(());
                    }
                )*
                Poll::Pending
            }).await;    
        };
    };
}

impl<T0: 'static, T1: 'static> Watchable for (Ov<T0>, Ov<T1>) {
    type Target<'a> = (&'a T0, &'a T1);
    async fn wait_change(&mut self) {
        wait_change_tuple!(self, 0, 1);
    }
    fn with_ref<R>(&self, f: impl for<'a> FnOnce(Self::Target<'a>) -> R) -> R {
        self.0.with_ref(|v0| self.1.with_ref(|v1| f((v0, v1))))
    }
}

impl<T0: 'static, T1: 'static, T2: 'static> Watchable for (Ov<T0>, Ov<T1>, Ov<T2>) {
    type Target<'a> = (&'a T0, &'a T1, &'a T2);
    async fn wait_change(&mut self) {
        wait_change_tuple!(self, 0, 1, 2);
    }
    fn with_ref<R>(&self, f: impl for<'a> FnOnce(Self::Target<'a>) -> R) -> R {
        self.0.with_ref(|v0| self.1.with_ref(|v1| self.2.with_ref(|v2| f((v0, v1, v2))))
        )
    }
}

pub fn watch2<T0: 'static, T1: 'static>(o0: impl IntoOv<T0>, o1: impl IntoOv<T1>) -> (Ov<T0>, Ov<T1>) {
    let o0 = o0.into_ov();
    let o1 = o1.into_ov();
    (o0, o1)
}

pub fn watch3<T0: 'static, T1: 'static, T2: 'static>(o0: impl IntoOv<T0>, o1: impl IntoOv<T1>, o2: impl IntoOv<T2>) -> (Ov<T0>, Ov<T1>, Ov<T2>) {
    let o0 = o0.into_ov();
    let o1 = o1.into_ov();
    let o2 = o2.into_ov();
    (o0, o1, o2)
}

pub(crate) trait IntoWatchable {
    type W: Watchable;
    fn into_watchable(self) -> Self::W;
}

impl<T> IntoWatchable for T
    where T: Watchable
{
    type W = T;
    fn into_watchable(self) -> Self::W {
        self
    }
}



impl<'a, T: 'static> IntoWatchable for &'a Os<T> {
    type W = Ov<T>;
    fn into_watchable(self) -> Self::W {
        self.observe()
    }
}

trait IntoComputed<R> {
    fn into_computed(self) -> Ov<R>;
}

impl<T0: 'static, F: 'static, R: 'static> IntoComputed<R> for (Ov<T0>, F)
    where F: FnMut(&T0) -> R
{
    fn into_computed(self) -> Ov<R> {
        let (ov, mut f) = self;
        let init_state = ov.with_ref(|v| f(v));

        let computed_shared = Rc::new_cyclic(|weak_ref| {
            let cs0 = ComputedSource::new_from_ov(ov, weak_ref.clone() as rc::Weak<dyn ComputedNotifier>);
            let recompute = Box::new(move || {
                let new_state = cs0.with_ref(|v| f(v));
                new_state
            });
            OvComputedShared::new(recompute, init_state)
        });

        let index = computed_shared.allocate_observer();

        Ov {
            inner: OvInner::Computed(OvComputedHandle {
                shared: computed_shared,
                index,
            }),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_runtime::{TestRuntime, sleep_cycles};

    #[test]
    fn test_observable_state() {
        let mut runtime = TestRuntime::new();

        let mut state = Os::new(0usize);

        let mut ov = state.observe();
        ov.with_ref(|v| assert_eq!(*v, 0));

        let ov2 = state.observe();

        let mut mutator = state.mutator();

        runtime.spawn_task(async move {
            for i in 1..=3 {
                mutator.with_mut(|v| *v = i);
                poll_fn(|cx| Poll::Ready(cx.waker().wake_by_ref())).await;
                sleep_cycles(3).await;
            }
            println!("mutate done")
        });

        runtime.spawn_task(async move {
            loop {
                let done = ov.with_ref(|v| *v == 3);
                if done {
                    println!("observe done");
                    break;
                }
                ov.wait_change().await;    
            }
        });

        runtime.run();
        ov2.with_ref(|v| assert_eq!(3, *v));
    }

    #[test]
    fn test_ov_tuple() {
        let mut runtime = TestRuntime::new();

        let mut state = Os::new(0usize);

        let mut ov = state.observe();
        ov.with_ref(|v| assert_eq!(*v, 0));

        let ov2 = state.observe();

        let mut mutator = state.mutator();

        runtime.spawn_task(async move {
            for i in 1..=3 {
                mutator.with_mut(|v| *v = i);
                poll_fn(|cx| Poll::Ready(cx.waker().wake_by_ref())).await;
                sleep_cycles(3).await;
            }
            println!("mutate done")
        });

        runtime.spawn_task(async move {
            let mut tuple = (ov, ov2);
            loop {
                let done = tuple.with_ref(|(v1, v2)| *v1 == 3 && *v2 == 3);
                if done {
                    println!("observe done");
                    break;
                }
                tuple.wait_change().await;    
            }
        });

        runtime.run();
    }

    #[test]
    fn test_watchable() {
        let mut runtime = TestRuntime::new();

        let mut counter0 = Os::new(0usize);
        let mut counter1 = Os::new(1usize);
        let mut counter2 = Os::new(2usize);

        let mut tuple = watch3(&counter0, &counter1, &counter2);
        runtime.spawn_task(async move {
            tuple.wait_change().await;
            tuple.with_ref(|(v0, v1, v2)| {
                assert_eq!(*v0, 1);
                assert_eq!(*v1, 1);
                assert_eq!(*v2, 2);
            });
        });

        let mut om0 = counter0.mutator();
        runtime.spawn_task(async move {
            om0.replace_if_not_equal(1);
        });

        runtime.run();

        let mut tuple = watch3(&counter0, &counter1, &counter2);
        runtime.spawn_task(async move {
            tuple.wait_change().await;
            tuple.with_ref(|(v0, v1, v2)| {
                assert_eq!(*v0, 1);
                assert_eq!(*v1, 2);
                assert_eq!(*v2, 3);
            });
        });

        let mut oms = (counter0.mutator(), counter1.mutator(), counter2.mutator());
        runtime.spawn_task(async move {
            oms.0.replace_if_not_equal(1);
            oms.1.replace_if_not_equal(2);
            oms.2.replace_if_not_equal(3);
        });

        runtime.run();
    }

    #[test]
    fn test_computed() {
        let mut runtime = TestRuntime::new();

        let mut counter0 = Os::new(0usize);
        
        let mut computed = Ov::new_computed(&counter0, |v| *v + 1);
        computed.with_ref(|v| assert_eq!(*v, 1));

        runtime.spawn_task(async move {
            counter0.mutator().replace_if_not_equal(1);
        });

        runtime.spawn_task(async move {
            computed.wait_change().await;
            computed.with_ref(|v| assert_eq!(*v, 2));
        });

        runtime.run();
    }

    #[test]
    fn test_simultanous_read() {
        let mut counter = Os::new(0usize);
        let ov0 = counter.observe();
        let ov1 = ov0.clone();
        let ov2 = counter.observe();
        ov0.with_ref(|v0| {
            ov1.with_ref(|v1| {
                ov2.with_ref(|v2| {
                    assert_eq!(*v0, 0);
                    assert_eq!(*v1, 0);
                    assert_eq!(*v2, 0);
                });
            });
        });


        let c0 = Ov::new_computed(ov0, |v| *v + 1);
        let c1 = c0.clone();
        let c2 = c1.clone();
        counter.mutator().with_mut(|v| *v = 1);

        c0.with_ref(|v0| {
            c1.with_ref(|v1| {
                c2.with_ref(|v2| {
                    assert_eq!(*v0, 2);
                    assert_eq!(*v1, 2);
                    assert_eq!(*v2, 2);
                });
            });
        });
    }
}