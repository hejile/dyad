use std::collections::BinaryHeap;
use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::rc::{self, Rc};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::time::Instant;

use futures_util::{Future, StreamExt};
use futures_util::future::RemoteHandle;
use futures_util::stream::FuturesUnordered;
use futures_util::task::{LocalSpawnExt, LocalFutureObj, LocalSpawn, SpawnError, ArcWake};
use mio::{Token, Events, Interest};
use scoped_tls::scoped_thread_local;
use slab::Slab;

pub mod net;
pub mod timer;

// thread_local! { static EVENT_MANAGER: RefCell<Option<EventManager>> = RefCell::new(None); }
// thread_local! { static SPAWNER: RefCell<Option<LocalSpawner>> = RefCell::new(None); }

scoped_thread_local! { static RUNTIME_CONTEXT: RuntimeContext }

struct RuntimeContext {
    event_manager: RefCell<EventManager>,
    spawner: LocalSpawner,
    poll_handle: Rc<RefCell<mio::Poll>>,
    timer_manager: RefCell<TimerManager>,
}

impl RuntimeContext {
    fn new(spawner: LocalSpawner, poll_handle: Rc<RefCell<mio::Poll>>) -> Self {
        Self {
            event_manager: RefCell::new(EventManager::new()),
            spawner,
            poll_handle,
            timer_manager: RefCell::new(TimerManager::new()),
        }
    }

    fn new_token(&self) -> mio::Token {
        let token = self.event_manager.borrow_mut().slab.insert(EventEntry::default());
        mio::Token(token)
    }
}

#[derive(Default)]
struct EventEntry {
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    read_ready: bool,
    write_ready: bool,
}

struct EventManager {
    slab: Slab<EventEntry>,
}

impl EventManager {
    fn new() -> Self {
        let mut slab = Slab::with_capacity(1024);
        // 0 is reserved for mio::Waker.
        let waker_token = slab.insert(EventEntry::default());
        assert_eq!(waker_token, 0);
        Self {
            slab
        }
    }
    /// clear read readies and set read waker
    pub fn clear_read_ready(&mut self, token: Token, waker: Waker) {
        let entry = &mut self.slab[token.0];
        entry.read_ready = false;
        entry.read_waker = Some(waker);
    }
    pub fn clear_write_ready(&mut self, token: Token, waker: Waker) {
        let entry = &mut self.slab[token.0];
        entry.write_ready = false;
        entry.write_waker = Some(waker);
    }
    pub fn set_read_ready(&mut self, token: Token) {
        let entry = &mut self.slab[token.0];
        entry.read_ready = true;
        if let Some(waker) = entry.read_waker.take() {
            waker.wake();
        }
    }
    pub fn set_write_ready(&mut self, token: Token) {
        let entry = &mut self.slab[token.0];
        entry.write_ready = true;
        if let Some(waker) = entry.write_waker.take() {
            waker.wake();
        }
    }
    pub fn is_read_ready(&self, token: Token) -> bool {
        self.slab[token.0].read_ready
    }
    pub fn is_write_ready(&self, token: Token) -> bool {
        self.slab[token.0].write_ready
    }
}

struct TimerManager {
    timers: BinaryHeap<TimerEntry>,
}

impl TimerManager {
    fn new() -> Self {
        Self {
            timers: BinaryHeap::with_capacity(16),
        }
    }

    fn next_check_duration(&self) -> Option<std::time::Duration> {
        self.timers.peek().map(|entry| {
            let now = Instant::now();
            if entry.wakeup_instant > now {
                entry.wakeup_instant - now
            } else {
                std::time::Duration::from_millis(0)
            }
        })
    }

    fn wakeup_timers(&mut self) {
        let now = Instant::now();
        while let Some(entry) = self.timers.peek() {
            if entry.wakeup_instant > now {
                break;
            }
            let entry = self.timers.pop().unwrap();
            entry.waker.wake();
        }
    }
}

struct TimerEntry {
    wakeup_instant: Instant,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.wakeup_instant == other.wakeup_instant
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.wakeup_instant.cmp(&self.wakeup_instant))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.wakeup_instant.cmp(&self.wakeup_instant)
    }
}

fn is_read_ready(token: Token) -> bool {
    RUNTIME_CONTEXT.with(|c| c.event_manager.borrow().is_read_ready(token))
}

fn is_write_ready(token: Token) -> bool {
    RUNTIME_CONTEXT.with(|c| c.event_manager.borrow().is_write_ready(token))
}

fn clear_read_ready(token: Token, waker: Waker) {
    RUNTIME_CONTEXT.with(|c| c.event_manager.borrow_mut().clear_read_ready(token, waker));
}

fn clear_write_ready(token: Token, waker: Waker) {
    RUNTIME_CONTEXT.with(|c| c.event_manager.borrow_mut().clear_write_ready(token, waker));
}

pub(crate) fn add_timer(wakeup_instant: Instant, waker: Waker) {
    RUNTIME_CONTEXT.with(|c| {
        c.timer_manager.borrow_mut().timers.push(TimerEntry {
            wakeup_instant,
            waker,
        });
    });
}

pub fn spawn(future: impl Future<Output=()> + 'static) {
    RUNTIME_CONTEXT.with(|c| {
        c.spawner.spawn_local(future).unwrap();
    });
}

pub fn spawn_with_handle<T>(future: impl Future<Output=T> + 'static) -> RemoteHandle<T> {
    RUNTIME_CONTEXT.with(|c| {
        c.spawner.spawn_local_with_handle(future).unwrap()
    })
}

struct LocalPool {
    inner: FuturesUnordered<LocalFutureObj<'static, ()>>,
    local_spawner: Rc<LocalSpawnerShared>,
    waker: Waker,
}

impl LocalPool {
    fn new(mio_waker: mio::Waker) -> Self {
        let local_pool_waker = Arc::new(LocalPoolWaker::new(mio_waker));
        let local_spawner = Rc::new(LocalSpawnerShared::new(local_pool_waker.clone()));
        let waker = futures_util::task::waker(local_pool_waker);

        Self {
            inner: FuturesUnordered::new(),
            local_spawner,
            waker,
        }
    }
    fn spawner(&self) -> LocalSpawner {
        LocalSpawner { inner: Rc::downgrade(&self.local_spawner) }
    }
    fn run_until_stalled(&mut self) {
        let mut cx = Context::from_waker(&self.waker);
        loop {
            for new_future in self.local_spawner.new_futures.borrow_mut().drain(..) {
                self.inner.push(new_future);
            }
            match self.inner.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

struct LocalSpawner {
    inner: rc::Weak<LocalSpawnerShared>,
}

impl LocalSpawn for LocalSpawner {
    fn spawn_local_obj(&self, future: LocalFutureObj<'static, ()>) -> Result<(), SpawnError> {
        let shared = self.inner.upgrade().ok_or(SpawnError::shutdown())?;
        shared.new_futures.borrow_mut().push(future);
        ArcWake::wake_by_ref(&shared.local_waker);
        Ok(())
    }
}

struct LocalSpawnerShared {
    local_waker: Arc<LocalPoolWaker>,
    new_futures: RefCell<Vec<LocalFutureObj<'static, ()>>>,
}

impl LocalSpawnerShared {
    fn new(local_waker: Arc<LocalPoolWaker>) -> Self {
        Self {
            local_waker,
            new_futures: RefCell::new(Vec::new()),
        }
    }
}

struct LocalPoolWaker {
    mio_waker: mio::Waker,
    waked: AtomicBool,
}

impl LocalPoolWaker {
    fn new(mio_waker: mio::Waker) -> Self {
        Self {
            mio_waker,
            waked: false.into(),
        }
    }
}

impl ArcWake for LocalPoolWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if let Ok(_) = arc_self.waked.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::Acquire
        ) {
            arc_self.mio_waker.wake().unwrap();
        }        
    }
}

pub struct Runtime {
    local_pool: LocalPool,
    mio_poll: Rc<RefCell<mio::Poll>>,
    runtime_context: Option<RuntimeContext>,
    shutdown: Arc<AtomicBool>,
}

impl Runtime {
    pub fn new() -> io::Result<Self> {
        let mio_poll = Rc::new(RefCell::new(mio::Poll::new()?));
        let mio_waker = mio::Waker::new(mio_poll.borrow().registry(), Token(0))?;
        let local_pool = LocalPool::new(mio_waker);
        let runtime_context = RuntimeContext::new(local_pool.spawner(), mio_poll.clone());
        Ok(Self {
            local_pool,
            mio_poll,
            runtime_context: Some(runtime_context),
            shutdown: Arc::new(false.into()),
        })
    }
    pub fn run(&mut self) -> io::Result<()> {
        let runtime_context = self.runtime_context.take().unwrap();
        let r: io::Result<()> = RUNTIME_CONTEXT.set(&runtime_context, || {
            let mut events = Events::with_capacity(1024);
            loop {
                self.local_pool.local_spawner.local_waker.waked.store(false, Ordering::Release);
                self.local_pool.run_until_stalled();
                if self.local_pool.is_empty() || self.shutdown.load(Ordering::Acquire) {
                    break;
                }
                let timeout = RUNTIME_CONTEXT.with(|runtime_context| {
                    runtime_context.timer_manager.borrow().next_check_duration()
                });
                self.mio_poll.borrow_mut().poll(&mut events, timeout)?;
                RUNTIME_CONTEXT.with(|runtime_context| {
                    self.handle_events(
                        &events, &mut *runtime_context.event_manager.borrow_mut());
                    runtime_context.timer_manager.borrow_mut().wakeup_timers();
                });
            }
            Ok(())
        });
        self.runtime_context = Some(runtime_context);
        r
    }
    pub fn spawn(&mut self, f: impl Future<Output = ()> + 'static) {
        self.local_pool.spawner().spawn_local(f).unwrap()
    }

    pub fn spawn_with_handle<T>(&mut self, f: impl Future<Output = T> + 'static) -> RemoteHandle<T> {
        self.local_pool.spawner().spawn_local_with_handle(f).unwrap()
    }
    pub fn shutdown_signal(&self) -> RuntimeShutdownSignal {
        RuntimeShutdownSignal {
            waker: self.local_pool.waker.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl Runtime {
    fn handle_events(&mut self, events: &Events, event_manager: &mut EventManager) {
        for event in events.iter() {
            let token = event.token();
            if event.is_readable() || event.is_read_closed() {
                event_manager.set_read_ready(token);
            }
            if event.is_writable() || event.is_write_closed() {
                event_manager.set_write_ready(token);
            }
        }
    }
}

pub struct RuntimeShutdownSignal {
    waker: Waker,
    shutdown: Arc<AtomicBool>,
}

impl RuntimeShutdownSignal {
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.waker.wake_by_ref();
    }
}

// inspired by tokio
#[derive(Debug)]
pub struct PollEvented<S: mio::event::Source> {
    inner: S,
    token: mio::Token,
    poll_handle: Rc<RefCell<mio::Poll>>,
}

impl<S: mio::event::Source> PollEvented<S> {
    pub fn new(mut inner: S) -> io::Result<Self> {
        let (token, poll_handle) = RUNTIME_CONTEXT.with(|c| {
            (c.new_token(), c.poll_handle.clone())
        });
        poll_handle.borrow().registry().register(&mut inner, token, Interest::READABLE | Interest::WRITABLE)?;
        Ok(Self { inner, token, poll_handle })
    }
    pub fn token(&self) -> mio::Token {
        self.token
    }
    pub fn poll_read<'a>(&'a self, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>>
        where &'a S: io::Read
    {
        match (&self.inner).read(buf) {
            Ok(n) => {
                Poll::Ready(Ok(n))
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        clear_read_ready(self.token, cx.waker().clone());
                        Poll::Pending
                    },
                    _ => {
                        Poll::Ready(Err(e))
                    },
                }
            }
        }
    }

    pub fn poll_write<'a>(&'a self, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>>
        where &'a S: io::Write
    {
        match (&self.inner).write(buf) {
            Ok(n) => {
                Poll::Ready(Ok(n))
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        clear_write_ready(self.token, cx.waker().clone());
                        Poll::Pending
                    },
                    _ => {
                        Poll::Ready(Err(e))
                    },
                }
            }
        }
    }

    pub async fn wait_next_read_event(&self) {
        let mut first_poll = true;
        poll_fn(|cx| {
            if first_poll {
                first_poll = false;
                clear_read_ready(self.token, cx.waker().clone());
                Poll::Pending
            } else {
                if is_read_ready(self.token) {
                    Poll::Ready(())
                } else {
                    clear_read_ready(self.token, cx.waker().clone());
                    Poll::Pending
                }
            }
        }).await
    }

    pub async fn wait_next_write_event(&self) {
        let mut first_poll = true;
        poll_fn(|cx| {
            if first_poll {
                first_poll = false;
                clear_write_ready(self.token, cx.waker().clone());
                Poll::Pending
            } else {
                if is_write_ready(self.token) {
                    Poll::Ready(())
                } else {
                    clear_write_ready(self.token, cx.waker().clone());
                    Poll::Pending
                }
            }
        }).await
    }

    pub fn is_ready_ready(&self) -> bool {
        is_read_ready(self.token)
    }

    pub fn is_write_ready(&self) -> bool {
        is_write_ready(self.token)
    }
}

impl<S: mio::event::Source> Drop for PollEvented<S> {
    fn drop(&mut self) {
        // ignore errors
        let _ = self.poll_handle.borrow().registry().deregister(&mut self.inner);
    }
}

impl<S: mio::event::Source> Deref for PollEvented<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: mio::event::Source> DerefMut for PollEvented<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use futures_util::pending;

    use super::*;

    #[test]
    fn test_runtime_basic1() {
        let mut runtime = Runtime::new().unwrap();
        let counter = Rc::new(RefCell::new(0));
        let counter_clone = counter.clone();
        runtime.spawn(async move {
            let mut counter = counter_clone.borrow_mut();
            for _ in 0..1000 {
                *counter += 1;
            }
            println!("counter is {}", counter);
        });
        runtime.run().unwrap();
        assert_eq!(*counter.borrow(), 1000);
    }

    #[test]
    fn test_runtime_basic2() {
        let mut runtime = Runtime::new().unwrap();
        let counter = Rc::new(RefCell::new(0usize));
        let counter_checker = counter.clone();
        runtime.spawn(async move {
            let waker1 = Rc::new(RefCell::new(None));
            let waker1_clone = waker1.clone();
            let waker2 = Rc::new(RefCell::new(Option::<Waker>::None));
            let waker2_clone = waker2.clone();
            let counter_clone = counter.clone();
            spawn(async move {
                for _ in 0..1000 {
                    *counter.borrow_mut() += 1;
                    println!("task1: counter is {}", *counter.borrow());
                    poll_fn(|cx| {
                        *waker1.borrow_mut() = Some(cx.waker().clone());
                        println!("set waker1");
                        Poll::Ready(())
                    }).await;
                    if let Some(waker) = waker2.take() {
                        waker.wake();
                        println!("waker2.wake()");
                    } else {
                        println!("waker2 is None");
                    }
                    pending!();
                }
                println!("task1 exit");
            });
            spawn(async move {
                let waker1 = waker1_clone;
                let waker2 = waker2_clone;
                let counter = counter_clone;
                loop {
                    println!("task2: counter is {}", *counter.borrow());
                    if let Some(waker) = waker1.take() {
                        waker.wake();
                        println!("waker1.wake()");
                    }
                    if *counter.borrow() == 1000 {
                        break;
                    }
                    poll_fn(|cx| {
                        *waker2.borrow_mut() = Some(cx.waker().clone());
                        println!("set waker2");
                        Poll::Ready(())
                    }).await;
                    pending!();
                }
                println!("task2 exit");
            });
        });
        runtime.run().unwrap();
        assert_eq!(*counter_checker.borrow(), 1000);
    }

    #[test]
    fn test_many_tasks() {
        let mut runtime = Runtime::new().unwrap();
        runtime.spawn(async {

        });
    }

    #[test]
    fn test_shutdown_signal() {
        let mut rt = Runtime::new().unwrap();
        rt.spawn(async {
            loop {
                futures_util::pending!()
            }
        });

        let shutdown_signal = rt.shutdown_signal();
        rt.spawn(async move {
            shutdown_signal.shutdown();
            println!("shutdown");
        });

        rt.run().unwrap();
        println!("run exit");
    }
}